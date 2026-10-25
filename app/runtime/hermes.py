"""hermes 适配器(自平台 services/cockpit_runtime.py 原样改造为 AgentRuntime 协议实现)。

hermes-runtime(gateway api_server)的会话级薄代理:
- ``Authorization: Bearer <agent_runtime_api_key>``(缺失/错误 → 401)
- ``X-Hermes-Session-Key``: 用户记忆作用域 ``f"t{tenant_id}:u{user_id}"``,
  稳定永不更改(hermes 侧只存 SHA-256 哈希)

错误语义见 base.py 协议注释。
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import httpx

from app.config import get_settings
from app.runtime.base import (
    AgentRuntimeAuthError,
    AgentRuntimeBadRequest,
    AgentRuntimeBusy,
    AgentRuntimeUnavailable,
)


def new_async_client(timeout) -> httpx.AsyncClient:
    # trust_env=False: 服务间本机直连不走系统代理;
    # 独立工厂便于单测用 httpx.MockTransport 替换
    return httpx.AsyncClient(timeout=timeout, trust_env=False)


def _headers(session_key: str | None = None) -> dict[str, str]:
    settings = get_settings()
    headers = {"Authorization": f"Bearer {settings.agent_runtime_api_key}"}
    if session_key:
        headers["X-Hermes-Session-Key"] = session_key
    return headers


def _base_url() -> str:
    return get_settings().agent_runtime_base_url.rstrip("/")


def _check_enabled() -> None:
    settings = get_settings()
    if not settings.agent_runtime_enabled:
        raise AgentRuntimeUnavailable("agent runtime 未启用(agent_runtime_enabled=false)")
    if not settings.agent_runtime_api_key:
        raise AgentRuntimeUnavailable("agent_runtime_api_key 未配置")


def _raise_for_status(response: httpx.Response) -> None:
    _raise_for_code(response.status_code, response.text)


def _raise_for_code(status_code: int, text: str) -> None:
    if status_code == 401:
        raise AgentRuntimeAuthError(f"agent runtime auth failed: {text[:200]}")
    if status_code == 400:
        raise AgentRuntimeBadRequest(f"agent runtime bad request: {text[:200]}")
    if status_code in (429, 503):
        raise AgentRuntimeBusy(f"agent runtime busy({status_code}): {text[:200]}")
    if status_code >= 400:
        raise AgentRuntimeUnavailable(f"agent runtime status {status_code}: {text[:200]}")


class HermesRuntime:
    """AgentRuntime 的 hermes HTTP 实现。"""

    async def create_session(self, *, title: str | None = None) -> str:
        """POST /api/sessions 新建空会话,返回 hermes 会话 id。"""
        _check_enabled()
        body: dict[str, Any] = {"source": "api_server"}
        if title:
            body["title"] = title
        try:
            async with new_async_client(30.0) as client:
                response = await client.post(
                    f"{_base_url()}/api/sessions", json=body, headers=_headers()
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        _raise_for_status(response)
        try:
            data = response.json()
            session = data["session"]
            return str(session["id"])
        except (ValueError, KeyError, TypeError) as exc:
            raise AgentRuntimeUnavailable(
                f"agent runtime response violates contract: {response.text[:200]}"
            ) from exc

    async def delete_session(self, session_id: str) -> None:
        """DELETE /api/sessions/{id};404 视为已删(幂等),不抛错。"""
        _check_enabled()
        try:
            async with new_async_client(30.0) as client:
                response = await client.delete(
                    f"{_base_url()}/api/sessions/{session_id}", headers=_headers()
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        if response.status_code == 404:
            return
        _raise_for_status(response)

    async def chat(self, session_id: str, message: str, *, session_key: str,
                   instructions: str | None = None) -> str:
        """POST /api/sessions/{id}/chat(非流式) → 返回 assistant 全文。

        契约(hermes api_server._handle_session_chat): 200 时
        ``{"object": "hermes.session.chat.completion", "message": {"role": "assistant",
        "content": <全文 str>}, ...}``;响应结构违约抛 AgentRuntimeUnavailable。
        读超时用 realtime_voice_task_timeout(长任务整包等待)。
        走 _run_agent,消息落 hermes 会话历史——文字模式可见同一轮次。
        instructions 透传 hermes 端 system_message(api_server.py:4617),
        用于快查通道注入本轮只读纪律(ASK_READONLY_INSTRUCTIONS)。
        """
        _check_enabled()
        settings = get_settings()
        payload: dict[str, Any] = {"message": message}
        if instructions:
            payload["system_message"] = instructions
        try:
            async with new_async_client(
                httpx.Timeout(
                    connect=10.0, read=settings.realtime_voice_task_timeout, write=30.0, pool=30.0
                )
            ) as client:
                response = await client.post(
                    f"{_base_url()}/api/sessions/{session_id}/chat",
                    json=payload,
                    headers=_headers(session_key),
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        _raise_for_status(response)
        try:
            data = response.json()
            content = data["message"]["content"]
            if not isinstance(content, str):
                raise TypeError("message.content is not a string")
            return content
        except (ValueError, KeyError, TypeError) as exc:
            raise AgentRuntimeUnavailable(
                f"agent runtime response violates contract: {response.text[:200]}"
            ) from exc

    async def submit_run(self, session_id: str, task: str, *, session_key: str,
                         instructions: str | None = None) -> str:
        """POST /v1/runs {input, session_id[, instructions]} → 返回 run_id。

        契约: 202 ``{"run_id","status":"started"}``;202 之外按 _raise_for_code。
        传了 session_id 的 run 落 hermes 会话历史,共享同会话串行租约;
        hermes 侧从 state.db 加载会话历史,跨轮上下文与 chat 通道等价。
        ``instructions`` 透传为 ephemeral system prompt(叠加于核心提示,仅本轮生效)。
        """
        _check_enabled()
        body: dict[str, Any] = {"input": task, "session_id": session_id}
        if instructions:
            body["instructions"] = instructions
        try:
            async with new_async_client(30.0) as client:
                response = await client.post(
                    f"{_base_url()}/v1/runs",
                    json=body,
                    headers=_headers(session_key),
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        if response.status_code != 202:
            _raise_for_status(response)
        try:
            run_id = response.json()["run_id"]
            if not isinstance(run_id, str) or not run_id:
                raise TypeError("run_id missing")
            return run_id
        except (ValueError, KeyError, TypeError) as exc:
            raise AgentRuntimeUnavailable(
                f"agent runtime response violates contract: {response.text[:200]}"
            ) from exc

    async def run_events(self, run_id: str) -> AsyncIterator[dict[str, Any]]:
        """GET /v1/runs/{id}/events → 逐事件 yield 解析后的 dict。

        契约: SSE 只发 ``data: <json>`` 行(**无 event: 行**,类型在 JSON 的
        ``event`` 字段);``: keepalive`` 注释行跳过;``: stream closed`` 或流结束即退出。
        读超时放宽到 realtime_voice_task_timeout。单订阅者。
        """
        _check_enabled()
        settings = get_settings()
        try:
            async with new_async_client(
                httpx.Timeout(
                    connect=10.0, read=settings.realtime_voice_task_timeout, write=30.0, pool=30.0
                )
            ) as client:
                async with client.stream(
                    "GET", f"{_base_url()}/v1/runs/{run_id}/events", headers=_headers()
                ) as response:
                    if response.status_code != 200:
                        body = (await response.aread()).decode("utf-8", "replace")
                        _raise_for_code(response.status_code, body)
                    data_lines: list[str] = []
                    async for line in response.aiter_lines():
                        if line.startswith(":"):
                            if "stream closed" in line:
                                return
                            continue  # keepalive 等注释行
                        if line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())
                            continue
                        if not line.strip():
                            if data_lines:
                                try:
                                    event = json.loads("".join(data_lines))
                                except ValueError:
                                    event = None
                                data_lines = []
                                if isinstance(event, dict):
                                    yield event
                            continue
                        # 其他 SSE 字段(event:/id:/retry:)按契约不应出现,忽略
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime stream failed: {exc}") from exc

    async def run_get(self, run_id: str) -> dict[str, Any]:
        """GET /v1/runs/{id} → 状态轮询(status/output/usage),启动恢复探活用。"""
        _check_enabled()
        try:
            async with new_async_client(30.0) as client:
                response = await client.get(f"{_base_url()}/v1/runs/{run_id}", headers=_headers())
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        _raise_for_status(response)
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise TypeError("run status payload is not an object")
            return data
        except (ValueError, TypeError) as exc:
            raise AgentRuntimeUnavailable(
                f"agent runtime response violates contract: {response.text[:200]}"
            ) from exc

    async def run_stop(self, run_id: str) -> None:
        """POST /v1/runs/{id}/stop;404 视为已终态(幂等),不抛错。"""
        _check_enabled()
        try:
            async with new_async_client(30.0) as client:
                response = await client.post(
                    f"{_base_url()}/v1/runs/{run_id}/stop", headers=_headers()
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        if response.status_code == 404:
            return
        _raise_for_status(response)

    async def run_steer(self, run_id: str, text: str) -> bool:
        """POST /v1/runs/{id}/steer {"input": text};仅 running 接受,409 → False。"""
        _check_enabled()
        try:
            async with new_async_client(30.0) as client:
                response = await client.post(
                    f"{_base_url()}/v1/runs/{run_id}/steer", json={"input": text}, headers=_headers()
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        if response.status_code == 409:
            return False
        _raise_for_status(response)
        return True

    async def run_approval(self, run_id: str, choice: str) -> None:
        """POST /v1/runs/{id}/approval {"choice": "once"|"session"|"always"|"deny"}。"""
        _check_enabled()
        try:
            async with new_async_client(30.0) as client:
                response = await client.post(
                    f"{_base_url()}/v1/runs/{run_id}/approval",
                    json={"choice": choice},
                    headers=_headers(),
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise AgentRuntimeUnavailable(f"agent runtime request failed: {exc}") from exc
        _raise_for_status(response)
