"""AgentRuntime 契约套件的实现绑定: FakeRuntime(内存) + HermesRuntime(假 HTTP 服务)。

FakeHermesService 是最小但忠实的 hermes api_server 替身(MockTransport handler),
覆盖适配器用到的全部端点:
- POST   /api/sessions            建会话(201 风格 200 + {"session": {"id"}})
- DELETE /api/sessions/{id}       删会话(未知 404,适配器幂等吞掉)
- POST   /api/sessions/{id}/chat  同步问答(200 + message.content 全文)
- POST   /v1/runs                 受理(202 {"run_id","status":"started"};force_busy 时 429)
- GET    /v1/runs/{id}            状态探活(completed 带 output)
- GET    /v1/runs/{id}/events     SSE(data-only 行 + 注释行 + ": stream closed" 收尾;
                                  服务完毕即把 run 落 completed,与真 hermes 时序一致)
- POST   /v1/runs/{id}/stop       停止(200;未知 404 适配器幂等吞掉)
- POST   /v1/runs/{id}/steer      running → 200;终态 → 409(适配器映射 False)
- POST   /v1/runs/{id}/approval   200 即过

鉴权: Authorization: Bearer <api_key>,不符 401(供错误分类用例)。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
from itertools import count

import httpx
import pytest

from app.config import get_settings
from app.runtime import hermes as hermes_module
from app.runtime.acp import AcpRuntime
from app.runtime.base import (
    AgentRuntimeAuthError,
    AgentRuntimeBusy,
    AgentRuntimeUnavailable,
)
from app.runtime.hermes import HermesRuntime

from tests.fake_acp_agent import install_fake_agent_settings
from tests.fakes import FakeRuntime
from tests.runtime_contract import AgentRuntimeContract


# --- FakeRuntime(内存实现)绑定 ------------------------------------------------------


class TestFakeRuntimeContract(AgentRuntimeContract):
    @pytest.fixture
    def runtime(self) -> FakeRuntime:
        return FakeRuntime()

    @pytest.fixture
    def broken_runtime(self) -> FakeRuntime:
        return FakeRuntime(available=False)


# --- 假 hermes HTTP 服务 -------------------------------------------------------------


class FakeHermesService:
    """最小但忠实的 hermes api_server 替身(见模块 docstring 端点表)。"""

    def __init__(self, *, api_key: str = "contract-key") -> None:
        self.api_key = api_key
        self.sessions: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self.requests: list[tuple[str, str, dict, str | None]] = []  # (method, path, body, session_key) 留痕
        self.force_busy = False  # True 时 POST /v1/runs 回 429
        self._seq = count(1)

    def _next(self, prefix: str) -> str:
        return f"{prefix}_{next(self._seq)}"

    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        body = json.loads(request.content) if request.content else {}
        if request.headers.get("Authorization") != f"Bearer {self.api_key}":
            return httpx.Response(401, json={"detail": "unauthorized"})
        self.requests.append((method, path, body, request.headers.get("X-Hermes-Session-Key")))

        if method == "POST" and path == "/api/sessions":
            session_id = self._next("sess")
            self.sessions[session_id] = {"title": body.get("title")}
            return httpx.Response(200, json={"session": {"id": session_id}})

        match = re.fullmatch(r"/api/sessions/([^/]+)", path)
        if match and method == "DELETE":
            if match.group(1) not in self.sessions:
                return httpx.Response(404, json={})
            del self.sessions[match.group(1)]
            return httpx.Response(200, json={"ok": True})

        match = re.fullmatch(r"/api/sessions/([^/]+)/chat", path)
        if match and method == "POST":
            session_id = match.group(1)
            if session_id not in self.sessions:
                return httpx.Response(404, json={})
            return httpx.Response(
                200,
                json={
                    "object": "hermes.session.chat.completion",
                    "session_id": session_id,
                    "message": {
                        "role": "assistant",
                        "content": f"hermes 回声:{body.get('message', '')}",
                    },
                    "usage": {},
                },
            )

        if method == "POST" and path == "/v1/runs":
            if self.force_busy:
                return httpx.Response(429, json={"detail": "busy"})
            run_id = self._next("run")
            self.runs[run_id] = {
                "status": "running",
                "input": body.get("input"),
                "session_id": body.get("session_id"),
                "output": None,
            }
            return httpx.Response(202, json={"run_id": run_id, "status": "started"})

        match = re.fullmatch(r"/v1/runs/([^/]+)(?:/(events|stop|steer|approval))?", path)
        if match:
            run_id, action = match.group(1), match.group(2)
            run = self.runs.get(run_id)
            if run is None:
                return httpx.Response(404, json={})
            if action is None and method == "GET":
                payload: dict = {"status": run["status"]}
                if run["status"] == "completed":
                    payload["output"] = run["output"]
                return httpx.Response(200, json=payload)
            if action == "events" and method == "GET":
                output = f"盘点完成:{(run['input'] or '')[:20]}"
                sse = (
                    ": keepalive\n\n"
                    'data: {"event":"tool.started","tool":"search_memory","preview":"检索记忆"}\n\n'
                    'data: {"event":"tool.completed","tool":"search_memory","duration":0.1}\n\n'
                    + "data: "
                    + json.dumps(
                        {"event": "run.completed", "run_id": run_id, "output": output},
                        ensure_ascii=False,
                    )
                    + "\n\n"
                    + ": stream closed\n\n"
                )
                # 与真 hermes 时序一致: 事件流服务完毕,run 即终态
                run["status"] = "completed"
                run["output"] = output
                return httpx.Response(
                    200, content=sse.encode(), headers={"content-type": "text/event-stream"}
                )
            if action == "stop" and method == "POST":
                return httpx.Response(200, json={"status": "stopping"})
            if action == "steer" and method == "POST":
                if run["status"] != "running":
                    return httpx.Response(409, json={})
                return httpx.Response(200, json={"status": "ok"})
            if action == "approval" and method == "POST":
                return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(404, json={})


def _patch_hermes_transport(monkeypatch: pytest.MonkeyPatch, service: FakeHermesService) -> None:
    """MockTransport 补丁点: hermes 适配器的 HTTP 客户端工厂(与移植语音测试同款)。"""
    transport = httpx.MockTransport(service.handler)
    monkeypatch.setattr(
        hermes_module,
        "new_async_client",
        lambda timeout: httpx.AsyncClient(transport=transport, trust_env=False, timeout=timeout),
    )


def _hermes_settings(monkeypatch: pytest.MonkeyPatch, *, api_key: str = "contract-key") -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "agent_runtime_enabled", True)
    monkeypatch.setattr(settings, "agent_runtime_api_key", api_key)
    monkeypatch.setattr(settings, "agent_runtime_base_url", "http://hermes.fake")


# --- HermesRuntime 绑定 ---------------------------------------------------------------


class TestHermesRuntimeContract(AgentRuntimeContract):
    @pytest.fixture
    def runtime(self, monkeypatch) -> HermesRuntime:
        _hermes_settings(monkeypatch)
        _patch_hermes_transport(monkeypatch, FakeHermesService())
        return HermesRuntime()

    @pytest.fixture
    def broken_runtime(self, monkeypatch) -> HermesRuntime:
        settings = get_settings()
        monkeypatch.setattr(settings, "agent_runtime_enabled", True)
        monkeypatch.setattr(settings, "agent_runtime_api_key", "")  # 未配置密钥
        return HermesRuntime()  # 任何方法在 _check_enabled 抛 AgentRuntimeUnavailable,不发 HTTP


# --- hermes 适配器私有语义: HTTP 状态码 → 四分类错误(契约套件之外) -----------------------


class TestHermesErrorClassification:
    def test_401_maps_to_auth_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _hermes_settings(monkeypatch, api_key="wrong-key")
        _patch_hermes_transport(monkeypatch, FakeHermesService(api_key="real-key"))
        with pytest.raises(AgentRuntimeAuthError):
            asyncio.run(HermesRuntime().create_session())

    def test_429_maps_to_busy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service = FakeHermesService()
        service.force_busy = True
        _hermes_settings(monkeypatch)
        _patch_hermes_transport(monkeypatch, service)
        with pytest.raises(AgentRuntimeBusy):
            asyncio.run(HermesRuntime().submit_run("sess_1", "x", session_key="t1:u1"))

    def test_disabled_maps_to_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(get_settings(), "agent_runtime_enabled", False)
        with pytest.raises(AgentRuntimeUnavailable):
            asyncio.run(HermesRuntime().create_session())

    def test_connection_failure_maps_to_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _hermes_settings(monkeypatch)
        # 死端口真连接(不打 MockTransport): ConnectError → AgentRuntimeUnavailable
        monkeypatch.setattr(get_settings(), "agent_runtime_base_url", "http://127.0.0.1:9")
        with pytest.raises(AgentRuntimeUnavailable):
            asyncio.run(HermesRuntime().create_session())

    def test_chat_contract_headers_and_body(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """假服务留痕断言: chat 带 X-Hermes-Session-Key + system_message 透传。"""
        service = FakeHermesService()
        _hermes_settings(monkeypatch)
        _patch_hermes_transport(monkeypatch, service)
        runtime = HermesRuntime()
        session_id = asyncio.run(runtime.create_session())
        answer = asyncio.run(
            runtime.chat(session_id, "查一下", session_key="t1:u7", instructions="只读")
        )
        assert answer == "hermes 回声:查一下"
        chat_request = next(r for r in service.requests if r[1].endswith("/chat"))
        assert chat_request[2] == {"message": "查一下", "system_message": "只读"}
        assert chat_request[3] == "t1:u7"  # X-Hermes-Session-Key 透传


# --- AcpRuntime 绑定(骨架:JSON-RPC over stdio,app/runtime/acp.py) --------------------------

_KIMI_ACP_AVAILABLE = shutil.which("kimi") is not None
_KIMI_CONTRACT_OPT_IN = os.environ.get("ACP_CONTRACT_KIMI") == "1"
# 完整契约需真 kimi-code;但骨架的 ACP 方法名/参数形状未经真实握手验证(见 acp.py 顶部
# 常量区),即使本机有 kimi 也须先人工核对再显式开启——故 skipif 双条件,防 CI/开发机误跑。
_REQUIRES_KIMI_ACP = pytest.mark.skipif(
    not (_KIMI_ACP_AVAILABLE and _KIMI_CONTRACT_OPT_IN),
    reason="需本机安装 kimi-code(`kimi acp`)且显式 ACP_CONTRACT_KIMI=1(骨架未验证真实 "
    "ACP 握手,先人工核对 acp.py 常量区再开);常开行为契约见 TestAcpFakeAgentContract",
)


class TestAcpRuntimeContract(AgentRuntimeContract):
    """ACP 适配器契约:broken(未配置 acp_command)九方法真跑;完整契约挂 skipif 需真 kimi。

    完整契约用例逐个转发基类方法并挂 skipif——若用类级 mark 会连 broken 用例一起跳过,
    违背「未配置路径必须真跑」的要求。
    """

    @pytest.fixture
    def runtime(self, monkeypatch):
        settings = get_settings()
        monkeypatch.setattr(settings, "acp_command", "kimi")
        monkeypatch.setattr(settings, "acp_args", "acp")  # ⚠️ 按 kimi-code acp 实际子命令核对
        runtime = AcpRuntime()
        yield runtime
        asyncio.run(runtime.aclose())

    @pytest.fixture
    def broken_runtime(self, monkeypatch):
        # 未配置 acp_command:九方法在 _check_enabled 抛 AgentRuntimeUnavailable,不起子进程
        monkeypatch.setattr(get_settings(), "acp_command", "")
        return AcpRuntime()

    # test_unavailable_raises_agent_runtime_unavailable 继承基类、不挂 mark → 真跑

    @_REQUIRES_KIMI_ACP
    def test_create_session_returns_non_empty_str(self, runtime):
        super().test_create_session_returns_non_empty_str(runtime)

    @_REQUIRES_KIMI_ACP
    def test_delete_session_idempotent(self, runtime):
        super().test_delete_session_idempotent(runtime)

    @_REQUIRES_KIMI_ACP
    def test_chat_returns_str(self, runtime):
        super().test_chat_returns_str(runtime)

    @_REQUIRES_KIMI_ACP
    def test_submit_run_returns_run_id(self, runtime):
        super().test_submit_run_returns_run_id(runtime)

    @_REQUIRES_KIMI_ACP
    def test_run_events_normalized_and_terminate(self, runtime):
        super().test_run_events_normalized_and_terminate(runtime)

    @_REQUIRES_KIMI_ACP
    def test_run_get_completed_has_status_and_output(self, runtime):
        super().test_run_get_completed_has_status_and_output(runtime)

    @_REQUIRES_KIMI_ACP
    def test_run_stop_idempotent(self, runtime):
        super().test_run_stop_idempotent(runtime)

    @_REQUIRES_KIMI_ACP
    def test_run_steer_running_true_terminal_false(self, runtime):
        super().test_run_steer_running_true_terminal_false(runtime)

    @_REQUIRES_KIMI_ACP
    def test_run_approval_does_not_raise(self, runtime):
        super().test_run_approval_does_not_raise(runtime)


class TestAcpFakeAgentContract(AgentRuntimeContract):
    """骨架行为契约:假 ACP 子进程(tests/fake_acp_agent.py)真跑全套契约。

    传输/归一化/审批/跨 loop 编排皆真实路径,仅 agent 是假的——本类即「acp.py 骨架
    可端到端兑现 AgentRuntime 契约」的常开证明(不依赖本机是否装 kimi-code)。
    """

    @pytest.fixture
    def runtime(self, monkeypatch):
        install_fake_agent_settings(monkeypatch)
        runtime = AcpRuntime()
        yield runtime
        asyncio.run(runtime.aclose())

    @pytest.fixture
    def broken_runtime(self, monkeypatch):
        monkeypatch.setattr(get_settings(), "acp_command", "")
        return AcpRuntime()
