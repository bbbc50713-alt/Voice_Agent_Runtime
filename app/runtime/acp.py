"""ACP(Agent Client Protocol)适配器骨架:JSON-RPC over stdio 拉起 CLI agent 子进程。

用法:`agent_runtime_kind=acp` + `ACP_COMMAND=kimi` + `ACP_ARGS="acp"` 即可把语音模块
接到任意实现 ACP 的 CLI agent 上;接入指引见 app/runtime/cli_agents.md。

设计取舍(写死,保持简单):
- **每会话一个子进程**:create_session 拉起 agent 进程并保持(initialize + session/new);
  delete_session 终止进程。舍内存取故障隔离与自然的"删除"语义(ACP 本无 delete),
  单语音租户的会话量级有限,可接受。
- **run 映射**:ACP 的 session/prompt 是同步轮次(响应返回=一轮结束),而 AgentRuntime
  的 run 是异步句柄。submit_run 本地生成 `acp-<uuid>` run_id + 内存 run↔session 映射,
  后台协程执行 prompt,把 session/update 通知归一化成契约 RunEvent 推入 run 队列;
  run_events 消费到终态事件即收口(单订阅者)。run 记录进程生命周期内保留(不 TTL),
  供 run_get 启动恢复探活。
- **专属 worker 事件循环**:子进程传输层绑定创建它的事件循环,而契约调用方
  (FastAPI 路由/契约测试)每次调用可能身处不同 loop——故全部子进程 I/O 收口到
  适配器私有线程的常驻 loop,公开方法经 run_coroutine_threadsafe 编排过去;
  跨线程只做 dict / queue.Queue 读写(GIL 原子性),不加锁。
- **审批**:agent 发来的 session/request_permission 入向请求挂起为 run 的
  approval.request 事件(传输层暂缓响应);run_approval 再经 JSON-RPC 回应 outcome;
  无 run 接收时默认拒绝,防 agent 卡死。
- **传输层与 JSON-RPC id 配对是真实实现**(假 agent 即可端到端跑通,见
  tests/fake_acp_agent.py);**但 ACP 方法名/参数形状集中在顶部常量区并标注
  「按 kimi-code acp 实际暴露核对」——本骨架未经真实 ACP 握手验证**。

错误语义(对齐 base.py 四分类):
- 未配置 acp_command / 子进程启动失败 / 请求超时 / 进程死亡 / JSON-RPC error 帧
  → AgentRuntimeUnavailable(调用方可降级);
- 未知会话(chat/submit)→ AgentRuntimeBadRequest(调用方 bug,对齐 FakeRuntime);
- 会话已有进行中 run(chat/submit 冲突)→ AgentRuntimeBusy(稍后重试,ask 转异步)。

已知不映射:`session_key`(用户记忆作用域)被接受并记录进会话 meta,但 ACP 无此概念,
骨架不传给 agent(kimi 侧的记忆作用域机制待核对)。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import shlex
import threading
import uuid
from itertools import count
from typing import Any, AsyncIterator, Callable

from app.config import get_settings
from app.runtime.base import (
    AgentRuntimeBadRequest,
    AgentRuntimeBusy,
    AgentRuntimeUnavailable,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ACP 方法名 / 字段形状常量区
# ⚠️ 形状参照 https://agentclientprotocol.com 协议草案,未经真实 kimi-code acp 验证;
# 接入时按 `kimi acp` 实际暴露核对,只改本区即可。
# ---------------------------------------------------------------------------
ACP_METHODS = {
    "initialize": "initialize",
    "session_new": "session/new",
    "session_prompt": "session/prompt",
    "session_cancel": "session/cancel",  # 通知(无响应),取消进行中的 prompt
    "session_update": "session/update",  # agent → 客户端通知:流式块/工具进度
    "session_request_permission": "session/request_permission",  # agent → 客户端入向请求
}
# session/update 的 update.sessionUpdate 取值(规范草案,按 kimi-code acp 实际暴露核对):
_UPDATE_MESSAGE_CHUNK = "agent_message_chunk"
_UPDATE_TOOL_CALL = "tool_call"
_UPDATE_TOOL_CALL_UPDATE = "tool_call_update"
# run_approval choice → permission 响应 outcome(规范草案,按 kimi-code acp 实际暴露核对):
_APPROVAL_OUTCOMES = {
    "once": {"outcome": "selected", "optionId": "allow_once"},
    "session": {"outcome": "selected", "optionId": "allow_always"},
    "always": {"outcome": "selected", "optionId": "allow_always"},
    "deny": {"outcome": "rejected", "optionId": "reject_once"},
}

_TERMINAL_EVENTS = ("run.completed", "run.failed", "run.cancelled")
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")
# on_request 处理器返回它 = 响应暂缓,稍后经 proc.respond 补发(审批挂起)
_DEFER = object()


class _RpcError(Exception):
    """on_request 处理器抛出 → 传输层以 JSON-RPC error 帧回应。"""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _JsonRpcProcess:
    """一个 ACP 子进程的传输层:换行分隔 JSON-RPC,stdin 写 / stdout 读,id 配对。

    - request():id 自增配对等待响应;超时 / error 帧 / 进程退出 → AgentRuntimeUnavailable;
    - notify():只写不等响应(尽力投递,如 session/cancel);
    - respond():回应 agent 发起的入向请求(如 session/request_permission);
    - 入向帧三分:响应(有 id 无 method)→ 按 id 收口 pending future;
      请求(有 id 有 method)→ on_request(method, params, req_id) 同步回调
      (返回 _DEFER 表示响应暂缓;抛 _RpcError 回应 error 帧);
      通知(无 id 有 method)→ on_notification(method, params) 同步回调。
    - 实例只在创建它的事件循环上使用(AcpRuntime 保证全部落在 worker loop)。
    """

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        self._ids = count(1)
        self._pending: dict[Any, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._dead = False
        self.on_notification: Callable[[str, dict], None] | None = None
        self.on_request: Callable[[str, dict, Any], Any] | None = None
        self._reader_task = asyncio.create_task(self._reader_loop())

    @classmethod
    async def spawn(cls, argv: list[str], cwd: str | None) -> "_JsonRpcProcess":
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,  # 骨架:丢弃;联调时可改 PIPE 收集 agent 日志
                cwd=cwd or None,
            )
        except OSError as exc:
            raise AgentRuntimeUnavailable(f"无法启动 ACP 子进程({argv!r}): {exc}") from exc
        return cls(proc)

    async def request(self, method: str, params: dict | None = None, timeout: float = 120.0):
        """发请求并等响应(id 配对);超时/error 帧/进程死亡 → AgentRuntimeUnavailable。"""
        if self._dead:
            raise AgentRuntimeUnavailable("ACP 子进程已退出")
        req_id = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self._write(
                {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}
            )
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise AgentRuntimeUnavailable(f"ACP 请求超时({timeout}s, method={method})") from exc
        finally:
            self._pending.pop(req_id, None)

    async def notify(self, method: str, params: dict | None = None) -> None:
        """发通知(无 id 不等响应);尽力投递,失败不产生新故障。"""
        if self._dead:
            return
        try:
            await self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})
        except AgentRuntimeUnavailable:
            pass

    async def respond(self, req_id, result=None, error: dict | None = None) -> None:
        """回应 agent 发起的入向请求(尽力投递;进程已死则静默)。"""
        if self._dead:
            return
        frame: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            frame["error"] = error
        else:
            frame["result"] = result if result is not None else {}
        try:
            await self._write(frame)
        except AgentRuntimeUnavailable:
            pass

    async def _write(self, frame: dict) -> None:
        if self._proc.stdin is None:
            raise AgentRuntimeUnavailable("ACP 子进程 stdin 不可用")
        data = (json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8")
        async with self._write_lock:
            try:
                self._proc.stdin.write(data)
                await self._proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                self._mark_dead()
                raise AgentRuntimeUnavailable("ACP 子进程 stdin 已断开") from exc

    async def _reader_loop(self) -> None:
        try:
            if self._proc.stdout is None:
                return
            while True:
                line = await self._proc.stdout.readline()
                if not line:  # EOF:进程退出
                    break
                try:
                    frame = json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue  # 非 JSON 输出(agent 打印的日志等)直接忽略
                await self._dispatch_frame(frame)
        except (ConnectionResetError, ValueError, OSError):
            pass
        finally:
            self._mark_dead()

    async def _dispatch_frame(self, frame: dict) -> None:
        method = frame.get("method")
        req_id = frame.get("id")
        if method is None:
            if req_id is None:
                return
            # 响应帧:按 id 收口 pending future
            fut = self._pending.pop(req_id, None)
            if fut is not None and not fut.done():
                if frame.get("error") is not None:
                    err = frame["error"] or {}
                    fut.set_exception(
                        AgentRuntimeUnavailable(
                            f"ACP 返回错误 {err.get('code')}: {err.get('message')}"
                        )
                    )
                else:
                    fut.set_result(frame.get("result"))
            return
        if req_id is not None:
            # agent 发起的入向请求(如 session/request_permission)
            handler = self.on_request
            if handler is None:
                await self.respond(
                    req_id, error={"code": -32601, "message": f"未处理的入向请求: {method}"}
                )
                return
            try:
                result = handler(method, frame.get("params") or {}, req_id)
            except _RpcError as exc:
                await self.respond(req_id, error={"code": exc.code, "message": exc.message})
            except Exception as exc:  # noqa: BLE001 —— 任何处理异常都不能打死 reader 循环
                logger.exception("ACP 入向请求处理失败: %s", method)
                await self.respond(req_id, error={"code": -32603, "message": str(exc)})
            else:
                if result is not _DEFER:  # _DEFER = 响应由 run_approval 稍后补发
                    await self.respond(req_id, result=result)
            return
        # 通知帧
        handler = self.on_notification
        if handler is not None:
            try:
                handler(method, frame.get("params") or {})
            except Exception:  # noqa: BLE001 —— 通知处理异常不扩散
                logger.exception("ACP 通知处理失败: %s", method)

    def _mark_dead(self) -> None:
        if self._dead:
            return
        self._dead = True
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(AgentRuntimeUnavailable("ACP 子进程已退出"))
        self._pending.clear()

    async def close(self) -> None:
        """终止子进程并收口 reader/管道传输层;幂等,尽力而为。"""
        self._mark_dead()
        self._reader_task.cancel()
        try:
            await self._reader_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001 —— CancelledError 3.8+ 不继承 Exception,须单列
            pass
        if self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    self._proc.kill()
                    await self._proc.wait()
                except (ProcessLookupError, OSError):
                    pass
        # 显式关闭管道传输层并留一个事件循环节拍:否则 Windows proactor 的管道关闭
        # 回调来不及执行,GC 时抛 "unclosed transport" ResourceWarning。
        transport = getattr(self._proc, "_transport", None)
        if transport is not None:
            transport.close()
            await asyncio.sleep(0.05)


class AcpRuntime:
    """AgentRuntime 的 ACP 实现骨架:每会话一个 CLI agent 子进程(取舍见模块 docstring)。"""

    def __init__(self) -> None:
        self._sessions: dict[str, _JsonRpcProcess] = {}
        self._session_meta: dict[str, dict[str, Any]] = {}
        self._proc_to_session: dict[int, str] = {}  # id(proc) → session_id(帧路由用)
        self._runs: dict[str, dict[str, Any]] = {}
        self._pending_permissions: dict[str, tuple[_JsonRpcProcess, Any]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._loop_lock = threading.Lock()

    # ---- worker loop(全部子进程 I/O 的归宿) ------------------------------------

    def _worker_loop(self) -> asyncio.AbstractEventLoop:
        """惰性启动适配器私有事件循环(daemon 线程,惰性以避免无谓占线)。"""
        with self._loop_lock:
            if self._loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever, name="acp-runtime-worker", daemon=True
                )
                thread.start()
                self._loop, self._loop_thread = loop, thread
            return self._loop

    async def _call(self, coro):
        """把协程编排到 worker loop 执行并等结果(在调用方 loop 上 await)。"""
        future = asyncio.run_coroutine_threadsafe(coro, self._worker_loop())
        return await asyncio.wrap_future(future)

    async def aclose(self) -> None:
        """关闭全部会话子进程并停掉 worker loop(测试装配/服务关停用)。"""
        if self._loop is None:
            return
        await self._call(self._close_all_w())
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread is not None:
            await asyncio.to_thread(self._loop_thread.join, 5)
        self._loop = None
        self._loop_thread = None

    async def _close_all_w(self) -> None:
        for record in self._runs.values():
            drive_cf = record.get("drive_cf")
            if drive_cf is not None:
                drive_cf.cancel()
        for proc in list(self._sessions.values()):
            try:
                await proc.close()
            except Exception:  # noqa: BLE001 —— 关停尽力而为
                logger.exception("关闭 ACP 子进程失败")
        self._sessions.clear()
        self._session_meta.clear()
        self._proc_to_session.clear()

    # ---- 内部工具(调用方线程) ---------------------------------------------------

    def _check_enabled(self):
        settings = get_settings()
        if not settings.acp_command.strip():
            raise AgentRuntimeUnavailable(
                "未配置 acp_command:ACP 适配器未启用(接入见 app/runtime/cli_agents.md)"
            )
        return settings

    def _session_proc(self, session_id: str) -> _JsonRpcProcess:
        proc = self._sessions.get(session_id)
        if proc is None:
            raise AgentRuntimeBadRequest(f"ACP 会话不存在或已删除: {session_id}")
        return proc

    # ---- 九方法 -----------------------------------------------------------------

    async def create_session(self, *, title: str | None = None) -> str:
        """拉起 agent 子进程 + initialize + session/new,返回 ACP sessionId(缺省回退本地 id)。"""
        settings = self._check_enabled()
        return await self._call(self._create_session_w(settings, title))

    async def delete_session(self, session_id: str) -> None:
        """ACP 无删除语义:取消其活跃 run + 终止子进程 + 本地遗忘(幂等,不存在不抛)。"""
        self._check_enabled()
        proc = self._sessions.get(session_id)
        if proc is None:
            self._session_meta.pop(session_id, None)
            return
        for run_id, record in list(self._runs.items()):
            if record["session_id"] == session_id and record["status"] not in _TERMINAL_STATUSES:
                await self.run_stop(run_id)
        self._sessions.pop(session_id, None)
        self._session_meta.pop(session_id, None)
        self._proc_to_session.pop(id(proc), None)
        await self._call(proc.close())

    async def chat(self, session_id: str, message: str, *, session_key: str,
                   instructions: str | None = None) -> str:
        """session/prompt 同步等终态,聚合 agent_message_chunk 返回全文。"""
        settings = self._check_enabled()
        self._session_proc(session_id)
        meta = self._session_meta[session_id]
        meta["session_key"] = session_key  # 仅记录:ACP 无用户记忆作用域概念(模块 docstring)
        return await self._call(self._chat_w(settings, session_id, message, instructions or ""))

    async def submit_run(self, session_id: str, task: str, *, session_key: str,
                         instructions: str | None = None) -> str:
        """受理即返回本地 `acp-<uuid>` run_id;后台 drive 协程执行 prompt 并归一化事件。"""
        self._check_enabled()
        self._session_proc(session_id)
        meta = self._session_meta[session_id]
        meta["session_key"] = session_key
        if meta.get("active_run_id"):
            raise AgentRuntimeBusy("该会话已有进行中的 run(ACP 骨架为单 run 串行)")
        run_id = f"acp-{uuid.uuid4().hex}"
        record: dict[str, Any] = {
            "run_id": run_id,
            "session_id": session_id,
            "status": "running",
            "output": "",
            "queue": queue.Queue(),  # stdlib:跨线程安全,worker 产 / 调用方消费
            "tool_names": {},  # toolCallId → title(tool.completed 配对显示名)
            "drive_cf": None,
        }
        self._runs[run_id] = record
        meta["active_run_id"] = run_id
        record["drive_cf"] = asyncio.run_coroutine_threadsafe(
            self._drive_run_w(run_id, session_id, task, instructions or ""),
            self._worker_loop(),
        )
        return run_id

    async def run_events(self, run_id: str) -> AsyncIterator[dict[str, Any]]:
        """消费 run 队列到终态事件(单订阅者);queue.Queue + to_thread 轮询,跨 loop 安全。"""
        self._check_enabled()
        record = self._runs.get(run_id)
        if record is None:
            raise AgentRuntimeUnavailable(f"未知 run: {run_id}")
        events: queue.Queue = record["queue"]
        while True:
            try:
                event = await asyncio.to_thread(events.get, True, 0.2)
            except queue.Empty:
                # 终态事件保证必达(drive 异常兜底 + run_stop 同步落);纯轮询等待。
                # 顺序约定:生产者先置状态再入队,故消费者见到终态事件时 run_get 必然已可读终态。
                continue
            yield event
            if event.get("event") in _TERMINAL_EVENTS:
                return

    async def run_get(self, run_id: str) -> dict[str, Any]:
        """查本地 run 状态表(至少含 status;completed 带 output)。未知 run → Unavailable。"""
        self._check_enabled()
        record = self._runs.get(run_id)
        if record is None:
            raise AgentRuntimeUnavailable(f"未知 run: {run_id}")
        info: dict[str, Any] = {
            "run_id": run_id,
            "session_id": record["session_id"],
            "status": record["status"],
        }
        if record["status"] == "completed":
            info["output"] = record["output"]
        return info

    async def run_stop(self, run_id: str) -> None:
        """session/cancel 通知 + 取消 drive 协程;已终态/未知幂等不抛。"""
        self._check_enabled()
        record = self._runs.get(run_id)
        if record is None or record["status"] in _TERMINAL_STATUSES:
            return
        # 顺序约定:先置状态再落终态事件(run_events 消费者见到事件时 run_get 已可读终态)
        record["status"] = "cancelled"
        record["queue"].put({"event": "run.cancelled", "run_id": run_id})
        await self._call(self._stop_w(record))

    async def run_steer(self, run_id: str, text: str) -> bool:
        """运行中向同 session 追加一轮 session/prompt(⚠️ 插话语义按 kimi acp 核对);终态 False。"""
        self._check_enabled()
        record = self._runs.get(run_id)
        if record is None or record["status"] != "running":
            return False
        proc = self._sessions.get(record["session_id"])
        meta = self._session_meta.get(record["session_id"])
        if proc is None or meta is None:
            return False
        asyncio.run_coroutine_threadsafe(self._steer_w(proc, meta, text), self._worker_loop())
        return True

    async def run_approval(self, run_id: str, choice: str) -> None:
        """回应挂起的 session/request_permission;无挂起(已抉择/无请求)仅落 approval.responded。"""
        self._check_enabled()
        record = self._runs.get(run_id)
        if record is None:
            raise AgentRuntimeUnavailable(f"未知 run: {run_id}")
        pending = self._pending_permissions.pop(run_id, None)
        if pending is not None:
            proc, request_id = pending
            outcome = _APPROVAL_OUTCOMES.get(choice, _APPROVAL_OUTCOMES["deny"])
            # ⚠️ 审批响应形状按 kimi acp 核对(规范草案: outcome.outcome=selected/rejected + optionId)
            await self._call(proc.respond(request_id, result={"outcome": outcome}))
        record["queue"].put({"event": "approval.responded", "run_id": run_id, "choice": choice})

    # ---- worker loop 侧实现 ------------------------------------------------------

    async def _create_session_w(self, settings, title: str | None) -> str:
        argv = [settings.acp_command] + shlex.split(settings.acp_args or "", posix=True)
        proc = await _JsonRpcProcess.spawn(argv, cwd=settings.acp_cwd or None)
        proc.on_notification = lambda method, params: self._on_notification(proc, method, params)
        proc.on_request = lambda method, params, req_id: self._on_request(proc, method, params, req_id)
        try:
            # ⚠️ initialize / session/new 参数形状按 kimi acp 核对
            await proc.request(
                ACP_METHODS["initialize"],
                {"protocolVersion": 1, "clientCapabilities": {}},
                timeout=settings.acp_timeout_seconds,
            )
            result = await proc.request(
                ACP_METHODS["session_new"],
                {"cwd": settings.acp_cwd or os.getcwd(), "mcpServers": []},
                timeout=settings.acp_timeout_seconds,
            )
        except Exception:
            await proc.close()
            raise
        session_id = f"acp-sess-{uuid.uuid4().hex}"  # 本地句柄:唯一性不依赖 agent 返回值
        # ⚠️ session/new 响应里的 sessionId 是 agent 侧真实句柄,prompt 一律用它寻址
        remote_session_id = str((result or {}).get("sessionId") or "") or session_id
        self._sessions[session_id] = proc
        self._proc_to_session[id(proc)] = session_id
        self._session_meta[session_id] = {
            "remote_session_id": remote_session_id,
            "title": title,
            "session_key": None,
            "active_run_id": None,  # 正在 drive 的 run(审批路由目的地)
            "prompt_lock": asyncio.Lock(),  # chat / drive 串行化(会话级单轮)
            "sink": None,  # 当前轮次的 session/update 落点(chat 收集器或 run 归一化器)
        }
        return session_id

    async def _chat_w(self, settings, session_id: str, message: str, instructions: str) -> str:
        proc = self._sessions[session_id]
        meta = self._session_meta[session_id]
        # active_run_id 在 submit_run 里先于 drive 调度落位(确定性 Busy 判据);
        # prompt_lock 仅兜底(chat 与 drive 同 loop 串行的最后一道)。
        if meta.get("active_run_id") or meta["prompt_lock"].locked():
            raise AgentRuntimeBusy("会话正忙(进行中的 run),请稍后重试")
        chunks: list[str] = []

        def _collect(update: dict) -> None:
            text = _chunk_text(update)
            if text:
                chunks.append(text)

        async with meta["prompt_lock"]:
            meta["sink"] = _collect
            try:
                # ⚠️ prompt 参数形状按 kimi acp 核对
                await proc.request(
                    ACP_METHODS["session_prompt"],
                    {
                        "sessionId": meta["remote_session_id"],
                        "prompt": _prompt_blocks(instructions, message),
                    },
                    timeout=settings.acp_timeout_seconds,
                )
            finally:
                meta["sink"] = None
        return "".join(chunks)

    async def _drive_run_w(self, run_id: str, session_id: str, task: str,
                           instructions: str) -> None:
        """后台协程:执行一轮 prompt,session/update 归一化进 run 队列;保证终态事件。"""
        record = self._runs[run_id]
        events: queue.Queue = record["queue"]
        proc = self._sessions.get(session_id)
        meta = self._session_meta.get(session_id)
        try:
            if proc is None or meta is None:
                raise AgentRuntimeUnavailable(f"ACP 会话已不存在: {session_id}")
            async with meta["prompt_lock"]:
                meta["sink"] = lambda update: self._sink_run_update(record, update)
                try:
                    # ⚠️ prompt 参数形状按 kimi acp 核对
                    await proc.request(
                        ACP_METHODS["session_prompt"],
                        {
                            "sessionId": meta["remote_session_id"],
                            "prompt": _prompt_blocks(instructions, task),
                        },
                        timeout=get_settings().acp_timeout_seconds,
                    )
                finally:
                    meta["sink"] = None
        except asyncio.CancelledError:
            raise  # run_stop 已落终态事件与状态,这里只负责退出
        except Exception as exc:  # noqa: BLE001 —— drive 必须兜底,绝不无终态事件死掉
            if record["status"] not in _TERMINAL_STATUSES:
                record["status"] = "failed"  # 顺序约定:先置状态再入队
                events.put({"event": "run.failed", "run_id": run_id,
                            "error": {"message": str(exc)}})
        else:
            if record["status"] not in _TERMINAL_STATUSES:
                record["status"] = "completed"  # 顺序约定:先置状态再入队
                events.put({"event": "run.completed", "run_id": run_id,
                            "output": record["output"]})
        finally:
            if meta is not None and meta.get("active_run_id") == run_id:
                meta["active_run_id"] = None

    def _sink_run_update(self, record: dict[str, Any], update: dict) -> None:
        """drive 期间的 session/update 落点:message chunk 聚合进 output(不产事件),
        tool_call/tool_call_update 归一化成 tool.started/tool.completed 入队。"""
        text = _chunk_text(update)
        if text:
            record["output"] += text
            return
        kind = update.get("sessionUpdate")
        if kind == _UPDATE_TOOL_CALL:
            title = str(update.get("title") or update.get("kind") or "tool")
            record["tool_names"][str(update.get("toolCallId") or "")] = title
            record["queue"].put({
                "event": "tool.started",
                "run_id": record["run_id"],
                "tool": title,
                "preview": title,
            })
        elif kind == _UPDATE_TOOL_CALL_UPDATE:
            status = str(update.get("status") or "")
            if status in ("completed", "failed"):
                tool_call_id = str(update.get("toolCallId") or "")
                event: dict[str, Any] = {
                    "event": "tool.completed",
                    "run_id": record["run_id"],
                    "tool": record["tool_names"].get(tool_call_id, tool_call_id or "tool"),
                }
                if status == "failed":
                    event["error"] = "tool call failed(ACP update 未携带详情,按 kimi acp 核对)"
                record["queue"].put(event)

    async def _stop_w(self, record: dict[str, Any]) -> None:
        proc = self._sessions.get(record["session_id"])
        meta = self._session_meta.get(record["session_id"])
        if proc is not None and meta is not None:
            # ⚠️ cancel 通知参数形状按 kimi acp 核对
            await proc.notify(
                ACP_METHODS["session_cancel"], {"sessionId": meta["remote_session_id"]}
            )
        drive_cf = record.get("drive_cf")
        if drive_cf is not None:
            drive_cf.cancel()

    async def _steer_w(self, proc: _JsonRpcProcess, meta: dict[str, Any], text: str) -> None:
        # ⚠️ steer 形状按 kimi acp 核对:骨架按「追加一轮 prompt」处理;不占
        # prompt_lock(插话本意是打断/补充进行中的轮次)。fire-and-forget,失败只记日志。
        try:
            await proc.request(
                ACP_METHODS["session_prompt"],
                {
                    "sessionId": meta["remote_session_id"],
                    "prompt": [{"type": "text", "text": text}],
                },
                timeout=get_settings().acp_timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("ACP steer 失败: %s", exc)

    # ---- agent → 客户端帧路由(worker loop 的 reader 协程内同步调用) -----------------

    def _on_notification(self, proc: _JsonRpcProcess, method: str, params: dict) -> None:
        if method != ACP_METHODS["session_update"]:
            return
        session_id = self._proc_to_session.get(id(proc))
        meta = self._session_meta.get(session_id) if session_id else None
        sink = meta.get("sink") if meta else None
        if sink is not None:
            sink(params.get("update") or {})

    def _on_request(self, proc: _JsonRpcProcess, method: str, params: dict, request_id):
        if method == ACP_METHODS["session_request_permission"]:
            session_id = self._proc_to_session.get(id(proc))
            meta = self._session_meta.get(session_id) if session_id else None
            run_id = meta.get("active_run_id") if meta else None
            record = self._runs.get(run_id) if run_id else None
            if record is None or record["status"] != "running":
                # 没有运行中的 run 接审批:默认拒绝,防 agent 卡死
                return {"outcome": _APPROVAL_OUTCOMES["deny"]}
            self._pending_permissions[run_id] = (proc, request_id)
            tool = str((params.get("toolCall") or {}).get("title") or "工具")
            record["queue"].put({
                "event": "approval.request",
                "run_id": run_id,
                "tool": tool,
                "description": tool,
                "options": params.get("options") or [],
                "params": params,
            })
            return _DEFER  # 响应由 run_approval 补发
        # 其他入向请求(fs/read_text_file 等宿主能力):骨架不支持,回 MethodNotFound
        raise _RpcError(-32601, f"骨架未支持的入向请求: {method}")


# ---------------------------------------------------------------------------
# 归一化(模块级纯函数,便于测试)
# ⚠️ update 字段形状参照 ACP 规范草案,按 kimi-code acp 实际暴露核对。
# ---------------------------------------------------------------------------


def _chunk_text(update: dict) -> str:
    """agent_message_chunk → 增量文本;其他 update 返回 ""。"""
    if update.get("sessionUpdate") != _UPDATE_MESSAGE_CHUNK:
        return ""
    content = update.get("content") or {}
    if isinstance(content, dict):
        return str(content.get("text") or "")
    return ""


def _prompt_blocks(instructions: str, message: str) -> list[dict[str, Any]]:
    """拼 prompt content blocks。ACP 草案无独立 instructions 字段(按 kimi acp 核对),
    骨架把本轮纪律前置为首块文本;接入时按目标 agent 实际支持调整。"""
    blocks: list[dict[str, Any]] = []
    if instructions.strip():
        blocks.append({"type": "text", "text": f"[本轮系统纪律]\n{instructions}"})
    blocks.append({"type": "text", "text": message})
    return blocks
