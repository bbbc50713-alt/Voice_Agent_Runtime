"""测试用假实现: FakeRuntime(内存 AgentRuntime,九方法全实现)。

用法一(契约套件): 直接实例化交给 tests/runtime_contract.py 的基类,
见 tests/test_runtime_contract.py::TestFakeRuntimeContract。

用法二(voice 服务层/路由测试): 把 app.runtime 门面临时接到 fake——

    from tests.fakes import install_fake_runtime

    def test_xxx(monkeypatch):
        fake = install_fake_runtime(monkeypatch)
        ...  # 测试内 app.runtime 门面的 chat_sync/runs_submit 等全部落到 fake

语义尽量贴近 hermes 适配器(假实现的价值在于"契约同形",不是同名不同义):
- submit_run 受理即返回 run_id;run_events 按 events_script 吐归一化 RunEvent
  (默认 tool.started → tool.completed → run.completed);吐到终态事件时
  run 落 completed/failed/cancelled,run_get 随之反映;
- chat 回声 ``fake-answer: <message>``(chat_reply 可覆盖),未知会话抛
  AgentRuntimeBadRequest;所有调用参数记录进 chat_calls/submit_calls 供断言;
- run_get 未知 run 抛 AgentRuntimeUnavailable(对齐 hermes 404 → Unavailable 映射);
- run_stop 幂等(未知 run 不抛,对齐 hermes 404 幂等);run_steer 仅 running 返回 True;
- run_approval 记录即过(未知 run 抛 Unavailable);
- ``FakeRuntime(available=False)`` → 全部九方法抛 AgentRuntimeUnavailable,
  供降级路径测试(契约套件的 broken_runtime 即用它)。
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable

import pytest

from app.runtime.base import (
    AgentRuntimeBadRequest,
    AgentRuntimeUnavailable,
)

# 终态事件类型(与 app/runtime/base.py 文件头契约一致)
TERMINAL_EVENTS = ("run.completed", "run.failed", "run.cancelled")


class FakeRuntime:
    """内存 AgentRuntime: 语义贴近 hermes 的假实现(见模块 docstring)。"""

    def __init__(
        self,
        *,
        available: bool = True,
        chat_reply: str | None = None,
        events_script: list[dict] | Callable[[str], list[dict]] | None = None,
    ) -> None:
        self.available = available
        self.chat_reply = chat_reply
        # 每个 run 的事件剧本: 定值 list 或 run_id → list 的工厂;None=默认成功剧本
        self.events_script = events_script
        self.sessions: dict[str, str | None] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        # 调用留痕(断言用)
        self.chat_calls: list[dict] = []
        self.submit_calls: list[dict] = []
        self.steered: list[tuple[str, str]] = []
        self.approvals: list[tuple[str, str]] = []
        self.stopped: list[str] = []
        self._seq = 0

    # -- 内部工具 -------------------------------------------------------------

    def _check(self) -> None:
        if not self.available:
            raise AgentRuntimeUnavailable("fake runtime 置为不可用(available=False)")

    def _next(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}_{self._seq}"

    def _script_for(self, run_id: str) -> list[dict]:
        script = self.events_script
        if callable(script):
            return [dict(ev) for ev in script(run_id)]
        if script is not None:
            return [dict(ev) for ev in script]
        return [
            {"event": "tool.started", "tool": "fake_tool", "preview": "假工具"},
            {"event": "tool.completed", "tool": "fake_tool", "duration": 0.01},
            {"event": "run.completed", "run_id": run_id, "output": "fake 结果"},
        ]

    # -- AgentRuntime 协议(九方法) ---------------------------------------------

    async def create_session(self, *, title: str | None = None) -> str:
        self._check()
        session_id = self._next("fake_sess")
        self.sessions[session_id] = title
        return session_id

    async def delete_session(self, session_id: str) -> None:
        self._check()
        self.sessions.pop(session_id, None)  # 幂等: 不存在视为已删

    async def chat(self, session_id: str, message: str, *, session_key: str,
                   instructions: str | None = None) -> str:
        self._check()
        if session_id not in self.sessions:
            raise AgentRuntimeBadRequest(f"未知会话 {session_id}")
        self.chat_calls.append({
            "session_id": session_id,
            "message": message,
            "session_key": session_key,
            "instructions": instructions,
        })
        return self.chat_reply if self.chat_reply is not None else f"fake-answer: {message}"

    async def submit_run(self, session_id: str, task: str, *, session_key: str,
                         instructions: str | None = None) -> str:
        self._check()
        run_id = self._next("fake_run")
        self.runs[run_id] = {
            "session_id": session_id, "task": task, "status": "running", "output": None,
        }
        self.submit_calls.append({
            "session_id": session_id,
            "task": task,
            "session_key": session_key,
            "instructions": instructions,
        })
        return run_id

    async def run_events(self, run_id: str) -> AsyncIterator[dict[str, Any]]:
        self._check()
        if run_id not in self.runs:
            raise AgentRuntimeUnavailable(f"未知 run {run_id}")  # 对齐 hermes 404 映射
        for event in self._script_for(run_id):
            etype = event.get("event")
            if etype == "run.completed":
                self.runs[run_id]["status"] = "completed"
                self.runs[run_id]["output"] = str(event.get("output") or "")
            elif etype == "run.failed":
                self.runs[run_id]["status"] = "failed"
            elif etype == "run.cancelled":
                self.runs[run_id]["status"] = "cancelled"
            yield event
            if etype in TERMINAL_EVENTS:
                return  # 终态即收尾(契约: 终态事件后不再产出)

    async def run_get(self, run_id: str) -> dict[str, Any]:
        self._check()
        run = self.runs.get(run_id)
        if run is None:
            raise AgentRuntimeUnavailable(f"未知 run {run_id}")
        info: dict[str, Any] = {"status": run["status"]}
        if run["status"] == "completed":
            info["output"] = run["output"] or ""
        return info

    async def run_stop(self, run_id: str) -> None:
        self._check()
        run = self.runs.get(run_id)
        if run is None:
            return  # 幂等: 已终态/不存在不抛
        if run["status"] == "running":
            run["status"] = "cancelled"
        self.stopped.append(run_id)

    async def run_steer(self, run_id: str, text: str) -> bool:
        self._check()
        run = self.runs.get(run_id)
        if run is None or run["status"] != "running":
            return False
        self.steered.append((run_id, text))
        return True

    async def run_approval(self, run_id: str, choice: str) -> None:
        self._check()
        if run_id not in self.runs:
            raise AgentRuntimeUnavailable(f"未知 run {run_id}")
        self.approvals.append((run_id, choice))


def install_fake_runtime(monkeypatch: pytest.MonkeyPatch, fake: FakeRuntime | None = None) -> FakeRuntime:
    """把 app.runtime 门面临时接到 FakeRuntime;monkeypatch 收尾自动还原,不污染下个测试。

    等价于 register_runtime("fake", lambda: fake) + settings.agent_runtime_kind="fake"
    + reset_runtime(),但直接替换 app.runtime.base._cached 单例缓存——
    monkeypatch 收尾恢复原值(None),比改注册表/配置更不易泄漏。
    """
    import app.runtime.base as base

    fake = fake or FakeRuntime()
    monkeypatch.setattr(base, "_cached", fake)
    return fake
