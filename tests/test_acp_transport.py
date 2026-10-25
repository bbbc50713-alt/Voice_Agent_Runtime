"""ACP 传输层与适配器行为测试:假 ACP 子进程(tests/fake_acp_agent.py)真跑。

- TestJsonRpcTransport:换行 JSON-RPC 的 id 配对(乱序响应)/error 帧/超时/进程死亡
  → AgentRuntimeUnavailable 映射;直接驱动 ``_JsonRpcProcess``(单 asyncio.run 内闭环)。
- TestAcpRuntimeWithFakeAgent:AcpRuntime 行为级用例(chat chunk 聚合/事件归一化/
  stop 幂等/steer True→False/审批挂起-抉择回环/未知会话 BadRequest/会话忙 Busy)。

契约套件绑定(broken 九方法真跑 + 假 agent 全量契约 + 真 kimi skipif)在
tests/test_runtime_contract.py。
"""

from __future__ import annotations

import asyncio
import shlex

import pytest

from app.config import get_settings
from app.runtime.acp import AcpRuntime, _JsonRpcProcess
from app.runtime.base import (
    AgentRuntimeBadRequest,
    AgentRuntimeBusy,
    AgentRuntimeUnavailable,
)

from tests.fake_acp_agent import install_fake_agent_settings


def _argv() -> list[str]:
    settings = get_settings()
    return [settings.acp_command] + shlex.split(settings.acp_args or "", posix=True)


# --- 传输层:_JsonRpcProcess(真实子进程,单事件循环内闭环) -----------------------------


class TestJsonRpcTransport:
    def test_request_response_id_pairing_out_of_order(self, monkeypatch) -> None:
        """并发两请求:慢请求 id 小、快请求 id 大,响应乱序返回,必须按 id 正确配对。"""
        install_fake_agent_settings(monkeypatch)

        async def main() -> None:
            proc = await _JsonRpcProcess.spawn(_argv(), cwd=None)
            try:
                slow = asyncio.create_task(
                    proc.request("echo", {"text": "slow", "delay": 0.4}, timeout=10)
                )
                fast = asyncio.create_task(
                    proc.request("echo", {"text": "fast", "delay": 0.05}, timeout=10)
                )
                r_slow, r_fast = await asyncio.gather(slow, fast)
                assert r_slow == {"echo": "slow"}
                assert r_fast == {"echo": "fast"}
            finally:
                await proc.close()

        asyncio.run(main())

    def test_error_frame_maps_unavailable(self, monkeypatch) -> None:
        """对端回 JSON-RPC error 帧(未知 method)→ AgentRuntimeUnavailable。"""
        install_fake_agent_settings(monkeypatch)

        async def main() -> None:
            proc = await _JsonRpcProcess.spawn(_argv(), cwd=None)
            try:
                with pytest.raises(AgentRuntimeUnavailable, match="-32601"):
                    await proc.request("no_such_method", {}, timeout=10)
            finally:
                await proc.close()

        asyncio.run(main())

    def test_request_timeout_maps_unavailable(self, monkeypatch) -> None:
        """对端沉睡超过 timeout → AgentRuntimeUnavailable(超时)。"""
        install_fake_agent_settings(monkeypatch)

        async def main() -> None:
            proc = await _JsonRpcProcess.spawn(_argv(), cwd=None)
            try:
                with pytest.raises(AgentRuntimeUnavailable, match="超时"):
                    await proc.request("sleep", {"seconds": 30}, timeout=0.3)
            finally:
                await proc.close()

        asyncio.run(main())

    def test_process_death_maps_unavailable(self, monkeypatch) -> None:
        """子进程死亡:在途请求与后续请求均 → AgentRuntimeUnavailable。"""
        install_fake_agent_settings(monkeypatch)

        async def main() -> None:
            proc = await _JsonRpcProcess.spawn(_argv(), cwd=None)
            try:
                alive = await proc.request("echo", {"text": "hi", "delay": 0}, timeout=10)
                assert alive == {"echo": "hi"}
                pending = asyncio.create_task(proc.request("sleep", {"seconds": 30}, timeout=30))
                await asyncio.sleep(0.1)  # 让 sleep 请求先发出
                await proc.notify("die")  # 假 agent 立即退出 → stdout EOF → 标记死亡
                with pytest.raises(AgentRuntimeUnavailable, match="已退出"):
                    await pending
                with pytest.raises(AgentRuntimeUnavailable, match="已退出"):
                    await proc.request("echo", {"text": "again", "delay": 0}, timeout=10)
            finally:
                await proc.close()

        asyncio.run(main())


# --- 适配器行为:AcpRuntime × 假 agent(跨 asyncio.run 调用,贴近契约套件节奏) ------------


class TestAcpRuntimeWithFakeAgent:
    @pytest.fixture
    def runtime(self, monkeypatch):
        install_fake_agent_settings(monkeypatch)
        runtime = AcpRuntime()
        yield runtime
        asyncio.run(runtime.aclose())

    def test_chat_aggregates_message_chunks(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            answer = await runtime.chat(session_id, "盘点客户", session_key="t1:u1")
            assert "fake 回答:" in answer and "盘点客户" in answer
            with_instructions = await runtime.chat(
                session_id, "查到院情况", session_key="t1:u1", instructions="只读纪律"
            )
            assert with_instructions  # instructions 前置块被接受且不影响返回

        asyncio.run(main())

    def test_run_events_normalized_and_run_get(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            run_id = await runtime.submit_run(session_id, "盘点客户", session_key="t1:u1")
            assert run_id.startswith("acp-")
            events = [ev async for ev in runtime.run_events(run_id)]
            kinds = [ev["event"] for ev in events]
            assert "tool.started" in kinds and "tool.completed" in kinds
            assert kinds[-1] == "run.completed"
            assert events[-1]["run_id"] == run_id and events[-1]["output"]
            assert "盘点客户" in events[-1]["output"]  # chunk 聚合进 output
            info = await runtime.run_get(run_id)
            assert info["status"] == "completed" and info["output"]

        asyncio.run(main())

    def test_run_stop_cancels_and_idempotent(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            run_id = await runtime.submit_run(session_id, "长任务", session_key="t1:u1")
            await runtime.run_stop(run_id)
            events = [ev async for ev in runtime.run_events(run_id)]
            assert events[-1]["event"] == "run.cancelled"
            assert (await runtime.run_get(run_id))["status"] == "cancelled"
            await runtime.run_stop(run_id)  # 重复停不抛
            await runtime.run_stop("acp-不存在")  # 未知 run 不抛

        asyncio.run(main())

    def test_run_steer_running_true_terminal_false(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            run_id = await runtime.submit_run(session_id, "盘点客户", session_key="t1:u1")
            assert await runtime.run_steer(run_id, "只看 90 天内的") is True
            async for _ in runtime.run_events(run_id):
                pass  # 跑到终态
            assert await runtime.run_steer(run_id, "再补充") is False

        asyncio.run(main())

    def test_approval_flow_allow_and_deny(self, runtime) -> None:
        """prompt 含「审批」→ 假 agent 发 request_permission;run_approval 回应 outcome。"""

        async def one_round(choice: str) -> tuple[list[str], str]:
            session_id = await runtime.create_session()
            run_id = await runtime.submit_run(session_id, "执行需要审批的操作", session_key="t1:u1")
            kinds: list[str] = []
            output = ""
            async for ev in runtime.run_events(run_id):
                kinds.append(ev["event"])
                if ev["event"] == "approval.request":
                    assert ev.get("tool") == "fake_dangerous_tool"
                    await runtime.run_approval(run_id, choice)
                if ev["event"] == "run.completed":
                    output = ev.get("output") or ""
            return kinds, output

        async def main() -> None:
            kinds, output = await one_round("once")
            assert "approval.request" in kinds and "approval.responded" in kinds
            assert kinds[-1] == "run.completed" and "审批已通过" in output
            kinds, output = await one_round("deny")
            assert kinds[-1] == "run.completed" and "操作已被拒绝" in output

        asyncio.run(main())

    def test_unknown_session_maps_bad_request(self, runtime) -> None:
        with pytest.raises(AgentRuntimeBadRequest):
            asyncio.run(runtime.chat("sess_不存在", "x", session_key="t1:u1"))
        with pytest.raises(AgentRuntimeBadRequest):
            asyncio.run(runtime.submit_run("sess_不存在", "x", session_key="t1:u1"))

    def test_submit_while_run_active_maps_busy(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            await runtime.submit_run(session_id, "长任务", session_key="t1:u1")
            with pytest.raises(AgentRuntimeBusy):
                await runtime.submit_run(session_id, "又来一个", session_key="t1:u1")
            with pytest.raises(AgentRuntimeBusy):
                await runtime.chat(session_id, "插话", session_key="t1:u1")

        asyncio.run(main())

    def test_delete_session_terminates_and_idempotent(self, runtime) -> None:
        async def main() -> None:
            session_id = await runtime.create_session()
            assert session_id in runtime._sessions
            await runtime.delete_session(session_id)
            assert session_id not in runtime._sessions
            await runtime.delete_session(session_id)  # 再删不抛(幂等)
            await runtime.delete_session("sess_从未存在")  # 不存在不抛
            with pytest.raises(AgentRuntimeBadRequest):
                await runtime.chat(session_id, "x", session_key="t1:u1")

        asyncio.run(main())

    def test_unconfigured_raises_unavailable(self, monkeypatch) -> None:
        """未配置 acp_command:九方法全抛 AgentRuntimeUnavailable(不依赖上方 fixture)。"""
        monkeypatch.setattr(get_settings(), "acp_command", "")
        runtime = AcpRuntime()

        async def probe() -> None:
            with pytest.raises(AgentRuntimeUnavailable, match="acp_command"):
                await runtime.create_session()
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.delete_session("sess_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.chat("sess_x", "x", session_key="t1:u1")
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.submit_run("sess_x", "x", session_key="t1:u1")
            with pytest.raises(AgentRuntimeUnavailable):
                async for _ in runtime.run_events("run_x"):
                    pass
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.run_get("run_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.run_stop("run_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.run_steer("run_x", "x")
            with pytest.raises(AgentRuntimeUnavailable):
                await runtime.run_approval("run_x", "once")

        asyncio.run(probe())
