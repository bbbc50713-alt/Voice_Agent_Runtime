"""AgentRuntime 契约套件(可复用): 九方法契约 + 错误分类。

新适配器(acp/codex/...)接入时: 子类化 AgentRuntimeContract 并提供两个 fixture
即获得全套契约测试——
- ``runtime``: 可用实例(测试期不得触外网: 内存实现或 MockTransport 隔离);
- ``broken_runtime``: 未配置/连不通的实例(任何方法必须抛 AgentRuntimeUnavailable)。

契约口径(与 app/runtime/base.py 文件头一致):
- create_session 返回非空 str;delete_session 幂等(删不存在不抛);
- chat 返回非空 str,instructions 参数被接受;
- submit_run 返回非空 run_id str;
- run_events 产出归一化 dict 事件(键 event 为 str),终态事件
  (run.completed 带 output / run.failed 带 error / run.cancelled)收尾即结束;
- run_get 至少含 status;completed 时带非空 output;
- run_stop 幂等(重复停/停不存在均不抛);
- run_steer 运行中 True、终态 False;
- run_approval 调用不抛错即算过;
- broken_runtime 九方法全部抛 AgentRuntimeUnavailable(未配置/连接失败分类)。

现有绑定: tests/test_runtime_contract.py(FakeRuntime + HermesRuntime×假 HTTP 服务
+ AcpRuntime×假 ACP 子进程常开 / 真 kimi-code skipif)。
"""

from __future__ import annotations

import asyncio

import pytest

from app.runtime.base import AgentRuntime, AgentRuntimeUnavailable


async def _drain_events(runtime: AgentRuntime, run_id: str) -> list[dict]:
    """把 run_events 事件流消费到结束(终态事件收尾后生成器即停)。"""
    return [ev async for ev in runtime.run_events(run_id)]


class AgentRuntimeContract:
    """AgentRuntime 契约测试基类(类名不以 Test 开头,pytest 不直接收集)。"""

    @pytest.fixture
    def runtime(self) -> AgentRuntime:
        raise NotImplementedError("子类必须提供 runtime fixture(可用适配器实例)")

    @pytest.fixture
    def broken_runtime(self) -> AgentRuntime:
        raise NotImplementedError("子类必须提供 broken_runtime fixture(未配置/连不通实例)")

    # --- 会话 CRUD ---------------------------------------------------------------

    def test_create_session_returns_non_empty_str(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        assert isinstance(session_id, str) and session_id
        titled = asyncio.run(runtime.create_session(title="契约测试"))
        assert isinstance(titled, str) and titled and titled != session_id

    def test_delete_session_idempotent(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        asyncio.run(runtime.delete_session(session_id))
        asyncio.run(runtime.delete_session(session_id))  # 再删不抛(幂等)
        asyncio.run(runtime.delete_session("sess_never_existed"))  # 不存在不抛

    # --- chat(ask 快查通道) -------------------------------------------------------

    def test_chat_returns_str(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        answer = asyncio.run(runtime.chat(session_id, "张三最近什么时候来过", session_key="t1:u1"))
        assert isinstance(answer, str) and answer
        with_instructions = asyncio.run(
            runtime.chat(session_id, "查一下到院情况", session_key="t1:u1", instructions="只读纪律")
        )
        assert isinstance(with_instructions, str) and with_instructions

    # --- runs 通道 ----------------------------------------------------------------

    def test_submit_run_returns_run_id(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "盘一下高价值客户", session_key="t1:u1"))
        assert isinstance(run_id, str) and run_id

    def test_run_events_normalized_and_terminate(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "盘点客户", session_key="t1:u1"))
        events = asyncio.run(_drain_events(runtime, run_id))
        assert events, "事件流不应为空"
        for ev in events:
            assert isinstance(ev, dict), f"事件必须是 dict: {ev!r}"
            assert isinstance(ev.get("event"), str), f"事件缺归一化 event 键: {ev!r}"
        terminal = events[-1]
        assert terminal["event"] in ("run.completed", "run.failed", "run.cancelled"), (
            f"事件流须以终态事件收尾,实际末条: {terminal!r}"
        )
        if terminal["event"] == "run.completed":
            assert isinstance(terminal.get("output"), str) and terminal["output"]
        if terminal["event"] == "run.failed":
            assert terminal.get("error"), "run.failed 必须带 error"

    def test_run_get_completed_has_status_and_output(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "盘点客户", session_key="t1:u1"))
        asyncio.run(_drain_events(runtime, run_id))  # 跑到终态
        info = asyncio.run(runtime.run_get(run_id))
        assert "status" in info, "run_get 至少含 status"
        assert info["status"] == "completed"
        assert isinstance(info.get("output"), str) and info["output"], "completed 必须带 output"

    def test_run_stop_idempotent(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "盘点客户", session_key="t1:u1"))
        asyncio.run(runtime.run_stop(run_id))
        asyncio.run(runtime.run_stop(run_id))  # 重复停不抛
        asyncio.run(runtime.run_stop("run_never_existed"))  # 不存在不抛(404 幂等)

    def test_run_steer_running_true_terminal_false(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "盘点客户", session_key="t1:u1"))
        assert asyncio.run(runtime.run_steer(run_id, "只看 90 天内的")) is True
        asyncio.run(_drain_events(runtime, run_id))  # 跑到终态
        assert asyncio.run(runtime.run_steer(run_id, "再补充")) is False

    def test_run_approval_does_not_raise(self, runtime) -> None:
        session_id = asyncio.run(runtime.create_session())
        run_id = asyncio.run(runtime.submit_run(session_id, "危险操作", session_key="t1:u1"))
        asyncio.run(runtime.run_approval(run_id, "once"))

    # --- 错误分类(未配置/连接失败 → AgentRuntimeUnavailable,九方法全覆盖) ------------

    def test_unavailable_raises_agent_runtime_unavailable(self, broken_runtime) -> None:
        async def probe() -> None:
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.create_session()
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.delete_session("sess_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.chat("sess_x", "x", session_key="t1:u1")
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.submit_run("sess_x", "x", session_key="t1:u1")
            with pytest.raises(AgentRuntimeUnavailable):
                async for _ in broken_runtime.run_events("run_x"):
                    pass
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.run_get("run_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.run_stop("run_x")
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.run_steer("run_x", "x")
            with pytest.raises(AgentRuntimeUnavailable):
                await broken_runtime.run_approval("run_x", "once")

        asyncio.run(probe())
