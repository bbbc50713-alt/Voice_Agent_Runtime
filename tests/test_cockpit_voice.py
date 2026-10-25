"""驾驶舱实时语音(cockpit_voice)单测 — 自平台 backend/tests/test_cockpit_voice.py 移植。

移植差异(独立服务 jwt-claims + AgentRuntime 抽象):
- 鉴权: 不建 User 行,PyJWT 铸 token(tests.conftest.mint_token);create_user 返回
  (user_id, token),login(token) 返回 headers——调用点与平台同形。
- agent 运行时 HTTP: MockTransport 补丁点改 app.runtime.hermes.new_async_client
  (+ app.runtime.new_async_client 门面 re-export,voice_tasks._llm_summary 经门面取 client);
  facade 函数(runs_submit/chat_sync 等)monkeypatch 风格不变(app.runtime 签名与平台一致)。
- 模型别名: VoiceConversation→CockpitConversation / VoiceTask→CockpitVoiceTask /
  SpeakerProfile→CockpitSpeakerProfile,正文零改动;hermes_session_id→agent_session_id。
- DB: tmp SQLite(conftest 顶部钉 DATABASE_URL,import 前生效)。
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.main import app
from app.models import (
    RealtimeEvent,
    SpeakerProfile as CockpitSpeakerProfile,
    VoiceConversation as CockpitConversation,
    VoiceTask as CockpitVoiceTask,
)
from app import runtime as cockpit_runtime
from app.routes import voice as cockpit_voice_routes
from app.runtime import CockpitRuntimeBusy, CockpitRuntimeUnavailable
from app.runtime import hermes as hermes_runtime
from app.services import cockpit_voice, voice_sidecar, voice_tasks

from tests.conftest import TENANT_ID, mint_token

client = TestClient(app)
TEST_API_KEY = "test-cockpit-key-0123456789"


# --- 公共装置 ----------------------------------------------------------------------


@pytest.fixture
def voice_settings(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "agent_runtime_enabled", True)
    monkeypatch.setattr(settings, "agent_runtime_api_key", TEST_API_KEY)
    monkeypatch.setattr(settings, "agent_runtime_base_url", "http://127.0.0.1:8642")
    monkeypatch.setattr(settings, "realtime_voice_enabled", True)
    monkeypatch.setattr(settings, "realtime_voice_task_timeout", 5.0)
    # 钉死多人开关:防 .env(MULTI_SPEAKER_ENABLED=true)泄漏进测试——
    # legacy 用例一旦为 True 会建真 VoiceSidecar 去连真实 DashScope ASR(2026-09-14)
    monkeypatch.setattr(settings, "multi_speaker_enabled", False)
    return settings


def _wait_voice_sessions_cleared(timeout: float = 3.0) -> None:
    """轮询等 WS 服务端 try/finally 清完注册表(TestClient 退出 with 不保证服务端收尾完成)。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not cockpit_voice._active_voice_sessions:
            return
        time.sleep(0.05)
    assert cockpit_voice._active_voice_sessions == {}, "WS 收尾后注册表未清空"


def _mock_transport(monkeypatch, handler) -> None:
    """MockTransport 同时扮演 hermes runs 三端点/chat 与 llm 摘要端点。

    补丁点两处(同一 factory): hermes 适配器自身的 HTTP 客户端工厂 + runtime 门面
    re-export 的 new_async_client(voice_tasks._llm_summary 经门面取 client)。
    """
    transport = httpx.MockTransport(handler)

    def factory(timeout) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=transport, trust_env=False, timeout=timeout)

    monkeypatch.setattr(hermes_runtime, "new_async_client", factory)
    monkeypatch.setattr(cockpit_runtime, "new_async_client", factory)


def _patch_llm(monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "llm_provider", "openai_compatible")
    monkeypatch.setattr(settings, "llm_base_url", "http://llm.test/v1")
    monkeypatch.setattr(settings, "llm_api_key", "test-llm-key")
    monkeypatch.setattr(settings, "llm_model", "test-model")


def create_user(*, role: str = "sales") -> tuple[int, str]:
    """jwt-claims 模式: 不落用户库,铸 token 即身份。返回 (user_id, token)(平台同形)。"""
    user_id = uuid4().int % 900_000_000 + 1000
    return user_id, mint_token(user_id, role=role)


def login(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def create_mirror_row(user_id: int, agent_session_id: str, *, tenant_id: int = TENANT_ID) -> int:
    with SessionLocal() as db:
        conv = CockpitConversation(
            tenant_id=tenant_id, user_id=user_id, title="语音单测", agent_session_id=agent_session_id
        )
        db.add(conv)
        db.commit()
        return conv.id


def _ctx(user_id: int) -> SimpleNamespace:
    """服务层 duck-type 上下文(仅 tenant_id/user_id 被读取)。"""
    return SimpleNamespace(tenant_id=TENANT_ID, user_id=user_id)


def _submit_row(user_id: int, conv_id: int, text: str, kind: str = "盘点", via: str = "submit") -> int:
    with SessionLocal() as db:
        conv = db.get(CockpitConversation, conv_id)
        task = voice_tasks.submit_task(db, _ctx(user_id), conv, text, kind, via=via)
        return task.id


def _get_row(task_id: int) -> CockpitVoiceTask:
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        assert task is not None
        db.expunge(task)
        return task


class FakeVoiceSession:
    """假 DashScopeVoiceSession:记录播报注入(§11 request_announce)与任务事件推送,不连真实阿里。"""

    instances: list["FakeVoiceSession"] = []

    proactive_response: bool = False  # 类属性:模拟系统主动发言(路由层闸门豁免判据)

    def __init__(self, *, instructions: str = "", on_event=None, voice_mode: str | None = None):
        self.instructions = instructions
        self.on_event = on_event
        self.voice_mode = voice_mode
        self.on_task_event = None
        self.on_announce_transition = None
        self.responding = False
        self.user_speaking = False
        self.current_response_proactive = FakeVoiceSession.proactive_response
        self.announced_texts: list[str] = []
        self.announce_calls: list[dict] = []
        self.deferred_ids: list[int] = []
        self.consumed_ids: list[int] = []
        self.followup_count = 0
        self.cancel_count = 0
        self.audio: list[bytes] = []
        self.function_outputs: list[str] = []
        self.closed = False
        FakeVoiceSession.instances.append(self)

    event_script: list[tuple[float, dict]] = []  # 类属性剧本:(延时秒,事件),connect 内依次 emit

    async def connect(self) -> None:
        if self.on_event is not None:
            await self.on_event({"type": "session.updated"})
            for delay, event in FakeVoiceSession.event_script:
                await asyncio.sleep(delay)
                await self.on_event(event)

    async def send_audio(self, pcm: bytes) -> None:
        self.audio.append(pcm)

    async def send_function_output(self, call_id: str, output: str) -> None:
        self.function_outputs.append(output)

    async def inject_text(self, text: str) -> None:
        pass

    async def create_response(self, instructions: str | None = None) -> None:
        pass

    async def cancel_response(self) -> None:
        """v3.5: dismiss_turn 配套的 response.cancel 截断记录。"""
        self.cancel_count += 1

    async def request_announce(self, task_id: int, text: str, **kwargs) -> None:
        """§11 新接口:记录注入文本与调度参数(mode/word/queue/attempts)。"""
        self.announced_texts.append(text)
        self.announce_calls.append({"task_id": task_id, **kwargs})

    def schedule_followup(self) -> None:
        """v3.3 静默闸门跟随发言登记(路由层函数回执后统一调用)。"""
        self.followup_count += 1

    def defer_tasks(self, task_ids: list[int]) -> None:
        self.deferred_ids.extend(task_ids)

    def consume_tasks(self, task_ids: list[int]) -> None:
        """mark_result_shared 配套:记录从调度器摘除的任务(与 defer_tasks 同语义)。"""
        self.consumed_ids.extend(task_ids)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def registered_fake_session():
    """注册/注销活跃语音会话;.register() 返回实例,任务事件推进 .pushed。"""
    holder = SimpleNamespace(current=None, pushed=[], key=None)

    def register(tenant_id: int, user_id: int) -> FakeVoiceSession:
        fake = FakeVoiceSession()

        async def sink(payload):
            holder.pushed.append(payload)

        fake.on_task_event = sink
        cockpit_voice.register_voice_session(tenant_id, user_id, fake)
        holder.current = fake
        holder.key = (tenant_id, user_id)
        return fake

    holder.register = register
    yield holder
    if holder.current is not None and holder.key is not None:
        cockpit_voice.unregister_voice_session(holder.key[0], holder.key[1], holder.current)


# --- status 端点(§6.1) --------------------------------------------------------------


def test_status_disabled(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_enabled", False)
    _, mobile = create_user()
    r = client.get("/api/v1/cockpit/realtime-voice/status", headers=login(mobile))
    assert r.status_code == 200, r.text
    assert r.json()["data"]["enabled"] is False


def test_status_enabled(monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_enabled", True)
    monkeypatch.setattr(settings, "dashscope_realtime_model", "qwen3.5-omni-plus-realtime")
    monkeypatch.setattr(settings, "realtime_voice_name", "Ethan")
    _, mobile = create_user()
    r = client.get("/api/v1/cockpit/realtime-voice/status", headers=login(mobile))
    assert r.status_code == 200, r.text
    body = r.json()["data"]
    assert body["enabled"] is True
    assert body["model"] == "qwen3.5-omni-plus-realtime"
    assert body["voice"] == "Ethan"


# --- WS 鉴权(§6.1) -------------------------------------------------------------------


def test_ws_no_token_4401() -> None:
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/api/v1/cockpit/realtime-voice/ws"):
            pass
    assert exc_info.value.code == 4401


def test_ws_disabled_4403(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_enabled", False)
    _, mobile = create_user()
    token = login(mobile)["Authorization"].split()[1]
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}"):
            pass
    assert exc_info.value.code == 4403


def test_ws_happy_path_with_fake_dashscope(voice_settings, monkeypatch) -> None:
    """WS 双工: start→ready→音频帧→ping/pong→stop,连接结束全清理(注册表注销)。"""
    FakeVoiceSession.instances = []
    monkeypatch.setattr(cockpit_voice, "DashScopeVoiceSession", FakeVoiceSession)
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"api_ws_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]

    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id})
        assert ws.receive_json() == {"type": "state", "state": "ready"}
        ws.send_bytes(b"\x00\x01\x02\x03")
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}
        ws.send_json({"type": "stop"})

    fake = FakeVoiceSession.instances[-1]
    assert fake.audio == [b"\x00\x01\x02\x03"]
    assert fake.closed is True
    _wait_voice_sessions_cleared()  # try/finally 全清理(服务端收尾异步,轮询等待)
    assert "语音前台工作指引" in fake.instructions  # instructions 含指引段


# --- cockpit_runtime runs 通道客户端层(§10.1.5) ----------------------------------------


def test_chat_sync_success_and_contract(voice_settings, monkeypatch) -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["session_key"] = request.headers.get("X-Hermes-Session-Key")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "object": "hermes.session.chat.completion",
                "session_id": "api_x1",
                "message": {"role": "assistant", "content": "张三最近一次到院是 8 月 30 日"},
                "usage": {},
            },
        )

    _mock_transport(monkeypatch, handler)
    result = asyncio.run(cockpit_runtime.chat_sync("api_x1", "张三最近什么时候来过", session_key="t1:u7"))
    assert result == "张三最近一次到院是 8 月 30 日"
    assert seen["session_key"] == "t1:u7"
    assert seen["body"] == {"message": "张三最近什么时候来过"}


def test_chat_sync_contract_violation(voice_settings, monkeypatch) -> None:
    _mock_transport(monkeypatch, lambda request: httpx.Response(200, json={"nope": 1}))
    with pytest.raises(CockpitRuntimeUnavailable):
        asyncio.run(cockpit_runtime.chat_sync("api_x1", "x", session_key="t1:u7"))


def test_runs_submit_202(voice_settings, monkeypatch) -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        seen["session_key"] = request.headers.get("X-Hermes-Session-Key")
        return httpx.Response(202, json={"run_id": "run_abc", "status": "started"})

    _mock_transport(monkeypatch, handler)
    run_id = asyncio.run(cockpit_runtime.runs_submit("api_x1", "盘一下高价值客户", session_key="t1:u7"))
    assert run_id == "run_abc"
    assert seen["body"] == {"input": "盘一下高价值客户", "session_id": "api_x1"}
    assert seen["session_key"] == "t1:u7"


def test_runs_submit_non202_taxonomy(voice_settings, monkeypatch) -> None:
    _mock_transport(monkeypatch, lambda request: httpx.Response(429, json={}))
    with pytest.raises(CockpitRuntimeBusy):
        asyncio.run(cockpit_runtime.runs_submit("api_x1", "x", session_key="t1:u7"))


def test_runs_events_parses_data_only_sse(voice_settings, monkeypatch) -> None:
    """SSE 只有 data: 行(无 event: 行),类型在 JSON event 字段;跳过注释行,stream closed 收尾。"""
    sse = (
        ": keepalive\n\n"
        'data: {"event":"tool.started","tool":"search_memory"}\n\n'
        'data: {"event":"run.completed","output":"done"}\n\n'
        ": stream closed\n\n"
        'data: {"event":"should.not.appear"}\n\n'
    )
    _mock_transport(
        monkeypatch,
        lambda request: httpx.Response(200, content=sse.encode(), headers={"content-type": "text/event-stream"}),
    )

    async def collect():
        return [ev async for ev in cockpit_runtime.runs_events("run_abc")]

    events = asyncio.run(collect())
    assert [ev["event"] for ev in events] == ["tool.started", "run.completed"]


def test_runs_stop_404_idempotent(voice_settings, monkeypatch) -> None:
    _mock_transport(monkeypatch, lambda request: httpx.Response(404, json={}))
    asyncio.run(cockpit_runtime.runs_stop("run_gone"))  # 不抛


def test_runs_steer_409_false(voice_settings, monkeypatch) -> None:
    _mock_transport(monkeypatch, lambda request: httpx.Response(409, json={}))
    assert asyncio.run(cockpit_runtime.runs_steer("run_abc", "只看 90 天内的")) is False


def test_runs_steer_ok_and_approval(voice_settings, monkeypatch) -> None:
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append({"path": request.url.path, "body": json.loads(request.content)})
        return httpx.Response(200, json={"status": "ok"})

    _mock_transport(monkeypatch, handler)
    assert asyncio.run(cockpit_runtime.runs_steer("run_abc", "补充")) is True
    asyncio.run(cockpit_runtime.runs_approval("run_abc", "once"))
    assert seen[0] == {"path": "/v1/runs/run_abc/steer", "body": {"input": "补充"}}
    assert seen[1] == {"path": "/v1/runs/run_abc/approval", "body": {"choice": "once"}}


# --- 账本: 受理与并发上限(§6.1) ----------------------------------------------------------


def test_submit_task_creates_ledger_row(voice_settings) -> None:
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_sub_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "盘一下最近值得跟进的高价值客户", "盘点")
    row = _get_row(task_id)
    assert row.status == "pending" and row.kind == "盘点"
    assert row.tenant_id == TENANT_ID and row.user_id == user_id and row.conversation_id == conv_id
    assert row.announced is False and row.run_ref is None
    assert row.progress[0]["step"] == "route" and row.progress[0]["via"] == "submit"


def test_submit_task_concurrency_cap(voice_settings) -> None:
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_cap_{uuid4().hex[:6]}")
    cap = get_settings().realtime_voice_max_tasks_per_user
    with SessionLocal() as db:
        conv = db.get(CockpitConversation, conv_id)
        ctx = _ctx(user_id)
        for i in range(cap):
            voice_tasks.submit_task(db, ctx, conv, f"t{i + 1}", "其他")
        with pytest.raises(voice_tasks.VoiceTaskBusy):
            voice_tasks.submit_task(db, ctx, conv, f"t{cap + 1}", "其他")


def test_submit_task_unknown_kind_falls_back(voice_settings) -> None:
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_kind_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "随便聊聊", "闲聊")
    assert _get_row(task_id).kind == "其他"


# --- 账本: runs 通道执行(§6.1/§10.1.5) ----------------------------------------------------


def _runs_success_handler(seen: dict):
    """MockTransport 同时扮演 runs 三端点与 llm 摘要端点。"""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/runs" and request.method == "POST":
            seen["submit_body"] = json.loads(request.content)
            return httpx.Response(202, json={"run_id": "run_x1", "status": "started"})
        if path == "/v1/runs/run_x1/events":
            sse = (
                'data: {"event":"tool.started","tool":"search_memory","preview":"检索记忆"}\n\n'
                'data: {"event":"tool.completed","tool":"search_memory","duration":0.8}\n\n'
                'data: {"event":"tool.started","tool":"query_customers"}\n\n'
                'data: {"event":"tool.completed","tool":"query_customers","duration":2.1}\n\n'
                'data: {"event":"run.completed","output":"盘点结果：高价值客户 12 位，明细略"}\n\n'
                ": stream closed\n\n"
            )
            return httpx.Response(200, content=sse.encode(), headers={"content-type": "text/event-stream"})
        if path.endswith("/chat/completions"):
            return httpx.Response(200, json={"choices": [{"message": {"content": "共 12 位高价值客户"}}]})
        return httpx.Response(404, json={})

    return handler


def test_run_task_success_updates_row_and_announces(voice_settings, monkeypatch, registered_fake_session) -> None:
    seen: dict = {}
    _mock_transport(monkeypatch, _runs_success_handler(seen))
    _patch_llm(monkeypatch)
    user_id, _ = create_user()
    hermes_id = f"api_run_{uuid4().hex[:6]}"
    conv_id = create_mirror_row(user_id, hermes_id)
    task_id = _submit_row(user_id, conv_id, "盘一下高价值客户", "盘点")
    fake = registered_fake_session.register(TENANT_ID, user_id)

    asyncio.run(voice_tasks._run(task_id))

    row = _get_row(task_id)
    assert row.status == "done"
    assert row.run_ref == "run_x1"
    assert row.result_text == "盘点结果：高价值客户 12 位，明细略"
    assert row.result_summary == "共 12 位高价值客户"  # llm 摘要通道
    assert row.finished_at is not None
    assert row.announced is True
    steps = {s["step"]: s for s in row.progress}
    assert steps["route"]["via"] == "submit"
    assert steps["search_memory"]["state"] == "done" and steps["search_memory"]["duration"] == 0.8
    assert steps["query_customers"]["state"] == "done"
    # 提交契约: input/session_id + 软阻断风险卡纪律 ephemeral 指令(voice 任务每轮注入)
    assert seen["submit_body"]["input"] == "盘一下高价值客户"
    assert seen["submit_body"]["session_id"] == hermes_id
    assert "软阻断风险卡纪律" in (seen["submit_body"].get("instructions") or "")
    # 播报注入: §11 备查条目(摘要+勿主动提);执行时长 ≤15s → mode="direct" 直念(仍走静默闸门)
    assert len(fake.announced_texts) == 1
    assert f"【后台任务结果】任务#{task_id}" in fake.announced_texts[0]
    assert "共 12 位高价值客户" in fake.announced_texts[0]
    assert "备查条目" in fake.announced_texts[0]
    assert fake.announce_calls[0]["mode"] == "direct"
    assert fake.announce_calls[0]["word"] in voice_tasks._FRESH_WORDS
    # 已登记播报调度;fake 不回写状态,DB 保持 queued 等播放结算
    assert row.announce_state == "queued"
    # 任务事件推送: task.updated(转圈/done) + task.progress
    pushed_types = [p["type"] for p in registered_fake_session.pushed]
    assert "task.updated" in pushed_types and "task.progress" in pushed_types
    assert registered_fake_session.pushed[-1]["task"]["status"] == "done"


def test_run_task_failure_marks_failed(voice_settings, monkeypatch, registered_fake_session) -> None:
    """hermes ConnectError(runs 与降级 chat_sync 双失败) → failed + error 人话版 + 失败播报。"""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    _mock_transport(monkeypatch, handler)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_fail_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "查一下到院情况", "查询")
    fake = registered_fake_session.register(TENANT_ID, user_id)

    asyncio.run(voice_tasks._run(task_id))

    row = _get_row(task_id)
    assert row.status == "failed"
    assert "暂时不可用" in (row.error or "")
    assert row.run_ref is None  # 提交即失败,无 run_ref
    assert len(fake.announced_texts) == 1
    assert "失败" in fake.announced_texts[0] and "错误码" in fake.announced_texts[0]
    # §11:失败任务 tell 直告模式,不询问不占次数
    assert fake.announce_calls[0]["mode"] == "tell"


def test_run_task_timeout_calls_runs_stop(voice_settings, monkeypatch) -> None:
    """超时守卫: asyncio.timeout → 先 runs_stop 再 failed(§10.1.2②④)。"""
    monkeypatch.setattr(get_settings(), "realtime_voice_task_timeout", 0.3)
    stopped: list[str] = []

    async def fake_submit(hermes_session_id, message, *, session_key, instructions=None):
        return "run_slow"

    async def fake_events(run_id):
        yield {"event": "tool.started", "tool": "slow_tool"}
        await asyncio.sleep(30)

    async def fake_stop(run_id):
        stopped.append(run_id)

    monkeypatch.setattr(cockpit_runtime, "runs_submit", fake_submit)
    monkeypatch.setattr(cockpit_runtime, "runs_events", fake_events)
    monkeypatch.setattr(cockpit_runtime, "runs_stop", fake_stop)

    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_slow_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "跑一个慢任务", "其他")

    asyncio.run(voice_tasks._run(task_id))

    assert stopped == ["run_slow"]
    row = _get_row(task_id)
    assert row.status == "failed" and "超时" in (row.error or "")


def test_run_task_premature_stream_end_marks_failed(voice_settings, monkeypatch) -> None:
    """防御: 事件流提前结束(未收到终态事件)也收口,不留 running。"""

    async def fake_submit(hermes_session_id, message, *, session_key, instructions=None):
        return "run_cut"

    async def fake_events(run_id):
        yield {"event": "tool.started", "tool": "x"}
        # 流结束,无终态事件

    monkeypatch.setattr(cockpit_runtime, "runs_submit", fake_submit)
    monkeypatch.setattr(cockpit_runtime, "runs_events", fake_events)

    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_cut_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "中断任务", "其他")

    asyncio.run(voice_tasks._run(task_id))
    row = _get_row(task_id)
    assert row.status == "failed" and "中断" in (row.error or "")


# --- 启动恢复(§6.1/§10.1.4) -----------------------------------------------------------------


def test_recover_interrupted_tasks(voice_settings, monkeypatch) -> None:
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_rec_{uuid4().hex[:6]}")
    # 无 run_ref 的 running → 直接 failed("服务重启中断")
    id_plain = _submit_row(user_id, conv_id, "恢复-无run", "其他")
    # 有 run_ref 且探活 completed → 补收尾走 done 管线
    id_probe = _submit_row(user_id, conv_id, "恢复-探活", "其他")
    with SessionLocal() as db:
        for task_id, run_ref in ((id_plain, None), (id_probe, "run_done")):
            task = db.get(CockpitVoiceTask, task_id)
            task.status = "running"
            task.run_ref = run_ref
        db.commit()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/runs/run_done":
            return httpx.Response(200, json={"status": "completed", "output": "补收尾结果"})
        return httpx.Response(404, json={})

    _mock_transport(monkeypatch, handler)
    voice_tasks.recover_interrupted_tasks()

    plain = _get_row(id_plain)
    assert plain.status == "failed" and plain.error == "服务重启中断"
    probed = _get_row(id_probe)
    assert probed.status == "done" and probed.result_text == "补收尾结果"
    assert probed.result_summary == "补收尾结果"  # llm 未配置 → 截断兜底


# --- 取消(§6.1/§10.2.4) ----------------------------------------------------------------------


def test_cancel_task_http_pending_and_ownership(voice_settings) -> None:
    u1, m1 = create_user()
    u2, m2 = create_user()
    conv_id = create_mirror_row(u1, f"api_cancel_{uuid4().hex[:6]}")
    task_id = _submit_row(u1, conv_id, "生成回访话术", "话术")

    # 跨用户 404
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/cancel", headers=login(m2))
    assert r.status_code == 404
    # 归属者取消 pending → canceled
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/cancel", headers=login(m1))
    assert r.status_code == 200, r.text
    assert r.json()["data"]["status"] == "canceled"
    assert _get_row(task_id).status == "canceled"
    # 已终态再取消 → 幂等 200
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/cancel", headers=login(m1))
    assert r.status_code == 200


def test_cancel_task_http_running_calls_runs_stop(voice_settings, monkeypatch) -> None:
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"api_cstop_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "长任务", "盘点")
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        task.status = "running"
        task.run_ref = "run_c1"
        db.commit()

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"status": "stopping"})

    _mock_transport(monkeypatch, handler)
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/cancel", headers=login(mobile))
    assert r.status_code == 200, r.text
    assert seen == ["/v1/runs/run_c1/stop"]
    assert _get_row(task_id).status == "running"  # 终态由 run.cancelled 事件收尾


def test_resolve_cancel_target_branches(voice_settings) -> None:
    """§10.2.2 消歧: hint 匹配/歧义列表/空候选/兜底最新。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_res_{uuid4().hex[:6]}")
    ctx = _ctx(user_id)

    with SessionLocal() as db:
        assert voice_tasks.resolve_cancel_target(db, ctx, conv_id, "") is None  # 空候选

    id_pan = _submit_row(user_id, conv_id, "盘一下高价值客户", "盘点")
    id_hua = _submit_row(user_id, conv_id, "生成 20 人回访话术", "话术")
    with SessionLocal() as db:
        # hint 命中 1 个(task_text 包含 / kind 包含)
        hit = voice_tasks.resolve_cancel_target(db, ctx, conv_id, "盘一下高价值")
        assert isinstance(hit, CockpitVoiceTask) and hit.id == id_pan
        hit2 = voice_tasks.resolve_cancel_target(db, ctx, conv_id, "刚才的话术")
        assert isinstance(hit2, CockpitVoiceTask) and hit2.id == id_hua
        # 无 hint 多个 → 候选列表(倒序)
        multi = voice_tasks.resolve_cancel_target(db, ctx, conv_id, "")
        assert isinstance(multi, list) and [t.id for t in multi] == [id_hua, id_pan]
        # hint 无命中 → 兜底最新一个
        fallback = voice_tasks.resolve_cancel_target(db, ctx, conv_id, "八竿子打不着的词")
        assert isinstance(fallback, CockpitVoiceTask) and fallback.id == id_hua


def test_cancel_business_task_receipts(voice_settings, monkeypatch) -> None:
    """§10.2.3 边界: 正常取消/竞态已完成/重复取消幂等。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_cvoice_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "盘点任务甲", "盘点")

    receipt = asyncio.run(voice_tasks.cancel_business_task(task_id))
    assert "已取消" in receipt and f"#{task_id}" in receipt
    assert _get_row(task_id).status == "canceled"
    # 重复取消幂等
    receipt2 = asyncio.run(voice_tasks.cancel_business_task(task_id))
    assert "已经取消过了" in receipt2
    # 竞态: 已完成 → 不报错
    done_id = _submit_row(user_id, conv_id, "盘点任务乙", "盘点")
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, done_id)
        task.status = "done"
        db.commit()
    receipt3 = asyncio.run(voice_tasks.cancel_business_task(done_id))
    assert "已经完成了" in receipt3


def test_cancel_running_via_voice_calls_stop(voice_settings, monkeypatch) -> None:
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_crun_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "跑动中的任务", "其他")
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        task.status = "running"
        task.run_ref = "run_v1"
        db.commit()
    stopped: list[str] = []

    async def fake_stop(run_id):
        stopped.append(run_id)

    monkeypatch.setattr(cockpit_runtime, "runs_stop", fake_stop)
    receipt = asyncio.run(voice_tasks.cancel_business_task(task_id))
    assert stopped == ["run_v1"]
    assert "已取消" in receipt


# --- 任务卡列表归属(§6.1) --------------------------------------------------------------------


def test_voice_tasks_list_ownership(voice_settings) -> None:
    u1, m1 = create_user()
    u2, m2 = create_user()
    conv1 = create_mirror_row(u1, f"api_l1_{uuid4().hex[:6]}")
    conv2 = create_mirror_row(u2, f"api_l2_{uuid4().hex[:6]}")
    _submit_row(u1, conv1, "第一条", "盘点")
    _submit_row(u1, conv1, "第二条", "话术")

    # 跨用户会话 404
    r = client.get(f"/api/v1/cockpit/voice-tasks?conversation_id={conv1}", headers=login(m2))
    assert r.status_code == 404
    # 归属者按 created_at 升序拿到两条
    r = client.get(f"/api/v1/cockpit/voice-tasks?conversation_id={conv1}", headers=login(m1))
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    assert [row["taskText"] for row in data] == ["第一条", "第二条"]
    assert data[0]["status"] == "pending"
    # 他人会话自己的列表为空
    r = client.get(f"/api/v1/cockpit/voice-tasks?conversation_id={conv2}", headers=login(m2))
    assert r.json()["data"] == []


# --- steer / approval 端点(§10.1.2④) ---------------------------------------------------------


def test_steer_endpoint(voice_settings, monkeypatch) -> None:
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"api_steer_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "盘点客户", "盘点")
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        task.status = "running"
        task.run_ref = "run_s1"
        db.commit()
    headers = login(mobile)

    _mock_transport(monkeypatch, lambda request: httpx.Response(200, json={"status": "ok"}))
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/steer", json={"text": "只看 90 天内的"}, headers=headers)
    assert r.status_code == 200 and r.json()["data"]["steered"] is True

    _mock_transport(monkeypatch, lambda request: httpx.Response(409, json={}))
    r = client.post(f"/api/v1/cockpit/voice-tasks/{task_id}/steer", json={"text": "x"}, headers=headers)
    assert r.status_code == 409  # 已不可引导


def test_steer_endpoint_not_running_409(voice_settings) -> None:
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"api_steer2_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "pending 任务", "其他")
    r = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/steer", json={"text": "x"}, headers=login(mobile)
    )
    assert r.status_code == 409


def test_approval_endpoint(voice_settings, monkeypatch) -> None:
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"api_appr_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "危险操作", "其他")
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        task.status = "waiting_approval"
        task.run_ref = "run_a1"
        db.commit()
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"status": "ok"})

    _mock_transport(monkeypatch, handler)
    r = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/approval", json={"choice": "once"}, headers=login(mobile)
    )
    assert r.status_code == 200, r.text
    assert seen == [{"choice": "once"}]
    assert _get_row(task_id).status == "running"  # 批准后回到 running
    # 非法 choice → 422
    r = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/approval", json={"choice": "hack"}, headers=login(mobile)
    )
    assert r.status_code == 422


# --- ask_business_agent 快查(§10.3.4) ----------------------------------------------------------


def test_ask_business_agent_direct_answer(voice_settings, monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/chat")
        return httpx.Response(
            200, json={"message": {"role": "assistant", "content": "张三最近一次到院是 8 月 30 日"}}
        )

    _mock_transport(monkeypatch, handler)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_ask_{uuid4().hex[:6]}")
    answer = asyncio.run(voice_tasks.ask_business_agent(_ctx(user_id), conv_id, "张三最近什么时候来过"))
    assert answer == "张三最近一次到院是 8 月 30 日"
    # 直通不落账本
    with SessionLocal() as db:
        rows = db.scalars(select(CockpitVoiceTask).where(CockpitVoiceTask.conversation_id == conv_id)).all()
        assert rows == []


def test_ask_business_agent_timeout_converts_to_async(voice_settings, monkeypatch) -> None:
    """超时自动转异步: 账本多一行(via=ask) + 回执转文案。"""
    monkeypatch.setattr(get_settings(), "realtime_voice_quick_timeout", 0.05)
    dispatched: list[int] = []

    async def slow_chat(*args, **kwargs):
        await asyncio.sleep(30)
        return "不该到达"

    monkeypatch.setattr(cockpit_runtime, "chat_sync", slow_chat)
    monkeypatch.setattr(voice_tasks, "dispatch_task", lambda tid: dispatched.append(tid))

    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_askt_{uuid4().hex[:6]}")
    answer = asyncio.run(voice_tasks.ask_business_agent(_ctx(user_id), conv_id, "查一个慢查询"))
    assert "已转后台处理" in answer
    assert len(dispatched) == 1
    row = _get_row(dispatched[0])
    assert row.status == "pending" and row.kind == "查询"
    assert row.progress[0]["step"] == "route" and row.progress[0]["via"] == "ask"


def test_ask_business_agent_failure_receipt(voice_settings, monkeypatch) -> None:
    async def broken_chat(*args, **kwargs):
        raise CockpitRuntimeUnavailable("down")

    monkeypatch.setattr(cockpit_runtime, "chat_sync", broken_chat)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_askf_{uuid4().hex[:6]}")
    answer = asyncio.run(voice_tasks.ask_business_agent(_ctx(user_id), conv_id, "查一下"))
    assert answer.startswith("查询失败")


# --- ask 通道写操作防线(voice-write-ledger-fix) ------------------------------------------------


def test_chat_sync_instructions_passthrough(voice_settings, monkeypatch) -> None:
    """chat_sync instructions 透传 hermes system_message(api_server.py:4617)。"""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"message": {"role": "assistant", "content": "好"}})

    _mock_transport(monkeypatch, handler)
    asyncio.run(cockpit_runtime.chat_sync("api_x1", "查一下", session_key="t1:u7", instructions="只读"))
    assert seen["body"] == {"message": "查一下", "system_message": "只读"}


def test_chat_sync_no_instructions_omits_system_message(voice_settings, monkeypatch) -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"message": {"role": "assistant", "content": "好"}})

    _mock_transport(monkeypatch, handler)
    asyncio.run(cockpit_runtime.chat_sync("api_x1", "查一下", session_key="t1:u7"))
    assert seen["body"] == {"message": "查一下"}


def test_ask_write_intent_keyword_match() -> None:
    """写意图关键词:发送/建任务/建会话/触达/草稿命中;纯查询不命中。"""
    for query in ("给Pepo发条消息", "帮我创建任务回访", "开一个新会话盘客户", "群发中秋活动", "生成触达草稿"):
        assert voice_tasks._ask_write_intent(query), query
    for query in ("张三最近什么时候来过", "查一下15天到院情况", "Pepo的基本情况"):
        assert not voice_tasks._ask_write_intent(query), query


def test_ask_business_agent_write_intent_hard_redirect(voice_settings, monkeypatch) -> None:
    """写操作错投 ask → 硬转回执:不调 chat_sync、不落账本、引导 submit。"""
    async def forbidden_chat(*args, **kwargs):
        raise AssertionError("写意图不应进入 chat_sync")

    monkeypatch.setattr(cockpit_runtime, "chat_sync", forbidden_chat)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_askw_{uuid4().hex[:6]}")
    answer = asyncio.run(voice_tasks.ask_business_agent(_ctx(user_id), conv_id, "给Pepo发条消息说晚上好"))
    assert "submit_business_task" in answer
    with SessionLocal() as db:
        rows = db.scalars(select(CockpitVoiceTask).where(CockpitVoiceTask.conversation_id == conv_id)).all()
        assert rows == []


def test_ask_business_agent_readonly_instructions_injected(voice_settings, monkeypatch) -> None:
    """正常快查注入 ASK_READONLY_INSTRUCTIONS 只读纪律。"""
    seen: dict = {}

    async def fake_chat(hermes_session_id, message, *, session_key, instructions=None):
        seen["instructions"] = instructions
        return "张三最近一次到院是 8 月 30 日"

    monkeypatch.setattr(cockpit_runtime, "chat_sync", fake_chat)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"api_askr_{uuid4().hex[:6]}")
    answer = asyncio.run(voice_tasks.ask_business_agent(_ctx(user_id), conv_id, "张三最近什么时候来过"))
    assert answer == "张三最近一次到院是 8 月 30 日"
    assert seen["instructions"] == voice_tasks.ASK_READONLY_INSTRUCTIONS


# --- §11 静默闸门 v3.3: 调度器(真实 DashScopeVoiceSession + 假发送层) --------------------


def _make_session(monkeypatch):
    """真实 DashScopeVoiceSession:_send 捕获 payload,transition 记录落库回调,不连阿里。"""
    events: list[dict] = []

    async def on_event(ev):
        events.append(ev)

    session = cockpit_voice.DashScopeVoiceSession(instructions="test", on_event=on_event)
    sent: list[dict] = []

    async def fake_send(payload):
        sent.append(payload)

    monkeypatch.setattr(session, "_send", fake_send)
    log: list[tuple] = []

    async def transition(task_ids, state, bump):
        log.append((tuple(sorted(task_ids)), state, bump))

    session.on_announce_transition = transition
    return session, sent, log


@pytest.fixture
def announce_slow(monkeypatch):
    """把静默窗口拉到 60s+,定时器在用例内绝不触发(测摘队/结算路径)。"""
    s = get_settings()
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_min_s", 60.0)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_max_s", 61.0)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_min_s", 60.0)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_max_s", 61.0)
    return s


@pytest.fixture
def announce_fast(monkeypatch):
    """静默窗口压到 0.05~0.06s,定时器立刻可触发(测静默追问路径)。"""
    s = get_settings()
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_min_s", 0.05)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_max_s", 0.06)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_min_s", 0.05)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_max_s", 0.06)
    return s


def _fire_instructions(sent: list[dict]) -> list[str]:
    """v3.4: 播报指令经 response.create 的 response.instructions 下发(不再注入对话流)。"""
    out = []
    for p in sent:
        if p.get("type") != "response.create":
            continue
        instr = (p.get("response") or {}).get("instructions")
        if instr:
            out.append(instr)
    return out


def test_announce_interrupted_on_cancel_after_fire(monkeypatch, announce_fast) -> None:
    """播报指令发出后被打断: response.cancelled + ack → interrupted,不立刻重排。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="对了")
        await asyncio.sleep(0.25)
        assert len(_fire_instructions(sent)) == 1
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.cancelled"})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "interrupted", False)
        assert 1 not in session.announce_queue
        assert session._attempts.get(1, 0) == 1

    asyncio.run(scenario())


def test_announce_interrupted_on_done_cancelled_status(monkeypatch, announce_fast) -> None:
    """response.done 带 status=cancelled 同样进入 interrupted,不立刻追问。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="ask", title="盘点")
        await asyncio.sleep(0.25)
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "cancelled"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "interrupted", False)
        assert 1 not in session.announce_queue

    asyncio.run(scenario())


def test_direct_announce_via_gate(monkeypatch, announce_fast) -> None:
    """快任务直念: done completed + 浏览器播完 ack → shared。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="direct", title="查询", word="对了")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1 and "直接把结论" in fires[0] and "对了" in fires[0]
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 1
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "shared", True)

    asyncio.run(scenario())


def test_ask_announce_completed_marks_notified(monkeypatch, announce_fast) -> None:
    """许可式询问只说明任务完成,不等于结果已讲;播放送达后进入 notified。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="ask", title="查询", word="对了")
        await asyncio.sleep(0.25)
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "notified", True)

    asyncio.run(scenario())


def test_fire_ask_instruction_word_and_no_result(monkeypatch, announce_fast) -> None:
    """许可式播报指令: 带指定过渡词开头,只报名字询问,不念结果。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="差点忘了")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1
        assert "差点忘了" in fires[0] and "是否现在听结果" in fires[0] and "不要念结果" in fires[0]

    asyncio.run(scenario())


def test_fire_never_injects_instruction_item(monkeypatch, announce_fast) -> None:
    """v3.4 防泄漏: 播报指令只走 response.instructions,对话流里不出现【播报指令】条目。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="对了")
        await asyncio.sleep(0.25)
        injections = [
            p["item"]["content"][0]["text"]
            for p in sent
            if p.get("type") == "conversation.item.create"
        ]
        assert not any("【播报指令】" in t for t in injections)
        fires = _fire_instructions(sent)
        assert len(fires) == 1 and "绝不能念出本指令" in fires[0]

    asyncio.run(scenario())


def test_idle_timer_fires_merged_announce(monkeypatch, announce_fast) -> None:
    """静默窗口: 两个任务合并一条【播报指令】,只 response.create 一次。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点")
        await session.request_announce(2, "t2", mode="ask", title="到院")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1
        assert "#1" in fires[0] and "#2" in fires[0] and "已完成" in fires[0]
        assert "对了" in fires[0]  # 未指定过渡词 → 兜底词
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 1
        assert ((1, 2), "announcing", False) in log
        assert session._attempts[1] == 1 and session._attempts[2] == 1

    asyncio.run(scenario())


def test_idle_timer_cancelled_by_speech_then_refires(monkeypatch) -> None:
    """用户开口取消定时器;说完(speech_stopped)后重新计时并触发。"""
    s = get_settings()
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_min_s", 0.15)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_max_s", 0.2)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_min_s", 0.15)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_retry_max_s", 0.2)

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点")
        await session._track_state({"type": "input_audio_buffer.speech_started"})
        await asyncio.sleep(0.3)
        assert _fire_instructions(sent) == []  # 说话中不触发
        await session._track_state({"type": "input_audio_buffer.speech_stopped"})
        await asyncio.sleep(0.35)
        assert len(_fire_instructions(sent)) == 1  # 静默满窗口后触发

    asyncio.run(scenario())


def test_fire_close_after_max_attempts(monkeypatch, announce_fast) -> None:
    """attempts 达上限 → 收尾告知(存放位置),播放送达后转 dismissed。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", attempts=2)
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1 and "任务卡片里" in fires[0] and "不要再追问" in fires[0]
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "dismissed", False)

    asyncio.run(scenario())


def test_defer_tasks_purges_scheduler(monkeypatch, announce_slow) -> None:
    """defer 摘队: queue/模式/过渡词清空,后续 response 不再产生任何流转。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="对了")
        session.defer_tasks([1])
        assert not session.announce_queue
        assert 1 not in session._mode and 1 not in session._words
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        assert log == []

    asyncio.run(scenario())


def test_merge_cap_overflow_mention(monkeypatch, announce_fast) -> None:
    """合并询问封顶 merge_max=3:5 个任务只点名 3 个,其余归并'另外还有 2 个'。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        for tid in range(1, 6):
            await session.request_announce(tid, f"t{tid}", mode="ask", title=f"任务{tid}")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1
        assert "#3" in fires[0] and "#4" not in fires[0]
        assert "另外还有 2 个任务" in fires[0]

    asyncio.run(scenario())


# --- v3.7 播报多样性: 过渡词池去重 + 句式变体轮换 ------------------------------------------


def test_pick_word_exclude() -> None:
    """v3.7: _pick_word 支持 exclude 近期去重,全被排除时退回全池;_repick_word 池内换词。"""
    assert voice_tasks._pick_word(None) in voice_tasks._STALE_WORDS  # finished_at=None → 旧词档
    for _ in range(20):
        word = voice_tasks._pick_word(None, exclude=("话说回来",))
        assert word in voice_tasks._STALE_WORDS and word != "话说回来"
    # 全被排除 → 退回全池,永远有词
    assert voice_tasks._pick_word(None, exclude=voice_tasks._STALE_WORDS) in voice_tasks._STALE_WORDS
    # _repick_word 在词所属档位池内换词(避开近期用过的)
    assert voice_tasks._repick_word("对了", ("对了", "刚刚", "差点忘了")) not in ("对了", "刚刚", "差点忘了")
    assert voice_tasks._repick_word("对了", voice_tasks._FRESH_WORDS) == "对了"  # 全池排除 → 原样


def test_fire_variants_rotate_and_words_dedup(monkeypatch, announce_fast) -> None:
    """v3.7: 连续两次 ask 播报 → 句式变体轮换(0→1)、过渡词去重、带上次开头词提示。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1
        assert "逐个报名字" in fires[0]  # 首次 = 变体 0 = 原模板
        assert "每次都要不一样" in fires[0]  # 指令带多样性要求
        assert "最近一次播报" not in fires[0]  # 首播无上次开头词提示
        assert session._recent_words and session._last_variant["ask"] == 0
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        await session.request_announce(2, "t2", mode="ask", title="到院")
        await asyncio.sleep(0.25)
        fires2 = _fire_instructions(sent)
        assert len(fires2) == 2
        assert fires2[1] != fires[0]  # 变体+开头词都换了,整条指令不同
        assert "告诉用户之前交代的事已经出结果了" in fires2[1]  # 变体 1
        assert "最近一次播报是用「对了」开的头" in fires2[1]  # 上次开头词提示
        assert session._last_variant["ask"] == 1

    asyncio.run(scenario())


def test_fire_avoids_recent_word(monkeypatch, announce_fast) -> None:
    """v3.7: 指定过渡词在本会话近期用过 → 池内换词,不再用同一开头。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        session._recent_words.append("差点忘了")  # 模拟上次播报用过
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="差点忘了")
        await asyncio.sleep(0.25)
        fires = _fire_instructions(sent)
        assert len(fires) == 1
        assert "“差点忘了”开头" not in fires[0]  # 开头词已换(提示句里的引用不算)
        assert list(session._recent_words)[-1] != "差点忘了"

    asyncio.run(scenario())


# --- §11 静默闸门 v3.3: 账本侧(voice_tasks + DB) ------------------------------------------


def _done_row(
    user_id: int,
    conv_id: int,
    text: str = "盘点高价值客户",
    *,
    status: str = "done",
    announce_state: str = "pending",
    attempts: int = 0,
    error: str | None = None,
    duration_s: float = 0.0,
) -> int:
    finished = datetime.utcnow()
    with SessionLocal() as db:
        task = CockpitVoiceTask(
            tenant_id=TENANT_ID,
            user_id=user_id,
            conversation_id=conv_id,
            kind="盘点",
            task_text=text,
            status=status,
            announced=False,
            announce_state=announce_state,
            announce_attempts=attempts,
            result_summary="共 8 位高价值客户" if status == "done" else None,
            result_text="明细略" if status == "done" else None,
            error=error,
            progress=[{"step": "route", "via": "submit"}],
            created_at=finished - timedelta(seconds=duration_s),
            finished_at=finished,
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        return task.id


def test_announce_task_no_session_keeps_pending(voice_settings) -> None:
    """不在通话: 不注入,announced=False,announce_state 保持 pending 等补注。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_nosess_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    asyncio.run(voice_tasks.announce_task(task_id))
    row = _get_row(task_id)
    assert row.announced is False and row.announce_state == "pending"


def test_announce_task_skips_non_pending(voice_settings, registered_fake_session) -> None:
    """announce_state≠pending(已播报/已压下)→ announce_task 幂等跳过。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_skip_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id, announce_state="consumed")
    fake = registered_fake_session.register(TENANT_ID, user_id)
    asyncio.run(voice_tasks.announce_task(task_id))
    assert fake.announce_calls == [] and fake.announced_texts == []


def test_announce_task_mode_not_affected_by_responding(voice_settings, registered_fake_session) -> None:
    """v3.3: 会话忙(responding)不影响 mode 判定——开口时机全部交给会话静默闸门。

    快任务(执行 ≤15s)即使会话忙也登记 mode="direct";慢任务登记 mode="ask"。
    """
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_busy_{uuid4().hex[:6]}")
    fast_id = _done_row(user_id, conv_id, "快查任务", duration_s=2.0)
    slow_id = _done_row(user_id, conv_id, "盘点慢任务", duration_s=60.0)
    fake = registered_fake_session.register(TENANT_ID, user_id)
    fake.responding = True
    asyncio.run(voice_tasks.announce_task(fast_id))
    asyncio.run(voice_tasks.announce_task(slow_id))
    by_id = {c["task_id"]: c for c in fake.announce_calls}
    assert by_id[fast_id]["mode"] == "direct"
    assert by_id[slow_id]["mode"] == "ask"
    assert _get_row(fast_id).announced is True


def test_announce_task_word_passed_to_session(voice_settings, registered_fake_session) -> None:
    """过渡词不进注入文本(备查勿主动提),经 word 参数交给会话写进【播报指令】。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_word_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    fake = registered_fake_session.register(TENANT_ID, user_id)
    asyncio.run(voice_tasks.announce_task(task_id))
    row = _get_row(task_id)
    assert row.announced is True
    assert fake.announce_calls[0]["word"] in voice_tasks._FRESH_WORDS  # 刚完成 → 新鲜档
    text = voice_tasks._announce_text(row)
    assert "备查条目" in text and "不要主动提起" in text and "播报指令" in text


def test_reinject_dual_templates(voice_settings, registered_fake_session) -> None:
    """建连补注: pending 标准模板重排队;deferred 静默模板 queue=False 仅上下文。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_rein_{uuid4().hex[:6]}")
    pending_id = _done_row(user_id, conv_id, "盘点客户")
    deferred_id = _done_row(user_id, conv_id, "查询到院", announce_state="deferred")
    fake = registered_fake_session.register(TENANT_ID, user_id)
    asyncio.run(voice_tasks.reinject_on_connect(fake, conv_id))
    assert len(fake.announce_calls) == 2
    by_id = {c["task_id"]: c for c in fake.announce_calls}
    assert by_id[pending_id].get("queue", True) is True
    assert by_id[deferred_id].get("queue") is False
    deferred_text = fake.announced_texts[1]
    assert "不要主动提起" in deferred_text
    assert _get_row(pending_id).announced is True
    assert _get_row(deferred_id).announced is True


def test_defer_task_result_single(voice_settings, registered_fake_session) -> None:
    """单候选: 回执确认 + announce_state→deferred + task.updated 推送。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_def1_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    holder = registered_fake_session.register(TENANT_ID, user_id)
    with SessionLocal() as db:
        receipt, ids = asyncio.run(voice_tasks.defer_task_result(db, _ctx(user_id), conv_id, "盘点"))
    assert ids == [task_id] and "不主动提" in receipt
    row = _get_row(task_id)
    assert row.announce_state == "deferred"
    updated = [p for p in registered_fake_session.pushed if p["type"] == "task.updated"]
    assert updated and updated[-1]["task"]["announceState"] == "deferred"


def test_defer_task_result_none(voice_settings) -> None:
    """无候选: 回执'没有待告知结果的任务',ids 空。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_def0_{uuid4().hex[:6]}")
    with SessionLocal() as db:
        receipt, ids = asyncio.run(voice_tasks.defer_task_result(db, _ctx(user_id), conv_id, ""))
    assert "没有待告知" in receipt and ids == []


def test_defer_task_result_empty_hint_defers_all(voice_settings) -> None:
    """空 hint('都先别讲')→ 全部候选压下。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_defall_{uuid4().hex[:6]}")
    id1 = _done_row(user_id, conv_id, "盘点客户")
    id2 = _done_row(user_id, conv_id, "查询到院")
    with SessionLocal() as db:
        receipt, ids = asyncio.run(voice_tasks.defer_task_result(db, _ctx(user_id), conv_id, ""))
    assert sorted(ids) == sorted([id1, id2])
    assert "都先不主动提" in receipt
    assert _get_row(id1).announce_state == "deferred"
    assert _get_row(id2).announce_state == "deferred"


def test_apply_announce_transition_guard_and_bump(voice_settings) -> None:
    """落库回调: 终态不被调度器翻回;活跃态可转 shared 并累加 attempts。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_trans_{uuid4().hex[:6]}")
    deferred_id = _done_row(user_id, conv_id, "稍后任务", announce_state="deferred")
    active_id = _done_row(user_id, conv_id, "通知中任务", announce_state="announcing")
    asyncio.run(voice_tasks.apply_announce_transition([deferred_id], "pending", False))
    assert _get_row(deferred_id).announce_state == "deferred"
    asyncio.run(voice_tasks.apply_announce_transition([active_id], "shared", True))
    row = _get_row(active_id)
    assert row.announce_state == "shared" and row.announce_attempts == 1


def test_recover_resets_announcing_to_interrupted(voice_settings) -> None:
    """启动恢复: announcing(播报死在半路)→ interrupted,重连后不立刻复读。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"an_rec_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id, announce_state="announcing")
    voice_tasks.recover_interrupted_tasks()
    assert _get_row(task_id).announce_state == "interrupted"


# --- 账本事件总线 + mark_result_shared(方案 rictor-starfire-orphan §A/§B) ------------------


def _cockpit_events(task_id: int, event_type: str | None = None) -> list[RealtimeEvent]:
    with SessionLocal() as db:
        q = select(RealtimeEvent).where(
            RealtimeEvent.topic == "cockpit",
            RealtimeEvent.resource_id == str(task_id),
        )
        if event_type is not None:
            q = q.where(RealtimeEvent.event_type == event_type)
        return list(db.scalars(q).all())


def _delete_cockpit_events(*task_ids: int) -> None:
    with SessionLocal() as db:
        for tid in task_ids:
            rows = db.scalars(
                select(RealtimeEvent).where(
                    RealtimeEvent.topic == "cockpit",
                    RealtimeEvent.resource_id == str(tid),
                )
            ).all()
            for row in rows:
                db.delete(row)
        db.commit()


def test_submit_task_emits_created_event(voice_settings) -> None:
    """受理即广播 cockpit.voice_task.created(topic=cockpit,文字模式卡片即时出现)。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_create_{uuid4().hex[:6]}")
    task_id = _submit_row(user_id, conv_id, "盘点最近客户")
    try:
        events = _cockpit_events(task_id, "cockpit.voice_task.created")
        assert len(events) == 1
        payload = events[0].payload["task"]
        assert payload["id"] == task_id and payload["status"] == "pending"
        assert payload["conversationId"] == conv_id
    finally:
        _delete_cockpit_events(task_id)


def test_finalize_done_emits_updated_event(voice_settings, monkeypatch) -> None:
    """终态落库同时广播 cockpit.voice_task.updated(账本即时翻新)。"""
    async def _fake_summary(text: str) -> str:
        return "摘要"

    monkeypatch.setattr(voice_tasks, "summarize_result", _fake_summary)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_fin_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id, status="running")
    try:
        asyncio.run(voice_tasks._finalize_done(task_id, "输出"))
        events = _cockpit_events(task_id, "cockpit.voice_task.updated")
        assert len(events) == 1
        assert events[0].payload["task"]["status"] == "done"
    finally:
        _delete_cockpit_events(task_id)


def test_consume_task_result_marks_shared(voice_settings, registered_fake_session) -> None:
    """mark_result_shared 落库: pending→shared + task.updated 推送 + 事件广播;幂等。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_cons_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    registered_fake_session.register(TENANT_ID, user_id)
    try:
        with SessionLocal() as db:
            task = asyncio.run(voice_tasks.consume_task_result(db, _ctx(user_id), conv_id, task_id))
        assert task is not None
        assert _get_row(task_id).announce_state == "shared"
        updated = [p for p in registered_fake_session.pushed if p["type"] == "task.updated"]
        assert updated and updated[-1]["task"]["announceState"] == "shared"
        assert len(_cockpit_events(task_id, "cockpit.voice_task.updated")) == 1
        with SessionLocal() as db:
            again = asyncio.run(voice_tasks.consume_task_result(db, _ctx(user_id), conv_id, task_id))
        assert again is None
        assert len(_cockpit_events(task_id, "cockpit.voice_task.updated")) == 1
    finally:
        _delete_cockpit_events(task_id)


def test_consume_task_result_ownership_guard(voice_settings) -> None:
    """归属校验: 别的会话/别人的任务不消费,状态不变。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_own_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    other_conv = create_mirror_row(user_id, f"ev_own2_{uuid4().hex[:6]}")
    with SessionLocal() as db:
        assert asyncio.run(voice_tasks.consume_task_result(db, _ctx(user_id), other_conv, task_id)) is None
    assert _get_row(task_id).announce_state == "pending"


def test_consume_deferred_result_can_be_marked_shared_later(voice_settings) -> None:
    """用户先说稍后,之后主动问起并听完结果时,deferred 可更新为 shared。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_def_share_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id, announce_state="deferred")
    with SessionLocal() as db:
        task = asyncio.run(voice_tasks.consume_task_result(db, _ctx(user_id), conv_id, task_id))
    assert task is not None
    assert _get_row(task_id).announce_state == "shared"


def test_reinject_stale_pending_goes_dismissed(voice_settings, registered_fake_session) -> None:
    """超 24h 的陈旧 pending: 静默模板 queue=False + 落库 dismissed。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_stale_{uuid4().hex[:6]}")
    fresh_id = _done_row(user_id, conv_id, "新鲜任务")
    stale_id = _done_row(user_id, conv_id, "陈旧任务")
    with SessionLocal() as db:
        row = db.get(CockpitVoiceTask, stale_id)
        row.finished_at = datetime.utcnow() - timedelta(hours=25)
        db.commit()
    fake = registered_fake_session.register(TENANT_ID, user_id)
    asyncio.run(voice_tasks.reinject_on_connect(fake, conv_id))
    by_id = {c["task_id"]: c for c in fake.announce_calls}
    assert by_id[fresh_id].get("queue", True) is True
    assert by_id[stale_id].get("queue") is False
    assert _get_row(stale_id).announce_state == "dismissed"
    assert _get_row(fresh_id).announce_state == "queued"


def test_reinject_interrupted_is_silent_context_only(voice_settings, registered_fake_session) -> None:
    """被用户打断的任务重连时只补上下文,不主动再次追问。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_interrupt_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id, "打断任务", announce_state="interrupted")
    fake = registered_fake_session.register(TENANT_ID, user_id)
    asyncio.run(voice_tasks.reinject_on_connect(fake, conv_id))
    by_id = {c["task_id"]: c for c in fake.announce_calls}
    assert by_id[task_id].get("queue") is False
    assert _get_row(task_id).announce_state == "interrupted"


def test_handle_mark_result_shared(voice_settings, registered_fake_session) -> None:
    """路由处理器: 落库 consumed + 摘调度队列 + 静默回执;缺编号回执提示。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_mark_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    fake = registered_fake_session.register(TENANT_ID, user_id)
    try:
        asyncio.run(
            cockpit_voice_routes._handle_mark_result_shared(
                fake, _ctx(user_id), conv_id, "call_1", {"task_id": task_id}
            )
        )
        assert _get_row(task_id).announce_state == "shared"
        assert fake.consumed_ids == [task_id]
        assert fake.function_outputs[-1] == "（已记录,无需开口）"
        asyncio.run(
            cockpit_voice_routes._handle_mark_result_shared(
                fake, _ctx(user_id), conv_id, "call_2", {"task_id": "x"}
            )
        )
        assert "缺少任务编号" in fake.function_outputs[-1]
    finally:
        _delete_cockpit_events(task_id)


def test_handle_mark_result_shared_batch_ids(voice_settings, registered_fake_session) -> None:
    """mark_result_shared 支持 task_ids 批量消费,用于一次讲完多个完成任务。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_mark_batch_{uuid4().hex[:6]}")
    id1 = _done_row(user_id, conv_id, "盘点客户", announce_state="notified")
    id2 = _done_row(user_id, conv_id, "查询到院", announce_state="notified")
    fake = registered_fake_session.register(TENANT_ID, user_id)
    try:
        asyncio.run(
            cockpit_voice_routes._handle_mark_result_shared(
                fake, _ctx(user_id), conv_id, "call_b", {"task_ids": [id1, id2]}
            )
        )
        assert _get_row(id1).announce_state == "shared"
        assert _get_row(id2).announce_state == "shared"
        assert sorted(fake.consumed_ids) == sorted([id1, id2])
    finally:
        _delete_cockpit_events(id1, id2)


def test_dispatch_mark_result_shared_exempts_followup(voice_settings, registered_fake_session) -> None:
    """分发闭环: mark_result_shared 豁免 followup(回执静默,防'已记录'被念出来)。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"ev_disp_{uuid4().hex[:6]}")
    task_id = _done_row(user_id, conv_id)
    fake = registered_fake_session.register(TENANT_ID, user_id)
    event = {"name": "mark_result_shared", "call_id": "c1", "arguments": json.dumps({"task_id": task_id})}
    try:
        asyncio.run(cockpit_voice_routes._dispatch_function_call(fake, _ctx(user_id), conv_id, event))
        assert fake.followup_count == 0
        assert _get_row(task_id).announce_state == "shared"
    finally:
        _delete_cockpit_events(task_id)


# --- §11 静默闸门 v3.3: followup 跟随发言 + 闸门时序 + 路由接线 -------------------------------


def _followup_fast(monkeypatch) -> None:
    """回执跟随窗口压到 0.05~0.06s,用例内可立即触发。"""
    s = get_settings()
    monkeypatch.setattr(s, "realtime_voice_followup_delay_min_s", 0.05)
    monkeypatch.setattr(s, "realtime_voice_followup_delay_max_s", 0.06)


def test_followup_fires_after_idle_window(monkeypatch) -> None:
    """P1 修复核心: schedule_followup 后静默窗口到点 → response.create 触发模型说出回执。"""
    _followup_fast(monkeypatch)

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        session.schedule_followup()
        assert session._followup_pending is True
        await asyncio.sleep(0.2)
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 1
        assert creates[0]["response"]["instructions"] == cockpit_voice._FOLLOWUP_HINT
        assert session._followup_pending is False

    asyncio.run(scenario())


def test_followup_cancelled_by_speech_started(monkeypatch) -> None:
    """回执等待中用户开口 → followup 让位取消,不再自动开口(回执已在上下文)。"""

    async def scenario() -> None:
        s = get_settings()
        monkeypatch.setattr(s, "realtime_voice_followup_delay_min_s", 0.1)
        monkeypatch.setattr(s, "realtime_voice_followup_delay_max_s", 0.15)
        session, sent, log = _make_session(monkeypatch)
        session.schedule_followup()
        await session._track_state({"type": "input_audio_buffer.speech_started"})
        assert session._followup_pending is False
        await asyncio.sleep(0.3)
        assert [p for p in sent if p.get("type") == "response.create"] == []

    asyncio.run(scenario())


def test_followup_cancelled_by_response_created(monkeypatch) -> None:
    """回执等待中模型已起答(用户驱动) → followup 让位取消。"""

    async def scenario() -> None:
        s = get_settings()
        monkeypatch.setattr(s, "realtime_voice_followup_delay_min_s", 0.1)
        monkeypatch.setattr(s, "realtime_voice_followup_delay_max_s", 0.15)
        session, sent, log = _make_session(monkeypatch)
        session.schedule_followup()
        await session._track_state({"type": "response.created"})
        assert session._followup_pending is False
        await asyncio.sleep(0.3)
        assert [p for p in sent if p.get("type") == "response.create"] == []

    asyncio.run(scenario())


def test_followup_priority_over_announce(monkeypatch) -> None:
    """followup 与播报同等待: 回执先开口;播报等回执说完后的下一拍(两句话隔开)。"""
    _followup_fast(monkeypatch)
    s = get_settings()
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_min_s", 0.05)
    monkeypatch.setattr(s, "realtime_voice_announce_idle_first_max_s", 0.06)

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="对了")
        session.schedule_followup()  # 后登记的回执优先于先到的播报
        # 轮询等 followup 触发,然后模拟服务端立即 response.created(模型开始说回执)
        for _ in range(100):
            await asyncio.sleep(0.01)
            if any(p.get("type") == "response.create" for p in sent):
                break
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 1 and creates[0]["response"]["instructions"] == cockpit_voice._FOLLOWUP_HINT
        await session._track_state({"type": "response.created"})
        await asyncio.sleep(0.15)  # 回执还在说:播报不得抢话
        assert not any("绝不能念出本指令" in i for i in _fire_instructions(sent))
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await asyncio.sleep(0.15)  # 说完后静默满窗口:播报单独起拍
        fires = [i for i in _fire_instructions(sent) if "绝不能念出本指令" in i]
        assert len(fires) == 1 and "对了" in fires[0]
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 2

    asyncio.run(scenario())


def test_announce_waits_for_idle_gate(monkeypatch) -> None:
    """P2 修复核心: 模型说话中任务完成 → 绝不立即播报;说完后静默满窗口才单独起拍。"""

    async def scenario() -> None:
        s = get_settings()
        monkeypatch.setattr(s, "realtime_voice_announce_idle_first_min_s", 0.1)
        monkeypatch.setattr(s, "realtime_voice_announce_idle_first_max_s", 0.15)
        session, sent, log = _make_session(monkeypatch)
        await session._track_state({"type": "response.created"})  # 模型正在说话
        await session.request_announce(1, "t1", mode="ask", title="盘点", word="对了")
        await asyncio.sleep(0.3)  # 远超窗口:说话中绝不触发
        assert _fire_instructions(sent) == []
        assert [p for p in sent if p.get("type") == "response.create"] == []
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await asyncio.sleep(0.04)  # 窗口未满,仍不触发
        assert _fire_instructions(sent) == []
        await asyncio.sleep(0.25)
        assert len(_fire_instructions(sent)) == 1  # 静默满窗口,单独起拍

    asyncio.run(scenario())


def test_dispatch_submit_schedules_followup(voice_settings, monkeypatch) -> None:
    """路由接线: 函数分发后统一登记跟随发言(受理回执经静默闸门开口)。"""
    monkeypatch.setattr(voice_tasks, "dispatch_task", lambda task_id: None)
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"disp_{uuid4().hex[:6]}")
    fake = FakeVoiceSession()
    event = {
        "name": "submit_business_task",
        "call_id": "c1",
        "arguments": json.dumps({"task": "盘一下高价值客户", "kind": "盘点"}),
    }
    asyncio.run(cockpit_voice_routes._dispatch_function_call(fake, _ctx(user_id), conv_id, event))
    receipt = next((o for o in fake.function_outputs if "已受理" in o), "")
    assert receipt, "受理回执缺失"
    # v3.4: 回执附开口指引,邀约话术来自随机池
    assert "开口指引" in receipt
    assert any(p in receipt for p in cockpit_voice.CONTINUE_PHRASES)
    assert fake.followup_count == 1


def test_dispatch_unknown_function_also_followup(voice_settings) -> None:
    """未知函数/兜底回执同样登记跟随发言。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"disp_unk_{uuid4().hex[:6]}")
    fake = FakeVoiceSession()
    event = {"name": "nope", "call_id": "c9", "arguments": "{}"}
    asyncio.run(cockpit_voice_routes._dispatch_function_call(fake, _ctx(user_id), conv_id, event))
    assert any("未知函数" in o for o in fake.function_outputs)
    assert fake.followup_count == 1


# --- §14 旁听模式 v3.5: OBSERVE 指引段 + 第 5 函数 dismiss_turn -------------------------------


def test_observe_guidance_in_instructions(monkeypatch) -> None:
    """开关开 → instructions 含【旁听规则】;开关关 → 不含(基础指引段仍在)。"""
    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", True)
    text_on = cockpit_voice.load_voice_instructions()
    assert "【旁听规则】" in text_on and "dismiss_turn" in text_on
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", False)
    text_off = cockpit_voice.load_voice_instructions()
    assert "【旁听规则】" not in text_off
    assert "语音前台工作指引" in text_off


def test_build_session_update_includes_announce_policy(monkeypatch) -> None:
    """开关开 → tools 7 个(常驻 6,含 set_announce_policy),末位为 dismiss_turn(无参数)。"""
    monkeypatch.setattr(get_settings(), "realtime_voice_observe_enabled", True)
    tools = cockpit_voice.build_session_update("instr")["session"]["tools"]
    assert len(tools) == 7
    names = [t["name"] for t in tools]
    assert "mark_result_shared" in names  # 常驻第 5 函数(说两遍根治)
    assert "set_announce_policy" in names  # 播报策略(2026-10-14 聚合闭环入库)
    assert tools[-1]["name"] == "dismiss_turn"
    assert tools[-1]["parameters"]["properties"] == {}


def test_observe_disabled_excludes_tool(monkeypatch) -> None:
    """开关关 → tools 回常驻 6 个,不含 dismiss_turn,仍含 set_announce_policy。"""
    monkeypatch.setattr(get_settings(), "realtime_voice_observe_enabled", False)
    tools = cockpit_voice.build_session_update("instr")["session"]["tools"]
    assert len(tools) == 6
    assert all(t["name"] != "dismiss_turn" for t in tools)
    assert any(t["name"] == "mark_result_shared" for t in tools)
    assert any(t["name"] == "set_announce_policy" for t in tools)


def test_observe_guidance_has_drift_section(monkeypatch) -> None:
    """ENGAGED_DRIFT Layer 0:开关开 → instructions 含【对话模式】段(第二版语义:
    叫醒后保持对话到明确叫停,唯一回旁听出口 = 用户叫停)。"""
    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", True)
    text_on = cockpit_voice.load_voice_instructions()
    assert "【对话模式】" in text_on
    assert "一直保持到用户明确叫停" in text_on
    assert "除了这类明确叫停，任何其他理由都不要回旁听" in text_on
    assert "【称呼线索】" in text_on
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", False)
    assert "【对话模式】" not in cockpit_voice.load_voice_instructions()


def test_observe_guidance_no_voiceprint_claims(monkeypatch) -> None:
    """防幻觉红线:指引绝不能暗示模型有声纹/听声辨人能力(会让模型硬猜名字)。"""
    monkeypatch.setattr(get_settings(), "realtime_voice_observe_enabled", True)
    text = cockpit_voice.OBSERVE_GUIDANCE
    for banned in ("声纹", "根据声音判断", "听声音就知道", "音色辨认"):
        assert banned not in text, f"指引不应含 {banned!r}"
    assert "绝不根据声音猜测" in text


def test_dismiss_dispatch_cancels_and_silent(voice_settings) -> None:
    """dismiss_turn 分发 → cancel_response 截断 + 静默回执;绝不登记 followup。"""
    fake = FakeVoiceSession()
    event = {"name": "dismiss_turn", "call_id": "c7", "arguments": "{}"}
    asyncio.run(cockpit_voice_routes._dispatch_function_call(fake, _ctx(1), 1, event))
    assert fake.cancel_count == 1
    assert any("继续旁听" in o for o in fake.function_outputs)
    assert fake.followup_count == 0


def test_other_functions_still_followup(voice_settings) -> None:
    """防回归: dismiss 豁免不影响其他函数——cancel 分发后仍登记 followup。"""
    user_id, _ = create_user()
    conv_id = create_mirror_row(user_id, f"disp_cf_{uuid4().hex[:6]}")
    fake = FakeVoiceSession()
    event = {
        "name": "cancel_business_task",
        "call_id": "c8",
        "arguments": json.dumps({"task_hint": "那个任务"}),
    }
    asyncio.run(cockpit_voice_routes._dispatch_function_call(fake, _ctx(user_id), conv_id, event))
    assert any("没有进行中的任务" in o for o in fake.function_outputs)
    assert fake.followup_count == 1


# --- 三模式(spirit-voice-modes-plan §4.1) ------------------------------------------------


def test_load_voice_instructions_modes(monkeypatch, tmp_path) -> None:
    """四模式指令: pure=纯前台指引;exclusive=+专有模式段;multi=+旁听+多人现场;
    None=legacy(全局开关)。"""
    soul = tmp_path / "SOUL.md"
    soul.write_text("测试SOUL", encoding="utf-8")
    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_soul_path", str(soul))
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", True)
    monkeypatch.setattr(settings, "multi_speaker_enabled", True)

    pure = cockpit_voice.load_voice_instructions("pure")
    assert "测试SOUL" in pure and "语音前台工作指引" in pure
    for banned in ("【旁听规则】", "【专有模式】", "【多人现场】"):
        assert banned not in pure

    exclusive = cockpit_voice.load_voice_instructions("exclusive")
    assert "【专有模式】" in exclusive
    assert "【旁听规则】" not in exclusive and "【多人现场】" not in exclusive

    multi = cockpit_voice.load_voice_instructions("multi")
    assert "【旁听规则】" in multi and "【多人现场】" in multi and "【对话模式】" in multi
    assert "【专有模式】" not in multi

    legacy = cockpit_voice.load_voice_instructions(None)
    assert "【旁听规则】" in legacy and "【多人现场】" in legacy


def test_build_session_update_tools_by_mode(monkeypatch) -> None:
    """tools 组装: multi=5 函数;pure/exclusive=4;None=legacy 由全局开关决定。"""

    def names(payload: dict) -> list[str]:
        return [t["name"] for t in payload["session"]["tools"]]

    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", True)
    assert "dismiss_turn" not in names(cockpit_voice.build_session_update("x", "pure"))
    assert "dismiss_turn" not in names(cockpit_voice.build_session_update("x", "exclusive"))
    assert "dismiss_turn" in names(cockpit_voice.build_session_update("x", "multi"))
    assert "dismiss_turn" in names(cockpit_voice.build_session_update("x", None))
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", False)
    assert "dismiss_turn" not in names(cockpit_voice.build_session_update("x", None))


class FakeSidecar:
    """假侧路:记录 kwargs/喂流/关闭,暴露 speakers 状态事件入口。

    verdict_plan: 专有闸门剧本 [(起始秒, verdict)],按 sidecar 创建起算的
    单调钟返回当前 verdict(模拟 diarization 慢 1-2s 后追上的时序)。
    """

    instances: list["FakeSidecar"] = []
    verdict_plan: list[tuple[float, bool | None]] | None = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.fed: list[bytes] = []
        self.closed = False
        self._t0 = time.monotonic()
        FakeSidecar.instances.append(self)

    @property
    def current_speaker_is_holder(self) -> bool | None:
        if not FakeSidecar.verdict_plan:
            return None
        elapsed = time.monotonic() - self._t0
        verdict = FakeSidecar.verdict_plan[0][1]
        for at, v in FakeSidecar.verdict_plan:
            if elapsed >= at:
                verdict = v
        return verdict

    @property
    def current_speaker_debug(self) -> str:
        return f"fake-sidecar verdict={self.current_speaker_is_holder}"

    async def start(self) -> bool:
        return True

    async def feed(self, pcm: bytes) -> None:
        self.fed.append(pcm)

    async def close(self) -> None:
        self.closed = True


def test_ws_voice_mode_invalid_4400(voice_settings) -> None:
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"ws_badm_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]
    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "loud"})
        err = ws.receive_json()
        assert err["type"] == "error" and "未知语音模式" in err["message"]
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
        assert exc_info.value.code == 4400


def test_ws_multi_mode_disabled_4400(voice_settings, monkeypatch) -> None:
    """multi 显式模式 + multi_speaker_enabled 全局关 → 拒连不静默降级。"""
    monkeypatch.setattr(get_settings(), "multi_speaker_enabled", False)
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"ws_md_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]
    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "multi"})
        err = ws.receive_json()
        assert err["type"] == "error" and "未启用" in err["message"]
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
        assert exc_info.value.code == 4400


def test_ws_exclusive_without_enrollment_4400(voice_settings, monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "multi_speaker_enabled", True)

    async def fake_healthy() -> bool:
        return True

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)
    user_id, mobile = create_user()  # 未录声纹
    conv_id = create_mirror_row(user_id, f"ws_ex_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]
    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "exclusive"})
        err = ws.receive_json()
        assert err["type"] == "error" and "声纹" in err["message"]
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
        assert exc_info.value.code == 4400


def test_ws_multi_mode_happy_path_with_fake_sidecar(voice_settings, monkeypatch) -> None:
    """multi 模式全链路(假会话+假侧路): 模式守卫过闸 → instructions 含多人现场段,
    PCM 副本喂侧路,断开时侧路关闭。"""
    FakeVoiceSession.instances = []
    monkeypatch.setattr(cockpit_voice, "DashScopeVoiceSession", FakeVoiceSession)
    monkeypatch.setattr(get_settings(), "multi_speaker_enabled", True)

    async def fake_healthy() -> bool:
        return True

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)
    FakeSidecar.instances = []
    monkeypatch.setattr(voice_sidecar, "VoiceSidecar", FakeSidecar)

    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"ws_mu_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]
    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "multi"})
        assert ws.receive_json() == {"type": "state", "state": "ready"}
        ws.send_bytes(b"\x00\x01\x02\x03")
        ws.send_json({"type": "stop"})

    fake = FakeVoiceSession.instances[-1]
    assert fake.voice_mode == "multi"
    assert "【多人现场】" in fake.instructions and "【专有模式】" not in fake.instructions
    sidecar = FakeSidecar.instances[-1]
    assert sidecar.kwargs["mode"] == "multi"
    assert sidecar.kwargs["conversation_id"] == conv_id
    assert sidecar.fed == [b"\x00\x01\x02\x03"]
    assert sidecar.closed is True
    _wait_voice_sessions_cleared()


# --- 声纹三端点(spirit-voice-modes-plan §4.3) ---------------------------------------------


def test_status_voiceprint_fields(voice_settings, monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "multi_speaker_enabled", False)
    _, mobile = create_user()
    r = client.get("/api/v1/cockpit/realtime-voice/status", headers=login(mobile))
    assert r.status_code == 200, r.text
    vp = r.json()["data"]["voiceprint"]
    assert vp == {"configured": False, "enrolled": False, "available": False}


def test_enroll_too_short_400(voice_settings) -> None:
    _, mobile = create_user()
    r = client.post(
        "/api/v1/cockpit/speaker-profiles/enroll", headers=login(mobile), content=b"\x00" * 1024
    )
    assert r.status_code == 400, r.text
    assert r.json()["detail"]["code"] == "VOICEPRINT_AUDIO_TOO_SHORT"


def test_enroll_service_unavailable_503(voice_settings, monkeypatch) -> None:
    async def fake_healthy() -> bool:
        return False

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)
    _, mobile = create_user()
    r = client.post(
        "/api/v1/cockpit/speaker-profiles/enroll",
        headers=login(mobile),
        content=b"\x00" * (10 * 16000 * 2),
    )
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["code"] == "VOICEPRINT_SERVICE_UNAVAILABLE"


def test_enroll_quality_rejected_422(voice_settings, monkeypatch) -> None:
    async def fake_healthy() -> bool:
        return True

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)

    async def fake_enroll(self, pcm: bytes):
        return None

    monkeypatch.setattr(voice_sidecar.DiarizationClient, "enroll", fake_enroll)
    _, mobile = create_user()
    r = client.post(
        "/api/v1/cockpit/speaker-profiles/enroll",
        headers=login(mobile),
        content=b"\x00" * (10 * 16000 * 2),
    )
    assert r.status_code == 422, r.text
    assert r.json()["detail"]["code"] == "VOICEPRINT_QUALITY_REJECTED"


def test_enroll_and_me_lifecycle(voice_settings, monkeypatch) -> None:
    """录入→me 显示→库行(本人名+active+覆盖语义)→delete→me 复位。"""
    async def fake_healthy() -> bool:
        return True

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)

    async def fake_enroll(self, pcm: bytes):
        return [0.1, 0.2, 0.3]

    monkeypatch.setattr(voice_sidecar.DiarizationClient, "enroll", fake_enroll)
    user_id, mobile = create_user()
    headers = login(mobile)
    r = client.post(
        "/api/v1/cockpit/speaker-profiles/enroll",
        headers=headers,
        content=b"\x00" * (10 * 16000 * 2),
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"] == {"enrolled": True, "embeddingDim": 3}

    r2 = client.get("/api/v1/cockpit/speaker-profiles/me", headers=headers)
    assert r2.status_code == 200, r2.text
    assert r2.json()["data"]["enrolled"] is True
    assert r2.json()["data"]["enrolledAt"]

    with SessionLocal() as db:
        row = db.scalars(
            select(CockpitSpeakerProfile).where(
                CockpitSpeakerProfile.tenant_id == TENANT_ID,
                CockpitSpeakerProfile.user_id == user_id,
                CockpitSpeakerProfile.status == "active",
            )
        ).first()
        assert row is not None
        assert row.display_name == f"用户{user_id}"  # 独立服务无 users 表:占位名
        assert row.role_hint == "staff"

    r3 = client.delete("/api/v1/cockpit/speaker-profiles/me", headers=headers)
    assert r3.status_code == 200, r3.text
    assert r3.json()["data"]["enrolled"] is False
    r4 = client.get("/api/v1/cockpit/speaker-profiles/me", headers=headers)
    assert r4.json()["data"]["enrolled"] is False


# --- 专有闸门三态 + None 延迟复核(2026-09-15, routes/cockpit_voice.py) ----------------------


def _exclusive_ws_setup(monkeypatch) -> None:
    """搭 exclusive 模式 WS 环境: 假会话+假侧路+模式守卫全过闸。"""
    FakeVoiceSession.instances = []
    FakeVoiceSession.event_script = []
    FakeVoiceSession.proactive_response = False
    FakeSidecar.instances = []
    FakeSidecar.verdict_plan = None
    monkeypatch.setattr(cockpit_voice, "DashScopeVoiceSession", FakeVoiceSession)
    monkeypatch.setattr(get_settings(), "multi_speaker_enabled", True)

    async def fake_healthy() -> bool:
        return True

    monkeypatch.setattr(voice_sidecar, "diarization_healthy", fake_healthy)
    monkeypatch.setattr(voice_sidecar, "has_active_profile", lambda *a, **k: True)
    monkeypatch.setattr(voice_sidecar, "VoiceSidecar", FakeSidecar)


def _run_exclusive_ws(conv_title: str, frames: int) -> None:
    """起一条 exclusive WS,收 frames 个帧后发 stop 关闭。"""
    user_id, mobile = create_user()
    conv_id = create_mirror_row(user_id, f"{conv_title}_{uuid4().hex[:6]}")
    token = login(mobile)["Authorization"].split()[1]
    with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
        assert ws.receive_json() == {"type": "state", "state": "connecting"}
        ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "exclusive"})
        for _ in range(frames):
            ws.receive_json()
        ws.send_json({"type": "stop"})


def test_ws_exclusive_gate_blocks_other_speaker(voice_settings, monkeypatch) -> None:
    """verdict False(他人) → response.created 直接 cancel,不下发 responding。"""
    _exclusive_ws_setup(monkeypatch)
    FakeSidecar.verdict_plan = [(0.0, False)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (0.2, {"type": "response.done"}),
    ]
    try:
        _run_exclusive_ws("ws_gate", frames=2)  # ready(session.updated), ready(response.done)
        fake = FakeVoiceSession.instances[-1]
        assert fake.cancel_count == 1
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []


def test_ws_exclusive_gate_recheck_cancels_after_delay(voice_settings, monkeypatch) -> None:
    """verdict None 先放行(responding 照常下发),0.8s 后 diarization 追上判他人 → 复核补刀。"""
    _exclusive_ws_setup(monkeypatch)
    FakeSidecar.verdict_plan = [(0.0, None), (0.4, False)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (1.4, {"type": "response.done"}),
    ]
    try:
        _run_exclusive_ws("ws_gaterc", frames=3)  # ready, responding, ready
        fake = FakeVoiceSession.instances[-1]
        assert fake.cancel_count == 1  # 复核补刀(非立即拦截)
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []


def test_ws_exclusive_gate_recheck_dropped_when_response_done(voice_settings, monkeypatch) -> None:
    """复核竞态: None 放行后 response.done 先于复核时点到达 → 复核被取消,绝不补刀。"""
    _exclusive_ws_setup(monkeypatch)
    FakeSidecar.verdict_plan = [(0.0, None), (0.4, False)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (0.3, {"type": "response.done"}),  # 远早于复核时点 0.8s
    ]
    try:
        user_id, mobile = create_user()
        conv_id = create_mirror_row(user_id, f"ws_gatedone_{uuid4().hex[:6]}")
        token = login(mobile)["Authorization"].split()[1]
        with client.websocket_connect(f"/api/v1/cockpit/realtime-voice/ws?token={token}") as ws:
            assert ws.receive_json() == {"type": "state", "state": "connecting"}
            ws.send_json({"type": "start", "conversation_id": conv_id, "voice_mode": "exclusive"})
            assert ws.receive_json() == {"type": "state", "state": "ready"}
            assert ws.receive_json() == {"type": "state", "state": "responding"}
            assert ws.receive_json() == {"type": "state", "state": "ready"}
            time.sleep(1.2)  # 睡过复核时点:若 task 没取消,这里会补刀
            ws.send_json({"type": "stop"})
        assert FakeVoiceSession.instances[-1].cancel_count == 0
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []


def test_ws_exclusive_gate_passes_holder(voice_settings, monkeypatch) -> None:
    """verdict True(本人) → 放行 responding,不 cancel,不挂复核。"""
    _exclusive_ws_setup(monkeypatch)
    FakeSidecar.verdict_plan = [(0.0, True)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (0.2, {"type": "response.done"}),
    ]
    try:
        _run_exclusive_ws("ws_gateok", frames=3)  # ready, responding, ready
        assert FakeVoiceSession.instances[-1].cancel_count == 0
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []


def test_ws_exclusive_gate_skips_proactive_response(voice_settings, monkeypatch) -> None:
    """系统主动发言(回执/播报)豁免闸门: verdict False 也不 cancel、不挂复核(任务#35 复读根因)。"""
    _exclusive_ws_setup(monkeypatch)
    FakeVoiceSession.proactive_response = True
    FakeSidecar.verdict_plan = [(0.0, False)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (1.4, {"type": "response.done"}),
    ]
    try:
        _run_exclusive_ws("ws_gate_proactive", frames=3)  # ready, responding, ready
        fake = FakeVoiceSession.instances[-1]
        assert fake.cancel_count == 0  # 主动发言不过闸,也不补刀
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []
        FakeVoiceSession.proactive_response = False


def test_ws_exclusive_gate_recheck_not_armed_for_proactive(voice_settings, monkeypatch) -> None:
    """主动发言 + verdict None: 不挂 0.8s 复核;diarization 随后判他人也不补刀。"""
    _exclusive_ws_setup(monkeypatch)
    FakeVoiceSession.proactive_response = True
    FakeSidecar.verdict_plan = [(0.0, None), (0.4, False)]
    FakeVoiceSession.event_script = [
        (0.1, {"type": "response.created"}),
        (1.4, {"type": "response.done"}),
    ]
    try:
        _run_exclusive_ws("ws_gate_proactive_rc", frames=3)
        fake = FakeVoiceSession.instances[-1]
        assert fake.cancel_count == 0
    finally:
        FakeSidecar.verdict_plan = None
        FakeVoiceSession.event_script = []
        FakeVoiceSession.proactive_response = False


def test_proactive_flag_lifecycle_on_session(monkeypatch, announce_fast) -> None:
    """会话级标志生命周期: 播报发起→response.created 期间为 True;done/cancelled 后复位;
    用户触发的响应(标志未置位)保持 False(闸门照常生效)。"""
    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="ask", title="盘点", word="对了")
        await asyncio.sleep(0.25)  # 静默窗口到点,播报指令已发出
        assert session.current_response_proactive is True  # pending(已发起,等 created)
        await session._track_state({"type": "response.created"})
        assert session.current_response_proactive is True  # active(本轮是系统主动发言)
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        assert session.current_response_proactive is False
        # 用户触发路径: send_text 式 create_response 不置位 → created 后仍是 False
        await session.create_response()
        await session._track_state({"type": "response.created"})
        assert session.current_response_proactive is False
        await session._track_state({"type": "response.cancelled"})
        assert session.current_response_proactive is False

    asyncio.run(scenario())


def test_settle_notified_injects_progress(monkeypatch, announce_fast) -> None:
    """ask 播放送达后注入【播报进展】已问过/等答复,防自由发言轮复读。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="ask", title="查询", word="对了")
        await asyncio.sleep(0.25)
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.done", "response": {"status": "completed"}})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "notified", True)
        injections = [
            p["item"]["content"][0]["text"]
            for p in sent
            if p.get("type") == "conversation.item.create"
        ]
        progress = [t for t in injections if "【播报进展】" in t]
        assert len(progress) == 1
        assert "任务#1" in progress[0] and "等答复" in progress[0]

    asyncio.run(scenario())


def test_settle_interrupted_injects_progress(monkeypatch, announce_fast) -> None:
    """播报被打断后注入【播报进展】被打断/别再主动提起。"""

    async def scenario() -> None:
        session, sent, log = _make_session(monkeypatch)
        await session.request_announce(1, "t", mode="ask", title="盘点", word="对了")
        await asyncio.sleep(0.25)
        await session._track_state({"type": "response.created"})
        await session._track_state({"type": "response.cancelled"})
        await session.ack_playback(session._playback_seq, interrupted=False)
        assert log[-1] == ((1,), "interrupted", False)
        injections = [
            p["item"]["content"][0]["text"]
            for p in sent
            if p.get("type") == "conversation.item.create"
        ]
        progress = [t for t in injections if "【播报进展】" in t]
        assert len(progress) == 1
        assert "被用户打断" in progress[0] and "别再主动提起" in progress[0]

    asyncio.run(scenario())


def test_followup_fire_carries_hint(monkeypatch) -> None:
    """followup 带防复读轻约束,且指令不进入对话流。"""

    async def scenario() -> None:
        s = get_settings()
        monkeypatch.setattr(s, "realtime_voice_followup_delay_min_s", 0.05)
        monkeypatch.setattr(s, "realtime_voice_followup_delay_max_s", 0.06)
        session, sent, log = _make_session(monkeypatch)
        session.schedule_followup()
        await asyncio.sleep(0.25)
        creates = [p for p in sent if p.get("type") == "response.create"]
        assert len(creates) == 1
        instr = creates[0]["response"]["instructions"]
        assert "播报进展" in instr and "复读" in instr
        injections = [
            p["item"]["content"][0]["text"]
            for p in sent
            if p.get("type") == "conversation.item.create"
        ]
        assert not any("播报进展" in t and "系统记录" in t for t in injections)

    asyncio.run(scenario())
