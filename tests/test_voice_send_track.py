"""发送追踪(风险放行触达结果回调)单测 — 自平台 backend/tests/test_voice_send_track.py 移植。

移植差异(独立服务无 Message/Customer/Conversation/ChannelAccount/User 业务表):
- 消息状态来源抽象为 TrackProbe(app/services/track_probe.py): 测试用 FakeTrackProbe
  按 dict 喂状态,结算口径与平台一致(探针返回什么状态就怎么结算);
  补丁点 voice_tasks.get_track_probe(模块级 from-import 绑定)。
- make_chain 只建 VoiceConversation 镜像行;message_id 为伪造 int(账本纯引用)。
- HttpTrackProbe 自身契约(平台桥 POST)用 httpx.MockTransport 测(§5,新增,
  替代平台时代直接查 Message 表的隐式覆盖)。
- DB: tmp SQLite(conftest 钉 DATABASE_URL);用例间靠唯一 id 隔离,无需快照清理。

覆盖(与平台同名同语义):
- create_send_track_task: 账本行(running/progress[0].step=send_track/message_ids/names/task_text)
  + RealtimeEvent(topic=cockpit, cockpit.send_track.started);
- _finalize_send_track 结算矩阵: 全 sent→done 名单摘要 / 部分 failed→failed 人话明细
  / 全 failed / 超时 timed_out→未确认计失败 / 非 running 幂等跳过;
- _send_track_watch: 全终态+稳定窗后结算;deadline 超时按未确认结算;
  首读 commit 竞速容忍窗(watcher 先于路由 commit 启动时能等到行提交);
- _recover_send_track: 已全终态直接结算,未终态重启盯梢;
- recover_interrupted_tasks: send_track 行路由到 _recover_send_track(不走 agent 探活)。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from itertools import count
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.models import (
    RealtimeEvent,
    VoiceConversation as CockpitConversation,
    VoiceTask as CockpitVoiceTask,
)
from app.services import track_probe, voice_tasks

TENANT_ID = 1

_mid_counter = count(10_000)  # 伪造 message_id 空间(避开自增主键,肉眼可辨)


# --- 公共装置 ----------------------------------------------------------------------


class FakeTrackProbe:
    """dict 喂状态的假探针: fetch_states 只回已登记的 mid(未登记=缺失,与平台无行同义)。"""

    def __init__(self) -> None:
        self.states: dict[int, track_probe.MessageState] = {}

    def set(self, mid: int, status: str, failure_reason: str | None = None) -> None:
        self.states[mid] = track_probe.MessageState(status, failure_reason)

    async def fetch_states(self, message_ids: list[int]) -> dict[int, track_probe.MessageState]:
        return {mid: self.states[mid] for mid in message_ids if mid in self.states}


@pytest.fixture(autouse=True)
def fake_probe(monkeypatch: pytest.MonkeyPatch) -> FakeTrackProbe:
    """所有结算/盯梢走假探针(平台时代是查 Message 表)。"""
    probe = FakeTrackProbe()
    monkeypatch.setattr(voice_tasks, "get_track_probe", lambda: probe)
    return probe


@pytest.fixture(autouse=True)
def no_spawn(monkeypatch: pytest.MonkeyPatch):
    """单测不起后台盯梢协程(需要时测试内显式 asyncio.run)。"""
    monkeypatch.setattr(voice_tasks, "_spawn", lambda coro: coro.close())


@pytest.fixture
def announce_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    called: list[int] = []

    async def _fake(task_id: int) -> None:
        called.append(task_id)

    monkeypatch.setattr(voice_tasks, "announce_task", _fake)
    return called


def _ctx(user_id: int) -> SimpleNamespace:
    return SimpleNamespace(tenant_id=TENANT_ID, user_id=user_id)


def make_chain(*, name: str | None = None) -> dict:
    """造 驾驶舱会话镜像行 的最小链路(独立服务无客户/消息业务表)。"""
    marker = uuid4().hex
    owner_id = uuid4().int % 900_000_000 + 1000
    with SessionLocal() as db:
        cockpit_conv = CockpitConversation(
            tenant_id=TENANT_ID,
            user_id=owner_id,
            title="发送追踪单测",
            agent_session_id=f"ut-{marker[:12]}",
        )
        db.add(cockpit_conv)
        db.commit()
        return {
            "customer_id": uuid4().int % 900_000_000 + 1000,
            "cockpit_conversation_id": cockpit_conv.id,
            "owner_id": owner_id,
            "name": name or f"SendTrack {marker[:8]}",
        }


def make_message(probe: FakeTrackProbe, *, status: str, failure_reason: str | None = None) -> int:
    """伪造一个 message_id 并在假探针里登记状态(替代平台 Message 建行)。"""
    mid = next(_mid_counter)
    probe.set(mid, status, failure_reason)
    return mid


def make_track_task(chain: dict, message_ids: list[int], *, status: str = "running") -> int:
    """直接调服务函数建行(走真实创建路径)。"""
    with SessionLocal() as db:
        task = voice_tasks.create_send_track_task(
            db,
            _ctx(chain["owner_id"]),
            conversation_id=chain["cockpit_conversation_id"],
            entries=[{"message_id": mid, "customer_id": chain["customer_id"]} for mid in message_ids],
            names={chain["customer_id"]: chain["name"]},
        )
        db.commit()
        if status != "running":
            task.status = status
            db.commit()
        return task.id


def get_task(task_id: int) -> CockpitVoiceTask:
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        assert task is not None
        db.expunge(task)
        return task


# ---------------------------------------------------------------------------
# 1. 创建: 账本行 + started 事件
# ---------------------------------------------------------------------------


def test_create_send_track_task_row_and_started_event(fake_probe) -> None:
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="queued")
    task_id = make_track_task(chain, [mid])

    task = get_task(task_id)
    assert task.status == "running"
    assert task.kind == "其他"
    assert "王测试" in task.task_text and "发消息" in task.task_text
    entry = task.progress[0]
    assert entry["step"] == "send_track"
    assert entry["message_ids"] == [mid]
    assert entry["names"] == {str(mid): "王测试"}
    assert task.announce_state == "pending"

    with SessionLocal() as db:
        event = db.scalars(
            select(RealtimeEvent).where(
                RealtimeEvent.topic == "cockpit",
                RealtimeEvent.event_type == "cockpit.send_track.started",
                RealtimeEvent.resource_id == str(task_id),
            )
        ).first()
        assert event is not None
        assert event.payload["task"]["id"] == task_id
        assert event.payload["task"]["status"] == "running"


# ---------------------------------------------------------------------------
# 2. 结算矩阵
# ---------------------------------------------------------------------------


def test_finalize_all_sent_done(fake_probe, announce_spy) -> None:
    chain = make_chain(name="王测试")
    mids = [make_message(fake_probe, status="sent") for _ in range(3)]
    task_id = make_track_task(chain, mids)

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "done"
    assert task.result_summary == "3 位客户的消息都发出去了:王测试、王测试、王测试"
    assert task.result_text == task.result_summary
    assert task.finished_at is not None
    assert announce_spy == [task_id]
    with SessionLocal() as db:
        event = db.scalars(
            select(RealtimeEvent).where(
                RealtimeEvent.topic == "cockpit",
                RealtimeEvent.event_type == "cockpit.send_track.settled",
                RealtimeEvent.resource_id == str(task_id),
            )
        ).first()
        assert event is not None and event.payload["task"]["status"] == "done"


def test_finalize_single_sent_done(fake_probe) -> None:
    chain = make_chain(name="欧伯全")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "done"
    assert task.result_summary == "欧伯全 的消息已经发出去了"


def test_finalize_partial_failure_human_detail(fake_probe, announce_spy) -> None:
    chain = make_chain(name="王测试")
    ok1 = make_message(fake_probe, status="sent")
    ok2 = make_message(fake_probe, status="sent")
    bad = make_message(
        fake_probe, status="failed", failure_reason="对方还不是你的朋友，请先申请添加好友"
    )
    task_id = make_track_task(chain, [ok1, ok2, bad])

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "failed"
    assert "王测试、王测试 已发出" in task.error
    assert "王测试 没发出去(对方已不是好友)" in task.error
    assert announce_spy == [task_id]


def test_finalize_all_failed(fake_probe) -> None:
    chain = make_chain(name="王测试")
    bad = make_message(fake_probe, status="failed", failure_reason="设备已离线,请稍后重试")
    task_id = make_track_task(chain, [bad])

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "failed"
    assert "已发出" not in task.error
    assert "没发出去(设备离线)" in task.error


def test_finalize_timed_out_marks_unconfirmed(fake_probe) -> None:
    chain = make_chain(name="王测试")
    ok = make_message(fake_probe, status="sent")
    stuck = make_message(fake_probe, status="sending")
    task_id = make_track_task(chain, [ok, stuck])

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=True))

    task = get_task(task_id)
    assert task.status == "failed"
    assert "王测试 已发出" in task.error
    assert "王测试 没发出去(发送超时未确认)" in task.error


def test_finalize_probe_error_settles_as_timed_out(fake_probe, monkeypatch) -> None:
    """探针失败(平台桥不在线) → 按超时口径收口,绝不误报成功(独立服务新增口径)。"""

    class BrokenProbe:
        async def fetch_states(self, message_ids):
            raise track_probe.TrackProbeError("down")

    monkeypatch.setattr(voice_tasks, "get_track_probe", lambda: BrokenProbe())
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "failed"
    assert "没发出去(消息记录缺失)" in task.error


def test_finalize_skips_when_not_running(fake_probe) -> None:
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid], status="canceled")

    asyncio.run(voice_tasks._finalize_send_track(task_id, timed_out=False))

    task = get_task(task_id)
    assert task.status == "canceled"  # 未被结算覆盖
    assert task.result_summary is None


# ---------------------------------------------------------------------------
# 3. 盯梢协程
# ---------------------------------------------------------------------------


def test_watch_settles_after_stable_window(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_POLL_S", 0.02)
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_STABLE_S", 0.03)
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_DEADLINE_S", 30.0)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._send_track_watch(task_id))

    assert get_task(task_id).status == "done"


def test_watch_deadline_settles_unconfirmed(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_POLL_S", 0.02)
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_DEADLINE_S", 0.05)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="queued")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._send_track_watch(task_id))

    task = get_task(task_id)
    assert task.status == "failed"
    assert "发送超时未确认" in (task.error or "")


def test_watch_exits_when_task_not_running(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_POLL_S", 0.02)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid], status="canceled")

    asyncio.run(voice_tasks._send_track_watch(task_id))

    assert get_task(task_id).status == "canceled"


def test_watch_tolerates_commit_race(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    """生产根因回归: watcher 先于路由 commit 启动,首读容忍窗内等到行提交后正常盯梢结算。

    另起线程在事务内建行但延迟 commit(模拟路由事务),主线程直接跑 watcher——
    无容忍窗时首读 None 静默退出,任务永远 running。
    """
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_POLL_S", 0.02)
    monkeypatch.setattr(voice_tasks, "_SEND_TRACK_STABLE_S", 0.03)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    holder: dict = {"task_id": None, "ready": threading.Event()}

    def _create_uncommitted() -> None:
        with SessionLocal() as db:
            task = voice_tasks.create_send_track_task(
                db,
                _ctx(chain["owner_id"]),
                conversation_id=chain["cockpit_conversation_id"],
                entries=[{"message_id": mid, "customer_id": chain["customer_id"]}],
                names={chain["customer_id"]: chain["name"]},
            )
            holder["task_id"] = task.id
            holder["ready"].set()
            time.sleep(0.4)  # 握着未提交事务,让 watcher 先进入首读容忍窗
            db.commit()

    thread = threading.Thread(target=_create_uncommitted)
    thread.start()
    assert holder["ready"].wait(timeout=5)

    asyncio.run(voice_tasks._send_track_watch(holder["task_id"]))

    thread.join(timeout=10)
    assert get_task(holder["task_id"]).status == "done"


# ---------------------------------------------------------------------------
# 4. 启动恢复
# ---------------------------------------------------------------------------


def test_recover_settles_terminal_send_track(fake_probe) -> None:
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._recover_send_track(task_id))

    assert get_task(task_id).status == "done"


def test_recover_restarts_watch_for_pending_send(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    watched: list[int] = []

    async def _fake_watch(task_id: int) -> None:
        watched.append(task_id)

    monkeypatch.setattr(voice_tasks, "_send_track_watch", _fake_watch)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sending")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._recover_send_track(task_id))

    assert watched == [task_id]
    assert get_task(task_id).status == "running"  # 盯梢接管,不直接结算


def test_recover_probe_error_restarts_watch(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    """探针失败(平台桥不在线) → 重启盯梢下轮再试,不按失败误结算(独立服务新增口径)。"""

    class BrokenProbe:
        async def fetch_states(self, message_ids):
            raise track_probe.TrackProbeError("down")

    watched: list[int] = []

    async def _fake_watch(task_id: int) -> None:
        watched.append(task_id)

    monkeypatch.setattr(voice_tasks, "get_track_probe", lambda: BrokenProbe())
    monkeypatch.setattr(voice_tasks, "_send_track_watch", _fake_watch)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sending")
    task_id = make_track_task(chain, [mid])

    asyncio.run(voice_tasks._recover_send_track(task_id))

    assert watched == [task_id]
    assert get_task(task_id).status == "running"


def test_recover_routes_send_track_rows(fake_probe, monkeypatch: pytest.MonkeyPatch) -> None:
    """recover_interrupted_tasks 把 send_track 行路由到 _recover_send_track(不走 agent 探活)。"""
    recovered: list[int] = []
    probed: list[int] = []

    async def _fake_recover_send_track(task_id: int) -> None:
        recovered.append(task_id)

    async def _fake_recover_one(task_id: int, run_ref) -> None:
        probed.append(task_id)

    monkeypatch.setattr(voice_tasks, "_recover_send_track", _fake_recover_send_track)
    monkeypatch.setattr(voice_tasks, "_recover_one", _fake_recover_one)
    chain = make_chain(name="王测试")
    mid = make_message(fake_probe, status="sent")
    task_id = make_track_task(chain, [mid])

    voice_tasks.recover_interrupted_tasks()

    assert task_id in recovered
    assert task_id not in probed


# ---------------------------------------------------------------------------
# 5. TrackProbe 自身契约(独立服务新增: 替代平台时代 Message 表直查)
# ---------------------------------------------------------------------------


def test_null_probe_settles_all_sent() -> None:
    """NullTrackProbe(独立部署默认): 不查外部状态,全部按已送达结算。"""
    probe = track_probe.NullTrackProbe()
    states = asyncio.run(probe.fetch_states([1, 2, 3]))
    assert {mid: st.status for mid, st in states.items()} == {1: "sent", 2: "sent", 3: "sent"}


def _http_probe(monkeypatch: pytest.MonkeyPatch, handler) -> track_probe.HttpTrackProbe:
    """MockTransport 扮演平台桥端点;HttpTrackProbe 内部直构 httpx.AsyncClient,
    补丁点为 track_probe 模块引用的 httpx.AsyncClient(monkeypatch 收尾自动还原)。"""
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        track_probe.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_client(*args, transport=transport, **kwargs),
    )
    return track_probe.HttpTrackProbe("http://platform.test/internal/track", "tok-internal")


def test_http_probe_posts_ids_and_parses_states(monkeypatch: pytest.MonkeyPatch) -> None:
    """契约: POST {"message_ids": [...]} 带 X-Internal-Token;states 键 str→int,
    status/failure_reason 原样落 MessageState。"""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        seen["token"] = request.headers.get("X-Internal-Token")
        return httpx.Response(
            200,
            json={
                "states": {
                    "11": {"status": "sent", "failure_reason": None},
                    "12": {"status": "failed", "failure_reason": "设备已离线,请稍后重试"},
                }
            },
        )

    probe = _http_probe(monkeypatch, handler)
    states = asyncio.run(probe.fetch_states([11, 12]))
    assert seen["body"] == {"message_ids": [11, 12]}
    assert seen["token"] == "tok-internal"
    assert states[11].status == "sent" and states[11].failure_reason is None
    assert states[12].status == "failed" and states[12].failure_reason == "设备已离线,请稍后重试"


def test_http_probe_empty_ids_skips_http(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("空列表不应发 HTTP")

    probe = _http_probe(monkeypatch, handler)
    assert asyncio.run(probe.fetch_states([])) == {}


def test_http_probe_500_raises_track_probe_error(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _http_probe(monkeypatch, lambda request: httpx.Response(500))
    with pytest.raises(track_probe.TrackProbeError):
        asyncio.run(probe.fetch_states([11]))


def test_http_probe_transport_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    probe = _http_probe(monkeypatch, handler)
    with pytest.raises(track_probe.TrackProbeError):
        asyncio.run(probe.fetch_states([11]))


def test_http_probe_contract_violation_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _http_probe(monkeypatch, lambda request: httpx.Response(200, json={"nope": 1}))
    with pytest.raises(track_probe.TrackProbeError):
        asyncio.run(probe.fetch_states([11]))


def test_get_track_probe_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    """配置选择: track_probe=null / http 无 url → Null;http+url → Http(进程内单例可 reset)。"""
    settings = get_settings()
    track_probe.reset_track_probe()
    try:
        monkeypatch.setattr(settings, "track_probe", "null")
        assert isinstance(track_probe.get_track_probe(), track_probe.NullTrackProbe)
        track_probe.reset_track_probe()
        monkeypatch.setattr(settings, "track_probe", "http")
        monkeypatch.setattr(settings, "track_probe_url", "")
        assert isinstance(track_probe.get_track_probe(), track_probe.NullTrackProbe)
        track_probe.reset_track_probe()
        monkeypatch.setattr(settings, "track_probe_url", "http://platform.test/x")
        probe = track_probe.get_track_probe()
        assert isinstance(probe, track_probe.HttpTrackProbe)
        assert track_probe.get_track_probe() is probe  # 单例缓存
    finally:
        track_probe.reset_track_probe()
