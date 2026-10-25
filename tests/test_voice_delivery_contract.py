"""语音播放确认与任务卡 announce 契约测试(按平台相关用例适配独立版)。

仅覆盖本次移植范围:playback.settle/ack/超时/幂等、announce 三 action、
归属与终态校验、静态 reset 路由和 interrupted 复活。
"""

import asyncio
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.db import SessionLocal
from app.main import app
from app.models import VoiceConversation, VoiceTask
from app.services import cockpit_voice, voice_tasks

from tests.conftest import TENANT_ID, mint_token

client = TestClient(app)


def _make_session(on_transition=None) -> cockpit_voice.DashScopeVoiceSession:
    async def _on_event(event: dict) -> None:
        return None

    session = cockpit_voice.DashScopeVoiceSession(instructions="", on_event=_on_event)
    session.on_announce_transition = on_transition
    return session


def test_defer_settle_waits_for_browser_ack(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_playback_ack_timeout_s", 60.0)
    transitions: list[tuple] = []

    async def _on_transition(task_ids, state, bump):
        transitions.append((list(task_ids), state, bump))

    pushed: list[dict] = []

    async def _sink(payload: dict) -> None:
        pushed.append(payload)

    session = _make_session(_on_transition)
    session.on_task_event = _sink
    session._in_flight_notify.add(101)

    async def scenario() -> None:
        await session._defer_settle(completed=True)
        assert session._pending_settle
        assert pushed and pushed[-1]["type"] == "playback.settle"
        seq = pushed[-1]["seq"]
        assert session._in_flight_notify == set()
        assert transitions == []
        await session.ack_playback(seq, interrupted=False)
        assert session._pending_settle == {}
        assert not session._settle_timers

    asyncio.run(scenario())
    assert transitions == [([101], "notified", True)]


def test_ack_playback_interrupted_settles_as_interrupted(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_playback_ack_timeout_s", 60.0)
    transitions: list[tuple] = []

    async def _on_transition(task_ids, state, bump):
        transitions.append((list(task_ids), state, bump))

    session = _make_session(_on_transition)
    session._in_flight_consume.add(102)

    async def scenario() -> None:
        await session._defer_settle(completed=True)
        seq = session._playback_seq
        await session.ack_playback(seq, interrupted=True)

    asyncio.run(scenario())
    assert transitions == [([102], "interrupted", False)]


def test_settle_fallback_timer_for_legacy_frontend(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_playback_ack_timeout_s", 0.05)
    transitions: list[tuple] = []

    async def _on_transition(task_ids, state, bump):
        transitions.append((list(task_ids), state, bump))

    session = _make_session(_on_transition)
    session._in_flight_notify.add(103)

    async def scenario() -> None:
        await session._defer_settle(completed=True)
        await asyncio.sleep(0.3)

    asyncio.run(scenario())
    assert transitions == [([103], "notified", True)]
    assert session._pending_settle == {}


def test_duplicate_ack_ignored(monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "realtime_voice_playback_ack_timeout_s", 60.0)
    transitions: list[tuple] = []

    async def _on_transition(task_ids, state, bump):
        transitions.append((list(task_ids), state, bump))

    session = _make_session(_on_transition)
    session._in_flight_notify.add(104)

    async def scenario() -> None:
        await session._defer_settle(completed=True)
        seq = session._playback_seq
        await session.ack_playback(seq, interrupted=False)
        await session.ack_playback(seq, interrupted=False)
        await session.ack_playback(9999, interrupted=False)

    asyncio.run(scenario())
    assert len(transitions) == 1


def _user() -> tuple[int, dict[str, str]]:
    user_id = uuid4().int % 900_000_000 + 1000
    return user_id, {"Authorization": f"Bearer {mint_token(user_id)}"}


def _conversation(user_id: int) -> int:
    with SessionLocal() as db:
        conv = VoiceConversation(
            tenant_id=TENANT_ID,
            user_id=user_id,
            title="announce 契约",
            agent_session_id=f"announce-{uuid4().hex[:10]}",
        )
        db.add(conv)
        db.commit()
        return conv.id


def _task(user_id: int, conversation_id: int, *, status: str = "done", announce_state: str = "pending") -> int:
    with SessionLocal() as db:
        task = VoiceTask(
            tenant_id=TENANT_ID,
            user_id=user_id,
            conversation_id=conversation_id,
            kind="查询",
            task_text="播报契约任务",
            status=status,
            announce_state=announce_state,
            result_summary="结果",
            progress=[],
        )
        db.add(task)
        db.commit()
        return task.id


def _state(task_id: int) -> str:
    with SessionLocal() as db:
        return db.get(VoiceTask, task_id).announce_state


@pytest.mark.parametrize(
    ("action", "expected"),
    [("defer", "deferred"), ("dismiss", "dismissed"), ("share", "shared")],
)
def test_announce_actions(action: str, expected: str) -> None:
    user_id, headers = _user()
    conv_id = _conversation(user_id)
    task_id = _task(user_id, conv_id, announce_state="notified")
    response = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/announce",
        json={"action": action},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["announceState"] == expected
    assert response.json()["data"]["status"] == "done"
    assert _state(task_id) == expected


def test_announce_non_terminal_409() -> None:
    user_id, headers = _user()
    conv_id = _conversation(user_id)
    task_id = _task(user_id, conv_id, status="running")
    response = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/announce",
        json={"action": "share"},
        headers=headers,
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "VOICE_TASK_NOT_TERMINAL"


def test_announce_other_user_404() -> None:
    owner_id, _ = _user()
    other_id, other_headers = _user()
    conv_id = _conversation(owner_id)
    task_id = _task(owner_id, conv_id)
    response = client.post(
        f"/api/v1/cockpit/voice-tasks/{task_id}/announce",
        json={"action": "share"},
        headers=other_headers,
    )
    assert response.status_code == 404
    assert _state(task_id) == "pending"


def test_reset_static_route_revives_policy_interrupted() -> None:
    user_id, headers = _user()
    conv_id = _conversation(user_id)
    task_id = _task(user_id, conv_id, announce_state="announcing")
    counts = asyncio.run(
        voice_tasks.set_announce_policy(
            TENANT_ID, user_id, "direct", task_ids=[task_id], conversation_id=conv_id
        )
    )
    assert counts["interrupted"] == 1
    assert counts["interrupted_ids"] == [task_id]
    assert counts["deferred_ids"] == []
    assert _state(task_id) == "interrupted"

    response = client.post(
        "/api/v1/cockpit/voice-tasks/announce-policy/reset",
        json={"conversation_id": conv_id},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"] == {"cleared": 1, "revived": 1}
    assert _state(task_id) == "pending"
