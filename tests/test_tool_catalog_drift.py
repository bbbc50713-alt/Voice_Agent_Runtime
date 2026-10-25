"""工具目录防漂移对账 + 播报策略复位测试（2026-10-14 聚合闭环）。

覆盖:
- 目录指纹金标:两侧(平台 backend/tests 同款金标)必须一致,任一侧单边改目录即红,
  强制"双改"纪律;指纹算法变更需双仓同步更新金标。
- 目录结构:set_announce_policy 存在且 policy 枚举对齐;submit 携带 4 个聚合参数。
- check_tool_catalog_drift:未配置→None/一致→False/漂移→True 且上报/端点异常→None/异常→None。
- start_tool_catalog_drift_watch:幂等 + 开关停用不起线程。
- reset_announce_policy:清策略 + deferred 复活 pending;无策略任务不动。
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from app.db import SessionLocal
from app.models import VoiceConversation, VoiceTask
from app.services import cockpit_voice, voice_tasks

GOLDEN_FINGERPRINT = "149e0e2c47e810ac"


# --- 目录指纹金标(双仓同值,防单边漂移) ----------------------------------------------------


def test_tool_catalog_fingerprint_matches_golden() -> None:
    assert cockpit_voice.tool_catalog_fingerprint() == GOLDEN_FINGERPRINT


def test_catalog_has_announce_policy_tool() -> None:
    tools = {t["name"]: t for t in cockpit_voice._CATALOG_TOOLS}  # noqa: SLF001
    assert "set_announce_policy" in tools
    policy = tools["set_announce_policy"]
    props = policy["parameters"]["properties"]
    assert "policy" in props and "task_ids" in props
    assert set(props["policy"]["enum"]) == {"direct", "file_ready", "silent_until_group"}


def test_submit_has_aggregation_params() -> None:
    tools = {t["name"]: t for t in cockpit_voice._CATALOG_TOOLS}  # noqa: SLF001
    submit = tools["submit_business_task"]
    props = submit["parameters"]["properties"]
    for key in ("delivery_mode", "artifact_format", "source_task_ids", "announce_policy"):
        assert key in props, f"submit 缺聚合参数 {key}"
    assert props["delivery_mode"]["enum"] == ["voice_summary", "task_card", "file"]
    assert "file_ready" in props["announce_policy"]["enum"]


# --- check_tool_catalog_drift ---------------------------------------------------------------


@pytest.fixture
def drift_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "platform_api_base", "http://platform.test")
    monkeypatch.setattr(get_settings(), "internal_token", "test-internal-token")
    return get_settings()


def test_drift_check_skips_without_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "platform_api_base", "")
    monkeypatch.setattr(get_settings(), "internal_token", "")
    assert cockpit_voice.check_tool_catalog_drift() is None


def test_drift_check_consistent_returns_false(drift_settings, monkeypatch: pytest.MonkeyPatch) -> None:
    local = cockpit_voice.tool_catalog_fingerprint()

    class _Resp:
        status_code = 200

        def json(self) -> dict:
            return {"fingerprint": local}

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    assert cockpit_voice.check_tool_catalog_drift() is False


def test_drift_check_mismatch_reports(drift_settings, monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict] = []

    class _Resp:
        status_code = 200

        def json(self) -> dict:
            return {"fingerprint": "deadbeef" * 4}

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    monkeypatch.setattr(
        "httpx.post",
        lambda url, **k: posted.append({"url": url, "json": k.get("json")}) or _Resp(),
    )
    assert cockpit_voice.check_tool_catalog_drift() is True
    assert len(posted) == 1
    assert posted[0]["json"]["local_fingerprint"] == cockpit_voice.tool_catalog_fingerprint()
    assert posted[0]["json"]["remote_fingerprint"] == "deadbeef" * 4


def test_drift_check_bad_status_returns_none(drift_settings, monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        status_code = 404

        def json(self) -> dict:  # pragma: no cover - 不应被调用
            return {}

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    assert cockpit_voice.check_tool_catalog_drift() is None


def test_drift_check_unreachable_returns_none(drift_settings, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*a, **k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr("httpx.get", _boom)
    assert cockpit_voice.check_tool_catalog_drift() is None


def test_drift_watch_disabled_starts_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "tool_catalog_drift_enabled", False)
    cockpit_voice._drift_watch_started = False  # noqa: SLF001
    cockpit_voice.start_tool_catalog_drift_watch()
    assert cockpit_voice._drift_watch_started is False  # noqa: SLF001


def test_drift_watch_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "tool_catalog_drift_enabled", True)
    monkeypatch.setattr(get_settings(), "tool_catalog_drift_interval_s", 60)
    spawned: list[str] = []
    monkeypatch.setattr(
        "threading.Thread",
        lambda *a, **k: spawned.append(k.get("name") or a[1][0].__name__) or type("T", (), {"start": lambda self: None})(),
    )
    cockpit_voice._drift_watch_started = False  # noqa: SLF001
    cockpit_voice.start_tool_catalog_drift_watch()
    cockpit_voice.start_tool_catalog_drift_watch()
    assert spawned == ["tool-catalog-drift"]  # 第二次调用不再起线程


# --- reset_announce_policy(voice 侧账本) -----------------------------------------------------


def _seed_conv_task(delivery: dict | None) -> int:
    with SessionLocal() as db:
        conv = VoiceConversation(
            tenant_id=1, user_id=1, title="对账单测", agent_session_id=f"drift-{datetime.utcnow().timestamp()}"
        )
        db.add(conv)
        db.flush()
        task = VoiceTask(
            tenant_id=1,
            user_id=1,
            conversation_id=conv.id,
            kind="查询",
            task_text="对账单测任务",
            status="done",
            announce_state="deferred",
            delivery=delivery,
        )
        db.add(task)
        db.commit()
        return task.id


def test_reset_announce_policy_revives_deferred() -> None:
    task_id = _seed_conv_task({"announce_policy": "silent_until_group"})
    result = asyncio.run(voice_tasks.reset_announce_policy(1, 1, _conv_of(task_id)))
    assert result["cleared"] == 1 and result["revived"] == 1
    with SessionLocal() as db:
        row = db.get(VoiceTask, task_id)
        assert "announce_policy" not in (row.delivery or {})
        assert row.announce_state == "pending"


def test_reset_announce_policy_revives_interrupted() -> None:
    task_id = _seed_conv_task({"announce_policy": "direct"})
    with SessionLocal() as db:
        row = db.get(VoiceTask, task_id)
        row.announce_state = "interrupted"
        db.commit()
    result = asyncio.run(voice_tasks.reset_announce_policy(1, 1, _conv_of(task_id)))
    assert result["cleared"] == 1 and result["revived"] == 1
    with SessionLocal() as db:
        row = db.get(VoiceTask, task_id)
        assert "announce_policy" not in (row.delivery or {})
        assert row.announce_state == "pending"


def test_reset_announce_policy_skips_plain_task() -> None:
    task_id = _seed_conv_task(None)
    result = asyncio.run(voice_tasks.reset_announce_policy(1, 1, _conv_of(task_id)))
    assert result["cleared"] == 0 and result["revived"] == 0


def _conv_of(task_id: int) -> int:
    with SessionLocal() as db:
        return db.get(VoiceTask, task_id).conversation_id
