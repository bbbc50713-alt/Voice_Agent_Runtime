"""独立服务运行时收口回归测试。"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select

from app import models
from app.db import SessionLocal
from app.main import _cors_origins
from app.services import voice_sidecar


def test_cors_origins_accepts_json_and_csv() -> None:
    assert _cors_origins('["http://localhost:5174","https://example.test"]') == [
        "http://localhost:5174",
        "https://example.test",
    ]
    assert _cors_origins("http://localhost:5174, https://example.test") == [
        "http://localhost:5174",
        "https://example.test",
    ]
    assert _cors_origins("*") == ["*"]


def test_speaker_event_has_created_at_and_purge_uses_it() -> None:
    now = datetime.utcnow()
    with SessionLocal() as db:
        old = models.SpeakerEvent(
            tenant_id=9901,
            conversation_id=9901,
            voice_session_id="test-session",
            speaker_label="speaker_1",
            speaker_role="unknown",
            start_ms=0,
            end_ms=100,
            text="old",
            created_at=now - timedelta(days=31),
        )
        fresh = models.SpeakerEvent(
            tenant_id=9901,
            conversation_id=9901,
            voice_session_id="test-session",
            speaker_label="speaker_1",
            speaker_role="unknown",
            start_ms=100,
            end_ms=200,
            text="fresh",
            created_at=now,
        )
        db.add_all([old, fresh])
        db.commit()
        old_id, fresh_id = old.id, fresh.id

    removed = voice_sidecar.purge_expired_speaker_events()

    with SessionLocal() as db:
        assert db.get(models.SpeakerEvent, old_id) is None
        assert db.get(models.SpeakerEvent, fresh_id) is not None
        assert removed >= 1
        db.execute(
            models.SpeakerEvent.__table__.delete().where(
                models.SpeakerEvent.tenant_id == 9901
            )
        )
        db.commit()
