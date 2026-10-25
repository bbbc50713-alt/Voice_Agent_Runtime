"""前端事件总线(平台 services/realtime.py + routes/realtime.py 的精简版)。

砍掉 outbox/celery/redis 唤醒通道——独立服务单进程小规模,database 轮询
模式(0.4s)足够;事件表 + WS 端点自带,topic 名保持 cockpit 前端零改动。
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import context_from_token
from app.db import SessionLocal
from app.models import RealtimeEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/realtime", tags=["realtime"])


def serialize_realtime_event(event: RealtimeEvent) -> dict:
    return {
        "id": event.id,
        "topic": event.topic,
        "event_type": event.event_type,
        "resource_type": event.resource_type,
        "resource_id": event.resource_id,
        "payload": event.payload,
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def create_realtime_event(
    db: Session,
    *,
    tenant_id: int,
    event_type: str,
    resource_type: str,
    resource_id: str,
    payload: dict,
    topic: str = "cockpit",
) -> RealtimeEvent:
    """写 RealtimeEvent 表(WS 推送源);WS 循环 0.4s 轮询本表,无需唤醒通道。"""
    event = RealtimeEvent(
        tenant_id=tenant_id,
        topic=topic,
        event_type=event_type,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=payload,
    )
    db.add(event)
    db.flush()
    return event


def _events_after(tenant_id: int, topic: str, cursor: int) -> list[dict]:
    with SessionLocal() as db:
        rows = db.scalars(
            select(RealtimeEvent)
            .where(
                RealtimeEvent.tenant_id == tenant_id,
                RealtimeEvent.topic == topic,
                RealtimeEvent.id > cursor,
            )
            .order_by(RealtimeEvent.id)
            .limit(200)
        ).all()
        return [serialize_realtime_event(row) for row in rows]


def _event_cursor(tenant_id: int) -> int:
    """不带 after_id 时从最新位置起推(不回放历史)。"""
    from sqlalchemy import func

    with SessionLocal() as db:
        return int(
            db.scalar(select(func.coalesce(func.max(RealtimeEvent.id), 0)).where(
                RealtimeEvent.tenant_id == tenant_id
            ))
            or 0
        )


@router.websocket("/ws")
async def realtime_websocket(websocket: WebSocket) -> None:
    """事件总线 WS:?token=<jwt>&topic=cockpit&after_id=N(断线重放游标)。"""
    token = websocket.query_params.get("token", "")
    context = context_from_token(token)
    if not context:
        await websocket.close(code=4401, reason="Invalid credentials or permission")
        return
    topic = websocket.query_params.get("topic", "cockpit")
    if topic != "cockpit":
        await websocket.close(code=4400, reason="Unsupported topic")
        return
    after_id = websocket.query_params.get("after_id")
    try:
        cursor = max(int(after_id), 0) if after_id is not None else _event_cursor(context.tenant_id)
    except ValueError:
        await websocket.close(code=4400, reason="Invalid after_id")
        return
    await websocket.accept()
    await websocket.send_json(
        {"type": "connected", "topic": topic, "tenant_id": context.tenant_id, "cursor": cursor}
    )
    try:
        while True:
            await asyncio.sleep(0.4)
            events = _events_after(context.tenant_id, topic, cursor)
            for event in events:
                await websocket.send_json({"type": "event", "data": event})
                cursor = event["id"]
    except WebSocketDisconnect:
        pass
