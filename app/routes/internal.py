"""平台桥内部端点(X-Internal-Token 共享密钥;internal_token 未配置=全部 404)。

POST /internal/send-track — 平台 risk-confirm 放行后调:落发送追踪任务 +
启动盯梢协程,结算后经播报系统告知用户(替代平台时代 cockpit_ops 直接
函数调用 voice_tasks.create_send_track_task 的进程内耦合)。
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from app.auth import TenantContext, require_internal
from app.db import SessionLocal
from app.models import VoiceConversation
from app.services import voice_tasks
from app.services.track_probe import get_track_probe  # noqa: F401 — 注册探针

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal", tags=["internal"], dependencies=[Depends(require_internal)])


class SendTrackEntry(BaseModel):
    message_id: int
    customer_id: int


class SendTrackBody(BaseModel):
    tenant_id: int
    user_id: int
    conversation_id: int
    entries: list[SendTrackEntry]
    names: dict[str, str] = {}  # customer_id(字符串) → 客户名


@router.post("/send-track")
def create_send_track(payload: SendTrackBody, request: Request) -> dict:
    """落发送追踪任务(running)+ started 事件 + 启动盯梢。

    会话必须在本地(或桥接影子)存在,否则 404——平台侧 risk-confirm 带
    conversation_id 的前提是该会话已经历过语音连接。
    """
    with SessionLocal() as db:
        conv = db.scalars(
            select(VoiceConversation).where(
                VoiceConversation.id == payload.conversation_id,
                VoiceConversation.tenant_id == payload.tenant_id,
                VoiceConversation.user_id == payload.user_id,
            )
        ).first()
        if conv is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "CONVERSATION_NOT_FOUND", "message": "语音会话不存在(未连接过)"},
            )
        context = TenantContext(
            tenant_id=payload.tenant_id, user_id=payload.user_id, role="internal"
        )
        names = {int(k): v for k, v in payload.names.items()}
        task = voice_tasks.create_send_track_task(
            db,
            context,
            conversation_id=payload.conversation_id,
            entries=[e.model_dump() for e in payload.entries],
            names=names,
        )
        db.commit()
        task_id = task.id
    voice_tasks.start_send_track_watch(task_id)
    return {"data": {"task_id": task_id}}
