"""会话 REST(local 模式):POST /conversations 建会话、GET /conversations 列表。

语音 WS start 必须绑定一个已存在会话(start 带 conversation_id);
平台桥模式下会话在平台侧创建,本路由仅供 local 独立模式使用。
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import TenantContext, require_context
from app.db import get_db
from app.models import VoiceConversation
from app.runtime import AgentRuntimeError, get_runtime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/conversations", tags=["voice-conversations"])


def serialize_conversation(row: VoiceConversation) -> dict:
    return {
        "id": row.id,
        "title": row.title,
        "agent_session_id": row.agent_session_id,
        "parent_conversation_id": row.parent_conversation_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


class CreateConversationBody(BaseModel):
    title: str | None = None


@router.post("")
async def create_conversation(
    payload: CreateConversationBody,
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    """建会话:先建 agent 侧会话,再落本地镜像行(agent 失败不落库)。"""
    try:
        agent_session_id = await get_runtime().create_session(title=payload.title)
    except AgentRuntimeError as exc:
        raise HTTPException(status_code=502, detail={"code": "AGENT_RUNTIME_ERROR", "message": str(exc)[:200]})
    row = VoiceConversation(
        tenant_id=context.tenant_id,
        user_id=context.user_id,
        title=payload.title or "新会话",
        agent_session_id=agent_session_id,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"data": serialize_conversation(row)}


@router.get("")
def list_conversations(
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    rows = db.scalars(
        select(VoiceConversation)
        .where(
            VoiceConversation.tenant_id == context.tenant_id,
            VoiceConversation.user_id == context.user_id,
        )
        .order_by(VoiceConversation.updated_at.desc())
        .limit(100)
    ).all()
    return {"data": [serialize_conversation(row) for row in rows]}
