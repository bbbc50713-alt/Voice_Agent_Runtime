"""会话管理:自有 conversations 表 CRUD + 平台桥接解析(ConversationResolver)。

两种模式(配置 conversation_resolver):
- local(默认,纯独立):会话由本服务 POST /conversations 创建,agent_session_id
  来自当前 AgentRuntime 适配器;WS start 归属校验查本地表。
- http(平台桥):会话在平台创建(平台 cockpit_conversations),WS start 时经平台
  内部端点解析归属 + agent_session_id,成功后在本地 upsert 影子行——
  下游 voice_tasks 的同步 DB 读取零改动(影子行即缓存)。
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import TenantContext
from app.config import get_settings
from app.db import SessionLocal
from app.models import VoiceConversation
from app.runtime import get_runtime, session_key_for  # noqa: F401 — re-export 供路由用

logger = logging.getLogger(__name__)


class ConversationResolutionError(RuntimeError):
    """平台桥解析失败(网络/违约)——与"不存在"区分,前端可重试。"""


def owned_conversation(
    db: Session, context: TenantContext, conversation_id: int
) -> VoiceConversation | None:
    return db.scalars(
        select(VoiceConversation).where(
            VoiceConversation.id == conversation_id,
            VoiceConversation.tenant_id == context.tenant_id,
            VoiceConversation.user_id == context.user_id,
        )
    ).first()


async def resolve_for_voice(
    context: TenantContext, conversation_id: int
) -> VoiceConversation | None:
    """WS start 的会话解析入口:local 查本地表;http 走平台桥 + 影子行 upsert。

    返回 None=不存在/不属于本用户(调用方 close 4403);
    桥调用异常抛 ConversationResolutionError(调用方 close 1011,前端重试)。
    """
    settings = get_settings()
    if settings.conversation_resolver != "http":
        with SessionLocal() as db:
            return owned_conversation(db, context, conversation_id)
    if not settings.platform_api_base:
        raise ConversationResolutionError("conversation_resolver=http 但未配置 platform_api_base")
    url = f"{settings.platform_api_base.rstrip('/')}{settings.api_prefix}/internal/conversations/{conversation_id}"
    headers = {"X-Internal-Token": settings.internal_token} if settings.internal_token else {}
    try:
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            resp = await client.get(url, headers=headers)
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise ConversationResolutionError(f"platform bridge request failed: {exc}") from exc
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise ConversationResolutionError(f"platform bridge status {resp.status_code}")
    try:
        data: dict[str, Any] = resp.json()
        agent_session_id = str(data["agent_session_id"])
        tenant_id = int(data["tenant_id"])
        user_id = int(data["user_id"])
        title = str(data.get("title") or "新会话")
    except (ValueError, KeyError, TypeError) as exc:
        raise ConversationResolutionError(
            f"platform bridge response violates contract: {resp.text[:200]}"
        ) from exc
    if tenant_id != context.tenant_id or user_id != context.user_id:
        return None  # 归属不符视同不存在(fail-closed)
    # 影子行 upsert:本地表即缓存,下游同步读取零改动
    with SessionLocal() as db:
        row = db.get(VoiceConversation, conversation_id)
        if row is None:
            row = VoiceConversation(
                id=conversation_id,
                tenant_id=tenant_id,
                user_id=user_id,
                title=title,
                agent_session_id=agent_session_id,
            )
            db.add(row)
        else:
            row.agent_session_id = agent_session_id
            if row.title == "新会话" and title != "新会话":
                row.title = title
        try:
            db.commit()
        except Exception:  # 并发首连撞唯一键:回滚后重读即可
            db.rollback()
            row = db.get(VoiceConversation, conversation_id)
    return row


async def generate_conversation_title(visible: list[tuple[str, str]]) -> str | None:
    """用 LLM 按前几轮对话生成短标题;未配置/调用失败返回 None(调用方保留原标题)。

    自平台 cockpit_runtime.generate_conversation_title 平移;llm 未配置即 None。
    """
    settings = get_settings()
    if not settings.llm_base_url or not settings.llm_api_key:
        return None
    lines = [f"{'用户' if role == 'user' else '助手'}:{content[:200]}" for role, content in visible[:6]]
    if not lines:
        return None
    payload = {
        "model": settings.llm_model,
        "messages": [
            {
                "role": "system",
                "content": "你是会话标题生成器。根据对话内容输出一个不超过16字的中文标题,"
                "概括用户的核心意图。只输出标题本身,不要引号、标点收尾、换行或解释。",
            },
            {"role": "user", "content": "\n".join(lines)},
        ],
        "temperature": 0.3,
        "max_tokens": 64,
    }
    headers = {"Authorization": f"Bearer {settings.llm_api_key}"}
    try:
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            response = await client.post(
                f"{settings.llm_base_url.rstrip('/')}/chat/completions", json=payload, headers=headers
            )
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return None
    # 收敛模型自由发挥:取首行,去引号/首尾标点,限长
    title = raw.strip().splitlines()[0].strip().strip("「」\"'《》")[:30].rstrip("。!?.!?…~")
    return title or None
