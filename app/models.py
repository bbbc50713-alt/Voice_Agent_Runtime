"""语音域自有表(从平台 models.py:5311-5490 剥离,外键改纯 Integer)。

独立服务没有 users/tenants 表:tenant_id/user_id 来自 JWT claims,只作隔离键。
五张表:conversations(会话镜像)/ voice_tasks(任务账本)/ speaker_profiles(声纹)
/ speaker_events(话语事件账本)/ realtime_events(前端事件总线)。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class VoiceConversation(TimestampMixin, Base):
    """语音会话镜像:agent 侧会话的归属映射(与平台 CockpitConversation 同构)。

    agent_session_id 是唯一透传键——hermes 适配器下即 hermes session id;
    换其它 agent 时是各适配器返回的会话标识。
    """

    __tablename__ = "voice_conversations"
    __table_args__ = (Index("ix_voice_conv_tenant_user", "tenant_id", "user_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(160), default="新会话", nullable=False)
    agent_session_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    context: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # 子会话编排:由哪个父会话创建;NULL=顶层,只允许一层
    parent_conversation_id: Mapped[int | None] = mapped_column(
        ForeignKey("voice_conversations.id"), index=True, nullable=True
    )


class VoiceTask(TimestampMixin, Base):
    """语音业务任务账本:状态机与结果载体(唯一可信进度源)。

    status: pending → running → (waiting_approval) → done/failed/canceled
    announce_state: pending → queued → announcing → notified/shared/dismissed/interrupted;
    另含 deferred 与旧数据兼容态 consumed。
    """

    __tablename__ = "voice_tasks"
    __table_args__ = (
        Index("ix_voice_tasks_conv", "conversation_id"),
        Index("ix_voice_tasks_tenant_user", "tenant_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    conversation_id: Mapped[int] = mapped_column(Integer, nullable=False)  # 纯 Integer:桥接模式下是平台会话 id,本地无行
    kind: Mapped[str] = mapped_column(String(16), default="其他", nullable=False)
    task_text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    announced: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    announce_state: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    announce_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # 交付契约(2026-10-14 对齐平台后端 delivery dict):delivery_mode/artifact_format/
    # source_task_ids/group_intent/announce_policy/requires_skill;NULL=老任务走旧逻辑
    delivery: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # 幂等键(对齐平台后端):同一请求重复调用带相同键,命中进行中任务直接复用不重复受理
    dedupe_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    run_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)  # agent run_id
    progress: Mapped[list | None] = mapped_column(JSON, nullable=True)
    cards: Mapped[list | None] = mapped_column(JSON, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class SpeakerProfile(TimestampMixin, Base):
    """声纹注册表:一段 10s 语音的 CAM++ embedding(192 维 float JSON,~3KB)。

    单租户数十人量级,numpy 内存余弦比对,不上向量库(KISS)。
    """

    __tablename__ = "speaker_profiles"
    __table_args__ = (Index("ix_speaker_profiles_tenant", "tenant_id", "status"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # NULL=外部人
    display_name: Mapped[str] = mapped_column(String(64), nullable=False)
    role_hint: Mapped[str] = mapped_column(String(16), default="staff", nullable=False)
    embedding_ref: Mapped[str] = mapped_column(Text, nullable=False)  # JSON 向量
    enrolled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False)


class SpeakerEvent(Base):
    """话语事件账本:每段对齐后的人声一条——谁/何时/说了什么/叫没叫 AI。

    隐私:现场人声转写落此表 → 表级租户隔离 + 30 天清理,不对外暴露原文列表。
    """

    __tablename__ = "speaker_events"
    __table_args__ = (
        Index("idx_speaker_events_conv", "conversation_id", "start_ms"),
        Index("ix_speaker_events_tenant", "tenant_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    conversation_id: Mapped[int] = mapped_column(Integer, nullable=False)  # 同上:桥接模式纯 Integer
    voice_session_id: Mapped[str] = mapped_column(String(64), nullable=False)
    speaker_label: Mapped[str] = mapped_column(String(32), nullable=False)
    speaker_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    speaker_role: Mapped[str] = mapped_column(String(16), default="unknown", nullable=False)
    start_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    end_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    asr_conf: Mapped[float | None] = mapped_column(Float, nullable=True)
    overlap: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    addressed_to: Mapped[str] = mapped_column(String(16), default="unknown", nullable=False)
    is_wake: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    intent: Mapped[str | None] = mapped_column(String(32), nullable=True)
    topic: Mapped[str | None] = mapped_column(String(64), nullable=True)
    targets_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class RealtimeEvent(Base):
    """前端事件总线(精简版:砍掉平台 outbox/celery 唤醒,仅 database 轮询模式)。

    /realtime/ws 每 0.4s 轮询本表,after_id 游标断线重放。
    """

    __tablename__ = "voice_realtime_events"
    __table_args__ = (Index("ix_realtime_events_tenant_topic", "tenant_id", "topic", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(32), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(32), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
