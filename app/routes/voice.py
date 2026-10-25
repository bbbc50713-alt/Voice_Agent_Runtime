"""实时语音路由(routes/cockpit_voice.py) — 方案 docs/REALTIME_VOICE_V2_PLAN.md §3.7/§10.1.2④。

挂载前缀 /cockpit(经 api_prefix → /api/v1/cockpit)。

- GET  /cockpit/realtime-voice/status — {enabled, model, voice, voiceprint{configured,enrolled,available}}(未启用前端隐藏开关)
- POST /cockpit/speaker-profiles/enroll — 声纹录入(body=PCM16/16k mono ≥10s;同人覆盖)
- GET  /cockpit/speaker-profiles/me — {enrolled, enrolledAt}
- DELETE /cockpit/speaker-profiles/me — 注销(置 inactive)
- GET  /cockpit/voice-tasks?conversation_id=&limit=50 — 任务卡列表(按 created_at 升序)
- POST /cockpit/voice-tasks/{id}/cancel — 取消(§10.2;pending 直接 canceled,
  running/waiting_approval → runs_stop,终态由事件流收尾)
- POST /cockpit/voice-tasks/{id}/steer — 补充指令(§10.1;409=已不可引导)
- POST /cockpit/voice-tasks/{id}/approval — 审批抉择(§10.1;once|deny)
- WS   /cockpit/realtime-voice/ws?token=<jwt> — 浏览器 ↔ DashScope 双工代理(§2.1/§2.2)
  start 可带 voice_mode=pure|exclusive|multi(三模式,spirit-voice-modes-plan;
  缺省=legacy 全局开关行为);下行新增 speakers 事件(多人/专有模式簇数+当前说话人)

鉴权照抄 realtime.py 的 _authenticate 模式(query token → 手工建 TenantContext);
token 无效 close 4401,无 customer.read 权限或功能未启用 close 4403(先 close 不 accept)。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import random
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.auth import TenantContext, context_from_token, require_context
from app.config import get_settings
from app.db import SessionLocal, get_db
from app.models import (
    VoiceConversation,
    SpeakerProfile as CockpitSpeakerProfile,
    VoiceTask as CockpitVoiceTask,
)
from app import runtime as cockpit_runtime
from app.services import cockpit_voice, voice_sidecar, voice_tasks
from app.services import conversations as conversations_service

router = APIRouter(prefix="/cockpit", tags=["cockpit-voice"])

logger = logging.getLogger(__name__)

# 专有闸门(spirit-voice-modes-plan §4.3;2026-09-15 三态复核):
# 判"他人"立即拦截;判"未知/弱置信"先放行(fail-open)并挂一次性延迟复核——
# diarization 侧路比 qwen 主路慢 1-2s,换人后第一轮 response 靠复核追上补刀。
EXCLUSIVE_GATE_RECHECK_DELAY_S = 0.8


# --- 鉴权(jwt-claims 模式,app/auth.py;WS 与 REST 共用) --------------------------------


def _authenticate(token: str) -> TenantContext | None:
    return context_from_token(token)


def has_permission(context: TenantContext, permission: str) -> bool:
    """权限判定(平台 realtime.py _authenticate 同款裁剪):白名单角色授予语音域全部能力。"""
    return permission in context.permissions


def _owned_conversation(
    db: Session, context: TenantContext, conversation_id: int
) -> VoiceConversation:
    """会话归属校验(local 模式):不存在/不属于本用户 → 404(与平台语义一致)。"""
    conv = db.scalars(
        select(VoiceConversation).where(
            VoiceConversation.id == conversation_id,
            VoiceConversation.tenant_id == context.tenant_id,
            VoiceConversation.user_id == context.user_id,
        )
    ).first()
    if conv is None:
        raise HTTPException(status_code=404, detail={"code": "CONVERSATION_NOT_FOUND", "message": "会话不存在"})
    return conv


def _owned_task(db: Session, context: TenantContext, task_id: int) -> CockpitVoiceTask:
    task = db.get(CockpitVoiceTask, task_id)
    if task is None or task.tenant_id != context.tenant_id or task.user_id != context.user_id:
        raise HTTPException(status_code=404, detail={"code": "VOICE_TASK_NOT_FOUND", "message": "任务不存在"})
    return task


# --- HTTP 端点 ---------------------------------------------------------------------------


@router.get("/realtime-voice/status")
async def realtime_voice_status(
    context: TenantContext = Depends(require_context),
) -> dict:
    """前端语音开关预检 + 三模式可用性(spirit-voice-modes-plan §4.3)。

    voiceprint: configured=multi_speaker_enabled 全局开;enrolled=本人 active 声纹;
    available=diarization 服务健康(30s 缓存探测)——exclusive/multi 模式的门槛。
    """
    settings = get_settings()
    configured = bool(settings.multi_speaker_enabled)
    enrolled = (
        voice_sidecar.has_active_profile(context.tenant_id, int(context.user_id)) if configured else False
    )
    available = await voice_sidecar.diarization_healthy() if configured else False
    return {
        "data": {
            "enabled": bool(settings.realtime_voice_enabled),
            "model": settings.dashscope_realtime_model,
            "voice": settings.realtime_voice_name,
            "voiceprint": {"configured": configured, "enrolled": enrolled, "available": available},
        }
    }


# --- 声纹注册(spirit-voice-modes-plan §4.3: 录入弹框后端) -----------------------------------

ENROLL_MIN_BYTES = 10 * 16000 * 2  # 10s PCM16k mono s16le


def _deactivate_my_profiles(db: Session, tenant_id: int, user_id: int) -> None:
    db.execute(
        update(CockpitSpeakerProfile)
        .where(
            CockpitSpeakerProfile.tenant_id == tenant_id,
            CockpitSpeakerProfile.user_id == user_id,
            CockpitSpeakerProfile.status == "active",
        )
        .values(status="inactive")
    )


@router.post("/speaker-profiles/enroll")
async def enroll_speaker_profile(
    request: Request,
    context: TenantContext = Depends(require_context),
) -> dict:
    """声纹录入:body=原始 PCM16/16k mono(≥10s);经 diarization-service /enroll 出 embedding。

    同人覆盖:旧 active 行置 inactive,插新行;成功后失效侧路声纹缓存。
    """
    body = await request.body()
    if len(body) < ENROLL_MIN_BYTES:
        raise HTTPException(
            status_code=400,
            detail={"code": "VOICEPRINT_AUDIO_TOO_SHORT", "message": "录音不足 10 秒，请重新录制"},
        )
    if not await voice_sidecar.diarization_healthy():
        raise HTTPException(
            status_code=503,
            detail={"code": "VOICEPRINT_SERVICE_UNAVAILABLE", "message": "声纹服务未就绪，请稍后再试"},
        )
    settings = get_settings()
    client = voice_sidecar.DiarizationClient(settings.multi_speaker_diarization_url)
    try:
        embedding = await client.enroll(bytes(body))
    except voice_sidecar.DiarizationUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": "VOICEPRINT_SERVICE_UNAVAILABLE", "message": "声纹服务未就绪，请稍后再试"},
        ) from exc
    if embedding is None:
        raise HTTPException(
            status_code=422,
            detail={"code": "VOICEPRINT_QUALITY_REJECTED", "message": "有效人声不足，请在安静环境用正常音量说满 10 秒"},
        )
    # jwt-claims 模式无 users 表(平台时代此处查 User.name):落占位名
    display_name = f"用户{context.user_id}"
    with SessionLocal() as db:
        _deactivate_my_profiles(db, context.tenant_id, int(context.user_id))
        db.add(
            CockpitSpeakerProfile(
                tenant_id=context.tenant_id,
                user_id=int(context.user_id),
                display_name=display_name,
                role_hint="staff",
                embedding_ref=json.dumps(embedding),
                enrolled_at=datetime.utcnow(),
                status="active",
            )
        )
        db.commit()
    voice_sidecar.invalidate_speaker_profiles(context.tenant_id)
    return {"data": {"enrolled": True, "embeddingDim": len(embedding)}}


@router.get("/speaker-profiles/me")
def my_speaker_profile(
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    row = db.scalars(
        select(CockpitSpeakerProfile)
        .where(
            CockpitSpeakerProfile.tenant_id == context.tenant_id,
            CockpitSpeakerProfile.user_id == context.user_id,
            CockpitSpeakerProfile.status == "active",
        )
        .order_by(CockpitSpeakerProfile.id.desc())
        .limit(1)
    ).first()
    return {
        "data": {
            "enrolled": row is not None,
            "enrolledAt": row.enrolled_at.isoformat() if row is not None else None,
            "displayName": row.display_name if row is not None else None,
            "status": row.status if row is not None else None,
            "roleHint": row.role_hint if row is not None else None,
        }
    }


@router.delete("/speaker-profiles/me")
def delete_my_speaker_profile(
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    _deactivate_my_profiles(db, context.tenant_id, int(context.user_id))
    db.commit()
    voice_sidecar.invalidate_speaker_profiles(context.tenant_id)
    return {"data": {"enrolled": False}}


@router.get("/voice-tasks")
def list_voice_tasks(
    conversation_id: int = Query(...),
    limit: int = Query(default=50, ge=1, le=200),
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    # 桥接模式下影子行可能尚未建立(从未语音连接过)→ 空列表而非 404;
    # 任务行本身带 tenant/user 过滤,数据隔离不变
    try:
        conv = _owned_conversation(db, context, conversation_id)
        conv_id = conv.id
    except HTTPException:
        if get_settings().conversation_resolver != "http":
            raise
        conv_id = conversation_id
    rows = db.scalars(
        select(CockpitVoiceTask)
        .where(
            CockpitVoiceTask.conversation_id == conv_id,
            CockpitVoiceTask.tenant_id == context.tenant_id,
            CockpitVoiceTask.user_id == context.user_id,
        )
        .order_by(CockpitVoiceTask.created_at.asc())
        .limit(limit)
    ).all()
    return {"data": [voice_tasks.serialize_task(row) for row in rows]}


class AnnouncePolicyResetPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: int


@router.post("/voice-tasks/announce-policy/reset")
async def reset_announce_policy_endpoint(
    payload: AnnouncePolicyResetPayload,
    context: TenantContext = Depends(require_context),
) -> dict:
    """撤销会话内播报策略(前端徽标 ✕;REST 通道,语音断线也可用)。

    注意声明顺序:必须先于 /voice-tasks/{task_id}/* 注册,否则 announce-policy 被
    {task_id} 段捕获。桥接模式影子行不存在时同样放行(按会话 id 直清,任务行自带走属过滤)。
    """
    result = await voice_tasks.reset_announce_policy(
        context.tenant_id, int(context.user_id), payload.conversation_id
    )
    return {"data": result}


@router.post("/voice-tasks/{task_id}/cancel")
async def cancel_voice_task(
    task_id: int,
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    try:
        task = await voice_tasks.cancel_task(db, context, task_id)
    except voice_tasks.VoiceTaskNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "VOICE_TASK_NOT_FOUND", "message": "任务不存在"}) from exc
    except voice_tasks.VoiceTaskNotCancellable as exc:
        raise HTTPException(status_code=409, detail={"code": "VOICE_TASK_NOT_CANCELLABLE", "message": str(exc)}) from exc
    except cockpit_runtime.CockpitRuntimeError as exc:
        raise HTTPException(
            status_code=502, detail={"code": "COCKPIT_RUNTIME_ERROR", "message": f"智能引擎暂不可用：{exc}"}
        ) from exc
    return {"data": voice_tasks.serialize_task(task)}


class SteerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=2000)


@router.post("/voice-tasks/{task_id}/steer")
async def steer_voice_task(
    task_id: int,
    payload: SteerPayload,
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    task = _owned_task(db, context, task_id)
    if task.status != "running" or not task.run_ref:
        raise HTTPException(status_code=409, detail={"code": "VOICE_TASK_NOT_STEERABLE", "message": "任务当前不可引导"})
    try:
        accepted = await cockpit_runtime.runs_steer(task.run_ref, payload.text)
    except cockpit_runtime.CockpitRuntimeError as exc:
        raise HTTPException(
            status_code=502, detail={"code": "COCKPIT_RUNTIME_ERROR", "message": f"智能引擎暂不可用：{exc}"}
        ) from exc
    if not accepted:
        raise HTTPException(status_code=409, detail={"code": "VOICE_TASK_NOT_STEERABLE", "message": "任务已不可引导"})
    return {"data": {"id": task_id, "steered": True}}


class ApprovalPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    choice: Literal["once", "deny"]


class AnnounceActionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["defer", "dismiss", "share"]


@router.post("/voice-tasks/{task_id}/approval")
async def approve_voice_task(
    task_id: int,
    payload: ApprovalPayload,
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    task = _owned_task(db, context, task_id)
    if task.status != "waiting_approval" or not task.run_ref:
        raise HTTPException(status_code=409, detail={"code": "VOICE_TASK_NOT_WAITING", "message": "任务当前不在待审批状态"})
    try:
        await cockpit_runtime.runs_approval(task.run_ref, payload.choice)
    except cockpit_runtime.CockpitRuntimeError as exc:
        raise HTTPException(
            status_code=502, detail={"code": "COCKPIT_RUNTIME_ERROR", "message": f"智能引擎暂不可用：{exc}"}
        ) from exc
    if payload.choice == "deny":
        # 拒绝后由 run.cancelled/run.failed 事件收尾;此处仅回执
        return {"data": voice_tasks.serialize_task(task)}
    task.status = "running"
    db.commit()
    refreshed = voice_tasks.serialize_task(task)
    await voice_tasks.push_task_event(context.tenant_id, int(context.user_id), {"type": "task.updated", "task": refreshed})
    return {"data": refreshed}


@router.post("/voice-tasks/{task_id}/announce")
async def update_voice_task_announce(
    task_id: int,
    payload: AnnounceActionPayload,
    context: TenantContext = Depends(require_context),
    db: Session = Depends(get_db),
) -> dict:
    """任务卡播报状态操作:只改语音通知层,不影响任务执行主状态。"""
    task = _owned_task(db, context, task_id)
    if task.status not in ("done", "failed"):
        raise HTTPException(
            status_code=409,
            detail={"code": "VOICE_TASK_NOT_TERMINAL", "message": "任务尚未完成"},
        )
    active_session = cockpit_voice.get_active_voice_session(
        context.tenant_id, int(context.user_id)
    )
    if payload.action == "share":
        changed = await voice_tasks.consume_task_results(
            db, context, task.conversation_id, [task_id]
        )
        if changed and active_session is not None:
            active_session.consume_tasks([task_id])
        if changed:
            task = changed[0]
        else:
            db.refresh(task)
    elif payload.action == "dismiss":
        changed_ids = await voice_tasks.dismiss_task_results(
            db, context, task.conversation_id, [task_id]
        )
        if changed_ids and active_session is not None:
            active_session.defer_tasks(changed_ids)
        db.refresh(task)
    else:
        changed_ids = (await voice_tasks.defer_task_result(
            db, context, task.conversation_id, "", [task_id]
        ))[1]
        if changed_ids and active_session is not None:
            active_session.defer_tasks(changed_ids)
        db.refresh(task)
    return {"data": voice_tasks.serialize_task(task)}


# --- WS 双工代理(§2.1/§2.2/§3.7) ------------------------------------------------------------


async def _sender(websocket: WebSocket, outbound: asyncio.Queue) -> None:
    """唯一写浏览器协程:所有下行帧经 outbound 队列串行化。"""
    while True:
        kind, payload = await outbound.get()
        if kind == "json":
            await websocket.send_json(payload)
        else:
            await websocket.send_bytes(payload)


async def _browser_pump(
    websocket: WebSocket,
    session: cockpit_voice.DashScopeVoiceSession,
    outbound: asyncio.Queue,
    stop_event: asyncio.Event,
    sidecar: voice_sidecar.VoiceSidecar | None = None,
    audio_ready: asyncio.Event | None = None,
) -> None:
    """浏览器 → backend:二进制帧 → send_audio(+侧路副本);文本 JSON 按 type=start/stop/ping 处理。

    audio_ready 未置位(握手进行中)时二进制帧直接丢弃:握手数秒内 socket 会积压
    音频,留到泵启动后爆发回放会冲爆侧路滑窗队列(丢窗 → diarization 流空洞 →
    同一声源炸成两簇,人数虚涨)。丢掉开场几秒,换整段会话归属稳定。
    """
    try:
        while not stop_event.is_set():
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
            chunk = message.get("bytes")
            if chunk is not None:
                if audio_ready is not None and not audio_ready.is_set():
                    continue  # 握手未完成,丢帧防积压爆发
                await session.send_audio(chunk)
                if sidecar is not None:
                    try:
                        await sidecar.feed(chunk)  # 侧路永不阻塞主路(feed 内部不抛,双保险)
                    except Exception:  # noqa: BLE001
                        pass
                continue
            text = message.get("text")
            if not text:
                continue
            try:
                msg = json.loads(text)
            except ValueError:
                continue
            mtype = msg.get("type")
            if mtype == "stop":
                break
            if mtype == "ping":
                outbound.put_nowait(("json", {"type": "pong"}))
            if mtype == "send_text":
                # 语音模式下的文字输入(快捷胶囊/卡片追问):注入实时会话并触发回答
                if audio_ready is not None and not audio_ready.is_set():
                    continue  # 握手未完成,session 未建连,注入必失败
                text_in = str(msg.get("text") or "").strip()
                if text_in:
                    await session.inject_text(text_in)
                    await session.create_response()
            if mtype in ("playback.completed", "playback.interrupted"):
                # 浏览器真实播放完成/打断回执;旧客户端由 session 超时兜底。
                try:
                    seq = int(msg.get("seq"))
                except (TypeError, ValueError):
                    continue
                await session.ack_playback(
                    seq, interrupted=(mtype == "playback.interrupted")
                )
            # start 在建连阶段已消费;重复 start 忽略
    except WebSocketDisconnect:
        pass
    finally:
        stop_event.set()


async def _dispatch_function_call(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    event: dict,
) -> None:
    """response.function_call_arguments.done 按 name 分发(§10.3.1/§2.3/§10.2.1/§14)。

    独立 asyncio task 运行——ask 快查最长 45s,绝不能堵住 DashScope 接收循环。
    """
    name = event.get("name")
    call_id = str(event.get("call_id") or "")
    logger.info("voice function call: %s(call_id=%s)", name, call_id)
    try:
        args = json.loads(event.get("arguments") or "{}")
        if not isinstance(args, dict):
            args = {}
    except ValueError:
        args = {}
    try:
        if name == "submit_business_task":
            await _handle_submit_business_task(session, context, conversation_id, call_id, args)
        elif name == "cancel_business_task":
            await _handle_cancel_business_task(session, context, conversation_id, call_id, args)
        elif name == "ask_business_agent":
            await _handle_ask_business_agent(session, context, conversation_id, call_id, args)
        elif name == "defer_task_result":
            await _handle_defer_task_result(session, context, conversation_id, call_id, args)
        elif name == "mark_result_shared":
            await _handle_mark_result_shared(session, context, conversation_id, call_id, args)
        elif name == "set_announce_policy":
            await _handle_set_announce_policy(session, context, conversation_id, call_id, args)
        elif name == "dismiss_turn":
            await _handle_dismiss_turn(session, call_id)
        else:
            await session.send_function_output(call_id, f"未知函数 {name}")
    except Exception:  # noqa: BLE001 - 函数分发失败也要有回执,不让 qwen 干等
        logger.exception("function call %s 处理失败", name)
        try:
            await session.send_function_output(call_id, "系统处理失败，请稍后再试")
        except Exception:  # noqa: BLE001
            pass
    finally:
        # v3.3: function_call_output 不会自动触发开口,统一登记静默闸门跟随发言;
        # v3.5: dismiss_turn 豁免——它的语义就是"不要开口",followup 会把"继续旁听"念出来
        # mark_result_shared 同理(登记"已播报",回执静默,防"已记录"被念出来)
        if name not in ("dismiss_turn", "mark_result_shared"):
            session.schedule_followup()


async def _handle_submit_business_task(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    task_text = str(args.get("task") or "").strip()
    kind = str(args.get("kind") or "其他")
    if not task_text:
        await session.send_function_output(call_id, "没有听清任务内容，请用户再说一遍")
        return
    settings = get_settings()
    # 交付契约(2026-10-14 对齐后端):枚举/类型收敛后组 delivery,老调用(仅 task/kind)全空 → None 走旧逻辑
    delivery: dict | None = None
    dedupe_key: str | None = None
    if settings.cockpit_announce_policy_enabled:
        delivery_mode = str(args.get("delivery_mode") or "").strip()
        if delivery_mode not in ("voice_summary", "task_card", "file"):
            delivery_mode = ""
        artifact_format = str(args.get("artifact_format") or "").strip()
        if artifact_format not in ("docx", "md", "txt", "xlsx", "csv", "pptx"):
            artifact_format = ""
        source_task_ids = [
            int(x)
            for x in (args.get("source_task_ids") or [])
            if str(x).lstrip("-").isdigit() and int(x) > 0
        ]
        source_task_ids = list(dict.fromkeys(source_task_ids))[:50]
        announce_policy = str(args.get("announce_policy") or "").strip()
        if announce_policy not in ("ask", "direct", "file_ready", "silent_until_group"):
            announce_policy = ""
        delivery = {
            k: v
            for k, v in {
                "delivery_mode": delivery_mode or None,
                "artifact_format": artifact_format or None,
                "source_task_ids": source_task_ids or None,
                "group_intent": True if args.get("group_intent") else None,
                "announce_policy": announce_policy or None,
                "requires_skill": True if args.get("requires_skill") else None,
            }.items()
            if v is not None
        } or None
        dedupe_key = str(args.get("dedupe_key") or "").strip()[:64] or None
    with SessionLocal() as db:
        conv = db.get(VoiceConversation, conversation_id)
        if conv is None or conv.tenant_id != context.tenant_id or conv.user_id != context.user_id:
            await session.send_function_output(call_id, "会话不存在，请刷新后重试")
            return
        try:
            task = voice_tasks.submit_task(
                db, context, conv, task_text, kind, delivery=delivery, dedupe_key=dedupe_key
            )
        except voice_tasks.VoiceTaskBusy:
            await session.send_function_output(
                call_id, f"已有 {settings.realtime_voice_max_tasks_per_user} 个任务在处理，请稍后"
            )
            return
        task_id = task.id
        payload = voice_tasks.serialize_task(task)
        # 聚合父任务保持闸门:任一源任务在途 → 暂不派发,等子任务终态钩子(_maybe_dispatch_group_parent)
        hold = voice_tasks.should_hold_group_parent(db, task)
    if hold:
        logger.info("聚合父任务 #%s 源任务未全部终态,保持等待", task_id)
    else:
        voice_tasks.dispatch_task(task_id)
    # v3.4: 回执附开口指引(随机邀约话术池),followup 开口时模型换成自己的话说
    phrase = random.choice(cockpit_voice.CONTINUE_PHRASES)
    await session.send_function_output(
        call_id,
        f"任务已受理，编号 #{task_id}，正在后台处理。"
        f"（开口指引：用一两句话自然告诉用户任务已在后台处理、结果一出来马上告诉TA，"
        f"然后以「{phrase}」的意思邀约用户继续聊或问还有啥要帮忙——换成你自己的话说，"
        "每次说法都不一样。结果还没回来，开口只说在查/在处理，"
        "绝对不要复述或猜测任务的答案、不要提查询对象的任何属性）",
    )
    await voice_tasks.push_task_event(
        context.tenant_id, int(context.user_id), {"type": "task.created", "task": payload}
    )


async def _handle_cancel_business_task(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    hint = str(args.get("task_hint") or "")
    with SessionLocal() as db:
        resolved = voice_tasks.resolve_cancel_target(db, context, conversation_id, hint)
        if isinstance(resolved, list):
            listing = "；".join(f"#{t.id} {t.kind}·{t.task_text[:20]}" for t in resolved)
        else:
            listing = ""
        target_id = resolved.id if isinstance(resolved, CockpitVoiceTask) else None
    if resolved is None:
        receipt = "当前没有进行中的任务"
    elif isinstance(resolved, list):
        receipt = f"有多个进行中的任务：{listing}。请用户说明要取消哪一个"
    else:
        receipt = await voice_tasks.cancel_business_task(int(target_id))
    await session.send_function_output(call_id, receipt)


async def _handle_ask_business_agent(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    query = str(args.get("query") or "").strip()
    if not query:
        await session.send_function_output(call_id, "没有听清问题，请用户再说一遍")
        return
    answer = await voice_tasks.ask_business_agent(context, conversation_id, query)
    await session.send_function_output(call_id, answer[:2000])


async def _handle_defer_task_result(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    """§11.4 压下播报:落库 deferred + 从会话调度器摘队(defer 优先于打断重排)。"""
    hint = str(args.get("task_hint") or "")
    raw_ids = args.get("task_ids")
    task_ids: list[int] = []
    if isinstance(raw_ids, list):
        for raw in raw_ids:
            try:
                task_ids.append(int(raw))
            except (TypeError, ValueError):
                continue
    with SessionLocal() as db:
        receipt, ids = await voice_tasks.defer_task_result(
            db, context, conversation_id, hint, task_ids or None
        )
    if ids:
        session.defer_tasks(ids)
    await session.send_function_output(call_id, receipt)


async def _handle_mark_result_shared(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    """模型已口头告知结果(方案 rictor-starfire-orphan §B1): 落库 shared + 摘调度队列。

    静默回执(回执文案不会被念出——本函数已豁免 followup)。
    task_ids 数组优先;都没有回退单个 task_id(2026-10-14 对齐后端目录)。
    """
    raw_ids = args.get("task_ids")
    wanted: list[int] = []
    if isinstance(raw_ids, list):
        for raw in raw_ids:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value > 0 and value not in wanted:
                wanted.append(value)
    if not wanted:
        try:
            wanted = [int(args.get("task_id"))]
        except (TypeError, ValueError):
            await session.send_function_output(call_id, "（缺少任务编号,未登记）")
            return
    with SessionLocal() as db:
        tasks = await voice_tasks.consume_task_results(
            db, context, conversation_id, wanted[:20]
        )
    if tasks:
        session.consume_tasks([task.id for task in tasks])
    await session.send_function_output(call_id, "（已记录,无需开口）")


async def _handle_set_announce_policy(
    session: cockpit_voice.DashScopeVoiceSession,
    context: TenantContext,
    conversation_id: int,
    call_id: str,
    args: dict,
) -> None:
    """播报策略批量更新(2026-10-14 对齐后端 §A)。

    终态任务压下(deferred)或停止在播(interrupted),在途任务改写 delivery.announce_policy;
    本次精确命中的任务同时从会话调度器摘队(同 defer 语义,说完立即生效)。
    """
    if not get_settings().cockpit_announce_policy_enabled:
        await session.send_function_output(call_id, "播报策略功能未启用")
        return
    policy = str(args.get("policy") or "").strip()
    raw_ids = args.get("task_ids")
    task_ids: list[int] = []
    if isinstance(raw_ids, list):
        for raw in raw_ids:
            try:
                task_ids.append(int(raw))
            except (TypeError, ValueError):
                continue
    try:
        counts = await voice_tasks.set_announce_policy(
            context.tenant_id,
            int(context.user_id),
            policy,
            task_ids or None,
            conversation_id=conversation_id,
        )
    except voice_tasks.VoiceTaskError as exc:
        await session.send_function_output(call_id, str(exc))
        return
    deferred_ids = counts.get("deferred_ids") or []
    interrupted_ids = counts.get("interrupted_ids") or []
    if deferred_ids or interrupted_ids:
        session.defer_tasks([*deferred_ids, *interrupted_ids])
    parts = []
    if counts["deferred"]:
        parts.append(f"{counts['deferred']} 个已压下")
    if counts.get("interrupted"):
        parts.append(f"{counts['interrupted']} 个已停止播报")
    if counts["policy_updated"]:
        parts.append(f"{counts['policy_updated']} 个在途任务策略已更新")
    summary = "；".join(parts) if parts else "没有受影响的任务"
    await session.send_function_output(
        call_id,
        f"播报策略已更新为 {policy}：{summary}。"
        "（开口指引：用一句话自然告知用户后续任务完成后不会再逐个播报，按新策略通知即可）",
    )


async def _handle_dismiss_turn(
    session: cockpit_voice.DashScopeVoiceSession,
    call_id: str,
) -> None:
    """§14 旁听模式(v3.5): 模型判定"不是叫我"时的沉默出口。

    补 response.cancel 截断 semantic_vad 可能已自动起拍的残余音频;
    回执仅作协议闭环(function call 必须有 output),不登记 followup——
    豁免在 _dispatch_function_call 的 finally 里。
    """
    await session.cancel_response()
    await session.send_function_output(call_id, "（已知晓，继续旁听，不要开口）")


@router.websocket("/realtime-voice/ws")
async def realtime_voice_ws(websocket: WebSocket) -> None:
    settings = get_settings()
    context = _authenticate(websocket.query_params.get("token", ""))
    if context is None:
        await websocket.close(code=4401, reason="Invalid credentials")
        return
    if not has_permission(context, "customer.read"):
        await websocket.close(code=4403, reason="Missing permission")
        return
    if not settings.realtime_voice_enabled:
        await websocket.close(code=4403, reason="Realtime voice disabled")
        return

    await websocket.accept()
    await websocket.send_json({"type": "state", "state": "connecting"})

    # 等 start(连接后第一条控制消息;ping 先答,start 前的二进制帧丢弃)
    conversation_id: int | None = None
    voice_mode: str | None = None
    try:
        while conversation_id is None:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                return
            text = message.get("text")
            if not text:
                continue
            try:
                msg = json.loads(text)
            except ValueError:
                continue
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
                continue
            if msg.get("type") != "start":
                continue
            try:
                conversation_id = int(msg.get("conversation_id"))
            except (TypeError, ValueError):
                await websocket.send_json({"type": "error", "message": "start 缺少 conversation_id"})
                await websocket.close(code=4400)
                return
            # v3.6 三模式:可选 voice_mode(pure/exclusive/multi);缺省=legacy 兼容
            raw_mode = msg.get("voice_mode")
            if raw_mode is not None:
                if raw_mode not in cockpit_voice.VOICE_MODES:
                    await websocket.send_json({"type": "error", "message": f"未知语音模式 {raw_mode}"})
                    await websocket.close(code=4400)
                    return
                voice_mode = raw_mode
    except WebSocketDisconnect:
        return


    # 会话解析:local=本地表;http=平台桥 + 影子行 upsert(见 services/conversations.py)
    try:
        conv_start = await conversations_service.resolve_for_voice(context, conversation_id)
    except conversations_service.ConversationResolutionError:
        await websocket.send_json({"type": "error", "message": "会话服务暂时不可用，请重试"})
        await websocket.close(code=1011)
        return
    if conv_start is None:
        await websocket.send_json({"type": "error", "message": "会话不存在"})
        await websocket.close(code=4403)
        return

    # 显式模式守卫(spirit-voice-modes-plan §4.3): 用户显式选择的模式不做静默降级,
    # 门槛不满足 → error 帧 + close;legacy(None)行为不变。
    if voice_mode in ("exclusive", "multi"):
        if not settings.multi_speaker_enabled:
            await websocket.send_json({"type": "error", "message": "多人识别功能未启用，请联系管理员开启"})
            await websocket.close(code=4400)
            return
        if not await voice_sidecar.diarization_healthy():
            await websocket.send_json({"type": "error", "message": "声纹/分离服务未就绪，请稍后再试"})
            await websocket.close(code=4400)
            return
        if voice_mode == "exclusive" and not voice_sidecar.has_active_profile(
            context.tenant_id, int(context.user_id)
        ):
            await websocket.send_json({"type": "error", "message": "请先录入声纹再使用专有模式"})
            await websocket.close(code=4400)
            return

    instructions = cockpit_voice.load_voice_instructions(voice_mode)
    outbound: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
    stop_event = asyncio.Event()
    session: cockpit_voice.DashScopeVoiceSession | None = None
    gate_recheck_task: asyncio.Task | None = None  # exclusive-gate one-shot delayed recheck
    response_active = False  # recheck race guard: skip cancel if response.done already arrived

    async def _gate_recheck() -> None:
        """None 放行后的延迟复核:等慢 1-2s 的 diarization 追上了再判一次,他人声音 →
        补刀取消本轮回答。response.done/cancelled 到达即被取消,不补刀。"""
        try:
            await asyncio.sleep(EXCLUSIVE_GATE_RECHECK_DELAY_S)
        except asyncio.CancelledError:
            return
        if not response_active:
            return
        if getattr(session, "current_response_proactive", False):
            return  # 系统主动发言不过闸(见 response.created 豁免说明)
        if sidecar is not None and sidecar.current_speaker_is_holder is False:
            logger.info("专有闸门复核拦截: %s", sidecar.current_speaker_debug)
            try:
                await session.cancel_response()
            except Exception:  # noqa: BLE001 - 复核失败不传染主链路
                pass

    async def on_event(event: dict) -> None:
        """DashScope → backend 事件映射(§2.2),经 outbound 队列由 _sender 统一下发。"""
        nonlocal gate_recheck_task, response_active
        etype = event.get("type")
        if etype == "closed":  # DashScope 连接终结(内部事件)
            outbound.put_nowait(("json", {"type": "state", "state": "closed"}))
            stop_event.set()
        elif etype in ("session.created", "session.updated"):
            outbound.put_nowait(("json", {"type": "state", "state": "ready"}))
        elif etype == "input_audio_buffer.speech_started":
            outbound.put_nowait(("json", {"type": "playback.clear"}))
        elif etype == "conversation.item.input_audio_transcription.delta":
            outbound.put_nowait(("json", {
                "type": "input_transcript.delta",
                "text": event.get("transcript") or "",
                "stash": event.get("stash") or "",
            }))
        elif etype == "conversation.item.input_audio_transcription.completed":
            outbound.put_nowait(("json", {
                "type": "input_transcript.completed",
                "transcript": event.get("transcript") or "",
            }))
        elif etype == "response.created":
            # 专有模式机械闸门(spirit-voice-modes-plan §4.3;2026-09-15 三态):
            # False=他人 → 直接取消本轮回答(转写照常入上下文);
            # None=未知/弱置信 → 先放行(fail-open)并挂一次性延迟复核(diarization
            # 比 qwen 慢 1-2s,换人后第一轮靠复核追上补刀);True=本人放行。
            # 豁免(2026-09-23 任务#35 复读根因): 系统主动发言(静默闸门的回执/播报)
            # 没有触发用户,判"当前说话人"只会拿环境声/陈旧判定误杀播报 → 整轮 cancel
            # 被状态机当用户打断重排 → 同一任务播 2~3 遍。主动发言一律不过闸。
            response_active = True
            proactive = getattr(session, "current_response_proactive", False)
            if voice_mode == "exclusive" and sidecar is not None and not proactive:
                verdict = sidecar.current_speaker_is_holder
                if verdict is False:
                    logger.info("专有闸门拦截: %s", sidecar.current_speaker_debug)
                    await session.cancel_response()
                else:
                    outbound.put_nowait(("json", {"type": "state", "state": "responding"}))
                    if verdict is None:
                        if gate_recheck_task is not None and not gate_recheck_task.done():
                            gate_recheck_task.cancel()
                        gate_recheck_task = asyncio.create_task(_gate_recheck())
            else:
                outbound.put_nowait(("json", {"type": "state", "state": "responding"}))
        elif etype in ("response.done", "response.cancelled"):
            # 本轮说完(或被打断)→ 前端状态回 ready,播报结算由 session 内部状态机处理
            response_active = False
            if gate_recheck_task is not None:
                gate_recheck_task.cancel()
                gate_recheck_task = None
            outbound.put_nowait(("json", {"type": "state", "state": "ready"}))
        elif etype == "response.audio.delta":
            delta = event.get("delta")
            if delta:
                try:
                    outbound.put_nowait(("bytes", base64.b64decode(delta)))
                except (ValueError, binascii.Error):
                    pass
        elif etype == "response.audio_transcript.delta":
            outbound.put_nowait(("json", {"type": "output_transcript.delta", "delta": event.get("delta") or ""}))
        elif etype == "response.audio_transcript.done":
            outbound.put_nowait(("json", {"type": "output_transcript.done", "transcript": event.get("transcript") or ""}))
        elif etype == "response.function_call_arguments.done":
            if session is not None:
                asyncio.create_task(_dispatch_function_call(session, context, conversation_id, event))
        elif etype == "error":
            err = event.get("error")
            detail = err.get("message") if isinstance(err, dict) else str(err or event)
            logger.warning("dashscope realtime error: %s", detail)
            outbound.put_nowait(("json", {"type": "error", "message": str(detail)[:300]}))
        # §11 播报搭车/追问/打断重排均由 session 内部调度器闭环,路由层不介入

    session = cockpit_voice.DashScopeVoiceSession(
        instructions=instructions, on_event=on_event, voice_mode=voice_mode
    )
    # §11 播报状态机落库回调(调度器在 session 内,DB 归 voice_tasks)
    session.on_announce_transition = voice_tasks.apply_announce_transition

    async def _task_sink(payload: dict) -> None:
        outbound.put_nowait(("json", payload))

    session.on_task_event = _task_sink

    sender_task = asyncio.create_task(_sender(websocket, outbound))
    browser_task: asyncio.Task | None = None
    registered = False
    sidecar: voice_sidecar.VoiceSidecar | None = None
    # 音频闸门(2026-09-14 修复):握手(qwen 主路 + ASR 侧路,各数秒)完成前,
    # 泵读到的浏览器音频帧直接丢弃。否则 socket 积压在泵启动瞬间爆发灌进侧路
    # 滑窗 → 窗队列撑爆丢窗 → diarization 流出现数秒空洞 → 同一声源被炸成
    # 两个簇(多人模式单声源"人数虚涨"的根因)。丢掉开场几秒,换整段会话稳定。
    audio_ready = asyncio.Event()
    try:
        # 多人认知层侧路(ENGAGED_DRIFT Layer 1 + spirit-voice-modes-plan §4.3):
        # - legacy(None): multi_speaker_enabled 时起,起不来=降级无归属旁听(旧行为);
        # - exclusive/multi(显式): 守卫已在建连前过了闸,这里失败也不静默降级——
        #   error 帧 + close(用户显式选了模式,哑火比降级更诚实)。
        want_sidecar = voice_mode in ("exclusive", "multi") or (
            voice_mode is None and settings.multi_speaker_enabled
        )
        if want_sidecar:
            def _speakers_sink(count: int, current: str | None) -> None:
                outbound.put_nowait(("json", {"type": "speakers", "count": count, "current": current}))

            sidecar = voice_sidecar.VoiceSidecar(
                tenant_id=context.tenant_id,
                user_id=int(context.user_id),
                conversation_id=conversation_id,
                inject=session.inject_text,
                mode="exclusive" if voice_mode == "exclusive" else "multi",
                on_speakers=_speakers_sink,
            )

        # 浏览器泵先行(有泵读帧才丢得掉);qwen 主路握手与 ASR 侧路握手互不依赖,
        # 并行起跑;主路握手一完成就开音频闸门(用户说话即刻进 qwen),
        # 侧路 feed 自带 started 自闸,侧路就绪前天然丢帧,不会在缓冲里积压。
        browser_task = asyncio.create_task(
            _browser_pump(websocket, session, outbound, stop_event, sidecar, audio_ready)
        )
        connect_task = asyncio.create_task(session.connect())
        sidecar_task = asyncio.create_task(sidecar.start()) if sidecar is not None else None
        try:
            await connect_task
        except Exception:  # noqa: BLE001 - 建连失败(key 错/网络) → error + close(§5)
            logger.warning("dashscope realtime 建连失败", exc_info=True)
            if sidecar_task is not None:
                sidecar_task.cancel()  # 侧路实例由 finally 的 sidecar.close 兜底
            await websocket.send_json({"type": "error", "message": "语音服务暂不可用"})
            await websocket.close(code=1011)
            return
        audio_ready.set()  # 主路先通:用户说话即刻进 qwen,不等侧路
        cockpit_voice.register_voice_session(context.tenant_id, int(context.user_id), session)
        registered = True
        if sidecar_task is not None and sidecar is not None:
            try:
                sidecar_ok = await sidecar_task
            except Exception:  # noqa: BLE001
                logger.warning("voice sidecar 启动失败", exc_info=True)
                sidecar_ok = False
            if not sidecar_ok:
                await sidecar.close()
                sidecar = None
                if voice_mode is not None:
                    # 显式模式不静默降级:ASR 侧路起不来 → 告知用户后关闭
                    await websocket.send_json({"type": "error", "message": "声纹/分离服务未就绪，请稍后再试"})
                    await websocket.close(code=1011)
                    return
                # legacy(None): 降级继续跑(旧行为——diarization/质心照常;
                # 已 close 的 sidecar 实例 feed 为 no-op,泵持有它无害)
        if stop_event.is_set():  # 握手期间用户已断开
            return

        # 补注(§11.2):断线期间完成/被压下的任务重新注入
        # (pending 标准模板重排队,deferred 静默模板仅作上下文)
        await voice_tasks.reinject_on_connect(session, conversation_id)

        await stop_event.wait()
    except Exception:  # noqa: BLE001
        logger.exception("realtime voice ws 处理异常")
    finally:
        # try/finally 铁律:连接结束全清理
        stop_event.set()
        if gate_recheck_task is not None:
            gate_recheck_task.cancel()
        if browser_task is not None:
            browser_task.cancel()
        sender_task.cancel()
        for pending in (browser_task, sender_task):
            if pending is not None:
                try:
                    await pending
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        await session.close()
        if sidecar is not None:
            try:
                await sidecar.close()  # 在 session.close 之后:flush 尾窗若再注入,ws 已关也只记日志
            except Exception:  # noqa: BLE001
                pass
        if registered:
            cockpit_voice.unregister_voice_session(context.tenant_id, int(context.user_id), session)
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001 - 对端可能已断开
            pass
