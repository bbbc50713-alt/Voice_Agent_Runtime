"""语音业务任务账本(services/voice_tasks.py)。

方案 docs/REALTIME_VOICE_V2_PLAN.md §3.5 + §10.1.2(runs 通道)/§10.2(语音取消)/§10.3(双函数分流):

- submit_task: 并发上限校验(realtime_voice_max_tasks_per_user)+ 落行(status=pending);
  progress 首条记路由维度 {"step":"route","via":"ask|submit"}(§10.3.3 可观测)
- dispatch_task/_run: 后台 asyncio task 走 hermes POST /v1/runs + SSE 事件流;
  tool.started/completed 写 progress 并推 task.progress,approval.request 置
  waiting_approval,run.completed→摘要+done,run.failed→failed(人话版),
  run.cancelled→canceled;asyncio.timeout(realtime_voice_task_timeout)超时
  先 runs_stop 再 failed;runs 提交 429/5xx 回落 chat_sync(§10.1.4);
  终态后 announce_task(§3.5 _announce)。全程异常兜底,绝不让账本停在 running
- summarize_result: ≤200 字中文摘要,复用 settings.llm_* openai 兼容端点,
  容错同款 generate_conversation_title(失败返回 None → 截断原文前 200 字兜底)
- announce_task(§11 播报状态机 v3.2): 终态后注入"摘要+尾随指引"复合条目,
  登记会话播报调度(搭车/静默窗口/快任务直念);不在线保持 pending 等补注
- reinject_on_connect: 语音 WS 建连补注,pending/queued 标准模板重排队(超 24h 窗口的
  陈旧活跃态转 dismissed 不再开口),notified/deferred/interrupted 等仅静默上下文;
  apply_announce_transition: 调度器状态落库回调
- consume_task_result(mark_result_shared 函数): 模型已口头告知结果后置 shared
  并摘除播报调度,根治"用户主动问结果→模型答了→播报指令又说一遍"的说两遍
- defer_task_result(§11.4): 用户拒绝播报,announce_state→deferred 终态,不再主动提
- recover_interrupted_tasks: 启动恢复;有 run_ref 的 running/waiting_approval 先
  GET /v1/runs/{id} 探活,completed 补收尾,否则 failed("服务重启中断");探活失败一律 failed
- cancel_task(HTTP)/resolve_cancel_target/cancel_business_task(§10.2.2 消歧)
- ask_business_agent: §10.3.2 快查(chat_sync + realtime_voice_quick_timeout 封顶,
  超时/Busy 自动转 submit_task 异步并返回转文案)

铁律: 所有 DB 操作用 ``with SessionLocal() as db:`` 短开短关,不复用请求级
db(长连接场景);后台 task 独立短生命周期 Session。
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import re
import threading
from collections.abc import Container
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.models import VoiceConversation as CockpitConversation, VoiceTask as CockpitVoiceTask
from app.services.track_probe import TrackProbeError, get_track_probe
from app import runtime as cockpit_runtime
from app.services import cockpit_voice
from app.realtime_bus import create_realtime_event

if TYPE_CHECKING:
    from app.auth import TenantContext

logger = logging.getLogger(__name__)

KINDS = {"盘点", "筛选", "查询", "话术", "总结", "其他"}

# 占用并发配额/可取消的活跃状态(waiting_approval 是 running 的挂起变体,一并计入)
ACTIVE_STATUSES = ("pending", "running", "waiting_approval")
TERMINAL_STATUSES = ("done", "failed", "canceled")
ACTIVE_ANNOUNCE_STATES = ("pending", "queued", "announcing", "interrupted")
REINJECT_QUEUE_STATES = ("pending", "queued")
SILENT_ANNOUNCE_STATES = ("notified", "deferred", "dismissed", "shared", "consumed")
SHAREABLE_ANNOUNCE_STATES = (
    "pending",
    "queued",
    "announcing",
    "notified",
    "interrupted",
    "deferred",
    "dismissed",
)
FINAL_ANNOUNCE_STATES = ("shared", "consumed", "deferred", "dismissed")

# set_announce_policy 合法值(2026-10-14 对齐平台后端;不含 ask:防误把已关的全又打开)
SETTABLE_POLICIES = ("file_ready", "silent_until_group", "direct")

# --- 交付契约 instructions(对齐平台后端;submit_business_task 新参数 → 执行指引) ----------

FILE_EXPORT_INSTRUCTIONS = """【文件导出】本任务要求产出文件。完成后必须用 export_file 工具把最终结果导出为 {fmt} 文件,
并在最终输出末尾附上文件标记:[[file:相对路径|显示名]]。不要在输出里粘贴文件全文,一句话说明文件已生成即可。"""

SKILL_INSTRUCTIONS = """【技能使用】本任务允许你创建或使用 skill(技能)来完成工作:现有工具不够时,
可以新建 skill 沉淀能力,按 skill 的步骤产出结果;已有可用 skill 直接复用。"""

GROUP_INSTRUCTIONS = """【汇总任务】本任务是把多个已有任务的结果汇总成一份产出。各源任务结果如下:
{items}
请交叉统计与主题归类(结论先行、数据表格化、差异与风险单列),优先参考 cockpit-report-writer 技能;
用 export_file 工具导出汇总文件,输出末尾附 [[file:相对路径|显示名]] 标记。"""


def _clean_source_ids(raw: Any) -> list[int]:
    """delivery.source_task_ids 收敛为正 int 列表(去重、封顶 50,防模型乱传)。"""
    ids: list[int] = []
    for item in raw or []:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in ids:
            ids.append(value)
    return ids[:50]


# 聚合素材注入上限(对齐后端 §C1):单任务 4000 字防超长结果撑爆上下文,
# 总 24000 字(约 24k token,hermes 128k 上下文内安全);超总 cap 按任务顺序截断并注明。
_GROUP_ITEM_CAP = 4000
_GROUP_TOTAL_CAP = 24000


def _group_instructions(source_ids: list[int], self_id: int | None = None) -> str:
    """聚合父任务 instructions:子任务结果全文注入(独立短会话,主流程 db 此时已关闭)。

    全文注入:result_text 优先于 result_summary,带状态标注,
    单任务封顶 _GROUP_ITEM_CAP 字,总量封顶 _GROUP_TOTAL_CAP 字。
    环防御:排除 delivery 非空的父任务(父任务不能再当源,防聚合死锁)+ 排除自身 id。
    """
    with SessionLocal() as gdb:
        rows = gdb.scalars(
            select(CockpitVoiceTask).where(CockpitVoiceTask.id.in_(source_ids))
        ).all()
        lines: list[str] = []
        total = 0
        shown = 0
        for row in rows:
            if self_id is not None and row.id == self_id:
                continue  # 环防御:排除自身
            if row.delivery is not None:
                continue  # 环防御:父任务不能再当源(防聚合死锁)
            outcome = (row.result_text or row.result_summary or row.error or "")[:_GROUP_ITEM_CAP]
            line = f"#{row.id}「{(row.task_text or '')[:40]}」({row.status}):{outcome}"
            if total + len(line) > _GROUP_TOTAL_CAP:
                lines.append(f"(以下源任务结果从略:共 {len(rows)} 个,已展示前 {shown} 个)")
                break
            lines.append(line)
            total += len(line)
            shown += 1
    joined = "\n".join(lines) if lines else "(源任务结果暂缺)"
    return GROUP_INSTRUCTIONS.format(items=joined)


class VoiceTaskError(RuntimeError):
    """语音账本操作失败基类。"""


class VoiceTaskBusy(VoiceTaskError):
    """并发任务超限(realtime_voice_max_tasks_per_user)。"""


class VoiceTaskNotFound(VoiceTaskError):
    """任务不存在或归属不符(路由层转 404)。"""


class VoiceTaskNotCancellable(VoiceTaskError):
    """任务已处终态,不可取消(路由层转 409)。"""


# --- 序列化与推送 -----------------------------------------------------------------------


def serialize_task(task: CockpitVoiceTask) -> dict[str, Any]:
    """任务卡 DTO(§4.2 VoiceTask + §10.1.2③ progress/cards)。

    approval 字段:waiting_approval 态时取最近一条等待中的审批步骤描述,供前端审批卡展示。
    """
    approval: str | None = None
    if task.status == "waiting_approval":
        for item in reversed(task.progress or []):
            if item.get("step") == "approval" and item.get("state") == "waiting":
                approval = str(item.get("description") or "") or None
                break
    return {
        "id": task.id,
        "conversationId": task.conversation_id,
        "kind": task.kind,
        "taskText": task.task_text,
        "status": task.status,
        "announced": bool(task.announced),
        "announceState": task.announce_state,
        "delivery": task.delivery,  # 交付契约透传(播报策略徽标/聚合标记,2026-10-14)
        "resultSummary": task.result_summary,
        "resultText": task.result_text,
        "error": task.error,
        "runRef": task.run_ref,
        "progress": task.progress,
        "cards": task.cards,
        "approval": approval,
        "createdAt": task.created_at.isoformat() if task.created_at else None,
        "finishedAt": task.finished_at.isoformat() if task.finished_at else None,
    }


async def push_task_event(tenant_id: int, user_id: int, payload: dict[str, Any]) -> None:
    """向该用户当前活跃语音会话推任务事件(task.created/updated/progress)。

    不在通话中则静默丢弃——任务卡由 HTTP 拉取兜底,事件只是实时增强。
    """
    session = cockpit_voice.get_active_voice_session(tenant_id, user_id)
    sink = getattr(session, "on_task_event", None) if session is not None else None
    if sink is None:
        return
    try:
        await sink(payload)
    except Exception:  # noqa: BLE001 - 推送失败不影响账本主链路
        logger.warning("task event 推送失败(%s)", payload.get("type"), exc_info=True)


async def _push_task_update(task: CockpitVoiceTask) -> None:
    """推送单条任务卡刷新;调用方负责已完成 DB commit。"""
    await push_task_event(
        task.tenant_id,
        task.user_id,
        {"type": "task.updated", "task": serialize_task(task)},
    )


async def set_announce_state_for_tasks(
    db: Session,
    context: TenantContext,
    conversation_id: int,
    task_ids: list[int],
    state: str,
    *,
    allowed_states: Container[str] = SHAREABLE_ANNOUNCE_STATES,
) -> list[int]:
    """批量设置播报状态(HTTP/UI 与工具共用),只影响播报层,不改任务主状态。"""
    if state not in cockpit_voice.ANNOUNCE_STATES:
        return []
    ids = sorted({int(tid) for tid in task_ids if int(tid) > 0})
    if not ids:
        return []
    rows = list(
        db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.id.in_(ids),
                CockpitVoiceTask.tenant_id == context.tenant_id,
                CockpitVoiceTask.user_id == int(context.user_id),
                CockpitVoiceTask.conversation_id == conversation_id,
                CockpitVoiceTask.status.in_(("done", "failed")),
                CockpitVoiceTask.announce_state.in_(tuple(allowed_states)),
            )
            .order_by(CockpitVoiceTask.created_at.desc())
        ).all()
    )
    changed: list[CockpitVoiceTask] = []
    for row in rows:
        row.announce_state = state
        row.announced = True
        _emit_voice_task_event(db, row, "cockpit.voice_task.updated")
        changed.append(row)
    if not changed:
        return []
    db.commit()
    for row in changed:
        await _push_task_update(row)
    return [row.id for row in changed]


# --- 受理与执行 -------------------------------------------------------------------------


def submit_task(
    db: Session,
    context: TenantContext,
    conv: CockpitConversation,
    task_text: str,
    kind: str,
    *,
    via: str = "submit",
    delivery: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> CockpitVoiceTask:
    """校验并发上限(统计该用户 pending+running+waiting_approval)→ 落行(pending)→ commit。

    超限抛 VoiceTaskBusy;kind 不在 KINDS 收敛为「其他」;via 记进 progress 首条(§10.3.3)。
    delivery: 交付契约(delivery_mode/artifact_format/source_task_ids/announce_policy 等)。
    dedupe_key 非空时先查同用户同键活跃任务,命中直接复用旧行(模型重复调用幂等,不重复受理)。
    """
    settings = get_settings()
    if dedupe_key:
        dedupe_key = dedupe_key[:64]
        existing = db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.tenant_id == context.tenant_id,
                CockpitVoiceTask.user_id == int(context.user_id),
                CockpitVoiceTask.dedupe_key == dedupe_key,
                CockpitVoiceTask.status.in_(ACTIVE_STATUSES),
            )
            .order_by(CockpitVoiceTask.id.desc())
            .limit(1)
        ).first()
        if existing is not None:
            logger.info("语音任务 dedupe 命中 #%s(dedupe_key=%s)", existing.id, dedupe_key)
            return existing
    active = db.scalar(
        select(func.count(CockpitVoiceTask.id)).where(
            CockpitVoiceTask.tenant_id == context.tenant_id,
            CockpitVoiceTask.user_id == context.user_id,
            CockpitVoiceTask.status.in_(ACTIVE_STATUSES),
        )
    ) or 0
    if active >= settings.realtime_voice_max_tasks_per_user:
        raise VoiceTaskBusy(f"已有 {active} 个任务在处理")
    task = CockpitVoiceTask(
        tenant_id=context.tenant_id,
        user_id=int(context.user_id),
        conversation_id=conv.id,
        kind=kind if kind in KINDS else "其他",
        task_text=task_text,
        status="pending",
        announced=False,
        delivery=delivery,
        dedupe_key=dedupe_key,
        progress=[{"step": "route", "via": via, "at": datetime.utcnow().isoformat()}],
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    _emit_voice_task_event(db, task, "cockpit.voice_task.created")
    db.commit()
    logger.info(
        "语音任务受理 #%s(tenant=%s user=%s via=%s): %s",
        task.id, task.tenant_id, task.user_id, via, task_text[:60],
    )
    return task


def dispatch_task(task_id: int) -> None:
    """由 WS 处理器调用:asyncio.create_task(_run(task_id))。

    独立短生命周期 SessionLocal,不复用请求级 db(长连接场景铁律)。
    """
    asyncio.create_task(_run(task_id))


async def _run(task_id: int) -> None:
    """账本执行器(§10.1.2②): runs 通道消费事件流至终态。

    全程异常兜底,绝不让账本停在 running/waiting_approval。
    """
    try:
        with SessionLocal() as db:
            task = db.get(CockpitVoiceTask, task_id)
            if task is None or task.status != "pending":
                return
            conv = db.get(CockpitConversation, task.conversation_id)
            if conv is None:
                task.status = "failed"
                task.error = "会话不存在"
                task.finished_at = datetime.utcnow()
                db.commit()
                return
            task.status = "running"
            db.commit()
            tenant_id, user_id = task.tenant_id, task.user_id
            agent_session_id = conv.agent_session_id
            task_text = task.task_text
            conversation_id = conv.id
            delivery = task.delivery

        session_key = cockpit_runtime.session_key_for(tenant_id, user_id)

        # 提交 run;429/5xx 回落 chat_sync 降级通道(§10.1.4,卡片无进度只有终态)
        # instructions 注入软阻断风险卡纪律(出卡轮一句话、文字确认轮禁止重复调用)
        # + 驾驶舱上下文(当前会话ID/子会话编排,语音里也能开子会话)
        # + 交付契约(2026-10-14):file→export_file 导出;requires_skill→允许建/用 skill;
        #   source_task_ids→聚合父任务(注入子任务结果全文,要求汇总导出)
        extra_instructions: list[str] = []
        if delivery:
            if delivery.get("delivery_mode") == "file":
                extra_instructions.append(
                    FILE_EXPORT_INSTRUCTIONS.format(fmt=delivery.get("artifact_format") or "docx")
                )
            if delivery.get("requires_skill"):
                extra_instructions.append(SKILL_INSTRUCTIONS)
            source_ids = _clean_source_ids(delivery.get("source_task_ids"))
            if source_ids:
                extra_instructions.append(_group_instructions(source_ids, self_id=task_id))
        instructions = (
            cockpit_runtime.RISK_CARD_INSTRUCTIONS
            + "\n"
            + cockpit_runtime.COCKPIT_CONTEXT_INSTRUCTIONS.format(conv_id=conversation_id)
        )
        if extra_instructions:
            instructions = instructions + "\n" + "\n".join(extra_instructions)
        try:
            run_id = await cockpit_runtime.runs_submit(
                agent_session_id,
                task_text,
                session_key=session_key,
                instructions=instructions,
            )
        except (cockpit_runtime.CockpitRuntimeBusy, cockpit_runtime.CockpitRuntimeUnavailable) as exc:
            logger.warning("runs_submit 失败(%s),任务 #%s 回落 chat_sync 通道", exc, task_id)
            output = await cockpit_runtime.chat_sync(
                agent_session_id, task_text, session_key=session_key
            )
            await _finalize_done(task_id, output)
            await announce_task(task_id)
            return

        # 落 run_ref + 推 task.updated(前端卡片开始转圈);同时广播 voice_task.updated
        # (topic=cockpit 持久化通道,文字模式账本即时翻"进行中")
        with SessionLocal() as db:
            task = db.get(CockpitVoiceTask, task_id)
            if task is None:
                return
            task.run_ref = run_id
            _emit_voice_task_event(db, task, "cockpit.voice_task.updated")
            db.commit()
            payload = serialize_task(task)
        await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})

        # 消费事件流;超时(realtime_voice_task_timeout)先 runs_stop 再 failed
        try:
            async with asyncio.timeout(get_settings().realtime_voice_task_timeout):
                await _consume_run_events(task_id, run_id)
        except TimeoutError:
            logger.warning(
                "语音任务 #%s 超时(%ss),停止 run %s",
                task_id, get_settings().realtime_voice_task_timeout, run_id,
            )
            try:
                await cockpit_runtime.runs_stop(run_id)
            except cockpit_runtime.CockpitRuntimeError:
                logger.warning("runs_stop(%s) 失败,忽略", run_id, exc_info=True)
            await _finalize_failed(task_id, "任务执行超时，已自动停止")

        # 防御:事件流提前结束(未收到终态事件)也收口,不留 running
        with SessionLocal() as db:
            task = db.get(CockpitVoiceTask, task_id)
            status = task.status if task else None
        if status in ACTIVE_STATUSES:
            await _finalize_failed(task_id, "任务执行通道中断")
            status = "failed"
        if status in ("done", "failed"):
            await announce_task(task_id)
    except Exception as exc:  # noqa: BLE001 - 兜底铁律:账本绝不停在 running
        logger.exception("语音任务 #%s 执行异常", task_id)
        try:
            await _finalize_failed(task_id, _human_error(exc))
            await announce_task(task_id)
        except Exception:  # noqa: BLE001
            logger.exception("语音任务 #%s 收尾失败", task_id)


async def _consume_run_events(task_id: int, run_id: str) -> None:
    """消费 runs_events;收到终态事件(completed/failed/cancelled)落库后返回。"""
    async for ev in cockpit_runtime.runs_events(run_id):
        etype = ev.get("event")  # 类型在 JSON 的 event 字段(SSE 无 event: 行)
        if etype == "tool.started":
            tool = str(ev.get("tool") or "工具")
            step: dict[str, Any] = {
                "step": tool,
                "at": datetime.utcnow().isoformat(),
                "state": "started",
            }
            if ev.get("preview"):
                step["preview"] = str(ev["preview"])[:200]
            with SessionLocal() as db:
                task = db.get(CockpitVoiceTask, task_id)
                if task is None or task.status not in ACTIVE_STATUSES:
                    return
                # 深拷贝:JSON 列内层 dict 原地突变不会被 SQLAlchemy 判脏,commit 丢更新
                progress = copy.deepcopy(task.progress) if task.progress else []
                progress.append(step)
                task.progress = progress
                db.commit()
                tenant_id, user_id = task.tenant_id, task.user_id
            await push_task_event(tenant_id, user_id, {"type": "task.progress", "taskId": task_id, "step": step})
        elif etype == "tool.completed":
            tool = str(ev.get("tool") or "工具")
            with SessionLocal() as db:
                task = db.get(CockpitVoiceTask, task_id)
                if task is None or task.status not in ACTIVE_STATUSES:
                    return
                progress = copy.deepcopy(task.progress) if task.progress else []
                closed: dict[str, Any] | None = None
                for item in reversed(progress):
                    if item.get("step") == tool and item.get("state") == "started":
                        closed = item
                        break
                if closed is None:
                    closed = {"step": tool, "at": datetime.utcnow().isoformat(), "state": "started"}
                    progress.append(closed)
                closed["state"] = "error" if ev.get("error") else "done"
                if ev.get("duration") is not None:
                    closed["duration"] = ev.get("duration")
                if ev.get("error"):
                    closed["error"] = str(ev["error"])[:200]
                task.progress = progress
                db.commit()
                tenant_id, user_id = task.tenant_id, task.user_id
            await push_task_event(tenant_id, user_id, {"type": "task.progress", "taskId": task_id, "step": closed})
        elif etype == "approval.request":
            step = {
                "step": "approval",
                "at": datetime.utcnow().isoformat(),
                "state": "waiting",
                "description": str(ev.get("description") or "")[:200],
            }
            with SessionLocal() as db:
                task = db.get(CockpitVoiceTask, task_id)
                if task is None or task.status not in ACTIVE_STATUSES:
                    return
                task.status = "waiting_approval"
                progress = copy.deepcopy(task.progress) if task.progress else []
                progress.append(step)
                task.progress = progress
                db.commit()
                tenant_id, user_id = task.tenant_id, task.user_id
                payload = serialize_task(task)
            await push_task_event(tenant_id, user_id, {"type": "task.progress", "taskId": task_id, "step": step})
            await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
        elif etype == "approval.responded":
            # 审批已抉择(经 /approval 端点):闭合等待中的审批步骤,状态回到 running
            with SessionLocal() as db:
                task = db.get(CockpitVoiceTask, task_id)
                if task is None or task.status != "waiting_approval":
                    return
                progress = copy.deepcopy(task.progress) if task.progress else []
                for item in reversed(progress):
                    if item.get("step") == "approval" and item.get("state") == "waiting":
                        item["state"] = "resolved"
                        item["choice"] = str(ev.get("choice") or "")
                        break
                task.progress = progress
                task.status = "running"
                db.commit()
                tenant_id, user_id = task.tenant_id, task.user_id
                payload = serialize_task(task)
            await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
        elif etype == "run.completed":
            await _finalize_done(task_id, str(ev.get("output") or ""))
            return
        elif etype == "run.failed":
            await _finalize_failed(task_id, _human_run_error(ev.get("error")))
            return
        elif etype == "run.cancelled":
            await _finalize_canceled(task_id)
            return
        # message.delta / reasoning.available / subagent.* / run.steered 等不影响账本,忽略


# --- 终态落库(调用方均在事件循环内: _run / _recover_one) -------------------------------------

# 与前端 RISK_CARD_RE 同款:闭合标签可有可无(流式截断兜底)
_RISK_CARD_RE = re.compile(r"\[RISK_CARD\]([\s\S]+?)(?:\[/RISK_CARD\]|$)")


# 文件卡 marker(对齐后端):export_file 工具输出尾部的 [[file:相对路径|显示名]]
_FILE_CARD_RE = re.compile(r"\[\[file:([^|\]]+)\|([^\]]+)\]\]")


def _extract_risk_card(output: str) -> dict[str, Any] | None:
    """run 输出里的软阻断风险卡 marker → payload;语音任务卡据此渲染放行按钮。

    语音路径的 result_summary 是 LLM 压缩摘要,marker 会丢——不提取出来,语音用户
    遇软阻断就没有卡片可点。字段不合法返回 None(fail-closed,与前端解析同口径)。
    """
    match = _RISK_CARD_RE.search(output or "")
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except ValueError:
        return None
    if (
        isinstance(data, dict)
        and isinstance(data.get("risk_token"), str)
        and isinstance(data.get("items"), list)
        and data["items"]
    ):
        return data
    return None


# 与前端 SUBCONV_CARD_RE 同款:闭合标签可有可无(流式截断兜底)
_SUBCONV_CARD_RE = re.compile(r"\[SUBCONV_CARD\]([\s\S]+?)(?:\[/SUBCONV_CARD\]|$)")


def _extract_subconv_card(output: str) -> dict[str, Any] | None:
    """run 输出里的子会话卡 marker → payload;语音任务卡据此渲染「查看子会话」入口。

    语音路径的 result_summary 是 LLM 压缩摘要,marker 会丢——不提取出来,语音用户
    开了子会话却找不到入口。字段不合法返回 None(fail-closed,与风险卡同口径)。
    """
    match = _SUBCONV_CARD_RE.search(output or "")
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except ValueError:
        return None
    if (
        isinstance(data, dict)
        and isinstance(data.get("conversation_id"), int)
        and isinstance(data.get("title"), str)
    ):
        return data
    return None


async def _finalize_done(task_id: int, output: str) -> None:
    """run.completed/探活补收尾/降级通道:摘要 → 写 result_text/result_summary/finished_at → done。"""
    summary = await summarize_result(output)
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status not in ACTIVE_STATUSES:
            return
        task.result_text = output
        task.result_summary = summary
        # 软阻断风险卡/子会话卡随任务上屏:语音用户点卡即放行/直达子会话
        cards: list[dict[str, Any]] = []
        risk = _extract_risk_card(output)
        if risk is not None:
            cards.append({"type": "cockpit_risk", **risk})
        subconv = _extract_subconv_card(output)
        if subconv is not None:
            cards.append({"type": "cockpit_subconv", **subconv})
        # 文件产出卡(2026-10-14 对齐后端):export_file 输出尾部的 [[file:...]] marker → 任务卡可预览/下载
        for match in _FILE_CARD_RE.finditer(output or ""):
            cards.append(
                {
                    "type": "cockpit_file",
                    "relpath": match.group(1).strip(),
                    "name": match.group(2).strip(),
                }
            )
        if cards:
            task.cards = cards
        task.status = "done"
        task.finished_at = datetime.utcnow()
        _emit_voice_task_event(db, task, "cockpit.voice_task.updated")
        _emit_subconv_progress(db, task, "done", summary)
        db.commit()
        tenant_id, user_id = task.tenant_id, task.user_id
        payload = serialize_task(task)
    await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
    await _maybe_dispatch_group_parent(task_id, tenant_id, user_id)


async def _finalize_failed(task_id: int, error: str) -> None:
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status not in ACTIVE_STATUSES:
            return
        task.status = "failed"
        task.error = error[:500]
        task.finished_at = datetime.utcnow()
        _emit_voice_task_event(db, task, "cockpit.voice_task.updated")
        _emit_subconv_progress(db, task, "failed", task.error)
        db.commit()
        tenant_id, user_id = task.tenant_id, task.user_id
        payload = serialize_task(task)
    await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
    await _maybe_dispatch_group_parent(task_id, tenant_id, user_id)


async def _finalize_canceled(task_id: int) -> None:
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status not in ACTIVE_STATUSES:
            return
        task.status = "canceled"
        task.finished_at = datetime.utcnow()
        _emit_voice_task_event(db, task, "cockpit.voice_task.updated")
        db.commit()
        tenant_id, user_id = task.tenant_id, task.user_id
        payload = serialize_task(task)
    await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
    await _maybe_dispatch_group_parent(task_id, tenant_id, user_id)


async def _maybe_dispatch_group_parent(child_id: int, tenant_id: int, user_id: int) -> None:
    """子任务终态后检查聚合父任务:source_task_ids 全部终态 → 派发父任务。

    父任务靠 delivery.source_task_ids 反查(不用 parent 指针,子任务可能早于父任务创建);
    候选量小(单用户 pending 父任务),Python 侧过滤,sqlite/pg 通用。
    """
    try:
        with SessionLocal() as db:
            candidates = db.scalars(
                select(CockpitVoiceTask).where(
                    CockpitVoiceTask.tenant_id == tenant_id,
                    CockpitVoiceTask.user_id == user_id,
                    CockpitVoiceTask.status == "pending",
                    CockpitVoiceTask.delivery.isnot(None),
                )
            ).all()
            for parent in candidates:
                source_ids = _clean_source_ids((parent.delivery or {}).get("source_task_ids"))
                if child_id not in source_ids:
                    continue
                terminal = db.scalar(
                    select(func.count(CockpitVoiceTask.id)).where(
                        CockpitVoiceTask.id.in_(source_ids),
                        CockpitVoiceTask.status.in_(TERMINAL_STATUSES),
                    )
                ) or 0
                if terminal == len(source_ids):
                    parent_id = parent.id
                    logger.info(
                        "聚合父任务 #%s 全部 %s 个源任务终态,派发执行", parent_id, len(source_ids)
                    )
                    dispatch_task(parent_id)
    except Exception:  # noqa: BLE001 - 聚合派发失败不影响子任务主链路
        logger.exception("聚合父任务检查失败(child=%s)", child_id)


def should_hold_group_parent(db: Session, task: CockpitVoiceTask) -> bool:
    """聚合父任务提交时是否暂不派发:任一源任务在途(或源 id 缺失)→ 等子任务终态钩子派发。

    全部源任务已终态(建聚合时子任务早跑完)→ False 立即派发,否则永远等不到终态钩子。
    """
    source_ids = _clean_source_ids((task.delivery or {}).get("source_task_ids"))
    if not source_ids:
        return False
    rows = db.scalars(select(CockpitVoiceTask).where(CockpitVoiceTask.id.in_(source_ids))).all()
    if len(rows) < len(source_ids):
        return True  # 源 id 缺失:与 _maybe_dispatch_group_parent 口径一致(不轻易派发)
    return any(r.status in ACTIVE_STATUSES for r in rows)


# --- 摘要(复用 settings.llm_* 通道,容错同款 generate_conversation_title) ---------------------


async def summarize_result(text: str) -> str:
    """≤200 字中文摘要;LLM 不可用/调用失败 → 截断原文前 200 字兜底。"""
    summary = await _llm_summary(text)
    if summary:
        return summary
    return text.strip()[:200]


async def _llm_summary(text: str) -> str | None:
    settings = get_settings()
    if settings.llm_provider != "openai_compatible" or not settings.llm_base_url or not settings.llm_api_key:
        return None
    if not text.strip():
        return None
    payload = {
        "model": settings.llm_model,
        "messages": [
            {
                "role": "system",
                "content": "你是业务结果摘要器。把业务执行结果压缩成不超过200字的中文口语摘要,"
                "先说结论,不要列表、表格、Markdown,不要引号收尾。",
            },
            {"role": "user", "content": text[:4000]},
        ],
        "temperature": 0.3,
        "max_tokens": 300,
    }
    headers = {"Authorization": f"Bearer {settings.llm_api_key}"}
    try:
        async with cockpit_runtime.new_async_client(30.0) as client:
            response = await client.post(
                f"{settings.llm_base_url.rstrip('/')}/chat/completions", json=payload, headers=headers
            )
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return None
    summary = str(raw).strip()[:200]
    return summary or None


# --- 发送追踪(风险放行触达结果回调,方案 plans/quasar-vixen-sandman) ----------------------------

_SEND_TRACK_POLL_S = 5.0  # 消息状态轮询间隔(秒)
_SEND_TRACK_STABLE_S = 10.0  # 全终态稳定窗(failed 可能因 retry 翻转 sent,稳定后再结算)
_SEND_TRACK_DEADLINE_S = 300.0  # 追踪上限(自任务创建起算):超时未确认的按失败侧计


def _is_send_track(task: CockpitVoiceTask) -> bool:
    return any(item.get("step") == "send_track" for item in task.progress or [])


def _send_track_entry(task: CockpitVoiceTask) -> dict[str, Any]:
    for item in task.progress or []:
        if item.get("step") == "send_track":
            return item
    return {}


def _send_track_message_ids(task: CockpitVoiceTask) -> list[int]:
    return [int(x) for x in _send_track_entry(task).get("message_ids") or []]


def _send_track_names(task: CockpitVoiceTask) -> dict[str, str]:
    return {str(k): str(v) for k, v in (_send_track_entry(task).get("names") or {}).items()}


def _emit_voice_task_event(db: Session, task: CockpitVoiceTask, event_type: str) -> None:
    """语音任务生命周期 WS 事件(/realtime/ws topic=cockpit,持久化+断线重放)。

    调用方负责 commit。语音 WS 推送在通话外静默丢弃,本通道保证文字模式账本/任务卡
    即时更新(方案 rictor-starfire-orphan §A)。
    """
    create_realtime_event(
        db,
        tenant_id=task.tenant_id,
        topic="cockpit",
        event_type=event_type,
        resource_type="cockpit_voice_task",
        resource_id=str(task.id),
        payload={"task": serialize_task(task)},
    )


def _emit_send_track_event(db: Session, task: CockpitVoiceTask, event_type: str) -> None:
    """发送追踪 WS 事件(cockpit.send_track.started/settled)。send_track 任务只用本类型,
    不再另发 voice_task.*(避免前端双气泡)。"""
    _emit_voice_task_event(db, task, event_type)


def _emit_subconv_progress(db: Session, task: CockpitVoiceTask, status: str, summary: str | None) -> None:
    """子会话任务的终态额外发 cockpit.subconv.progress(父会话冒泡+面板镜像即时刷新)。

    任务所属会话有 parent_conversation_id 才发;调用方负责 commit。
    """
    conv = db.get(CockpitConversation, task.conversation_id)
    if conv is None or conv.parent_conversation_id is None:
        return
    create_realtime_event(
        db,
        tenant_id=task.tenant_id,
        topic="cockpit",
        event_type="cockpit.subconv.progress",
        resource_type="cockpit_conversation",
        resource_id=str(conv.id),
        payload={
            "parent_id": conv.parent_conversation_id,
            "conversation_id": conv.id,
            "conversation_title": conv.title,
            "task_id": task.id,
            "status": status,
            "summary": (summary or "")[:200],
        },
    )


def _spawn(coro) -> None:
    """有运行中 loop 直接 create_task;sync 路由线程池无 loop 则起 daemon 线程跑。
    仿 task_queue.schedule_database_mode_execution。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        loop.create_task(coro)
    else:
        threading.Thread(target=asyncio.run, args=(coro,), daemon=True).start()


def create_send_track_task(
    db: Session,
    context: TenantContext,
    *,
    conversation_id: int,
    entries: list[dict[str, Any]],
    names: dict[int, str],
) -> CockpitVoiceTask:
    """风险放行确认后落发送追踪任务(running)+ WS started 事件。

    entries: risk_confirm_send 的 sent 列表(含 message_id/customer_id);
    names: customer_id → 客户名(摘要/气泡用)。复用请求级 db 仅 flush,
    commit 由路由收尾;盯梢协程由路由 commit 后经 start_send_track_watch 启动
    (不可在此 _spawn:本函数运行在请求事务内,协程另开 Session 读不到未提交行)。
    """
    message_ids = [int(e["message_id"]) for e in entries if e.get("message_id")]
    mid_names = {
        str(int(e["message_id"])): names.get(int(e["customer_id"])) or f"客户{int(e['customer_id'])}"
        for e in entries
        if e.get("message_id")
    }
    title = f"给 {'、'.join(mid_names.values())} 发消息"
    task = CockpitVoiceTask(
        tenant_id=context.tenant_id,
        user_id=int(context.user_id),
        conversation_id=conversation_id,
        kind="其他",
        task_text=title[:60],
        status="running",
        announced=False,
        progress=[{
            "step": "send_track",
            "message_ids": message_ids,
            "names": mid_names,
            "at": datetime.utcnow().isoformat(),
        }],
    )
    db.add(task)
    db.flush()
    db.refresh(task)
    _emit_send_track_event(db, task, "cockpit.send_track.started")
    logger.info(
        "发送追踪任务 #%s(tenant=%s user=%s): %s → %d 条消息",
        task.id, task.tenant_id, task.user_id, title[:40], len(message_ids),
    )
    return task


def start_send_track_watch(task_id: int) -> None:
    """路由 commit 后调用: 启动发送追踪盯梢协程(不可在事务内提前启动)。"""
    _spawn(_send_track_watch(task_id))


async def _send_track_watch(task_id: int) -> None:
    """后台盯梢: 每 5s 查本批消息状态 → 全终态 + 10s 稳定窗 → 结算;
    自任务创建起 300s 上限,超时未确认按失败侧计。全程异常兜底,绝不留 running。
    """
    try:
        # 与调用方 commit 竞速: _spawn 可能先于事务提交,首读容忍短窗(15×0.2s=3s)
        created: datetime | None = None
        for _ in range(15):
            with SessionLocal() as db:
                row = db.get(CockpitVoiceTask, task_id)
                if row is not None:
                    if row.status != "running":
                        return
                    created = row.created_at or datetime.utcnow()
                    break
            await asyncio.sleep(0.2)
        if created is None:
            return
        # naive UTC 差值比较(同 _pick_word 口径);不要 .timestamp()(naive 会按本地时区解释)
        stable_since: datetime | None = None
        last_snapshot: tuple[tuple[int, str], ...] | None = None
        while True:
            await asyncio.sleep(_SEND_TRACK_POLL_S)
            with SessionLocal() as db:
                task = db.get(CockpitVoiceTask, task_id)
                if task is None or task.status != "running":
                    return
                message_ids = _send_track_message_ids(task)
            # 消息状态经 TrackProbe(独立服务无 Message 业务表);
            # 探针失败按非终态处理(继续盯梢,不误结算)
            try:
                states = await get_track_probe().fetch_states(message_ids)
            except TrackProbeError:
                logger.warning("发送追踪 #%s 探针失败,本轮跳过", task_id)
                continue
            by_id = {mid: st.status for mid, st in states.items()}
            snapshot = tuple(sorted((mid, by_id.get(mid, "missing")) for mid in message_ids))
            now = datetime.utcnow()
            if (now - created).total_seconds() >= _SEND_TRACK_DEADLINE_S:
                await _finalize_send_track(task_id, timed_out=True)
                return
            all_terminal = bool(snapshot) and all(s in ("sent", "failed") for _, s in snapshot)
            if not all_terminal:
                stable_since = None
                last_snapshot = None
                continue
            if snapshot != last_snapshot:
                last_snapshot = snapshot
                stable_since = now
                continue
            if stable_since is not None and (now - stable_since).total_seconds() >= _SEND_TRACK_STABLE_S:
                await _finalize_send_track(task_id, timed_out=False)
                return
    except Exception:  # noqa: BLE001 - 兜底收口,绝不让账本停在 running(铁律)
        logger.exception("发送追踪任务 #%s 盯梢异常,按超时口径结算", task_id)
        try:
            await _finalize_send_track(task_id, timed_out=True)
        except Exception:  # noqa: BLE001 - 结算再失败等启动恢复兜底
            logger.exception("发送追踪任务 #%s 结算失败", task_id)


def _human_send_error(reason: str) -> str:
    """发送失败原因人话版(不念英文/错误码)。"""
    text = (reason or "").strip()
    if not text:
        return "发送失败"
    if "好友申请已提交" in text:
        return "好友申请待通过"
    if "还不是你的朋友" in text:
        return "对方已不是好友"
    if "设备已离线" in text:
        return "设备离线"
    if "not open" in text or "会话" in text:
        return "会话已关闭"
    if "Channel provider rejected" in text:
        return "渠道拒绝发送"
    return text[:30]


async def _finalize_send_track(task_id: int, *, timed_out: bool) -> None:
    """结算: 全 sent → done + 名单摘要;有失败 → failed + 人话明细;
    WS settled 事件 + push_task_event + announce_task 播报。"""
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status != "running":
            return
        message_ids = _send_track_message_ids(task)
        names = _send_track_names(task)
    # 消息状态经 TrackProbe(MessageState 有 status/failure_reason,与旧 Message 行同口径);
    # 探针失败按超时口径结算(绝不误报成功)
    try:
        by_id = await get_track_probe().fetch_states(message_ids)
    except TrackProbeError:
        logger.exception("发送追踪 #%s 结算时探针失败,按超时口径收口", task_id)
        by_id = {}
        timed_out = True
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status != "running":
            return
        sent_names: list[str] = []
        failed: list[tuple[str, str]] = []
        for mid in message_ids:
            name = names.get(str(mid)) or f"消息{mid}"
            msg = by_id.get(mid)
            if msg is not None and msg.status == "sent":
                sent_names.append(name)
                continue
            if msg is None:
                reason = "消息记录缺失"
            elif timed_out and msg.status not in ("sent", "failed"):
                reason = "发送超时未确认"
            else:
                reason = _human_send_error(msg.failure_reason or "")
            failed.append((name, reason))
        if not failed:
            task.status = "done"
            if len(sent_names) <= 1:
                summary = f"{sent_names[0] if sent_names else '客户'} 的消息已经发出去了"
            else:
                summary = f"{len(sent_names)} 位客户的消息都发出去了:{'、'.join(sent_names)}"
            task.result_summary = summary
            task.result_text = summary
        else:
            task.status = "failed"
            parts: list[str] = []
            if sent_names:
                parts.append(f"{'、'.join(sent_names)} 已发出")
            parts.extend(f"{name} 没发出去({reason})" for name, reason in failed)
            task.error = ";".join(parts)[:500]
        task.finished_at = datetime.utcnow()
        db.flush()
        _emit_send_track_event(db, task, "cockpit.send_track.settled")
        db.commit()
        tenant_id, user_id = task.tenant_id, task.user_id
        payload = serialize_task(task)
    await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
    await announce_task(task_id)


async def _recover_send_track(task_id: int) -> None:
    """发送追踪启动恢复: 消息已全终态 → 直接结算;否则重启盯梢
    (deadline 按创建时间起算,服务停机超期的重启后即按超时结算)。
    探针失败(平台桥不在线)重启盯梢,下轮再试。"""
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status != "running":
            return
        message_ids = _send_track_message_ids(task)
    try:
        states = await get_track_probe().fetch_states(message_ids)
    except TrackProbeError:
        await _send_track_watch(task_id)
        return
    statuses = {mid: st.status for mid, st in states.items()}
    all_terminal = bool(message_ids) and all(
        statuses.get(mid) in ("sent", "failed") for mid in message_ids
    )
    if all_terminal:
        await _finalize_send_track(task_id, timed_out=False)
    else:
        await _send_track_watch(task_id)


# --- 播报(§11 静默闸门 v3.3: 备查注入 + 播报指令开口/快任务直念/defer) ------------------------

# 过渡词池(按任务完成新鲜度分档,注入时随机选,写进【播报指令】开头用词,防模型复读)
# v3.6: 池扩容 + exclude 近期去重——3 选 1 无去重时"差点忘了"会被连续抽中,播报整句复读
# 注意: 池同时用于失败(tell)播报,只放中性词,不放"好消息"这类成败偏向词
_FRESH_WORDS = (
    "对了", "刚刚", "差点忘了", "跟你说", "正好",
    "哦对了", "说到这个", "巧了", "汇报一下", "有个进展",
)
_STALE_WORDS = (
    "话说回来", "之前那个", "顺便说下",
    "对了还有", "提一嘴", "之前让你等的", "想起来", "补一句",
)
_FRESH_WINDOW_S = 120.0  # 完成距今 <120s 视为"刚完成"


def _pick_word(finished_at: datetime | None, exclude: Container[str] = ()) -> str:
    """按新鲜度档位选过渡词;exclude 里的词优先避开,全被排除时退回全池。"""
    fresh = (
        finished_at is not None
        and (datetime.utcnow() - finished_at).total_seconds() < _FRESH_WINDOW_S
    )
    pool = _FRESH_WORDS if fresh else _STALE_WORDS
    choices = [w for w in pool if w not in exclude]
    return random.choice(choices or pool)


def _repick_word(word: str, exclude: Container[str] = ()) -> str:
    """在 word 所属档位池内换一个词(优先避开 exclude);无可用候选时原样返回。"""
    pool = _FRESH_WORDS if word in _FRESH_WORDS else _STALE_WORDS
    choices = [w for w in pool if w not in exclude]
    return random.choice(choices) if choices else word


def _announce_text(task: CockpitVoiceTask) -> str:
    """成功任务:摘要备查条目(v3.3 开口一律等【播报指令】,不附带搭车指引)。"""
    title = task.task_text[:30]
    # 软阻断任务(结果含风险卡):引导用户点屏幕上任务卡里的「确认放行发送」,
    # 一句话带过,不多轮追问(卡片放行后系统会另发播报告知发送结果)
    has_risk_card = any(
        isinstance(c, dict) and c.get("type") == "cockpit_risk" for c in (task.cards or [])
    )
    risk_hint = (
        "本任务命中软阻断未发送,屏幕上任务卡里有「确认放行发送」卡片;用户点确认后立即发送。"
        "播报时只说一句引导用户点卡片即可,不要追问、不要复述原因细节。"
        if has_risk_card
        else ""
    )
    # 子会话任务(结果含子会话卡):告知子会话已建好、任务已派发,屏幕上任务卡有入口
    has_subconv_card = any(
        isinstance(c, dict) and c.get("type") == "cockpit_subconv" for c in (task.cards or [])
    )
    subconv_hint = (
        "本任务已创建子会话并在其中继续执行,屏幕上任务卡里有「查看子会话」入口。"
        "播报时只说子会话已建好、任务已派发即可,不要追问。"
        if has_subconv_card
        else ""
    )
    return (
        f"【后台任务结果】任务#{task.id}「{title}」已完成。"
        f"结果摘要:{(task.result_summary or '')[:200]}。{risk_hint}{subconv_hint}"
        "（备查条目，不要主动提起，也不要打断当前话题；系统会另发【播报指令】，"
        "收到指令后再开口。用户主动问起时，按摘要口语化回答，完整数据让用户看任务卡片。）"
    )


def _failure_text(task: CockpitVoiceTask) -> str:
    """失败任务:失败原因备查条目(v3.3 开口等【播报指令】直告,不询问不占次数)。"""
    title = task.task_text[:30]
    return (
        f"【后台任务结果】任务#{task.id}「{title}」失败:{(task.error or '未知原因')[:200]}。"
        "（备查条目，不要主动提起，也不要打断当前话题；系统会另发【播报指令】，"
        "收到指令后再开口。用户主动问起时，自然告知没跑成、可以稍后再试，不要念错误码。）"
    )


def _silent_reinject_text(task: CockpitVoiceTask) -> str:
    """deferred 重连补注:仅上下文备查,不主动提。"""
    title = task.task_text[:30]
    outcome = "已完成" if task.status == "done" else "已失败"
    detail = (task.result_summary or task.error or "")[:200]
    return (
        f"【后台任务结果】任务#{task.id}「{title}」{outcome}。结果摘要:{detail}。"
        "（用户已知悉此任务结果但暂未细听，仅在用户主动问起时回答，不要主动提起。）"
    )


def _is_silenced_by_group(db: Session, task: CockpitVoiceTask) -> bool:
    """announce_policy=silent_until_group:同用户存在活跃聚合父任务引用本任务时不单独播报。

    父任务完成时会统一播报汇总结果;父任务已终态/不存在则放行播报(结果永不丢)。
    (2026-10-14 对齐平台后端;静默判定作用于终态播报与重连补注两处闸门)
    """
    delivery = task.delivery or {}
    if delivery.get("announce_policy") != "silent_until_group":
        return False
    candidates = db.scalars(
        select(CockpitVoiceTask).where(
            CockpitVoiceTask.tenant_id == task.tenant_id,
            CockpitVoiceTask.user_id == task.user_id,
            CockpitVoiceTask.status.in_(ACTIVE_STATUSES),
            CockpitVoiceTask.delivery.isnot(None),
        )
    ).all()
    return any(
        task.id in _clean_source_ids((c.delivery or {}).get("source_task_ids"))
        for c in candidates
    )


async def announce_task(task_id: int) -> None:
    """任务终态回调: 注入备查条目 + 登记会话静默闸门播报调度(§11 v3.3)。

    不在通话/注入失败 → announce_state 保持 pending,等下次语音连接 reinject_on_connect。
    mode 只按成败与执行时长判定(不再判空闲,开口时机全部交给会话静默闸门):
    失败→tell 直告;成功且执行 ≤realtime_voice_announce_direct_under_s→direct 直念;
    其余→ask 许可式询问。canceled 不注入(§10.2.3)。
    """
    settings = get_settings()
    if not settings.realtime_voice_announcer_enabled:
        return
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None or task.status not in ("done", "failed") or task.announce_state != "pending":
            return
        if _is_silenced_by_group(db, task):
            logger.info("语音任务 #%s 被聚合父任务静默,暂不播报", task_id)
            return
        tenant_id, user_id = task.tenant_id, task.user_id
        status, finished_at = task.status, task.finished_at
        duration_s = (
            (finished_at - task.created_at).total_seconds()
            if finished_at is not None and task.created_at is not None
            else None
        )
        attempts = task.announce_attempts or 0
        is_track = _is_send_track(task)
        title = (task.task_text or "")[:30]
        word = _pick_word(finished_at)
        text = _announce_text(task) if status == "done" else _failure_text(task)
    session = cockpit_voice.get_active_voice_session(tenant_id, user_id)
    if session is None:
        return
    with SessionLocal() as db:
        result = db.execute(
            update(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.id == task_id,
                CockpitVoiceTask.status.in_(("done", "failed")),
                CockpitVoiceTask.announce_state == "pending",
            )
            .values(announce_state="queued", announced=True)
        )
        if result.rowcount != 1:
            db.commit()
            return
        db.commit()
    mode = "tell" if status == "failed" else "ask"
    if (
        status == "done"
        and settings.realtime_voice_announce_direct_under_s > 0
        and duration_s is not None
        and duration_s <= settings.realtime_voice_announce_direct_under_s
    ):
        mode = "direct"
    if status == "done" and is_track:
        mode = "direct"  # 发送追踪结果直念(发送成败这种短结论不问"要听吗")
    try:
        await session.request_announce(
            task_id,
            text,
            mode=mode,
            title=title,
            attempts=attempts,
            word=word,
        )
    except Exception:  # noqa: BLE001 - 注入失败回滚 pending,下次连接补
        logger.warning("语音任务 #%s 播报注入失败", task_id, exc_info=True)
        with SessionLocal() as db:
            task = db.get(CockpitVoiceTask, task_id)
            if task is not None and task.announce_state == "queued":
                task.announce_state = "pending"
                task.announced = False
                db.commit()
        return


def tasks_for_reinject(db: Session, conversation_id: int) -> list[CockpitVoiceTask]:
    """语音 WS 建连补注候选: 按状态选择主动重排或静默上下文。"""
    return list(
        db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.conversation_id == conversation_id,
                CockpitVoiceTask.status.in_(("done", "failed")),
                CockpitVoiceTask.announce_state.in_(ACTIVE_ANNOUNCE_STATES + SILENT_ANNOUNCE_STATES),
            )
            .order_by(CockpitVoiceTask.created_at)
        ).all()
    )


_REINJECT_WINDOW_S = 86400.0  # 建连补播窗口(24h):超窗活跃态转 dismissed 仅上下文


async def reinject_on_connect(
    session: cockpit_voice.DashScopeVoiceSession, conversation_id: int
) -> None:
    """pending/queued 重排;其他状态与超窗任务只静默补注,不主动复读。"""
    with SessionLocal() as db:
        rows = tasks_for_reinject(db, conversation_id)
    now = datetime.utcnow()
    announcer_enabled = get_settings().realtime_voice_announcer_enabled
    for row in rows:
        stale_pending = (
            row.announce_state in ACTIVE_ANNOUNCE_STATES
            and row.finished_at is not None
            and (now - row.finished_at).total_seconds() > _REINJECT_WINDOW_S
        )
        with SessionLocal() as gdb:
            silenced = _is_silenced_by_group(gdb, row)
        should_queue = False
        try:
            should_queue = (
                announcer_enabled
                and row.announce_state in REINJECT_QUEUE_STATES
                and not stale_pending
                and not silenced
            )
            if not should_queue:
                await session.request_announce(
                    row.id,
                    _silent_reinject_text(row),
                    title=(row.task_text or "")[:30],
                    queue=False,
                )
            else:
                if row.announce_state == "pending":
                    with SessionLocal() as db:
                        result = db.execute(
                            update(CockpitVoiceTask)
                            .where(
                                CockpitVoiceTask.id == row.id,
                                CockpitVoiceTask.announce_state == "pending",
                            )
                            .values(announce_state="queued", announced=True)
                        )
                        if result.rowcount != 1:
                            db.commit()
                            continue
                        db.commit()
                word = _pick_word(row.finished_at)
                text = _announce_text(row) if row.status == "done" else _failure_text(row)
                await session.request_announce(
                    row.id,
                    text,
                    mode="tell" if row.status == "failed" else "ask",
                    title=(row.task_text or "")[:30],
                    attempts=row.announce_attempts or 0,
                    word=word,
                )
        except Exception:  # noqa: BLE001 - 单条失败不影响其他补注
            logger.warning("语音任务 #%s 补注重建失败", row.id, exc_info=True)
            if should_queue:
                with SessionLocal() as db:
                    task = db.get(CockpitVoiceTask, row.id)
                    if task is not None and task.announce_state == "queued":
                        task.announce_state = "pending"
                        task.announced = False
                        db.commit()
            continue
        with SessionLocal() as db:
            task = db.get(CockpitVoiceTask, row.id)
            if task is not None:
                task.announced = True
                if stale_pending and task.announce_state in ACTIVE_ANNOUNCE_STATES:
                    task.announce_state = "dismissed"
                elif not should_queue and task.announce_state == "announcing":
                    task.announce_state = "interrupted"
                db.commit()


async def consume_task_result(
    db: Session,
    context: TenantContext,
    conversation_id: int,
    task_id: int,
) -> CockpitVoiceTask | None:
    """单任务 mark_result_shared 包装;可分享态转 shared,终态幂等返回 None。"""
    rows = await consume_task_results(db, context, conversation_id, [task_id])
    return rows[0] if rows else None


async def consume_task_results(
    db: Session,
    context: TenantContext,
    conversation_id: int,
    task_ids: list[int],
) -> list[CockpitVoiceTask]:
    """批量登记用户已听到结果,一次 commit 并逐条实时推送。"""
    ids = sorted({int(tid) for tid in task_ids if int(tid) > 0})
    if not ids:
        return []
    rows = list(
        db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.id.in_(ids),
                CockpitVoiceTask.tenant_id == context.tenant_id,
                CockpitVoiceTask.user_id == int(context.user_id),
                CockpitVoiceTask.conversation_id == conversation_id,
                CockpitVoiceTask.status.in_(("done", "failed")),
                CockpitVoiceTask.announce_state.in_(SHAREABLE_ANNOUNCE_STATES),
            )
        ).all()
    )
    if not rows:
        return []
    for task in rows:
        task.announce_state = "shared"
        task.announced = True
        _emit_voice_task_event(db, task, "cockpit.voice_task.updated")
    db.commit()
    for task in rows:
        await _push_task_update(task)
    return rows


async def dismiss_task_results(
    db: Session,
    context: TenantContext,
    conversation_id: int,
    task_ids: list[int],
) -> list[int]:
    """用户表示不用讲/已知悉: 置 dismissed,不再主动追问。"""
    return await set_announce_state_for_tasks(
        db,
        context,
        conversation_id,
        task_ids,
        "dismissed",
        allowed_states=SHAREABLE_ANNOUNCE_STATES + ("deferred",),
    )


async def apply_announce_transition(
    task_ids: list[int], state: str, bump_attempts: bool
) -> None:
    """调度器状态落库回调;终态不被后续调度流转翻回。"""
    if not task_ids or state not in cockpit_voice.ANNOUNCE_STATES:
        return
    with SessionLocal() as db:
        rows = list(
            db.scalars(
                select(CockpitVoiceTask).where(CockpitVoiceTask.id.in_(task_ids))
            ).all()
        )
        changed: list[tuple[int, int, dict]] = []
        for row in rows:
            if row.announce_state in FINAL_ANNOUNCE_STATES:
                continue
            row.announce_state = state
            if bump_attempts:
                row.announce_attempts = (row.announce_attempts or 0) + 1
            _emit_voice_task_event(db, row, "cockpit.voice_task.updated")
            changed.append((row.tenant_id, row.user_id, serialize_task(row)))
        db.commit()
    for tenant_id, user_id, payload in changed:
        await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})


# --- 播报策略(2026-10-14 对齐平台后端 voice-announce-fix §A) ---------------------------------


async def set_announce_policy(
    tenant_id: int,
    user_id: int,
    policy: str,
    task_ids: list[int] | None = None,
    conversation_id: int | None = None,
) -> dict[str, Any]:
    """批量更新播报策略,保留独立版精确摘队与 reset 策略标记。

    pending/queued→deferred;announcing→interrupted;终态跳过;在途任务写 delivery。
    返回精确 deferred_ids/interrupted_ids 供路由只摘除本次命中的任务。
    """
    if policy not in SETTABLE_POLICIES:
        raise VoiceTaskError(f"非法播报策略:{policy}")
    with SessionLocal() as db:
        query = select(CockpitVoiceTask).where(
            CockpitVoiceTask.tenant_id == tenant_id,
            CockpitVoiceTask.user_id == user_id,
        )
        if task_ids:
            ids = sorted({int(t) for t in task_ids if int(t) > 0})[:50]
            query = query.where(CockpitVoiceTask.id.in_(ids))
        else:
            query = query.where(CockpitVoiceTask.status != "canceled")
            if conversation_id is not None:
                query = query.where(CockpitVoiceTask.conversation_id == conversation_id)
        rows = list(db.scalars(query.order_by(CockpitVoiceTask.created_at.desc())).all())

        deferred = interrupted = policy_updated = 0
        deferred_ids: list[int] = []
        interrupted_ids: list[int] = []
        pushes: list[tuple[int, int, dict]] = []
        for row in rows:
            if row.status in TERMINAL_STATUSES:
                if row.announce_state in ("pending", "queued"):
                    row.announce_state = "deferred"
                    row.announced = True
                    deferred += 1
                    deferred_ids.append(row.id)
                elif row.announce_state == "announcing":
                    row.announce_state = "interrupted"
                    row.announced = True
                    interrupted += 1
                    interrupted_ids.append(row.id)
                else:
                    continue
                # 策略标记同时用于 reset 精确复活 deferred/interrupted。
                delivery = dict(row.delivery or {})
                delivery["announce_policy"] = policy
                row.delivery = delivery
            elif row.status in ACTIVE_STATUSES:
                delivery = dict(row.delivery or {})
                delivery["announce_policy"] = policy
                row.delivery = delivery
                progress = list(row.progress or [])
                progress.append({
                    "step": "announce_policy",
                    "state": policy,
                    "at": datetime.utcnow().isoformat(),
                })
                row.progress = progress
                policy_updated += 1
            else:
                continue
            _emit_voice_task_event(db, row, "cockpit.voice_task.updated")
            pushes.append((row.tenant_id, row.user_id, serialize_task(row)))
        db.commit()
    for t, u, payload in pushes:
        await push_task_event(t, u, {"type": "task.updated", "task": payload})
    return {
        "deferred": deferred,
        "interrupted": interrupted,
        "policy_updated": policy_updated,
        "deferred_ids": deferred_ids,
        "interrupted_ids": interrupted_ids,
    }


async def reset_announce_policy(
    tenant_id: int, user_id: int, conversation_id: int
) -> dict[str, int]:
    """撤销播报策略(前端徽标 ✕,REST 通道不依赖语音在线):

    清掉会话内经策略设置的 delivery.announce_policy;被策略压下或停止(deferred/interrupted)
    的终态任务复活回 pending 并立即重走播报(会话不在线则等下次建连补注)。用户主动 defer 的
    任务(delivery 无策略标记)不受影响。
    """
    revived_ids: list[int] = []
    cleared = 0
    pushes: list[tuple[int, int, dict]] = []
    with SessionLocal() as db:
        rows = list(
            db.scalars(
                select(CockpitVoiceTask).where(
                    CockpitVoiceTask.tenant_id == tenant_id,
                    CockpitVoiceTask.user_id == user_id,
                    CockpitVoiceTask.conversation_id == conversation_id,
                    CockpitVoiceTask.delivery.isnot(None),
                )
            ).all()
        )
        for row in rows:
            delivery = dict(row.delivery or {})
            if "announce_policy" not in delivery:
                continue
            delivery.pop("announce_policy")
            row.delivery = delivery or None
            cleared += 1
            if row.status in ("done", "failed") and row.announce_state in ("deferred", "interrupted"):
                row.announce_state = "pending"
                row.announced = False
                revived_ids.append(row.id)
            _emit_voice_task_event(db, row, "cockpit.voice_task.updated")
            pushes.append((row.tenant_id, row.user_id, serialize_task(row)))
        db.commit()
    for t, u, payload in pushes:
        await push_task_event(t, u, {"type": "task.updated", "task": payload})
    for task_id in revived_ids:
        await announce_task(task_id)  # 复活即补播;会话不在线时保持 pending 等建连
    return {"cleared": cleared, "revived": len(revived_ids)}


# --- 启动恢复(§10.1.4 升级版) ---------------------------------------------------------------


def recover_interrupted_tasks() -> None:
    """启动恢复(lifespan 后台线程调用): running/waiting_approval → 探活或 failed。

    有 run_ref 先 GET /v1/runs/{id} 探活:completed 走正常 done 管线补收尾;
    非终态/404/探活失败一律 failed("服务重启中断")。仿 _recover_stuck_outbound_sends 先例。
    """
    with SessionLocal() as db:
        refs = list(
            db.scalars(
                select(CockpitVoiceTask).where(CockpitVoiceTask.status.in_(("running", "waiting_approval")))
            ).all()
        )
        candidates = [(row.id, row.run_ref, _is_send_track(row)) for row in refs]
        # 重启时 queued/announcing 均回 interrupted,重连只静默备查,不立刻复读抢话。
        db.execute(
            update(CockpitVoiceTask)
            .where(CockpitVoiceTask.announce_state.in_(("queued", "announcing")))
            .values(announce_state="interrupted")
        )
        db.commit()
    for task_id, run_ref, is_track in candidates:
        try:
            if is_track:
                asyncio.run(_recover_send_track(task_id))
            else:
                asyncio.run(_recover_one(task_id, run_ref))
        except Exception:  # noqa: BLE001 - 单条失败不影响其他恢复
            logger.exception("语音任务 #%s 恢复失败", task_id)
    if candidates:
        logger.info("语音任务启动恢复完成: %d 条", len(candidates))
    # 工具目录防漂移对账慢拍(2026-10-14):与启动恢复同钩拉起,失败只日志
    try:
        cockpit_voice.start_tool_catalog_drift_watch()
    except Exception:  # noqa: BLE001 - 对账慢拍拉起失败不影响启动恢复主链路
        logger.exception("工具目录对账慢拍拉起失败")


async def _recover_one(task_id: int, run_ref: str | None) -> None:
    if run_ref:
        try:
            info = await cockpit_runtime.runs_get(run_ref)
        except cockpit_runtime.CockpitRuntimeError:
            info = None
        if info and info.get("status") == "completed":
            await _finalize_done(task_id, str(info.get("output") or ""))
            await announce_task(task_id)
            return
    await _finalize_failed(task_id, "服务重启中断")


# --- 取消(§10.2) ---------------------------------------------------------------------------


async def cancel_task(
    db: Session, context: TenantContext, task_id: int
) -> CockpitVoiceTask:
    """HTTP 取消入口:归属校验(tenant_id+user_id)不符 → VoiceTaskNotFound(404)。

    pending → 直接 canceled;running/waiting_approval → runs_stop(run_ref),
    终态由 _run 的 run.cancelled 分支置 canceled(§10.1.2②⑤);已 canceled 幂等返回;
    done/failed → VoiceTaskNotCancellable(409)。
    """
    task = db.get(CockpitVoiceTask, task_id)
    if task is None or task.tenant_id != context.tenant_id or task.user_id != context.user_id:
        raise VoiceTaskNotFound("任务不存在")
    if task.status == "canceled":
        return task
    if task.status in ("done", "failed"):
        raise VoiceTaskNotCancellable("任务已结束，不可取消")
    if task.status == "pending" or not task.run_ref:
        task.status = "canceled"
        task.finished_at = datetime.utcnow()
        db.commit()
        payload = serialize_task(task)
        await push_task_event(task.tenant_id, task.user_id, {"type": "task.updated", "task": payload})
        return task
    await cockpit_runtime.runs_stop(task.run_ref)  # 404 幂等;事件流收尾
    return task


def resolve_cancel_target(
    db: Session, context: TenantContext, conversation_id: int, hint: str
) -> CockpitVoiceTask | list[CockpitVoiceTask] | None:
    """§10.2.2 消歧:候选 = 该会话该用户 status ∈ (pending,running,waiting_approval),
    按 created_at 倒序。
    - 0 个 → None(回执'当前没有进行中的任务')
    - hint 非空 → 按 kind 或 task_text 包含过滤;命中 1 个即目标;命中多个 → 候选列表
    - 无 hint 且有多个 → 候选列表(回执列出编号+名称,让 qwen 反问)
    - 兜底(hint 无命中/恰好 1 个候选) → 最新一个
    """
    candidates = list(
        db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.conversation_id == conversation_id,
                CockpitVoiceTask.tenant_id == context.tenant_id,
                CockpitVoiceTask.user_id == context.user_id,
                CockpitVoiceTask.status.in_(ACTIVE_STATUSES),
            )
            .order_by(CockpitVoiceTask.created_at.desc())
        ).all()
    )
    if not candidates:
        return None
    hint = (hint or "").strip()
    if hint:
        matched = [
            t for t in candidates
            if hint in (t.task_text or "") or (t.kind and t.kind != "其他" and t.kind in hint)
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            return matched
    if len(candidates) == 1:
        return candidates[0]
    if hint:
        return candidates[0]  # hint 无命中 → 兜底最新一个
    return candidates


async def cancel_business_task(task_id: int) -> str:
    """§10.2.2 语音取消执行:返回给 qwen 的回执文案。

    pending/未取到 run_ref → 直接 canceled;running/waiting_approval → runs_stop(run_ref),
    终态由 _run 的 run.cancelled 分支置 canceled。竞态(§10.2.3):已 done → '那个任务已经
    完成了,结果在卡片里';重复取消幂等 → '已经取消过了'。
    """
    with SessionLocal() as db:
        task = db.get(CockpitVoiceTask, task_id)
        if task is None:
            return "那个任务不存在了"
        title = task.task_text[:30]
        if task.status == "done":
            return f"任务#{task.id}「{title}」已经完成了，结果在任务卡片里"
        if task.status == "failed":
            return f"任务#{task.id}「{title}」已经失败了，不用取消"
        if task.status == "canceled":
            return f"任务#{task.id}「{title}」已经取消过了"
        if task.status == "pending" or not task.run_ref:
            task.status = "canceled"
            task.finished_at = datetime.utcnow()
            db.commit()
            tenant_id, user_id = task.tenant_id, task.user_id
            payload = serialize_task(task)
        else:
            tenant_id, user_id = task.tenant_id, task.user_id
            run_ref = task.run_ref
            payload = None
    if payload is not None:
        await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
        return f"好的，任务#{task_id}「{title}」已取消"
    try:
        await cockpit_runtime.runs_stop(run_ref)
    except cockpit_runtime.CockpitRuntimeError as exc:
        logger.warning("cancel_business_task runs_stop(%s) 失败: %s", run_ref, exc)
        return f"取消指令发送失败，请稍后再试"
    return f"好的，任务#{task_id}「{title}」已取消"


# --- 压下播报(§11.4 defer_task_result) ---------------------------------------------------------

DEFERRABLE_ANNOUNCE_STATES = ("pending", "queued", "announcing", "notified", "interrupted")


def resolve_defer_target(
    db: Session, context: TenantContext, conversation_id: int, hint: str
) -> CockpitVoiceTask | list[CockpitVoiceTask] | None:
    """消歧(仿 resolve_cancel_target):候选 = 该会话 status∈(done,failed)
    且 announce_state∈(pending,announcing),按 created_at 倒序。
    - 0 个 → None;hint 命中 1 个 → 单个;命中多个 → 候选列表(回执让用户选)
    - hint 无命中 → 兜底最新一个;空 hint("都先别讲") → 全部候选
    """
    candidates = list(
        db.scalars(
            select(CockpitVoiceTask)
            .where(
                CockpitVoiceTask.conversation_id == conversation_id,
                CockpitVoiceTask.tenant_id == context.tenant_id,
                CockpitVoiceTask.user_id == context.user_id,
                CockpitVoiceTask.status.in_(("done", "failed")),
                CockpitVoiceTask.announce_state.in_(DEFERRABLE_ANNOUNCE_STATES),
            )
            .order_by(CockpitVoiceTask.created_at.desc())
        ).all()
    )
    if not candidates:
        return None
    hint = (hint or "").strip()
    if hint:
        matched = [
            t for t in candidates
            if hint in (t.task_text or "") or (t.kind and t.kind != "其他" and t.kind in hint)
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            return matched
        return candidates[0]
    return candidates


async def defer_task_result(
    db: Session,
    context: TenantContext,
    conversation_id: int,
    hint: str,
    task_ids: list[int] | None = None,
) -> tuple[str, list[int]]:
    """defer_task_result 执行:命中任务 announce_state→deferred 并推 task.updated。

    返回 (给 qwen 的回执文案, 被压下的 task_id 列表);路由层拿 ids 调
    session.defer_tasks 从调度器摘队(defer 优先于打断重排)。
    task_ids 显式给出时按编号压下(归属+会话+可压状态三重过滤),跳过 hint 消歧。
    """
    if task_ids:
        ids = await set_announce_state_for_tasks(
            db,
            context,
            conversation_id,
            task_ids,
            "deferred",
            allowed_states=DEFERRABLE_ANNOUNCE_STATES,
        )
        if not ids:
            return "没有待告知结果的任务", []
        if len(ids) == 1:
            return f"好，任务#{ids[0]}先不主动提了，想听的时候说一声", ids
        return f"好，这 {len(ids)} 个任务都先不主动提了，想听的时候说一声", ids
    else:
        resolved = resolve_defer_target(db, context, conversation_id, hint)
        if resolved is None:
            return "没有待告知结果的任务", []
        if isinstance(resolved, list) and (hint or "").strip():
            listing = "；".join(f"#{t.id} {t.kind}·{t.task_text[:20]}" for t in resolved)
            return f"有多个待告知的任务：{listing}。请用户说明先压下哪一个", []
        targets = resolved if isinstance(resolved, list) else [resolved]
    ids: list[int] = []
    pushes: list[tuple[int, int, dict]] = []
    for t in targets:
        t.announce_state = "deferred"
        ids.append(t.id)
        pushes.append((t.tenant_id, t.user_id, serialize_task(t)))
    db.commit()
    for tenant_id, user_id, payload in pushes:
        await push_task_event(tenant_id, user_id, {"type": "task.updated", "task": payload})
    if len(ids) == 1:
        return f"好，任务#{ids[0]}先不主动提了，想听的时候说一声", ids
    return f"好，这 {len(ids)} 个任务都先不主动提了，想听的时候说一声", ids


# --- 快查(§10.3.2) --------------------------------------------------------------------------

# ask 通道写意图硬转(voice-write-ledger-fix): 语音快查在 hermes 侧以全量工具执行,
# 发送/建任务/建会话等写操作若错投 ask 通道会不留账本行、前端无任务卡(暗道)。
# 命中任一写意图模式即不调 chat_sync,直接回执让 qwen 改调 submit_business_task。
# 口语变体多(发条消息/发个微信/开个新会话),用正则覆盖间隔字;误伤(如"查发送记录")
# 代价仅是走异步任务卡,结果照样拿到,不漏账。
_ASK_WRITE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"发.{0,3}(消息|信息|微信|短信|话术)"),
    re.compile(r"(群发|触达|私信|派发)"),
    re.compile(r"(创建|新建|建|提醒|回访|安排).{0,4}任务"),
    re.compile(r"(创建|新建|建|开).{0,3}(会话|对话)"),
    re.compile(r"草稿"),
)

# ask 通道只读纪律: 作为 system_message 透传 hermes(api_server.py:4617),
# 防写工具被快查轮次执行(纵深防御第 3 道,第 2 道硬转已覆盖绝大多数)。
ASK_READONLY_INSTRUCTIONS = (
    "本轮是语音快查通道，只读：只允许查询/检索类工具。"
    "严禁 send_contact_message / task_create / task_draft_create / cockpit_session_create 等一切写工具。"
    "若用户需求是发送、创建任务、创建会话等写操作，不要执行任何工具，"
    "只回复：这个操作需要走后台任务通道，请改用提交任务受理。"
)


def _ask_write_intent(query: str) -> bool:
    """写意图预检:命中任一写操作模式即视为写操作,ask 通道不受理。"""
    text = query.replace(" ", "")
    return any(p.search(text) for p in _ASK_WRITE_PATTERNS)


async def ask_business_agent(context: TenantContext, conversation_id: int, query: str) -> str:
    """ask_business_agent 快查路径: chat_sync + realtime_voice_quick_timeout(45s)封顶。

    成功 → 全文直接返回给 qwen 组织口语回答;超时/Busy → 自动转 submit_task 异步,
    返回转文案;失败 → 人话版回执让 qwen 自然致歉。快查轮次同样经 chat_sync 落
    hermes 历史,文字模式可见(§10.3.3)。
    写操作(发送/建任务/建会话等)硬转:不写 hermes、不落暗道,回执引导 qwen
    改调 submit_business_task → 落账本 + 出任务卡。
    """
    if _ask_write_intent(query):
        logger.info("ask_business_agent 写意图硬转: %s", query[:80])
        return (
            "这是写操作(发送/创建类)，快查通道不受理，请立即改用 submit_business_task "
            "受理同一需求(任务原文照搬用户需求)，不要再向用户重复确认。"
        )
    settings = get_settings()
    with SessionLocal() as db:
        conv = db.get(CockpitConversation, conversation_id)
        if conv is None or conv.tenant_id != context.tenant_id or conv.user_id != context.user_id:
            return "会话不存在，请刷新后重试"
        agent_session_id = conv.agent_session_id
    session_key = cockpit_runtime.session_key_for(context.tenant_id, int(context.user_id))
    try:
        async with asyncio.timeout(settings.realtime_voice_quick_timeout):
            return await cockpit_runtime.chat_sync(
                agent_session_id, query, session_key=session_key,
                instructions=ASK_READONLY_INSTRUCTIONS,
            )
    except (TimeoutError, cockpit_runtime.CockpitRuntimeBusy):
        # 比预想慢/引擎繁忙 → 自动转异步,体验不崩(§10.3.3 服务端双保险)
        with SessionLocal() as db:
            conv = db.get(CockpitConversation, conversation_id)
            try:
                task = submit_task(db, context, conv, query, "查询", via="ask")
            except VoiceTaskBusy:
                return f"当前已有 {settings.realtime_voice_max_tasks_per_user} 个任务在处理，请稍后再问"
            task_id = task.id
            payload = serialize_task(task)
        dispatch_task(task_id)
        await push_task_event(context.tenant_id, int(context.user_id), {"type": "task.created", "task": payload})
        return f"这个查询比预想慢，已转后台处理（任务#{task_id}），好了我会主动告诉您"
    except cockpit_runtime.CockpitRuntimeError as exc:
        return f"查询失败：{_human_error(exc)}"


# --- 人话版错误 ----------------------------------------------------------------------------


def _human_error(exc: BaseException) -> str:
    if isinstance(exc, cockpit_runtime.CockpitRuntimeAuthError):
        return "智能引擎配置异常，请联系管理员"
    if isinstance(exc, cockpit_runtime.CockpitRuntimeBadRequest):
        return "任务请求异常，请换个说法再试"
    if isinstance(exc, cockpit_runtime.CockpitRuntimeBusy):
        return "智能引擎当前繁忙，请稍后再试"
    if isinstance(exc, cockpit_runtime.CockpitRuntimeUnavailable):
        return "智能引擎暂时不可用，请稍后再试"
    if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
        return "任务执行超时，已自动停止"
    return "任务执行失败，请稍后再试"


def _human_run_error(error: Any) -> str:
    """run.failed 的 error 字段 → 人话版(截断,不念错误码由 instructions 约束)。"""
    text = str(error or "").strip()
    if not text:
        return "任务执行失败，请稍后再试"
    return text[:200]
