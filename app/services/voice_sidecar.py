"""语音侧路(services/voice_sidecar.py)——多人认知层 Layer 1 的侧路管线。

契约(docs/ENGAGED_DRIFT_AND_SPEAKER_ATTRIBUTION_PLAN.md §5.1/§5.2,
底座 v3.5 = docs/REALTIME_VOICE_V2_PLAN.md §14):

浏览器 PCM ─▶ 主路 qwen 会话(不动,semantic_vad 照旧)
                 └─▶ 本模块 VoiceSidecar(每语音会话一个)
                        ├─ SidecarBuffer(1.5s 窗/0.75s hop,背压丢旧窗)
                        │     └─▶ DiarizationClient(独立服务 :8791, 超时 800ms)
                        ├─ AsrSidecar(DashScope 实时 ASR 独立 WSS, 句级文本)
                        │     └─ 旁听转写不进 qwen(破 600s 上限的关键)
                        ├─ Aligner(ASR 句 × diarization 段, 重叠≥50% 归主)
                        ├─ SpeakerMatcher(注册声纹余弦, 持有人锚定)
                        ├─ 事件账本(cockpit_speaker_events, 谁/何时/说什么/叫没叫AI)
                        └─ SummaryRefresher(30s 缓存节流 → 常驻【现场摘要】注入 qwen)

侧路三原则(继承 v2 账本哲学):
1. 主路永远通——侧路任何一环挂掉只计数降级, 绝不抛进主链路;
2. 侧路只读不写主路——对语音对话零阻塞零侵入(注入摘要走既有 inject_text 原语);
3. 归属真相在事件账本——qwen 上下文里只放 ≤300 字的摘要快照。

时钟约定: 全部用 loop.time() 单调钟换算的相对毫秒(VoiceSidecar 建连为 0)。
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

import httpx
import numpy as np
import websockets
from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.models import SpeakerEvent as CockpitSpeakerEvent, SpeakerProfile as CockpitSpeakerProfile
from app.services.speaker_matcher import (
    EnrolledProfile,
    LocalCosineMatcher,
    MatchResult,
    parse_embedding_json,
)

logger = logging.getLogger(__name__)

# --- 常量(ENGAGED_DRIFT §5.1 / MULTI_SPEAKER §4.2) ----------------------------------------

WINDOW_MS = 1_500  # 1.5s 滑窗
HOP_MS = 750  # 0.75s hop(50% 重叠,段边界说话不丢)
MAX_PENDING_WINDOWS = 4  # 背压:diarization 慢于实时时最多积压 4 窗,超出丢旧窗
BYTES_PER_MS = 32  # PCM16k mono s16le: 16000 * 2 / 1000
SEGMENT_KEEP_MS = 12_000  # 对齐窗口内保留的最近 diarization 段时长
CURRENT_SPEAKER_TTL_MS = 3_000  # "当前说话人"时效:最近一段人声结束后超时即视为无人说话
HOLDER_SEGMENT_MIN_CONF = 0.55  # "判本人"的归簇得分下限:低于此值降级"不确定"交闸门复核,防锚定簇误纳(2026-09-15)
SUMMARY_MAX_CHARS = 300
SUMMARY_MAX_EVENTS = 20  # 摘要取最近 N 条事件
_SPEAKER_LABEL_RE = re.compile(r"^speaker_(\d+)$")  # speaker_N → 「第N位」


def mono_now_ms() -> int:
    """会话无关的单调毫秒(相对进程启动);VoiceSidecar 建连时记 0 点。"""
    return int(time.monotonic() * 1000)


# --- 数据结构 -----------------------------------------------------------------------------


@dataclass(frozen=True)
class DiarSegment:
    """diarization 服务返回的一段人声(时间轴=会话内 mono ms)。"""

    speaker_label: str
    start_ms: int
    end_ms: int
    confidence: float = 0.0
    overlap: bool = False


@dataclass(frozen=True)
class AsrSentence:
    """ASR 侧路的最终句(时间轴已换算为会话内 mono ms)。"""

    text: str
    begin_ms: int
    end_ms: int
    confidence: float = 0.0


@dataclass(frozen=True)
class SpeakerUtterance:
    """对齐产物 = 事件账本写入单元(MULTI_SPEAKER §6.3)。"""

    speaker_label: str
    start_ms: int
    end_ms: int
    text: str
    asr_conf: float = 0.0
    overlap: bool = False


# --- SidecarBuffer(滑窗切窗) ---------------------------------------------------------------


class SidecarBuffer:
    """PCM 累积 → 按 1.5s 窗/0.75s hop 切窗 → 异步窗回调。

    feed 同步(浏览器每 ~100ms 一帧);切出的窗进 asyncio 队列,由 _window_loop
    消费。队列满 = 下游慢于实时 → 丢最旧窗并计数(侧路允许丢,主路绝不堵)。

    时钟(2026-09-14 修复):窗起点以首帧到达的 mono_now_ms() 为锚,全程 mono 毫秒——
    与 AsrSidecar 句时间戳(_t0+begin)同轴,Aligner 重叠/TTL 判断才对得上。
    此前窗起点从 0 起算,而 ASR 句是原始 mono(~1e8),两轴永不相交:
    align_utterances 全归 unknown,current_speaker 的 TTL 恒过期(闸门失效)。
    """

    def __init__(self, on_window: Callable[[bytes, int], Awaitable[None]]) -> None:
        self._on_window = on_window
        self._queue: asyncio.Queue[tuple[bytes, int]] = asyncio.Queue(maxsize=MAX_PENDING_WINDOWS)
        self._buf = bytearray()
        self._buf_start_ms = 0  # 首帧后 = mono 毫秒(见 feed)
        self._started_mono_ms: int | None = None  # 首帧 mono 锚点;None=还没收到音频
        self._emitted = 0  # 已切出的窗序号(窗 i 覆盖 [t0+i*HOP, t0+i*HOP+WINDOW])
        self.dropped_windows = 0

    def feed(self, pcm: bytes) -> None:
        if self._started_mono_ms is None:
            self._started_mono_ms = mono_now_ms()
            self._buf_start_ms = self._started_mono_ms
        self._buf.extend(pcm)
        self._advance()

    def _advance(self) -> None:
        t0 = self._started_mono_ms
        if t0 is None:  # feed 保证先锚定;防御性返回
            return
        while True:
            win_idx = self._emitted
            win_start = t0 + win_idx * HOP_MS
            win_end = win_start + WINDOW_MS
            # 窗 [win_start, win_end) 必须完整落在 buf 内(buf 覆盖 [buf_start_ms, ...))
            offset_start = (win_start - self._buf_start_ms) * BYTES_PER_MS
            offset_end = (win_end - self._buf_start_ms) * BYTES_PER_MS
            if offset_start < 0 or len(self._buf) < offset_end:
                return
            # 50% 重叠的关键:只消费到"下一窗起点",窗内后半段留给下一窗复用
            win_bytes = bytes(self._buf[offset_start:offset_end])
            consumed_to = win_start + HOP_MS
            drop = (consumed_to - self._buf_start_ms) * BYTES_PER_MS
            if drop > 0:
                del self._buf[:drop]
                self._buf_start_ms = consumed_to
            self._emitted += 1
            self._enqueue(win_bytes, win_start)

    def _enqueue(self, window: bytes, start_ms: int) -> None:
        try:
            self._queue.put_nowait((window, start_ms))
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()  # 丢最旧窗
            except asyncio.QueueEmpty:
                pass
            self.dropped_windows += 1
            try:
                self._queue.put_nowait((window, start_ms))
            except asyncio.QueueFull:
                self.dropped_windows += 1
                logger.warning("侧路窗队列仍然满,丢弃窗口 start=%sms", start_ms)

    def flush(self) -> None:
        """连接结束:不足一窗的尾巴也发出去(末段说话不丢)。"""
        if not self._buf:
            return
        tail = bytes(self._buf)
        start_ms = self._buf_start_ms
        self._buf = bytearray()
        self._buf_start_ms += len(tail) // BYTES_PER_MS
        self._emitted += 1
        self._enqueue(tail, start_ms)

    async def consume_forever(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                window, start_ms = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                await self._on_window(window, start_ms)
            except Exception:  # noqa: BLE001 - 侧路处理失败丢窗即可
                logger.warning("侧路窗处理失败(start=%s)", start_ms, exc_info=True)


# --- DiarizationClient(独立服务 HTTP) ------------------------------------------------------


class DiarizationUnavailable(RuntimeError):
    """diarization-service 5xx(模型未加载/服务不可用)——与"质量不足"显式区分,避免误报。"""


class DiarizationClient:
    """diarization-service 的 HTTP 客户端;失败只计数返回 None(丢窗降级)。"""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self.errors = 0

    def _new_client(self, timeout: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(trust_env=False, timeout=timeout)

    async def diarize(
        self, session_key: str, pcm: bytes, win_start_ms: int, holder_embedding: list[float] | None = None
    ) -> dict[str, Any] | None:
        """返回 {"segments": [DiarSegment dict...], "centroids": {label: [floats]}};失败 None。

        holder_embedding=持有人注册声纹(192 维),仅服务端首次建会话实例时消费
        (预建 speaker_0 锚定簇);之后每窗带上也无副作用(服务端忽略)。
        """
        payload: dict[str, Any] = {
            "session_key": session_key,
            "pcm_b64": base64.b64encode(pcm).decode("ascii"),
            "win_start_ms": win_start_ms,
        }
        if holder_embedding:
            payload["holder_embedding"] = holder_embedding
        try:
            async with self._new_client(0.8) as client:
                response = await client.post(f"{self._base_url}/diarize", json=payload)
                response.raise_for_status()
                return response.json()
        except (httpx.HTTPError, ValueError):
            self.errors += 1
            return None

    async def enroll(self, pcm: bytes) -> list[float] | None:
        """注册:一段 ≥10s PCM → 平均 embedding(存 cockpit_speaker_profiles)。

        5xx(模型未加载/服务不可用)抛 DiarizationUnavailable,由路由层映射 503;
        4xx/空 embedding 返回 None(质量不足,路由层映射 422)。不再把 5xx 吞成"质量不足"。
        """
        try:
            async with self._new_client(10.0) as client:
                response = await client.post(
                    f"{self._base_url}/enroll",
                    json={"pcm_b64": base64.b64encode(pcm).decode("ascii")},
                )
                if response.status_code >= 500:
                    raise DiarizationUnavailable(f"diarization-service /enroll {response.status_code}")
                response.raise_for_status()
                embedding = response.json().get("embedding")
                return list(map(float, embedding)) if isinstance(embedding, list) and embedding else None
        except DiarizationUnavailable:
            self.errors += 1
            raise
        except (httpx.HTTPError, ValueError):
            self.errors += 1
            return None

    async def reset(self, session_key: str) -> None:
        """会话结束释放服务端实例;404/任何失败忽略(幂等)。"""
        try:
            async with self._new_client(2.0) as client:
                await client.post(f"{self._base_url}/reset", json={"session_key": session_key})
        except httpx.HTTPError:
            pass


# --- AsrSidecar(DashScope 实时 ASR 独立 WSS) ------------------------------------------------


class AsrSidecar:
    """DashScope 实时识别 WSS(qwen3-asr-flash-realtime 同族, run-task 协议)。

    旁听转写走这条独立侧路,不进 qwen 会话上下文(破 600s/100 轮上限的关键)。
    task-started 到达前/任何失败后的帧直接丢弃(毫秒级,可忽略);
    失败标记 dead,上层降级=无转写(事件账本/摘要自动停,diarization 仍在跑)。
    """

    def __init__(self, on_sentence: Callable[[AsrSentence], Awaitable[None]]) -> None:
        self._on_sentence = on_sentence
        self._ws: Any = None
        self._recv_task: asyncio.Task | None = None
        self._t0_ms = 0  # run-task 发出时的 mono 时(ASR 时钟 → 会话时钟换算)
        self._ready = False  # task-started 之后才收发音频
        self._dead = False
        self.errors = 0

    @property
    def dead(self) -> bool:
        return self._dead

    async def start(self, model: str, api_key: str) -> bool:
        """建连 + run-task;返回是否成功(失败置 dead,上层降级)。"""
        if not api_key:
            self._dead = True
            return False
        try:
            self._ws = await websockets.connect(
                "wss://dashscope.aliyuncs.com/api-ws/v1/inference",
                additional_headers={"Authorization": f"Bearer {api_key}"},
                max_size=2**22,
                open_timeout=20,  # 实测握手 5s+,默认 10s 会误杀慢网
            )
        except Exception:  # noqa: BLE001 - 网络/鉴权失败一律降级
            self._mark_dead("ASR 侧路建连失败")
            return False
        self._t0_ms = mono_now_ms()
        task_id = uuid.uuid4().hex
        try:
            await self._ws.send(json.dumps({
                "header": {"action": "run-task", "task_id": task_id, "streaming": "duplex"},
                "payload": {
                    "task_group": "audio",
                    "task": "asr",
                    "function": "recognition",
                    "model": model,
                    "parameters": {"format": "pcm", "sample_rate": 16000},
                    "input": {},  # run-task 协议必填(2026-09-14 修复: 缺了会被 task-failed 拒)
                },
                "context": {"sdk": {"name": "hanxiaomi-voice-sidecar", "version": "1.0"}},
            }))
        except Exception:  # noqa: BLE001
            self._mark_dead("ASR 侧路 run-task 发送失败")
            return False
        self._recv_task = asyncio.create_task(self._recv_loop())
        return True

    async def feed(self, pcm: bytes) -> None:
        if self._dead or not self._ready or self._ws is None:
            return
        try:
            await self._ws.send(pcm)
        except Exception:  # noqa: BLE001
            self._mark_dead("ASR 侧路发送失败")

    async def _recv_loop(self) -> None:
        try:
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    continue
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                event = (message.get("header") or {}).get("event")
                if event == "task-started":
                    self._ready = True
                    continue
                if event == "task-failed":
                    self._mark_dead(f"ASR 侧路 task-failed: {(message.get('header') or {}).get('error_message', '')}")
                    return
                if event != "result-generated":
                    continue
                sentence = ((message.get("payload") or {}).get("output") or {}).get("sentence")
                if not isinstance(sentence, dict):
                    continue
                text = str(sentence.get("text") or "").strip()
                if not text:
                    continue
                # 终句判定:有 sentence_end 字段按字段;无字段视为终句(容错不同模型)
                if sentence.get("sentence_end") is False:
                    continue
                begin, end = self._sentence_times(sentence)
                if end <= begin:
                    continue
                try:
                    await self._on_sentence(AsrSentence(text, begin, end))
                except Exception:  # noqa: BLE001 - 回调失败不影响接收
                    logger.warning("ASR 句回调失败", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self._mark_dead("ASR 侧路接收循环异常")

    def _sentence_times(self, sentence: dict) -> tuple[int, int]:
        """句时间戳(ASR 任务内 ms) → 会话内 mono ms;字段缺失按累计字节估算。"""
        begin = sentence.get("begin_time")
        end = sentence.get("end_time")
        if not isinstance(begin, (int, float)) or not isinstance(end, (int, float)):
            return self._t0_ms, mono_now_ms()
        return self._t0_ms + int(begin), self._t0_ms + int(end)

    def _mark_dead(self, reason: str) -> None:
        if not self._dead:
            logger.warning("ASR 侧路降级: %s", reason)
        self._dead = True
        self.errors += 1

    async def close(self) -> None:
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._recv_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001
                pass
            self._ws = None


# --- Aligner(ASR 句 × diarization 段) ------------------------------------------------------


def align_utterances(segments: list[DiarSegment], sentence: AsrSentence) -> SpeakerUtterance | None:
    """对齐规则(MULTI_SPEAKER §6.3): 覆盖句时长 ≥50% 的重叠最大段归主;否则 unknown。

    段列表为空(侧路降级/diarization 挂) → 返回 speaker_label="unknown" 的句
    (事件照存,归属降级,摘要自动用"有人提到"措辞)。
    """
    duration = sentence.end_ms - sentence.begin_ms
    if duration <= 0:
        return None
    best: tuple[int, DiarSegment] | None = None
    for seg in segments:
        overlap = max(0, min(sentence.end_ms, seg.end_ms) - max(sentence.begin_ms, seg.start_ms))
        if overlap <= 0:
            continue
        if best is None or overlap > best[0]:
            best = (overlap, seg)
    if best is None:
        return SpeakerUtterance("unknown", sentence.begin_ms, sentence.end_ms, sentence.text, sentence.confidence, True)
    overlap_ms, seg = best
    if overlap_ms * 2 < duration:
        return SpeakerUtterance("unknown", sentence.begin_ms, sentence.end_ms, sentence.text, sentence.confidence, True)
    return SpeakerUtterance(seg.speaker_label, sentence.begin_ms, sentence.end_ms, sentence.text, sentence.confidence, seg.overlap)


# --- 声纹注册表(模块级缓存,enroll API 写库后 invalidate) ------------------------------------


_profiles_cache: dict[int, list[EnrolledProfile]] = {}


def invalidate_speaker_profiles(tenant_id: int | None = None) -> None:
    if tenant_id is None:
        _profiles_cache.clear()
    else:
        _profiles_cache.pop(tenant_id, None)


def load_enrolled_profiles(tenant_id: int) -> list[EnrolledProfile]:
    """active 注册声纹(懒加载 + 缓存);损坏的 embedding 行跳过并告警。"""
    cached = _profiles_cache.get(tenant_id)
    if cached is not None:
        return cached
    profiles: list[EnrolledProfile] = []
    with SessionLocal() as db:
        rows = db.scalars(
            select(CockpitSpeakerProfile).where(
                CockpitSpeakerProfile.tenant_id == tenant_id,
                CockpitSpeakerProfile.status == "active",
            )
        ).all()
    for row in rows:
        try:
            embedding = parse_embedding_json(row.embedding_ref)
        except (ValueError, TypeError):
            logger.warning("声纹注册行 embedding 损坏,跳过 profile#%s", row.id)
            continue
        profiles.append(EnrolledProfile(row.user_id, row.display_name, row.role_hint, embedding))
    _profiles_cache[tenant_id] = profiles
    return profiles


# --- SummaryRefresher(常驻现场摘要) ---------------------------------------------------------


class SummaryRefresher:
    """每写一条事件刷新一次【现场摘要】并注入 qwen 上下文(ENGAGED_DRIFT §5.2)。

    - 30s 缓存节流:距上次注入 < multi_speaker_summary_cache_s 直接跳过;
    - 无新事件跳过(话题不变不重生的等价简化:事件前缀未变就不重生);
    - LLM 走 settings.llm_* 通道(容错同 voice_tasks._llm_summary);
      LLM 不可用 → 直接拼事件模板(宁旧勿缺,纯本地兜底);
    - 注入失败只记日志(注入原语由调用方提供)。
    """

    def __init__(
        self,
        *,
        load_events: Callable[[], Awaitable[list[CockpitSpeakerEvent]]],
        inject: Callable[[str], Awaitable[None]],
        caption_of: Callable[[CockpitSpeakerEvent], str],
    ) -> None:
        self._load_events = load_events
        self._inject = inject
        self._caption_of = caption_of
        self._last_injected_at = 0.0
        self._last_event_id = 0
        self.refreshes = 0

    async def maybe_refresh(self, force: bool = False) -> None:
        settings = get_settings()
        now = time.monotonic()
        if not force and now - self._last_injected_at < settings.multi_speaker_summary_cache_s:
            return
        events = await self._load_events()
        if not events:
            return
        newest_id = events[-1].id
        if newest_id == self._last_event_id:
            return  # 没有新话语,摘要不会变
        summary = await self._compose_summary(events)
        try:
            await self._inject(summary)
        except Exception:  # noqa: BLE001 - 注入失败沿用旧摘要
            logger.warning("现场摘要注入失败", exc_info=True)
            return
        self._last_injected_at = now
        self._last_event_id = newest_id
        self.refreshes += 1

    async def _compose_summary(self, events: list[CockpitSpeakerEvent]) -> str:
        text = await self._llm_summary(events)
        if not text:
            text = self._fallback_summary(events)
        return text[:SUMMARY_MAX_CHARS]

    @classmethod
    def _fallback_summary(cls, events: list[CockpitSpeakerEvent], caption_of: Callable[[CockpitSpeakerEvent], str] | None = None) -> str:
        caption = caption_of or (lambda e: "有人")
        parts = [f"{caption(e)}说“{e.text.strip()[:40]}”" for e in events[-SUMMARY_MAX_EVENTS:]]
        return (
            "【现场摘要·以最新一条为准】刚才的讨论：" + "；".join(parts)
            + "。——仅供你参考，不要主动提起、不要念出来；被问到看法时按它承接。"
        )

    async def _llm_summary(self, events: list[CockpitSpeakerEvent]) -> str | None:
        settings = get_settings()
        if settings.llm_provider != "openai_compatible" or not settings.llm_base_url or not settings.llm_api_key:
            return None
        lines = [f"{self._caption_of(e)}: {e.text.strip()[:80]}" for e in events[-SUMMARY_MAX_EVENTS:]]
        payload = {
            "model": settings.llm_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是多人讨论的现场记录员。把发言序列压缩成不超过300字的中文摘要："
                        "逐人概括各自的观点（按人称名，如“小王认为售后关键”）；说话人不明的用“有人提到”；"
                        "最后以“当前话题：xxx”收尾。只输出摘要正文，不要任何前缀、列表或引号。"
                    ),
                },
                {"role": "user", "content": "\n".join(lines)},
            ],
            "temperature": 0.3,
            "max_tokens": 400,
        }
        headers = {"Authorization": f"Bearer {settings.llm_api_key}"}
        try:
            from app import runtime as cockpit_runtime

            async with cockpit_runtime.new_async_client(30.0) as client:
                response = await client.post(
                    f"{settings.llm_base_url.rstrip('/')}/chat/completions", json=payload, headers=headers
                )
                response.raise_for_status()
                raw = response.json()["choices"][0]["message"]["content"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return None
        return str(raw).strip() or None


# --- VoiceSidecar(每会话编排器) --------------------------------------------------------------


class VoiceSidecar:
    """一条语音连接的侧路总编排:主路 PCM 副本进来,事件+摘要出去。

    生命周期与 DashScopeVoiceSession 同生共死(WS handler try/finally close)。
    """

    def __init__(
        self,
        *,
        tenant_id: int,
        user_id: int,
        conversation_id: int,
        inject: Callable[[str], Awaitable[None]],
        mode: Literal["exclusive", "multi"] = "multi",
        on_speakers: Callable[[int, str | None], None] | None = None,
    ) -> None:
        settings = get_settings()
        self.tenant_id = int(tenant_id)
        self.user_id = int(user_id)
        self.conversation_id = int(conversation_id)
        self.mode = mode
        self.session_key = f"t{tenant_id}:u{user_id}:conv{conversation_id}:{uuid.uuid4().hex[:8]}"
        self._inject = inject
        self._on_speakers = on_speakers
        self._diar = DiarizationClient(settings.multi_speaker_diarization_url)
        self._segments: deque[DiarSegment] = deque()
        self._centroids: dict[str, list[float]] = {}
        self._matcher: LocalCosineMatcher | None = None
        self._name_cache: dict[int | None, str] = {}  # speaker_user_id → 注册名(caption 用)
        # 当前说话人追踪(专有模式闸门 + speakers 状态事件)
        self._current_speaker_label: str | None = None
        self._current_speaker_end_ms = 0
        self._current_speaker_display: str | None = None
        self._current_is_holder: bool | None = None  # True=本人;False=他人;None=未知/弱置信(三态,2026-09-15)
        self._last_speakers_push: tuple[int, str | None] | None = None
        # 簇数去抖(2026-09-14):原始簇数连续 2 窗一致才采信并推送,数字不跳变
        self._stable_count = 0
        self._count_candidate: int | None = None
        self._count_candidate_seen = 0
        self._holder_embedding: list[float] | None = None  # 本人注册声纹(diarize 锚点用)
        self._active_count = 0  # 在场说话人数(diarize active_speakers,活跃 TTL 过滤)
        self._asr_model = settings.multi_speaker_asr_model or settings.realtime_voice_asr_model
        self._asr = AsrSidecar(self._on_sentence)
        self._buffer = SidecarBuffer(self._on_window)
        self._refresher = SummaryRefresher(
            load_events=self._load_recent_events,
            inject=self._inject_summary,
            caption_of=self._caption_of,
        )
        self._stop = asyncio.Event()
        self._consumer_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self.started = False

    # -- 当前说话人(专有模式闸门判据,spirit-voice-modes-plan §4.2) --

    @property
    def current_speaker_is_holder(self) -> bool | None:
        """最近 3s 内的当前说话人是否本人。

        True=本人;False=他人(注册过的别人或未注册声源);None=未知/无人说话
        (diarization 无数据或超时)或弱置信"本人"(归簇得分 < HOLDER_SEGMENT_MIN_CONF,
        2026-09-15)——专有闸门对 None 先放行并挂延迟复核(fail-open,宁可多答不可哑火)。
        """
        if self._current_speaker_label is None:
            return None
        if mono_now_ms() - self._current_speaker_end_ms > CURRENT_SPEAKER_TTL_MS:
            return None
        return self._current_is_holder

    @property
    def current_speaker_debug(self) -> str:
        """专有闸门日志用:当前说话人一行描述(label/展示名/归属判定/距末段 ms)。"""
        verdict = {True: "holder", False: "other", None: "unknown"}[self.current_speaker_is_holder]
        age = mono_now_ms() - self._current_speaker_end_ms if self._current_speaker_end_ms else -1
        return f"label={self._current_speaker_label} display={self._current_speaker_display} verdict={verdict} age_ms={age}"

    def _track_current_speaker(self, segments: list[DiarSegment]) -> None:
        """用本窗最新段更新当前说话人(标签+归属+展示名);调用方需持锁。

        三态归属(2026-09-15): 判"本人"但归簇得分 < HOLDER_SEGMENT_MIN_CONF 时
        降级 None(不确定)——锚定簇曾把音色相近的人吸进 speaker_0,而后端看到的
        锚定质心=注册声纹,matcher 恒以 cos 1.0 命中本人;段级 confidence(归簇得分)
        才是"这句话像不像本人"的诚实信号。
        """
        latest: DiarSegment | None = None
        for seg in segments:
            if latest is None or seg.end_ms >= latest.end_ms:
                latest = seg
        if latest is None:
            return
        match = self._resolve(latest.speaker_label, self._centroids)
        is_holder = bool(match.matched and match.user_id is not None and match.user_id == self.user_id)
        holder_state: bool | None = is_holder
        if is_holder and latest.confidence < HOLDER_SEGMENT_MIN_CONF:
            holder_state = None  # 弱置信"本人" → 不确定,交专有闸门延迟复核
        display: str | None
        if is_holder:
            display = "你"
        elif match.matched:
            display = match.display_name
        else:
            display = self._speaker_display(latest.speaker_label)
        self._current_speaker_label = latest.speaker_label
        self._current_speaker_end_ms = latest.end_ms
        self._current_is_holder = holder_state
        self._current_speaker_display = display

    def _speaker_display(self, label: str) -> str:
        """簇标签 → 展示名:speaker_0=持有人锚定簇 → 你;speaker_N → 第N位;其余 → 有人。"""
        if label == "speaker_0":
            return "你"
        m = _SPEAKER_LABEL_RE.match(label or "")
        return f"第{m.group(1)}位" if m else "有人"

    def _maybe_push_speakers(self) -> None:
        """簇数(去抖后)/当前说话人有变化才推(0.75s 窗节奏下降噪)。"""
        if self._on_speakers is None:
            return
        current = self._current_speaker_display
        if mono_now_ms() - self._current_speaker_end_ms > CURRENT_SPEAKER_TTL_MS:
            current = None
        # 簇数去抖:原始值连续 2 窗一致才采信(单窗噪声炸簇不推到前端);
        # 从未推送过时首次直接采信(开场不该显示"0人")
        raw_count = self._active_count
        if self._last_speakers_push is None:
            self._stable_count = raw_count
        elif raw_count == self._stable_count:
            self._count_candidate = None
            self._count_candidate_seen = 0
        elif raw_count == self._count_candidate:
            self._count_candidate_seen += 1
            if self._count_candidate_seen >= 2:
                self._stable_count = raw_count
                self._count_candidate = None
                self._count_candidate_seen = 0
        else:
            self._count_candidate = raw_count
            self._count_candidate_seen = 1
        snapshot = (self._stable_count, current)
        if snapshot == self._last_speakers_push:
            return
        self._last_speakers_push = snapshot
        try:
            self._on_speakers(snapshot[0], current)
        except Exception:  # noqa: BLE001 - 状态事件推送失败不影响侧路
            logger.warning("speakers 状态事件推送失败", exc_info=True)

    # -- 生命周期 --

    async def start(self) -> bool:
        """启动窗消费循环 + ASR 侧路(失败降级)。返回 ASR 是否可用。

        消费循环先起跑:started 一置位 feed 即落缓冲,diarization 不等 ASR
        握手(端到端数秒)——主路音频闸门一开,人数识别立刻在线;ASR 转写晚
        几秒就绪,事件账本/摘要自动晚到(与 ASR 中途挂掉同义的降级)。
        """
        settings = get_settings()
        profiles = load_enrolled_profiles(self.tenant_id)
        self._matcher = LocalCosineMatcher(
            profiles,
            threshold=settings.multi_speaker_resolve_threshold,
            holder_user_id=self.user_id,
        )
        # 本人注册声纹 → diarization 锚点(speaker_0):我开口即归"我",不靠事后比对
        for profile in profiles:
            if profile.user_id is not None and profile.user_id == self.user_id:
                self._holder_embedding = profile.embedding.astype(float).tolist()
                break
        self._consumer_task = asyncio.create_task(self._buffer.consume_forever(self._stop))
        self.started = True
        return await self._asr.start(self._asr_model, settings.dashscope_realtime_api_key)

    async def close(self) -> None:
        """收尾:flush 尾窗、释放 diarization 实例、关 ASR。幂等。"""
        if not self.started:
            return
        self.started = False
        self._buffer.flush()
        # 给尾窗消费一点时间(不 await 消费,只短暂让步)
        await asyncio.sleep(0.2)
        self._stop.set()
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._consumer_task = None
        await self._asr.close()
        await self._diar.reset(self.session_key)

    # -- 数据面 --

    async def feed(self, pcm: bytes) -> None:
        """主路二进制帧的副本:喂滑窗缓冲 + ASR 流。永不抛。"""
        if not self.started:
            return
        self._buffer.feed(pcm)
        await self._asr.feed(pcm)

    async def _on_window(self, window: bytes, start_ms: int) -> None:
        """一窗 PCM → diarization → 段表+质心表滚动更新 + 当前说话人追踪。"""
        result = await self._diar.diarize(self.session_key, window, start_ms, self._holder_embedding)
        if result is None:
            return
        window_segments: list[DiarSegment] = []
        async with self._lock:
            for raw in result.get("segments") or []:
                seg = DiarSegment(
                    speaker_label=str(raw.get("speaker_label") or "unknown"),
                    start_ms=int(raw.get("start_ms") or 0),
                    end_ms=int(raw.get("end_ms") or 0),
                    confidence=float(raw.get("confidence") or 0.0),
                    overlap=bool(raw.get("overlap")),
                )
                if seg.end_ms > seg.start_ms:
                    self._segments.append(seg)
                    window_segments.append(seg)
            centroids = result.get("centroids")
            if isinstance(centroids, dict):
                self._centroids.update(centroids)
            active = result.get("active_speakers")
            if isinstance(active, list):
                self._active_count = len(active)  # 在场人数(活跃 TTL 过滤后,噪声簇不占人头)
            while self._segments and self._segments[0].end_ms < start_ms - SEGMENT_KEEP_MS:
                self._segments.popleft()
            if window_segments:
                self._track_current_speaker(window_segments)
            self._maybe_push_speakers()

    async def _on_sentence(self, sentence: AsrSentence) -> None:
        """ASR 终句 → 对齐 → 声纹归属 → 事件落库 → 摘要刷新。"""
        async with self._lock:
            segments = list(self._segments)
            centroids = dict(self._centroids)
        utterance = align_utterances(segments, sentence)
        if utterance is None:
            return
        match = self._resolve(utterance.speaker_label, centroids)
        event_id = self._write_event(utterance, match, sentence)
        if event_id is not None:
            await self._refresher.maybe_refresh()

    def _resolve(self, speaker_label: str, centroids: dict[str, list[float]]) -> MatchResult:
        """簇标签 → 注册身份;无质心/无注册表 → unknown 降级。"""
        if self._matcher is None:
            return MatchResult(None, speaker_label, "unknown", 0.0, False)
        centroid = centroids.get(speaker_label)
        if not centroid:
            return MatchResult(None, speaker_label, "unknown", 0.0, False)
        match = self._matcher.resolve(speaker_label, np.asarray(centroid, dtype=np.float32))
        if match.matched and match.user_id is not None:
            self._name_cache[match.user_id] = match.display_name
        return match

    def _caption_of(self, event: CockpitSpeakerEvent) -> str:
        """事件 → 摘要里的人称(spirit-voice-modes-plan §3.2)。

        降级规则:overlap 一律"有人";unknown 且是 speaker_N 簇 → "第N位"(多人模式
        无声纹也能区分人);me → "你";其余注册命中 → 注册名;无任何线索 → "有人"。
        """
        if event.overlap:
            return "有人"
        if event.speaker_role == "me":
            return "你"
        if event.speaker_role == "unknown":
            return self._speaker_display(event.speaker_label)
        return self._name_cache.get(event.speaker_user_id) or "有人"

    def _write_event(self, utterance: SpeakerUtterance, match: MatchResult, sentence: AsrSentence) -> int | None:
        """事件落库(含规则层唤醒词判定);失败返回 None(侧路不阻塞)。"""
        settings = get_settings()
        wake_words = [w.strip() for w in settings.multi_speaker_wake_words.split(",") if w.strip()]
        is_wake = any(w in utterance.text for w in wake_words)
        speaker_role = match.role if match.matched else "unknown"
        try:
            with SessionLocal() as db:
                event = CockpitSpeakerEvent(
                    tenant_id=self.tenant_id,
                    conversation_id=self.conversation_id,
                    voice_session_id=self.session_key,
                    speaker_label=utterance.speaker_label,
                    speaker_user_id=match.user_id if match.matched else None,
                    speaker_role=speaker_role,
                    start_ms=utterance.start_ms,
                    end_ms=utterance.end_ms,
                    text=utterance.text[:2000],
                    asr_conf=utterance.asr_conf or None,
                    overlap=utterance.overlap,
                    addressed_to="ai" if is_wake else ("me" if speaker_role == "me" else "unknown"),
                    is_wake=is_wake,
                )
                db.add(event)
                db.commit()
                return event.id
        except Exception:  # noqa: BLE001 - DB 抖动不阻塞侧路
            logger.warning("话语事件落库失败", exc_info=True)
            return None

    async def _load_recent_events(self) -> list[CockpitSpeakerEvent]:
        """本会话最近 N 条事件(摘要原料)。"""
        with SessionLocal() as db:
            rows = db.scalars(
                select(CockpitSpeakerEvent)
                .where(
                    CockpitSpeakerEvent.conversation_id == self.conversation_id,
                    CockpitSpeakerEvent.voice_session_id == self.session_key,
                )
                .order_by(CockpitSpeakerEvent.id.desc())
                .limit(SUMMARY_MAX_EVENTS)
            ).all()
            for row in rows:
                db.expunge(row)
        return list(reversed(rows))

    async def _inject_summary(self, text: str) -> None:
        await self._inject(text)


# --- 隐私清理(30 天,挂后台/运维调用) --------------------------------------------------------


def purge_expired_speaker_events() -> int:
    """删除超期话语事件(隐私承诺);返回删除行数。"""
    from datetime import datetime, timedelta

    from sqlalchemy import delete

    settings = get_settings()
    cutoff = datetime.utcnow() - timedelta(days=settings.multi_speaker_event_retention_days)
    with SessionLocal() as db:
        result = db.execute(
            delete(CockpitSpeakerEvent).where(CockpitSpeakerEvent.created_at < cutoff)
        )
        db.commit()
        return int(result.rowcount or 0)


# --- diarization 健康探测(status 端点/WS 守卫共用,30s 模块级缓存) --------------------------


_health_cache: tuple[float, bool] | None = None
HEALTH_CACHE_S = 30.0


async def diarization_healthy() -> bool:
    """diarization-service /health 探测;失败/超时(0.5s) False,结果缓存 30s。

    模型未加载(funasr 缺失等)时 /health 仍返回 200 但 model_loaded=false——
    必须判不可用,否则 enroll 的 503 会被误报成"有效人声不足"(2026-09-18 实测踩坑)。
    """
    global _health_cache
    now = time.monotonic()
    if _health_cache is not None and now - _health_cache[0] < HEALTH_CACHE_S:
        return _health_cache[1]
    settings = get_settings()
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=0.5) as client:
            response = await client.get(f"{settings.multi_speaker_diarization_url.rstrip('/')}/health")
            ok = response.status_code == 200
            if ok:
                payload = response.json()
                if isinstance(payload, dict) and "model_loaded" in payload:
                    ok = bool(payload.get("model_loaded"))
    except Exception:  # noqa: BLE001 - 任何网络问题都视为不可用
        ok = False
    _health_cache = (now, ok)
    return ok


def has_active_profile(tenant_id: int, user_id: int) -> bool:
    """本人是否存在 active 声纹(专有模式守卫/状态展示)。"""
    with SessionLocal() as db:
        row = db.scalar(
            select(CockpitSpeakerProfile.id).where(
                CockpitSpeakerProfile.tenant_id == tenant_id,
                CockpitSpeakerProfile.user_id == user_id,
                CockpitSpeakerProfile.status == "active",
            )
        )
        return row is not None
