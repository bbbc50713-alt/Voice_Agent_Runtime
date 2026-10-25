"""多人认知层 Layer 1 单测(services/voice_sidecar.py + speaker_matcher.py)。

方案: docs/ENGAGED_DRIFT_AND_SPEAKER_ATTRIBUTION_PLAN.md §5.1 + MULTI_SPEAKER §4.2/§6.3/§7.4。
自平台 backend/tests/test_voice_sidecar.py 移植;差异: 无 users/conversations 业务表
(speaker_events.conversation_id 纯 Integer),_write_event 用例直接铸 int id,不再建行/清理。

覆盖:
- SidecarBuffer 滑窗(1.5s 窗/0.75s hop/50% 重叠) + 背压丢窗 + flush 尾巴;
- Aligner(句×段 ≥50% 归主/<50% unknown/空段表降级);
- LocalCosineMatcher 三档(命中/疑似/未注册) + 持有人锚定;
- SummaryRefresher 兜底模板措辞(你/注册名/有人) + 缓存节流 + 无新事件跳过;
- VoiceSidecar._write_event(唤醒词→is_wake/addressed_to=ai;归属落列);
- DiarizationClient 降级(HTTP 500 → None + 计数,绝不抛);
- instructions 拼接(multi_speaker_enabled → 【多人现场】段)。

无外部依赖: 不连 DashScope/diarization-service,模型层全 mock/降级路径。
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from app.config import get_settings
from app.db import SessionLocal
from app.models import SpeakerEvent as CockpitSpeakerEvent
from app.services import cockpit_voice, voice_sidecar
from app.services.speaker_matcher import EnrolledProfile, LocalCosineMatcher, parse_embedding_json
from app.services.voice_sidecar import (
    AsrSentence,
    DiarSegment,
    SidecarBuffer,
    SpeakerUtterance,
    SummaryRefresher,
    align_utterances,
)



# --- 工具 -------------------------------------------------------------------------------


def ms_pcm(ms: int) -> bytes:
    """造 n 毫秒的 PCM(非零,便于重叠字节比对)。"""
    return bytes((i * 7 + 13) % 256 for i in range(ms * 32))  # BYTES_PER_MS=32


def drained(buffer: SidecarBuffer) -> list[tuple[bytes, int]]:
    out: list[tuple[bytes, int]] = []
    while not buffer._queue.empty():
        out.append(buffer._queue.get_nowait())
    return out


# --- SidecarBuffer(滑窗/重叠/背压/flush) ---------------------------------------------------


def test_sidecar_buffer_windows_have_50pct_overlap() -> None:
    """喂 2250ms → 窗0 [t0,t0+1500) + 窗1 [t0+750,t0+2250):均为完整 1.5s,前窗后半==后窗前半。"""
    buffer = SidecarBuffer(lambda w, s: asyncio.sleep(0))
    buffer.feed(ms_pcm(750))
    assert drained(buffer) == []  # 不足一窗
    t0 = buffer._started_mono_ms  # 首帧 mono 锚点(2026-09-14 时钟修复:窗起点走 mono 轴)
    assert t0 is not None
    buffer.feed(ms_pcm(750))  # 累计 1500ms → 窗 0
    windows = drained(buffer)
    assert len(windows) == 1
    win0, start0 = windows[0]
    assert start0 == t0
    assert len(win0) == 1500 * 32
    buffer.feed(ms_pcm(750))  # 累计 2250ms → 窗 1
    windows = drained(buffer)
    assert len(windows) == 1
    win1, start1 = windows[0]
    assert start1 == t0 + 750
    assert len(win1) == 1500 * 32  # 修复点:完整 1.5s,而非 750ms 新数据
    assert win1[: 750 * 32] == win0[750 * 32 :]  # 50% 重叠字节一致


def test_sidecar_buffer_backpressure_drops_oldest() -> None:
    """队列满 4 窗:继续喂 → 丢最旧,dropped_windows 增长,队列有界。"""
    buffer = SidecarBuffer(lambda w, s: asyncio.sleep(0))
    for _ in range(26):  # 26 窗 = ~20s 音频
        buffer.feed(ms_pcm(750))
    assert buffer._queue.qsize() <= 4
    assert buffer.dropped_windows >= 20


def test_sidecar_buffer_flush_emits_tail() -> None:
    """1800ms:1 整窗 + flush 后补尾巴窗([t0+750,t0+1800) 全量,宁重复不丢末段说话)。"""
    buffer = SidecarBuffer(lambda w, s: asyncio.sleep(0))
    buffer.feed(ms_pcm(1500))
    assert len(drained(buffer)) == 1
    t0 = buffer._started_mono_ms
    assert t0 is not None
    buffer.feed(ms_pcm(300))
    buffer.flush()
    tails = drained(buffer)
    assert len(tails) == 1
    tail, start = tails[0]
    assert start == t0 + 750
    assert len(tail) == 1050 * 32  # buf 全量:重叠的 [750,1500) + 新增 [1500,1800)


def test_window_clock_matches_asr_sentence_clock() -> None:
    """回归(2026-09-14):窗/段时间轴必须与 ASR 句时间戳同轴(mono 毫秒)。

    此前窗起点从 0 起算、ASR 句是原始 mono(~1e8),align_utterances 零重叠
    → 话语归属全 unknown;current_speaker TTL 恒过期 → 专有闸门永远 fail-open。
    """
    buffer = SidecarBuffer(lambda w, s: asyncio.sleep(0))
    buffer.feed(ms_pcm(1500))
    _, start = buffer._queue.get_nowait()
    # 同一时刻的段(窗轴)与句(ASR 轴):重叠判定必须生效
    out = align_utterances(
        [DiarSegment("speaker_1", start, start + 1500)],
        AsrSentence("同一时刻的话", start, start + 1500, 0.9),
    )
    assert out is not None and out.speaker_label == "speaker_1"


# --- Aligner(句 × diarization 段) ----------------------------------------------------------


def test_align_major_overlap_attributed() -> None:
    """句 [1000,3000) 与段 A [950,3050) 重叠 2000/2000 ≥50% → 归 A。"""
    segments = [
        DiarSegment("speaker_1", 950, 3050),
        DiarSegment("speaker_2", 4000, 5000),
    ]
    out = align_utterances(segments, AsrSentence("明天售后例会谁讲", 1000, 3000, 0.9))
    assert out is not None
    assert out.speaker_label == "speaker_1"
    assert out.overlap is False


def test_align_partial_overlap_becomes_unknown() -> None:
    """句 [1000,3000) 与段 [1600,2000) 仅重叠 400ms <50% → unknown(不硬猜)。"""
    segments = [DiarSegment("speaker_1", 1600, 2000)]
    out = align_utterances(segments, AsrSentence("边缘碎片句", 1000, 3000, 0.9))
    assert out is not None
    assert out.speaker_label == "unknown"
    assert out.overlap is True


def test_align_no_segments_degrades_to_unknown() -> None:
    """diarization 挂了(段表空) → 句照存,归属 unknown。"""
    out = align_utterances([], AsrSentence("降级句", 0, 900, 0.5))
    assert out is not None
    assert out.speaker_label == "unknown"


# --- LocalCosineMatcher(三档 + 持有人锚定) --------------------------------------------------


def _profile(user_id: int, name: str, role: str, direction: list[float]) -> EnrolledProfile:
    import numpy as np

    vec = np.asarray(direction, dtype=np.float32)
    vec = vec / np.linalg.norm(vec)
    return EnrolledProfile(user_id, name, role, vec)


def test_matcher_hit_suspect_miss() -> None:
    import numpy as np

    enrolled = [
        _profile(11, "小王", "staff", [1, 1, 1, 1, 0, 0, 0, 0]),
        _profile(12, "小张", "staff", [0, 0, 0, 0, 1, 1, 1, 1]),
    ]
    matcher = LocalCosineMatcher(enrolled, threshold=0.65, holder_user_id=99)

    # 与小王同向(另一坐标全 0) → cos=1 命中
    hit = matcher.resolve("speaker_1", np.array([1, 1, 1, 1, 0, 0, 0, 0], dtype=np.float32))
    assert hit.matched and hit.user_id == 11 and hit.display_name == "小王" and hit.role == "staff"

    # 正交方向(cos=0) → 未注册,无疑似名
    miss = matcher.resolve("speaker_2", np.array([1, 0, 0, 0, 0, 0, 0, 0], dtype=np.float32))
    assert not miss.matched and miss.suspect_name is None


def test_matcher_suspect_band_keeps_name() -> None:
    """cos 落 0.55~0.65 疑似带:不命中但留候选名(摘要降级措辞用)。"""
    import numpy as np

    enrolled = [_profile(11, "小王", "staff", [1, 1, 1, 1, 0, 0, 0, 0])]
    matcher = LocalCosineMatcher(enrolled, threshold=0.65, holder_user_id=99)
    # 合成向量: 与小王方向 cos=0.6(0.6*主方向 + 0.8*正交分量)
    wang = np.array([0.5, 0.5, 0.5, 0.5, 0, 0, 0, 0], dtype=np.float32)
    ortho = np.array([0.5, -0.5, 0.5, -0.5, 0, 0, 0, 0], dtype=np.float32)
    vec = 0.6 * wang + 0.8 * ortho
    suspect = matcher.resolve("speaker_1", vec)
    assert not suspect.matched
    assert suspect.suspect_name == "小王"


def test_matcher_holder_boost_promotes_me() -> None:
    """持有人锚定:本人原始 cos=0.58,×1.2=0.696 ≥ 阈值 → 命中且 role=me(防本人被误判成同事)。"""
    import numpy as np

    enrolled = [
        _profile(11, "小王", "staff", [1, 1, 1, 1, 0, 0, 0, 0]),
        _profile(99, "我", "me", [1, 1, 1, 1, 1, 1, 1, 1]),
    ]
    matcher = LocalCosineMatcher(enrolled, threshold=0.65, holder_user_id=99)
    me_dir = np.full(8, 1.0 / np.sqrt(8), dtype=np.float32)
    ortho = np.array([1, -1, 1, -1, 1, -1, 1, -1], dtype=np.float32) / np.sqrt(8)
    vec = 0.58 * me_dir + 0.8145 * ortho  # cos(我)=0.58; cos(小王)≈0.41
    result = matcher.resolve("speaker_1", vec)
    assert result.matched and result.user_id == 99 and result.role == "me"


def test_parse_embedding_json_roundtrip() -> None:
    import numpy as np

    vec = np.ones(4, dtype=np.float32) / 2.0
    parsed = parse_embedding_json(json.dumps([0.5, 0.5, 0.5, 0.5]))
    assert np.allclose(parsed, vec)


# --- SummaryRefresher(兜底模板 + 节流) ------------------------------------------------------


def _fake_event(eid: int, *, role: str = "unknown", user_id: int | None = None, text: str = "内容") -> SimpleNamespace:
    return SimpleNamespace(id=eid, speaker_role=role, speaker_user_id=user_id, overlap=False, text=text)


def test_fallback_summary_captions() -> None:
    """兜底模板人称: me→你、注册名、unknown→有人;含【现场摘要·以最新一条为准】锚。"""
    events = [
        _fake_event(1, role="me", text="我觉得价格还能谈"),
        _fake_event(2, role="staff", user_id=11, text="售后是关键"),
        _fake_event(3, role="unknown", text="预算有限"),
    ]

    def caption(e: SimpleNamespace) -> str:
        if e.speaker_role == "me":
            return "你"
        if e.speaker_role == "unknown":
            return "有人"
        return "小王"

    text = SummaryRefresher._fallback_summary(events, caption_of=caption)
    assert "【现场摘要·以最新一条为准】" in text
    assert "你说" in text
    assert "小王说" in text
    assert "有人说" in text
    assert "不要主动提起" in text


def test_summary_refresher_throttle_and_no_new_events() -> None:
    """30s 缓存节流 + 无新事件跳过:两次 maybe_refresh 只注入一次。"""
    injected: list[str] = []
    events = [_fake_event(1, text="第一句"), _fake_event(2, text="第二句")]

    async def _load_events() -> list:
        return events

    async def _inject(text: str) -> None:
        injected.append(text)

    refresher = SummaryRefresher(load_events=_load_events, inject=_inject, caption_of=lambda e: "有人")
    asyncio.run(refresher.maybe_refresh(force=True))
    asyncio.run(refresher.maybe_refresh())  # 距上次 <30s → 跳过
    assert len(injected) == 1
    events.append(_fake_event(3, text="第三句"))
    asyncio.run(refresher.maybe_refresh())  # 仍 <30s → 跳过(时间窗优先)
    assert len(injected) == 1


# --- VoiceSidecar._write_event(事件账本写流) ------------------------------------------------


def _make_sidecar(tenant_id: int, user_id: int, conversation_id: int) -> voice_sidecar.VoiceSidecar:
    async def _inject(text: str) -> None:  # pragma: no cover - 不触发
        return None

    return voice_sidecar.VoiceSidecar(
        tenant_id=tenant_id, user_id=user_id, conversation_id=conversation_id, inject=_inject
    )


def test_write_event_wake_word_marks_addressed_to_ai() -> None:
    """句含唤醒词 → is_wake=True + addressed_to=ai;matcher 命中 → 归属列落列。

    独立服务无 users/conversations 业务表: 直接铸 int id(平台建行仅为了满足 FK)。
    """
    user_id, conv_id = 7001, 9001
    sidecar = _make_sidecar(1, user_id, conv_id)
    utterance = SpeakerUtterance("speaker_1", 100, 1600, "汉二密，帮我盘一下本周新增客户", 0.92, False)
    match = voice_sidecar.MatchResult(user_id, "本人", "me", 0.81, True)
    sentence = AsrSentence(utterance.text, 100, 1600, 0.92)
    event_id = sidecar._write_event(utterance, match, sentence)

    assert event_id is not None
    with SessionLocal() as db:
        row = db.get(CockpitSpeakerEvent, event_id)
        assert row is not None
        assert row.is_wake is True
        assert row.addressed_to == "ai"
        assert row.speaker_user_id == user_id
        assert row.speaker_role == "me"
        assert row.voice_session_id == sidecar.session_key


def test_write_event_plain_staff_speech_not_wake() -> None:
    """同事说话无唤醒词 → is_wake=False + addressed_to=unknown + speaker_role=staff。"""
    user_id, conv_id = 7002, 9002
    sidecar = _make_sidecar(1, user_id, conv_id)
    utterance = SpeakerUtterance("speaker_2", 0, 900, "这个客户预算大概三十万", 0.88, False)
    match = voice_sidecar.MatchResult(12, "小王", "staff", 0.7, True)
    event_id = sidecar._write_event(utterance, match, AsrSentence(utterance.text, 0, 900, 0.88))

    assert event_id is not None
    with SessionLocal() as db:
        row = db.get(CockpitSpeakerEvent, event_id)
        assert row is not None
        assert row.is_wake is False
        assert row.addressed_to == "unknown"
        assert row.speaker_role == "staff"


# --- DiarizationClient 降级 ----------------------------------------------------------------


def test_diarization_client_http_failure_degrades() -> None:
    """diarization-service 500 → diarize 返回 None + errors 计数,绝不抛(主路不传染)。"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = voice_sidecar.DiarizationClient("http://127.0.0.1:8791")
    original = voice_sidecar.DiarizationClient._new_client
    voice_sidecar.DiarizationClient._new_client = lambda self, timeout: httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False, timeout=timeout
    )
    try:
        result = asyncio.run(client.diarize("t1:u1:conv1:ab", b"\x00" * 100, 0))
    finally:
        voice_sidecar.DiarizationClient._new_client = original
    assert result is None
    assert client.errors == 1


# --- instructions 拼接(Layer 1 段) ----------------------------------------------------------


def test_instructions_sidecar_section_toggle(monkeypatch) -> None:
    """multi_speaker_enabled 开 → instructions 末尾有【多人现场】段;关 → 无。"""
    settings = get_settings()
    monkeypatch.setattr(settings, "realtime_voice_observe_enabled", True)
    monkeypatch.setattr(settings, "multi_speaker_enabled", True)
    text_on = cockpit_voice.load_voice_instructions()
    assert "【多人现场】" in text_on
    assert "现场摘要·以最新一条为准" in text_on
    monkeypatch.setattr(settings, "multi_speaker_enabled", False)
    assert "【多人现场】" not in cockpit_voice.load_voice_instructions()


# --- 三模式:当前说话人追踪 + 「第N位」caption + on_speakers(spirit-voice-modes-plan §4.2) ----


def test_speaker_display_and_caption_nth() -> None:
    """speaker_2 → 第2位;非 speaker_N → 有人;me → 你;注册名命中照用。"""
    sidecar = _make_sidecar(1, 1, 1)
    assert sidecar._speaker_display("speaker_2") == "第2位"
    assert sidecar._speaker_display("speaker_12") == "第12位"
    assert sidecar._speaker_display("unknown") == "有人"
    assert sidecar._speaker_display("") == "有人"

    assert sidecar._caption_of(
        _fake_event(1, role="me", text="我来")
    ) == "你"
    # unknown + 簇标签 → 第N位(多人模式无声纹仍能区分人)
    ev = _fake_event(2, role="unknown", text="观点")
    ev.speaker_label = "speaker_3"
    assert sidecar._caption_of(ev) == "第3位"
    # overlap 一律有人
    ev2 = _fake_event(3, role="staff", user_id=11, text="重叠")
    ev2.overlap = True
    assert sidecar._caption_of(ev2) == "有人"


def test_current_speaker_tracking_and_gate() -> None:
    """专有闸门判据:他人簇 → False(拦截);无数据/超时 → None(fail-open)。"""
    sidecar = _make_sidecar(1, 1, 1)
    # 无任何 diarization 数据 → None
    assert sidecar.current_speaker_is_holder is None
    # 本窗最新段=他人簇(无注册表 → unknown 归属,非持有人) → False
    sidecar._centroids = {"speaker_2": [1.0, 0.0]}
    now = voice_sidecar.mono_now_ms()
    sidecar._track_current_speaker(
        [voice_sidecar.DiarSegment("speaker_2", now - 1500, now - 200, 0.9, False)]
    )
    assert sidecar.current_speaker_is_holder is False
    # 时间推移超 3s → 回 None
    sidecar._current_speaker_end_ms = voice_sidecar.mono_now_ms() - 4000
    assert sidecar.current_speaker_is_holder is None


def test_weak_confidence_holder_segment_is_uncertain() -> None:
    """弱置信"本人"段(归簇得分 < HOLDER_SEGMENT_MIN_CONF) → 判 None 不确定交闸门复核;
    高置信本人段 → True。锚定簇曾吞音色相近的人(cos 恒 1.0 命中),段级置信度
    才是"这句话像不像本人"的诚实信号(2026-09-15)。"""
    sidecar = _make_sidecar(1, 1, 1)
    sidecar._matcher = voice_sidecar.LocalCosineMatcher(
        [voice_sidecar.EnrolledProfile(1, "主管", "staff", voice_sidecar.parse_embedding_json("[1.0, 0.0]"))],
        threshold=0.65,
        holder_user_id=1,
    )
    sidecar._centroids = {"speaker_0": [1.0, 0.0]}
    now = voice_sidecar.mono_now_ms()
    # 弱置信段(0.47 < 0.55):matcher cos 1.0 命中本人,但归簇得分不足 → None(不确定)
    sidecar._track_current_speaker(
        [voice_sidecar.DiarSegment("speaker_0", now - 1500, now - 200, 0.47, False)]
    )
    assert sidecar.current_speaker_is_holder is None
    assert "verdict=unknown" in sidecar.current_speaker_debug
    # 高置信段(0.8 ≥ 0.55) → True(本人)
    sidecar._track_current_speaker(
        [voice_sidecar.DiarSegment("speaker_0", now - 100, now, 0.8, False)]
    )
    assert sidecar.current_speaker_is_holder is True
    assert "verdict=holder" in sidecar.current_speaker_debug


def test_on_speakers_push_on_change_only() -> None:
    """on_speakers 状态事件:簇数/当前说话人变化才推,重复快照不重推。"""
    pushed: list[tuple[int, str | None]] = []
    sidecar = voice_sidecar.VoiceSidecar(
        tenant_id=1, user_id=1, conversation_id=1,
        inject=lambda text: None, on_speakers=lambda count, current: pushed.append((count, current)),
    )
    now = voice_sidecar.mono_now_ms()
    sidecar._centroids = {"speaker_1": [1.0, 0.0], "speaker_2": [0.0, 1.0]}
    sidecar._active_count = 2  # 人数来自 diarize 的 active_speakers(活跃 TTL 过滤后)
    sidecar._track_current_speaker(
        [voice_sidecar.DiarSegment("speaker_2", now - 1400, now - 100, 0.9, False)]
    )
    sidecar._maybe_push_speakers()
    sidecar._maybe_push_speakers()  # 快照未变 → 不重推
    assert pushed == [(2, "第2位")]
    # 当前说话人变化(切到 speaker_1,未注册 → 第1位) → 再推一次
    sidecar._track_current_speaker(
        [voice_sidecar.DiarSegment("speaker_1", now - 50, now, 0.9, False)]
    )
    sidecar._maybe_push_speakers()
    assert pushed == [(2, "第2位"), (2, "第1位")]


def test_asr_run_task_payload_has_input(monkeypatch) -> None:
    """回归(2026-09-14): run-task 必须带 payload.input,否则 DashScope task-failed,
    整条 ASR 侧路静默断掉(无转写/无事件/无摘要)。"""
    sent: list[dict] = []

    class FakeWs:
        async def send(self, raw: str) -> None:
            sent.append(json.loads(raw))

        async def close(self) -> None:
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    async def fake_connect(*args, **kwargs):
        return FakeWs()

    monkeypatch.setattr(voice_sidecar.websockets, "connect", fake_connect)

    async def scenario() -> voice_sidecar.AsrSidecar:
        asr = voice_sidecar.AsrSidecar(lambda sentence: asyncio.sleep(0))
        ok = await asr.start("qwen3-asr-flash-realtime", "test-key")
        assert ok is True
        await asr.close()
        return asr

    asyncio.run(scenario())
    run_task = next(m for m in sent if m.get("header", {}).get("action") == "run-task")
    payload = run_task["payload"]
    assert "input" in payload, "run-task 缺 payload.input(DashScope 必填)"
    assert payload["parameters"]["sample_rate"] == 16000
    # 回归(2026-09-15): 实时识别必须 duplex(双向流);错用 out 时服务端不收音频,
    # task-failed NO_VALID_AUDIO_ERROR,整条 ASR 侧路静默断掉
    assert run_task["header"]["streaming"] == "duplex"
