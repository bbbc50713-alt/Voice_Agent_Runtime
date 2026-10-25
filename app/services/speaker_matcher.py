"""声纹匹配(services/speaker_matcher.py)——ENGAGED_DRIFT Layer 1 / MULTI_SPEAKER §7.4。

SpeakerMatcher 抽象:把 diarization 的匿名簇(speaker_N)解析为注册身份。
- 本地实现(LocalCosineMatcher): 注册 embedding(JSON 向量) × 簇质心 numpy 余弦,
  零外部依赖(backend venv 已有 numpy);持有人锚定(本人簇权重 ×1.2)。
- 阿里云 ISI 实现: 协议位预留(ENROLL/1:N 比对),需要时按
  https://help.aliyun.com/document_detail/xxxx 官方协议补——P2 先本地。

阈值语义(与 MULTI_SPEAKER §7.4 对齐):
  cos ≥ resolve_threshold(0.65)      → 命中(registered, 显示名字)
  0.55 ≤ cos < resolve_threshold     → 疑似(candidate, 摘要用"疑似某人"降级措辞)
  cos < 0.55                        → 未注册(speaker_N/unknown, "有人提到")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Protocol

import numpy as np

logger = logging.getLogger(__name__)

SUSPECT_FLOOR = 0.55  # 疑似下限(与命中阈值之间的区间记 candidate)
HOLDER_BOOST = 1.2  # 持有人锚定权重:本人声纹得分放大(防本人被误判成同事)


@dataclass(frozen=True)
class MatchResult:
    """一次簇归属解析结果。"""

    user_id: int | None  # 命中的注册用户;None=未注册/疑似
    display_name: str  # 展示名:注册名/speaker_N 原样
    role: str  # me | staff | customer | unknown
    confidence: float  # 原始余弦得分(0~1)
    matched: bool  # True=命中注册;False=疑似/未注册
    suspect_name: str | None = None  # 疑似区间(0.55~阈值)时的候选名,摘要降级措辞用


class SpeakerMatcher(Protocol):
    """簇标签 → 注册身份的匹配协议(本地/云可换,配置切换)。"""

    def resolve(self, speaker_label: str, centroid: np.ndarray) -> MatchResult: ...


@dataclass
class EnrolledProfile:
    user_id: int | None
    display_name: str
    role_hint: str
    embedding: np.ndarray


def parse_embedding_json(raw: str) -> np.ndarray:
    """cockpit_speaker_profiles.embedding_ref(JSON 文本) → 单位化向量。"""
    vec = np.asarray(json.loads(raw), dtype=np.float32)
    norm = float(np.linalg.norm(vec))
    if norm > 0:
        vec = vec / norm
    return vec


class LocalCosineMatcher:
    """本地余弦比对:注册表全量 × 簇质心,内存 numpy,数十人量级无压力。

    centroid 由调用方保证已单位化(diarization-service 返回的质心)。
    """

    def __init__(
        self,
        enrolled: list[EnrolledProfile],
        *,
        threshold: float,
        holder_user_id: int | None = None,
    ) -> None:
        self._enrolled = enrolled
        self._threshold = threshold
        self._holder_user_id = holder_user_id

    def resolve(self, speaker_label: str, centroid: np.ndarray) -> MatchResult:
        if not self._enrolled:
            return MatchResult(None, speaker_label, "unknown", 0.0, False)
        centroid = np.asarray(centroid, dtype=np.float32)
        norm = float(np.linalg.norm(centroid))
        if norm <= 0:
            return MatchResult(None, speaker_label, "unknown", 0.0, False)
        centroid = centroid / norm

        best: tuple[float, EnrolledProfile] | None = None
        for profile in self._enrolled:
            score = float(np.dot(profile.embedding, centroid))
            if profile.user_id is not None and profile.user_id == self._holder_user_id:
                score *= HOLDER_BOOST  # 持有人锚定:得分封顶 1.0 再比阈值
                score = min(score, 1.0)
            if best is None or score > best[0]:
                best = (score, profile)
        assert best is not None
        score, profile = best
        if score >= self._threshold:
            role = "me" if (profile.user_id is not None and profile.user_id == self._holder_user_id) else profile.role_hint
            return MatchResult(profile.user_id, profile.display_name, role, score, True)
        if score >= SUSPECT_FLOOR:
            return MatchResult(None, speaker_label, "unknown", score, False, suspect_name=profile.display_name)
        return MatchResult(None, speaker_label, "unknown", score, False)
