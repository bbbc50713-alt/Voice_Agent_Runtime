"""发送追踪的消息状态来源(TrackProbe 协议)。

平台时代 voice_tasks 直接查 Message 表;独立服务没有业务库,抽象为探针:
- NullTrackProbe(默认): 不查任何外部状态,全部按已送达结算——
  独立部署没有消息系统时,追踪任务走完整流程后报"已发出"。
- HttpTrackProbe(平台桥): POST track_probe_url,body {"message_ids": [...]},
  返回 {"states": {"<id>": {"status": "sent|failed|...", "failure_reason": str|null}}};
  失败/超时抛 TrackProbeError,调用方按"非终态"处理(继续盯梢,不误结算)。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


class TrackProbeError(RuntimeError):
    """探针调用失败(网络/违约)——调用方按非终态处理,绝不误结算。"""


@dataclass
class MessageState:
    status: str  # sent | failed | 其他(视为非终态)
    failure_reason: str | None = None


class TrackProbe(Protocol):
    async def fetch_states(self, message_ids: list[int]) -> dict[int, MessageState]:
        ...


class NullTrackProbe:
    async def fetch_states(self, message_ids: list[int]) -> dict[int, MessageState]:
        return {mid: MessageState("sent") for mid in message_ids}


class HttpTrackProbe:
    def __init__(self, url: str, token: str) -> None:
        self._url = url
        self._token = token

    async def fetch_states(self, message_ids: list[int]) -> dict[int, MessageState]:
        if not message_ids:
            return {}
        headers = {"X-Internal-Token": self._token} if self._token else {}
        try:
            async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
                resp = await client.post(self._url, json={"message_ids": message_ids}, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise TrackProbeError(f"track probe request failed: {exc}") from exc
        if resp.status_code != 200:
            raise TrackProbeError(f"track probe status {resp.status_code}")
        try:
            states = resp.json()["states"]
            return {
                int(mid): MessageState(
                    status=str(st.get("status") or ""),
                    failure_reason=st.get("failure_reason"),
                )
                for mid, st in states.items()
            }
        except (ValueError, KeyError, AttributeError, TypeError) as exc:
            raise TrackProbeError(f"track probe response violates contract: {resp.text[:200]}") from exc


_cached: TrackProbe | None = None


def get_track_probe() -> TrackProbe:
    """按配置取探针(进程内单例);track_probe=null 或缺 url 时用 Null。"""
    global _cached
    if _cached is None:
        settings = get_settings()
        if settings.track_probe == "http" and settings.track_probe_url:
            _cached = HttpTrackProbe(settings.track_probe_url, settings.internal_token)
        else:
            _cached = NullTrackProbe()
    return _cached


def reset_track_probe() -> None:
    """测试用:清缓存。"""
    global _cached
    _cached = None
