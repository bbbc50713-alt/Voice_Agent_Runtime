"""Voice Agent Runtime — 独立实时语音服务。

装配:CORS + 路由(voice/conversations/realtime/internal)+ lifespan(启动恢复/定时清理)。
启动: uvicorn app.main:app --host 0.0.0.0 --port 8200
"""

from __future__ import annotations

import logging
import json
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings

logger = logging.getLogger(__name__)


def _cors_origins(raw: str) -> list[str]:
    """接受逗号字符串或 JSON 数组，兼容平台 .env 的共享配置格式。"""
    value = (raw or "").strip()
    if not value:
        return []
    if value == "*":
        return ["*"]
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(origin).strip() for origin in parsed if str(origin).strip()]
    return [origin.strip() for origin in value.split(",") if origin.strip()]


def _purge_loop(stop: threading.Event) -> None:
    """话语事件 30 天清理(每小时一轮,与平台 main.py:207 同款)。"""
    from app.services import voice_sidecar

    while not stop.wait(3600):
        try:
            voice_sidecar.purge_expired_speaker_events()
        except Exception:
            logger.exception("purge_expired_speaker_events 失败")


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.db import init_db
    from app.services import voice_tasks

    init_db()
    # 启动恢复:running/waiting_approval 探活收尾;announcing 回 pending 等重连
    threading.Thread(target=voice_tasks.recover_interrupted_tasks, daemon=True).start()
    stop = threading.Event()
    purger = threading.Thread(target=_purge_loop, args=(stop,), daemon=True)
    purger.start()
    yield
    stop.set()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="voice-agent-runtime", lifespan=lifespan)
    origins = _cors_origins(settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "service": "voice-agent-runtime"}

    from app import realtime_bus
    from app.routes import conversations, internal, voice

    app.include_router(voice.router, prefix=settings.api_prefix)
    app.include_router(conversations.router, prefix=settings.api_prefix)
    app.include_router(realtime_bus.router, prefix=settings.api_prefix)
    app.include_router(internal.router, prefix=settings.api_prefix)
    return app


app = create_app()
