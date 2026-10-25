"""WS 握手冒烟:铸造测试 JWT → 连 /realtime/ws → 断言 connected 帧;坏 token 断言 4401。"""
import asyncio
import time

import jwt
import websockets

from app.config import get_settings


def _mint(settings, sub="9001", tenant_id=1, role="sales") -> str:
    now = int(time.time())
    claims = {
        "sub": sub,
        "tenant_id": tenant_id,
        "role": role,
        "type": "access",
        "iss": settings.app_name,
        "iat": now,
        "exp": now + 600,
    }
    return jwt.encode(claims, settings.auth_secret, algorithm="HS256")


async def main() -> None:
    settings = get_settings()
    base = f"ws://127.0.0.1:8200{settings.api_prefix}/realtime/ws"

    good = _mint(settings)
    async with websockets.connect(f"{base}?token={good}&topic=cockpit") as ws:
        msg = await ws.recv()
        print("good-token first frame:", msg)

    bad = "invalid.token.here"
    try:
        async with websockets.connect(f"{base}?token={bad}&topic=cockpit") as ws:
            await ws.recv()
            print("bad-token: UNEXPECTEDLY ACCEPTED")
    except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidStatus) as exc:
        code = getattr(exc, "code", None) or getattr(getattr(exc, "response", None), "status_code", None)
        print("bad-token rejected:", code)


asyncio.run(main())
