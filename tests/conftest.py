"""测试装配: tmp SQLite + JWT 铸造 + 网络隔离。

import 顺序敏感: app.db 模块级建 engine——本文件顶部先钉 DATABASE_URL 等 env,
再 import app.*(pytest 保证 conftest.py 先于测试模块 import)。

凭据类 env 一律显式覆盖(.env 里的真实密钥绝不进测试进程配置):
环境变量优先级高于 .env 文件,pydantic-settings 取 env。
"""

from __future__ import annotations

import os
import tempfile
import time

# --- 环境钉死(必须在任何 app import 之前) -------------------------------------
_TEST_DIR = tempfile.mkdtemp(prefix="voice-runtime-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DIR}/voice_test.db"
os.environ["AUTH_SECRET"] = "test-auth-secret-0123456789abcdef"  # 32 字节,消 InsecureKeyLengthWarning
os.environ["INTERNAL_TOKEN"] = "test-internal-token"
os.environ["AGENT_RUNTIME_ENABLED"] = "true"
os.environ["AGENT_RUNTIME_API_KEY"] = "test-cockpit-key-0123456789"
os.environ["AGENT_RUNTIME_BASE_URL"] = "http://127.0.0.1:8642"
os.environ["MULTI_SPEAKER_ENABLED"] = "false"
os.environ["CONVERSATION_RESOLVER"] = "local"
os.environ["TRACK_PROBE"] = "null"
# LLM/DashScope 钉空: 防 .env 真实端点/密钥泄漏进测试(llm_api_key 的
# AliasChoices 含 DASHSCOPE_API_KEY,必须显式压掉);需要 LLM 的用例自行
# monkeypatch settings + MockTransport。
os.environ["LLM_PROVIDER"] = "openai_compatible"
os.environ["LLM_BASE_URL"] = ""
os.environ["LLM_API_KEY"] = ""
os.environ["LLM_MODEL"] = "qwen-plus"
os.environ["DASHSCOPE_REALTIME_API_KEY"] = ""
os.environ["DASHSCOPE_API_KEY"] = ""
os.environ["DASHSCOPE_BASE_URL"] = ""
os.environ["DASHSCOPE_MODEL"] = ""

import jwt  # noqa: E402

from app.config import get_settings  # noqa: E402

get_settings.cache_clear()  # 保险: 防上方 env 设置前已被别的 import 链缓存

from app import models  # noqa: E402
from app.db import SessionLocal, engine  # noqa: E402

models.Base.metadata.create_all(engine)

TENANT_ID = 1


def mint_token(user_id: int, *, role: str = "sales", tenant_id: int = TENANT_ID) -> str:
    """铸测试 JWT(jwt-claims 模式,不查用户库);claims 结构与平台签发的 token 一致。"""
    settings = get_settings()
    now = int(time.time())
    claims = {
        "sub": str(user_id),
        "tenant_id": tenant_id,
        "role": role,
        "type": "access",
        "iss": settings.app_name,
        "iat": now,
        "exp": now + 3600,
    }
    return jwt.encode(claims, settings.auth_secret, algorithm=settings.auth_algorithm)


def auth_headers(user_id: int, *, role: str = "sales", tenant_id: int = TENANT_ID) -> dict[str, str]:
    return {"Authorization": f"Bearer {mint_token(user_id, role=role, tenant_id=tenant_id)}"}


__all__ = ["SessionLocal", "TENANT_ID", "auth_headers", "mint_token"]
