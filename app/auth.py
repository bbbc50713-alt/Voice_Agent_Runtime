"""鉴权(jwt-claims 模式):验签后 claims 直出身份,不查用户库。

与平台兼容:共享 auth_secret + issuer(=app_name)+ claims 结构
(sub/tenant_id/role/type/iss/iat/exp),平台签发的 token 直接可用。
权限模型收敛为角色白名单(VOICE_ALLOWED_ROLES):命中即授予语音域全部能力。
取舍:用户被禁用后 token 未过期期间仍可用——语音场景可接受;
需要严格收口时配置 AUTH_INTROSPECT_URL 回调平台校验(预留,未实现)。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import jwt
from fastapi import Request

from app.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class TenantContext:
    """轻量身份上下文(平台 TenantContext 的语音域裁剪版)。"""

    tenant_id: int
    user_id: int
    role: str
    permissions: set[str] = field(default_factory=set)


def decode_access_token(token: str) -> dict[str, Any] | None:
    settings = get_settings()
    try:
        claims = jwt.decode(
            token,
            settings.auth_secret,
            algorithms=[settings.auth_algorithm],
            issuer=settings.app_name,
            options={"require": ["sub", "tenant_id", "role", "type", "iss", "iat", "exp"]},
        )
    except jwt.PyJWTError:
        return None
    if claims.get("type") != "access":
        return None
    return claims


def context_from_token(token: str) -> TenantContext | None:
    """token → TenantContext;验签失败或角色不在白名单返回 None。"""
    claims = decode_access_token(token)
    if claims is None:
        return None
    settings = get_settings()
    role = str(claims.get("role") or "")
    if role not in settings.allowed_role_set:
        logger.info("角色 %s 不在语音白名单", role)
        return None
    try:
        return TenantContext(
            tenant_id=int(claims["tenant_id"]),
            user_id=int(claims["sub"]),
            role=role,
            permissions={"customer.read"},  # 白名单角色授予语音域全部能力
        )
    except (KeyError, TypeError, ValueError):
        return None


# --- FastAPI 依赖(REST 端点用) -------------------------------------------------


def require_context(request: Request) -> TenantContext:
    """Authorization: Bearer <jwt> → TenantContext;失败 401,角色不在白名单 403。"""
    from fastapi import HTTPException

    auth = request.headers.get("Authorization") or ""
    token = auth[7:] if auth.startswith("Bearer ") else ""
    claims = decode_access_token(token) if token else None
    if claims is None:
        raise HTTPException(status_code=401, detail={"code": "UNAUTHORIZED", "message": "未登录或登录已过期"})
    settings = get_settings()
    role = str(claims.get("role") or "")
    if role not in settings.allowed_role_set:
        raise HTTPException(status_code=403, detail={"code": "FORBIDDEN", "message": "无权限"})
    try:
        return TenantContext(
            tenant_id=int(claims["tenant_id"]),
            user_id=int(claims["sub"]),
            role=role,
            permissions={"customer.read"},
        )
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=401, detail={"code": "UNAUTHORIZED", "message": "凭证无效"})


def require_internal(request: Request) -> None:
    """/internal/* 平台桥端点:X-Internal-Token 共享密钥校验;未配置=关闭(404)。"""
    from fastapi import HTTPException

    settings = get_settings()
    if not settings.internal_token:
        raise HTTPException(status_code=404, detail={"code": "NOT_FOUND", "message": "不存在"})
    token = request.headers.get("X-Internal-Token") or ""
    if token != settings.internal_token:
        raise HTTPException(status_code=401, detail={"code": "UNAUTHORIZED", "message": "内部令牌无效"})
