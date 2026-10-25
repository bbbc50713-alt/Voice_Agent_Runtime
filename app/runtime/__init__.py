"""AgentRuntime 门面(facade):模块级函数委托给当前适配器。

voice_tasks / routes 经 ``from app import runtime as cockpit_runtime`` 使用,
函数签名与平台 cockpit_runtime 完全一致——搬移代码零改动;换 agent 只需
配置 agent_runtime_kind + 注册新适配器,调用方无感。
"""

from __future__ import annotations

from typing import Any, AsyncIterator

# 异常/工具/常量原样 re-export(搬移代码引用名不变)
from app.runtime.base import (  # noqa: F401
    AgentRuntimeAuthError,
    AgentRuntimeBadRequest,
    AgentRuntimeBusy,
    AgentRuntimeError,
    AgentRuntimeUnavailable,
    CockpitRuntimeAuthError,
    CockpitRuntimeBadRequest,
    CockpitRuntimeBusy,
    CockpitRuntimeError,
    CockpitRuntimeUnavailable,
    get_runtime,
    register_runtime,
    reset_runtime,
    session_key_for,
)
from app.runtime.hermes import new_async_client  # noqa: F401 — 摘要 LLM 调用复用
from app.runtime.prompts import (  # noqa: F401
    COCKPIT_CONTEXT_INSTRUCTIONS,
    RISK_CARD_INSTRUCTIONS,
)


async def create_session(*, title: str | None = None) -> str:
    return await get_runtime().create_session(title=title)


async def delete_session(session_id: str) -> None:
    await get_runtime().delete_session(session_id)


async def chat_sync(session_id: str, message: str, *, session_key: str,
                    instructions: str | None = None) -> str:
    return await get_runtime().chat(
        session_id, message, session_key=session_key, instructions=instructions
    )


async def runs_submit(session_id: str, message: str, *, session_key: str,
                      instructions: str | None = None) -> str:
    return await get_runtime().submit_run(
        session_id, message, session_key=session_key, instructions=instructions
    )


def runs_events(run_id: str) -> AsyncIterator[dict[str, Any]]:
    return get_runtime().run_events(run_id)


async def runs_get(run_id: str) -> dict[str, Any]:
    return await get_runtime().run_get(run_id)


async def runs_stop(run_id: str) -> None:
    await get_runtime().run_stop(run_id)


async def runs_steer(run_id: str, text: str) -> bool:
    return await get_runtime().run_steer(run_id, text)


async def runs_approval(run_id: str, choice: str) -> None:
    await get_runtime().run_approval(run_id, choice)
