"""AgentRuntime 协议 + 归一化事件 + 注册表。

语音模块与"任务处理后端"的唯一接口。任何 agent(hermes / kimi-code(ACP) /
codex / claude-code)实现本协议并 `register_runtime("kind", factory)` 即可接入,
配置 `agent_runtime_kind` 切换。

归一化 RunEvent(dict,键 `event` 标识类型),voice_tasks 的消费逻辑面向此契约:
- tool.started / tool.completed: {event, tool, preview?, args?, duration?, error?, result?}
- approval.request / approval.responded: {event, ...原样}
- run.completed: {event, run_id, output, usage?}
- run.failed:    {event, error: {message} | str}
- run.cancelled: {event}

错误语义(全部适配器必须遵守,路由层按此分流):
- AgentRuntimeAuthError:  401 类配置问题,不重试不降级
- AgentRuntimeBadRequest: 400 类调用方 bug,不重试
- AgentRuntimeBusy:       429/503 类并发/租约超限,可稍后重试(ask 通道转异步)
- AgentRuntimeUnavailable: 未启用/未配置/连接失败/超时/5xx,调用方可降级
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Protocol


class AgentRuntimeError(RuntimeError):
    """agent 运行时调用失败的基类。"""


class AgentRuntimeUnavailable(AgentRuntimeError):
    """未启用/未配置/连接失败/超时/5xx,调用方可降级提示。"""


class AgentRuntimeAuthError(AgentRuntimeError):
    """密钥错误,属配置问题,不重试不降级。"""


class AgentRuntimeBadRequest(AgentRuntimeError):
    """请求非法(session id 非法等),属调用方 bug,不重试。"""


class AgentRuntimeBusy(AgentRuntimeError):
    """并发或会话租约超限,可稍后重试。"""


# 兼容别名:搬移的 voice_tasks/routes 沿用 Cockpit* 名,零改动
CockpitRuntimeError = AgentRuntimeError
CockpitRuntimeUnavailable = AgentRuntimeUnavailable
CockpitRuntimeAuthError = AgentRuntimeAuthError
CockpitRuntimeBadRequest = AgentRuntimeBadRequest
CockpitRuntimeBusy = AgentRuntimeBusy


def session_key_for(tenant_id: int, user_id: int) -> str:
    """多用户契约的用户作用域键:稳定、永不更改,跨会话复用以延续用户记忆。"""
    return f"t{int(tenant_id)}:u{int(user_id)}"


class AgentRuntime(Protocol):
    """任务处理后端适配协议。9 个方法 + 会话 CRUD,全部 async。"""

    async def create_session(self, *, title: str | None = None) -> str:
        """新建会话,返回 agent_session_id。"""
        ...

    async def delete_session(self, session_id: str) -> None:
        """删除会话;不存在视为已删(幂等),不抛错。"""
        ...

    async def chat(self, session_id: str, message: str, *, session_key: str,
                   instructions: str | None = None) -> str:
        """同步问答(ask 快查通道):返回 assistant 全文。instructions=本轮纪律。"""
        ...

    async def submit_run(self, session_id: str, task: str, *, session_key: str,
                         instructions: str | None = None) -> str:
        """异步任务(submit 通道):受理返回 run_id。instructions=本轮纪律。"""
        ...

    def run_events(self, run_id: str) -> AsyncIterator[dict[str, Any]]:
        """run 事件流(归一化 RunEvent),流结束/终态即退出。单订阅者。"""
        ...

    async def run_get(self, run_id: str) -> dict[str, Any]:
        """run 状态探活(启动恢复用):至少含 status;completed 时带 output。"""
        ...

    async def run_stop(self, run_id: str) -> None:
        """停止 run;已终态幂等不抛错。"""
        ...

    async def run_steer(self, run_id: str, text: str) -> bool:
        """运行中补充指令;已不可引导返回 False。"""
        ...

    async def run_approval(self, run_id: str, choice: str) -> None:
        """审批抉择:once|session|always|deny。"""
        ...


# --- 注册表 ------------------------------------------------------------------

_REGISTRY: dict[str, Callable[[], AgentRuntime]] = {}
_cached: AgentRuntime | None = None


def register_runtime(kind: str, factory: Callable[[], AgentRuntime]) -> None:
    _REGISTRY[kind] = factory


def get_runtime() -> AgentRuntime:
    """按 agent_runtime_kind 取当前适配器(进程内单例)。"""
    global _cached
    if _cached is not None:
        return _cached
    from app.config import get_settings

    kind = get_settings().agent_runtime_kind
    if kind not in _REGISTRY:
        # 内置适配器惰性注册(避免循环 import)
        if kind == "hermes":
            from app.runtime.hermes import HermesRuntime

            register_runtime("hermes", HermesRuntime)
        elif kind == "acp":
            from app.runtime.acp import AcpRuntime

            register_runtime("acp", AcpRuntime)
    if kind not in _REGISTRY:
        raise AgentRuntimeUnavailable(f"未知 agent_runtime_kind: {kind}(已注册: {sorted(_REGISTRY)})")
    _cached = _REGISTRY[kind]()
    return _cached


def reset_runtime() -> None:
    """测试用:清掉缓存的适配器单例。"""
    global _cached
    _cached = None
