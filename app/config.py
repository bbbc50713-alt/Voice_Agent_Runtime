"""Voice Agent Runtime 配置(独立服务,与平台解耦)。

全部配置项经环境变量或 .env 注入。四个外部依赖均为配置不是代码:
- DashScope 凭据(qwen realtime 主路 + 侧路 ASR)
- diarization 服务 URL(声纹,8791)
- LLM 摘要端点(llm_*,openai 兼容)
- JWT 密钥(与平台共享 auth_secret 即可直接吃平台签发的 token)
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", populate_by_name=True, extra="ignore"
    )

    # --- 服务基础 ---
    app_name: str = "汉小密智能客户中台"  # JWT issuer 兼容:与平台 app_name 保持一致
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8200
    cors_origins: str = "*"  # 逗号分隔白名单;*=放开(独立部署前端跨域用)

    # --- 数据库(默认 SQLite 落项目根;生产可切 PostgreSQL) ---
    database_url: str = "sqlite:///./voice.db"

    # --- 鉴权(jwt-claims 模式:验签后 claims 直出身份,不查用户库) ---
    auth_secret: str = "dev-secret-change-me"
    auth_algorithm: str = "HS256"
    voice_allowed_roles: str = "super_admin,admin,supervisor,operator,sales"  # 逗号分隔角色白名单(对齐平台 ROLE_PERMISSIONS 五角色)
    auth_introspect_url: str = ""  # 可选:平台内省端点(留扩展,默认不回调)
    internal_token: str = ""  # /internal/* 平台桥端点共享密钥;空=内部端点关闭

    # --- qwen realtime 主路 ---
    realtime_voice_enabled: bool = True  # 独立服务默认开(平台是嵌进来才默认关)
    dashscope_realtime_api_key: str = Field(
        default="",
        repr=False,
        validation_alias=AliasChoices(
            "dashscope_realtime_api_key", "DASHSCOPE_REALTIME_API_KEY", "DASHSCOPE_API_KEY"
        ),
    )
    dashscope_realtime_url: str = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
    dashscope_realtime_model: str = "qwen3.5-omni-plus-realtime"
    realtime_voice_name: str = "Ethan"  # 音色;Qwen3.5-Omni-Realtime 音色表与 Qwen3 不同
    realtime_voice_asr_model: str = "qwen3-asr-flash-realtime"
    realtime_voice_task_timeout: float = 300.0  # 账本任务最长执行秒数
    realtime_voice_quick_timeout: float = 45.0  # ask 快查封顶(超时转异步)
    realtime_voice_max_tasks_per_user: int = 8
    realtime_voice_announce_idle_first_min_s: float = 2.0
    realtime_voice_announce_idle_first_max_s: float = 3.0
    realtime_voice_announce_idle_retry_min_s: float = 7.0
    realtime_voice_announce_idle_retry_max_s: float = 9.0
    realtime_voice_announce_max_attempts: int = 2
    realtime_voice_announce_direct_under_s: float = 15.0
    realtime_voice_announce_merge_max: int = 3
    realtime_voice_announcer_enabled: bool = True
    realtime_voice_playback_ack_timeout_s: float = 20.0
    realtime_voice_followup_delay_min_s: float = 2.0
    realtime_voice_followup_delay_max_s: float = 3.0
    realtime_voice_observe_enabled: bool = True
    # 播报策略工具(set_announce_policy)+聚合交付契约总开关(2026-10-14);
    # false=工具目录回退旧 5+1 形态、submit 拒绝新参数(秒级回退)
    cockpit_announce_policy_enabled: bool = True
    # 工具目录防漂移对账慢拍(2026-10-14):启动+每 N 秒与平台后端指纹对账,
    # 不一致 → 后端系统异常表落 voice_tool_catalog_drift;platform_api_base/internal_token
    # 未配置=无法对账(仅跳过,不影响任何主链路)
    tool_catalog_drift_enabled: bool = True
    tool_catalog_drift_interval_s: int = 600
    realtime_voice_soul_path: str = ""  # 空=<repo>/SOUL.md(自带默认人格)

    # --- 多人认知层/声纹侧路 ---
    multi_speaker_enabled: bool = False
    multi_speaker_diarization_url: str = "http://127.0.0.1:8791"
    multi_speaker_asr_model: str = "paraformer-realtime-v2"
    multi_speaker_wake_words: str = "汉二密,二密,小助手,韩二密,汉尔密"
    multi_speaker_summary_cache_s: float = 30.0
    multi_speaker_resolve_threshold: float = 0.65
    multi_speaker_event_retention_days: int = 30

    # --- LLM(任务结果摘要 + 会话自动命名 + 现场摘要,openai 兼容端点) ---
    llm_provider: str = "openai_compatible"  # 搬移代码的既有判据;独立服务仅此一档
    llm_base_url: str = Field(
        default="",
        validation_alias=AliasChoices("llm_base_url", "LLM_BASE_URL", "DASHSCOPE_BASE_URL"),
    )
    llm_api_key: str = Field(
        default="",
        repr=False,
        validation_alias=AliasChoices("llm_api_key", "LLM_API_KEY", "DASHSCOPE_API_KEY"),
    )
    llm_model: str = Field(
        default="qwen-plus",
        validation_alias=AliasChoices("llm_model", "LLM_MODEL", "DASHSCOPE_MODEL"),
    )
    llm_timeout_seconds: int = 60
    llm_max_tokens: int = Field(
        default=2000,
        validation_alias=AliasChoices("llm_max_tokens", "LLM_MAX_TOKENS", "DASHSCOPE_MAX_TOKENS"),
    )
    llm_temperature: float = 0.7

    # --- Agent 适配器(任务处理后端,可插拔) ---
    agent_runtime_kind: str = "hermes"  # hermes | acp | ...(runtime/ 注册表)
    agent_runtime_enabled: bool = True
    agent_runtime_base_url: str = "http://127.0.0.1:8642"
    agent_runtime_api_key: str = Field(default="", repr=False)
    agent_runtime_timeout: float = 180
    agent_runtime_profile: str = "hanxiaomi"  # hermes 适配器用:profile 名

    # --- ACP 适配器(kimi-code acp 等 CLI agent;JSON-RPC over stdio,app/runtime/acp.py) ---
    acp_command: str = ""  # ACP 子进程命令(如 "kimi");空=未启用,调任何方法抛 AgentRuntimeUnavailable
    acp_args: str = ""  # 额外参数,POSIX shlex 空格拆分(Windows 路径用正斜杠),如 "acp --verbose"
    acp_cwd: str = ""  # 子进程工作目录;空=当前目录
    acp_timeout_seconds: float = 120  # 单次 JSON-RPC 请求超时(秒)

    # --- 平台桥(可选;不配=纯独立模式) ---
    conversation_resolver: str = "local"  # local=自有会话表 | http=平台会话解析桥
    platform_api_base: str = ""  # http 模式:平台后端地址,如 http://127.0.0.1:8100
    event_bridge_url: str = ""  # 可选:平台事件接收端点(配置后账本事件双写到平台总线)
    track_probe: str = "null"  # null=发送追踪按已送达直接结算 | http=回调平台查消息状态
    track_probe_url: str = ""  # http 模式:平台消息状态批量查询端点

    @property
    def allowed_role_set(self) -> set[str]:
        return {r.strip() for r in self.voice_allowed_roles.split(",") if r.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
