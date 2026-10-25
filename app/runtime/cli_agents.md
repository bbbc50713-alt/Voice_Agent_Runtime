# CLI agent 接入指南(语音模块 × 任务处理后端)

语音模块与"任务处理后端"的唯一接口是 `app/runtime/base.py` 的 **AgentRuntime 协议**
(9 个 async 方法 + 归一化 RunEvent + 四分类错误语义,详见该文件头注释)。任何一个
CLI agent(kimi-code / codex / claude-code / …)只要按下文实现适配器,语音模块即可
把它当任务处理模块使用,调用方(voice_tasks / routes)完全无感。

## ① 接入三步

1. **实现九方法**。新建 `app/runtime/<your>.py`,类不必继承(Protocol 结构化匹配),
   签名逐字对齐 `AgentRuntime`:

   | 方法 | 语义 | 要点 |
   |---|---|---|
   | `create_session(*, title=None) -> str` | 建会话,返回 agent_session_id | 非空 str |
   | `delete_session(session_id) -> None` | 删会话 | 幂等,不存在不抛 |
   | `chat(session_id, message, *, session_key, instructions=None) -> str` | 同步问答(ask 快查通道) | 返回全文;`instructions`=本轮纪律 |
   | `submit_run(session_id, task, *, session_key, instructions=None) -> str` | 异步受理(submit 通道) | 受理即返回 run_id |
   | `run_events(run_id) -> AsyncIterator[dict]` | 归一化事件流 | 终态事件收尾即停,单订阅者 |
   | `run_get(run_id) -> dict` | 状态探活(启动恢复用) | 至少含 `status`;completed 带 `output` |
   | `run_stop(run_id) -> None` | 停止 | 已终态/不存在幂等不抛 |
   | `run_steer(run_id, text) -> bool` | 运行中补充指令 | 终态返回 False |
   | `run_approval(run_id, choice) -> None` | 审批抉择 | choice ∈ once/session/always/deny |

   归一化 RunEvent(dict,键 `event` 标识类型):
   `tool.started`/`tool.completed`(`{event, tool, preview?, duration?, error?}`)、
   `approval.request`/`approval.responded`(`{event, ...}`)、
   `run.completed`(`{event, run_id, output}`)/`run.failed`(`{event, error:{message}}`)/
   `run.cancelled`(`{event}`)。

   错误四分类(路由层按此分流,必须遵守):`AgentRuntimeAuthError`(401 类,不重试)、
   `AgentRuntimeBadRequest`(调用方 bug)、`AgentRuntimeBusy`(并发/租约超限,可重试)、
   `AgentRuntimeUnavailable`(未配置/连接失败/超时/5xx,可降级)。

2. **一行注册**。`register_runtime("<kind>", factory)`;内置适配器走
   `app/runtime/base.py` `get_runtime()` 的惰性注册分支(照 hermes/acp 的样子加一行
   `elif` 即可,避免循环 import)。

3. **配置切换**。`.env` 设 `AGENT_RUNTIME_KIND=<kind>` 加适配器自身配置项
   (加进 `app/config.py`,env 大写同名);语音服务重启即生效。

## ② 三个具体映射方案

### kimi-code(ACP)——本仓库已实现:`app/runtime/acp.py`

`agent_runtime_kind=acp` + `ACP_COMMAND=kimi` + `ACP_ARGS="acp"`(另见
`acp_cwd` / `acp_timeout_seconds`)。每会话一个子进程,换行分隔 JSON-RPC over stdio。

| 契约方法 | ACP 映射 |
|---|---|
| create_session | 拉起子进程 → `initialize` → `session/new`,返回 ACP sessionId |
| chat | `session/prompt`,同步等响应,聚合 `agent_message_chunk` 返回全文 |
| submit_run | 本地生成 `acp-<uuid>` run_id,后台协程发 `session/prompt`,内存维护 run↔session |
| run_events | 消费 `session/update` 通知:chunk 聚合进 output;`tool_call`→tool.started;`tool_call_update`→tool.completed;`session/request_permission`→approval.request |
| run_get | 查本地 run 状态表 |
| run_stop | `session/cancel` 通知 + 取消本地 drive 协程 |
| run_steer | 运行中同 session 追加一轮 `session/prompt`;终态 False |
| run_approval | 回应挂起的 permission 请求(outcome=selected/rejected) |
| delete_session | ACP 无删除语义:cancel + 终止子进程 + 本地遗忘(幂等) |

⚠️ 骨架状态:传输层/id 配对/归一化/审批回环均为真实实现(假 agent 常开契约
`TestAcpFakeAgentContract` 背书);但方法名/参数形状按 ACP 规范草案填写,**未经真实
kimi-code 握手验证**——联调时先核对 `acp.py` 顶部常量区(`ACP_METHODS` /
`_UPDATE_*` / `_APPROVAL_OUTCOMES`),再 `ACP_CONTRACT_KIMI=1 pytest tests/test_runtime_contract.py -q`
跑 `TestAcpRuntimeContract` 真机契约。

### codex(`codex exec --json`,一次性进程映射)

codex 无常驻 ACP 服务,用一次性非交互进程映射:

- `create_session`:本地造会话句柄(目录/元数据),不起进程;续轮历史由适配器自存
  或在 prompt 里回放(codex exec 本身无服务端会话)。
- `submit_run`:起 `codex exec --json "<task>"`(工作目录隔离),受理即返回本地
  `codex-<uuid>` run_id;后台协程逐行读 stdout JSONL。
- `run_events`:解析 JSONL 事件行(`item.completed` 里 agent message → output 聚合;
  command/tool 类 item → tool.started/tool.completed),进程退出码 0 → run.completed,
  非 0 → run.failed。
- `chat`:同款进程,同步等退出,返回聚合全文。
- `run_stop`:kill 进程树。`run_get`:本地 run 表 + 进程存活探查。
- `run_steer`:**不能兑现**(一次性进程无插话通道)——运行中也返回 False 或
  退化为"记下文本,下轮 prompt 附带"(择一写进 docstring)。
- `run_approval`:**不能兑现运行中审批**——以启动参数预声明权限等级
  (`--full-auto` / `--sandbox read-only` 等)代替;`deny` 语义 = kill 进程。

### claude-code(`claude -p --output-format stream-json`)

- `create_session`:首轮 `claude -p --output-format stream-json "<首条或占位>"`,
  从首条 system init 事件取 `session_id` 落库(进程即退,会话在 claude 侧持久)。
- `chat` / `submit_run`:`claude -p --output-format stream-json --resume <session_id>
  "<message>"` 续会话;chat 同步等退出聚合全文,submit_run 后台协程消费。
- `run_events`:stream-json 逐行(assistant 消息块 → output;tool_use/tool_result →
  tool.started/tool.completed;result 行 → run.completed)。
- `run_stop`:kill 进程。`run_get`:本地 run 表。
- `run_steer`:print 模式不支持运行中插话 → False(同 codex 的退化记法可选)。
- `run_approval`:print 模式无交互审批——启动时以
  `--permission-mode` / `--allowedTools` 预声明;`deny` 语义 = kill。

## ③ 契约方法兑现矩阵

| 契约方法 | hermes(参照系) | kimi-code(ACP) | codex(exec) | claude-code(-p) |
|---|---|---|---|---|
| create_session | ✓ | ✓ | ✓(本地句柄) | ✓(首轮取 session_id) |
| delete_session | ✓ | ✓(终止进程) | ✓(本地遗忘) | ✓(本地遗忘) |
| chat | ✓ | ✓ | ✓(等进程退出) | ✓(--resume) |
| submit_run | ✓ | ✓ | ✓ | ✓(--resume) |
| run_events | ✓ | ✓ | ✓(JSONL 解析) | ✓(stream-json 解析) |
| run_get | ✓ | ✓(本地表) | ✓(本地表) | ✓(本地表) |
| run_stop | ✓ | ✓(cancel+kill) | △(只有 kill) | △(只有 kill) |
| run_steer | ✓ | △(追加 prompt,语义待核对) | ✗ | ✗ |
| run_approval | ✓ | ✓(permission 回环) | ✗(启动期预声明) | ✗(启动期预声明) |

✗ 不代表不能接入:steer 返回 False、approval 走预声明,均符合契约的退化口径
(契约只要求"终态 steer 返回 False"、"approval 调用不抛")。

## ④ 新适配器验收清单

1. **契约测试子类**:在 `tests/test_runtime_contract.py` 加
   `TestXxxContract(AgentRuntimeContract)`,提供两个 fixture——
   `runtime`(可用实例,禁触外网:内存实现/MockTransport/假子进程)与
   `broken_runtime`(未配置实例,九方法必须全抛 `AgentRuntimeUnavailable`)。
   参照 `TestAcpFakeAgentContract`(假子进程常开契约)与 `tests/fake_acp_agent.py`。
2. `pytest tests/ -q` 全绿(含既有基线,不许弄坏);`python -m compileall -q app tests` 无错。
3. **错误分类抽查**:401→AuthError、400→BadRequest、429/503→Busy、
   超时/连接失败/5xx→Unavailable(对照 hermes 适配器的 `_raise_for_code`)。
4. **语音全链路冒烟**(真配置接入后):起服务 → 建语音会话 → ask 快查(chat)
   → submit 账本任务 → 事件流进 progress → run_get 恢复探活 → run_stop →
   审批回环(若支持) → delete_session。
5. `base.py get_runtime()` 惰性注册分支 + `config.py` 配置项 + 本文档矩阵同步更新。
