# Voice Agent Runtime 协作约定

## 服务定位

独立实时语音服务（FastAPI 8200），语音域拆分后前端语音流量全走本服务（`VITE_VOICE_ORIGIN`）。
平台后端：`A:\server_mask\hanxiaomi-customer-platform`（8100）。

## 工具目录镜像铁律（2026-10-14 起，最高优先）

- 本仓库 `app/services/cockpit_voice.py` 的工具目录（工具名/参数 schema/枚举/required/description）
  是平台后端 `backend/app/services/cockpit_voice.py` 目录的**逐字段镜像**。
- **任何一侧改动必须双改**：改工具名、参数、枚举、required、description，两侧同提交；
  改完两侧 `tool_catalog_fingerprint()` 产出必须一致（当前 `149e0e2c47e810ac`）。
- **事故教训（2026-10-14）**：后端加了 `set_announce_policy` 与 submit 聚合参数而镜像没同步，
  漂移后线上模型物理上看不见新能力——"不用播报了/整理成文档"只能走旧逻辑口头答应。

## 防漂移对账（自动兜底，防第三次漂移）

- 启动 + 每 600s 慢拍（`cockpit_voice.start_tool_catalog_drift_watch`，由
  `voice_tasks.recover_interrupted_tasks` 启动钩拉起）：
  拉后端 `GET {PLATFORM_API_BASE}/api/v1/internal/cockpit/voice/tool-catalog-fingerprint`
  比对本地指纹；不一致 → 本地 warning + POST `.../tool-catalog-drift` → 后端系统异常表落
  `voice_tool_catalog_drift`（幂等）。
- 开关：`TOOL_CATALOG_DRIFT_ENABLED=false` 停用；`TOOL_CATALOG_DRIFT_INTERVAL_S` 调间隔。
- 对账失败仅跳过（日志），**绝不影响任何语音主链路**。

## 解冻限定（2026-10-14 起）

仅以下文件可改：`app/services/cockpit_voice.py`、`app/services/voice_tasks.py`、
`app/models.py`、`app/routes/voice.py`、`app/db.py`、`app/config.py`；其余文件仍冻结。

## 播报策略 + 聚合文档（2026-10-14）

- 工具：`set_announce_policy`（policy: silent_until_group/file_ready/direct）、
  `submit_business_task` 聚合参数（delivery_mode/artifact_format/source_task_ids/announce_policy）。
- 账本：`voice_tasks` 表 `delivery` JSON 列承载策略与聚合契约；
  前端撤销走 `POST /api/v1/cockpit/voice-tasks/announce-policy/reset`（REST，不依赖语音在线）。
- 开关：`COCKPIT_ANNOUNCE_POLICY_ENABLED=false` 秒级回退旧目录。

## 播报交付与播放确认（2026-10-24 对齐平台 8100）

- 播报状态：`pending/queued/announcing/notified/shared/deferred/dismissed/interrupted`；`consumed` 仅作老数据兼容。ask 播完→`notified`，direct/tell 播完→`shared`，收尾提示播完→`dismissed`，用户打断→`interrupted`。
- **模型 `response.done` 不等于用户听完**：服务端先发 `playback.settle`，等浏览器 `playback.completed/interrupted` 回执再结算；旧前端无回执时由 `REALTIME_VOICE_PLAYBACK_ACK_TIMEOUT_S`（默认 20s）兜底，重复 ack 幂等。
- 打断后不再立即重排；启动恢复把 `queued/announcing` 置 `interrupted`，重连只静默补上下文。播报结算后注入【播报进展】，followup 不得复读已问/已讲/已压下/被打断内容。
- 任务卡操作：`POST /api/v1/cockpit/voice-tasks/{task_id}/announce`，action=`defer|dismiss|share`，仅终态任务可操作；静态 `/voice-tasks/announce-policy/reset` 必须保持在 `/{task_id}/*` 动态路由之前。
- `set_announce_policy` 返回精确 `deferred_ids/interrupted_ids` 供当前会话摘队；`reset_announce_policy` 可复活策略压下的 `deferred/interrupted`，不得改回全量扫描其他会话任务。
- 回归：`./.venv/Scripts/python.exe -m pytest tests/ -q`；播放契约集中在 `tests/test_voice_delivery_contract.py`。

## 重启

8200 已纳入平台 `scripts/restart-local.sh` 托管（2026-10-19 起：端口清理+拉起+就绪等待，平台五服务一键重启）。
单独手动重启仍可：

```bash
cd A:\Voice_Agent_Runtime && nohup ./.venv/Scripts/python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8200 >> logs/voice-8200.out.log 2>> logs/voice-8200.err.log &
```
