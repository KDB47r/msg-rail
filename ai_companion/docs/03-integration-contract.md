# 03 · Phase 2 集成契约（AI Companion ↔ MsgRail）

> 状态：P6.1 已实现。实际链路为 Logic → Kafka `ai_trigger` → AI Worker → AI Companion SSE；
> 流式帧经 Logic 内部 HTTP 转回 `push_to_comet`。本文档已按实现更新。

## 1. 目标

把 AI Companion 作为 MsgRail 里的一个“AI 用户”：

```text
浏览器 ──WebSocket──> Comet ──gRPC──> Logic ──HTTP SSE / gRPC──> AI Companion
                                        │                         ├─ LLM
                                        └── Kafka ──> Job ──> MySQL
```

- MsgRail 负责连接、鉴权、路由、持久化。
- AI Companion 只负责生成，不碰 IM 连接管理。
- 两边通过内部接口契约解耦。

## 2. 方案对比

| 方案 | 优点 | 缺点 | 建议 |
|---|---|---|---|
| HTTP + SSE（内部） | 复用 P0 实现，调试简单 | 需要把 SSE 转成 WS 帧 | **首选** |
| gRPC stream | 类型强、性能好、适合内部服务 | 要改 proto、调试成本高 | P6 稳定后再考虑 |

先做 HTTP SSE，契约设计成可平移到 gRPC。

## 3. 接口定义

### 3.1 `POST /internal/v1/agent/stream`

请求头：

```text
Authorization: Bearer <INTERNAL_TOKEN>
Content-Type: application/json
Accept: text/event-stream
Idempotency-Key: <msg_id>
```

请求体（P6 实际实现）：

```json
{
  "request_id": "job_...",
  "mode": "private",
  "user_id": "im:910000000000003",
  "session_id": "single:910000000000001:910000000000003",
  "bot_user_id": 910000000000001,
  "trigger_message_id": "single:...-1",
  "trigger_sender_id": 910000000000003,
  "trigger_sender_name": "p6alice",
  "context_messages": [
    {
      "message_id": "...",
      "sender_id": 910000000000003,
      "sender_name": "p6alice",
      "sender_type": "human",
      "content": "你好",
      "timestamp_ms": 1760000000000
    }
  ],
  "persona_id": "xiaoheng",
  "idempotency_key": "..."
}
```

字段说明：

- `mode=private`：启用 P4 记忆/RAG；`mode=shared`：只使用 `context_messages`，禁用记忆/RAG。
- `session_id`/`trigger_message_id`/`bot_user_id` 是 MsgRail 侧 ID；AI Companion **不创建会话、不保存 A-B 历史**。
- `trigger_message_id` 必须出现在上下文最后；服务端会丢弃触发消息之后的上下文。
- `context_messages` 最多 30 条（服务端也按 token 预算裁剪）。

响应：`text/event-stream`，沿用公共 SSE 事件：

```text
event: message_start
data: {"conversation_id":"single:...","message_id":"job_...","run_id":"job_..."}

event: content_delta
data: {"delta":"你好呀"}

event: tool_call
data: {"call_id":"call_01","name":"local__get_current_time","arguments":{}}

event: tool_result
data: {"call_id":"call_01","status":"ok","preview":"..."}

event: message_end
data: {"finish_reason":"stop","message_id":"job_...","run_id":"job_...","usage":{...}}

event: error
data: {"code":"...","message":"...","retryable":false,"run_id":"job_..."}
```

## 4. 事件到 MsgRail WS 帧的映射

AI Worker 把 SSE 转成 Logic 内部 HTTP，再通过现有 `push_to_comet` 下发。
实际 WS 帧是**顶层对象**（不是 `single_chat` 包装）：

| SSE/阶段 | WS 顶层 `type` | 字段 |
|---|---|---|
| `content_delta` 批量 | `ai_stream` | `job_id`, `session_id`, `bot_user_id`, `sender_name`, `mentioner_id`, `delta` |
| 失败 | `ai_stream` | 同上 + `error:{code,message}` |
| 停止 | `ai_stream` | 同上 + `stopped:true` |
| `message_end` 后最终落库 | `text` | `ai_job_id`, `session_id`, `from_user_id`, `sender_name`, `content` |
| 邀请/同意/拒绝/移除 | `system` | `session_id`, `event`, `text`, `bot_id`, `inviter_id`/`responder_id` |

前端对 `ai_stream` 创建占位气泡，收到最终 `text` 帧后按 `ai_job_id` 替换；
系统事件用居中的系统卡片展示，邀请 pending 时非邀请方显示同意/拒绝按钮。

## 5. ID 映射

| 概念 | MsgRail | AI Companion |
|---|---|---|
| 用户 | `user_id` | `users.external_id = "im:<user_id>"` |
| 会话 | `session_id` | 仅作为请求字段，不落 `conversations` |
| 用户消息 | `message_id` (`session_id-seq`) | `context_messages[].message_id`，不落 `messages` |
| AI 回复 | `session_id-seq` | 由 Logic 在 `finalize` 时分配并落 `message` |
| 请求链路 | `job_id` | `runs.id` 仅用于 AI 内部观测；P6.1 不持久化 |

实现建议：

- 私人模式按 `im:<human_user_id>` 隔离记忆；气氛组不检索记忆。
- 幂等由 MsgRail `ai_job` 唯一键 `(session_id, trigger_message_id, bot_user_id)` 保证。

## 6. 鉴权与网络

- 内部接口**不暴露公网**；只允许 MsgRail 所在网段访问。
- `Authorization: Bearer <INTERNAL_TOKEN>`；token 定期轮换。
- 生产可升级 mTLS；不要把用户级 JWT 传给 AI 服务。
- AI 服务的 Provider Key 只存在 AI 服务侧，MsgRail 不接触。

## 7. 幂等与重试

- Logic 用 `message_id` 作为 `Idempotency-Key`。
- 重试策略：
  - **首 token 之前失败**：可安全重试。
  - **首 token 之后断流**：不要重新生成整段，先尝试 `Last-Event-ID` 续传；失败则把已收到部分作为最终消息，并标记中断。
- AI 服务对重复请求：
  - 已完成：返回已有消息（`message_end` 带 `replayed=true`）。
  - 进行中：返回 `409 AI_REQUEST_IN_PROGRESS`，Logic 可选择等待或提示稍后。
- 确保用户只看到一条 AI 回复。

## 8. 取消

- 用户在 IM 里点“停止”或断开连接 → Logic 取消 HTTP 请求。
- AI 服务检测断开 → 取消上游模型请求 → 部分结果标 `aborted` 落库。
- 已推送的 WS 帧不回滚；前端把消息标为“已停止”。

## 9. 错误码

| 错误码 | HTTP | 含义 | Logic 处理 |
|---|---|---|---|
| `AI_INVALID_REQUEST` | 400 | 参数错误 | 不重试，记录日志 |
| `AI_UNAUTHORIZED` | 401 | 内部 token 错 | 告警 |
| `AI_RATE_LIMITED` | 429 | AI 服务/上游限流 | 退避重试或提示 |
| `AI_CONTENT_BLOCKED` | 422 | 内容审核命中 | 返回固定提示 |
| `AI_UPSTREAM_TIMEOUT` | 504 | 模型超时 | 可重试 |
| `AI_UPSTREAM_UNAVAILABLE` | 503 | 模型/服务不可用 | 可重试 + 降级提示 |
| `AI_INTERNAL` | 500 | 未预期错误 | 记录 request_id，告警 |
| `STREAM_EXPIRED` | 409 | 重连缓冲过期 | 用幂等键重新请求 |

## 10. 可靠性与降级

- AI 服务不可用：Logic 发送固定提示“AI 暂时不可用，请稍后再试”，不阻塞 IM 主流程。
- 上游限流：AI 服务返回 `AI_RATE_LIMITED`，Logic 可排队或提示。
- 超时：默认 120s；超过则中断并提示。
- 日志：两边都记录 `request_id`，便于跨服务追踪。
- 监控：AI 服务暴露 `/metrics`（P5），MsgRail 侧记录调用耗时/错误率。

## 11. 版本化

- 路径带版本：`/internal/v1/...`。
- 只做增量字段；删除/改语义必须升 `v2`。
- 事件类型新增时，旧客户端忽略未知事件。
- 契约变更同步更新本文件，并在两个仓库的 PR 中互相引用。

## 12. 验收标准

1. 在 IM 里给 AI 用户发消息，浏览器收到逐段流式回复。
2. 工具调用时，前端能看到“正在调用工具”的状态，结束后继续输出。
3. 点“停止”能中断，已输出部分保留。
4. 同一 `message_id` 重试不会产生两条 AI 回复。
5. AI 服务挂掉时，IM 仍能正常收发其他消息，AI 会话显示友好提示。
6. 断线重连后，未完成的回复可通过 `Last-Event-ID` 续传（或明确提示已中断）。
