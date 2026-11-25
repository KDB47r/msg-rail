# 11 · P6 技术方案：AI Companion 接入 MsgRail

> 状态：**P6.1 已实现并打通**（2026-11-23）。
>
> 已落地：`ai_assistant`/`ai_vibe` 两个 bot、`session_bot` 双方同意、`ai_job` 幂等、
> 结构化 `mentions`、Kafka `ai_trigger`、AI Worker、Logic 内部接口、Comet 流式帧、
> 私人 1:1、A-B 气氛组、mentioner-only 停止、最终消息落库。
>
> 与最初草案的差异：
>
> - AI Worker 放在 AI_Companion 仓库（Python），而不是 Job 进程；Logic 只负责
>   触发、上下文读取和内部 HTTP 转发，保持 C++ 热路径干净。
> - Comet 没有新增 protobuf 字段：流式增量复用 `ChatMessage.content_json` 的
>   `{"type":"ai_stream"}` 帧，最终消息复用普通 `single_chat`。
> - 私人模式接入 P4 记忆/RAG；气氛组禁用。
> - 连续消费 Kafka 需要 AI_Companion 的可选依赖 `aiokafka`。
>
> P6 做两个入口：
>
> 1. **私人 AI 助手**：用户和 AI 的 1:1 会话，复用 IM 聊天窗口，启用 P4 记忆/RAG。
> 2. **A-B 气氛组**：AI 作为虚拟参与者进入 A、B 单聊，@小桁 时结合最近 30 条消息回复；禁用用户级记忆/RAG。
>
> 两个 bot 分开：私人助手与气氛组各自独立 bot 用户、独立人设。

## 1. 目标与非目标

### 目标

- IM 内两个入口：
  - 加号菜单 → “AI 助手”：打开私人 1:1 会话；
  - 加号菜单 → “邀请小桁”：邀请气氛组进入当前 A-B 单聊。
- 气氛组只有被 @ 时回复，流式输出，A/B 都能看到。
- 气氛组只能看到“双方同意加入之后”的消息，最多最近 30 条。
- 私人助手启用 P4 用户级记忆/RAG；气氛组禁用。
- 停止权限：气氛组只允许 mentioner 停止；私人助手由用户自己停止。
- 错误先正常返回给前端（带 code/message），便于调试；后续再做友好降级。
- Logic 热路径不受影响：只检查当前消息 mention / 接收方是否 bot，不读 30 条、不调 AI。

### 非目标

- 群聊 @bot（P6.3）
- AI 头像/浮窗/主动搭话
- 私人助手记忆跨用户共享
- 气氛组长期记忆/RAG
- 多 bot 管理后台
- P3.2 Streamable HTTP MCP 不阻塞 P6

## 2. 两种模式

| | 私人 AI 助手 | A-B 气氛组 |
|---|---|---|
| 会话 | `session.type=0`，human + bot | `session.type=0`，A + B |
| bot 角色 | 正常单聊参与者（user2） | 虚拟第三发送者（session_bot） |
| 入口 | 加号菜单“AI 助手” | 加号菜单“邀请小桁” |
| 触发 | 给 bot 发消息 | 当前消息 mention 小桁 |
| 上下文 | 最近 30 条 + P4 记忆/RAG | 加入后最近 30 条 |
| 记忆/RAG | 启用（按 human 隔离） | 禁用 |
| 流式接收者 | 当前 human | A、B 两人 |
| 停止权限 | 当前 human | 仅 mentioner |
| 人设 | 私人助手 | 气氛组：轻松、简短、不抢话 |
| bot 用户 | `ai_assistant`（显示名小桁） | `ai_vibe`（显示名小桁） |

> 两个 bot 显示名都可以叫“小桁”，但 user_id、persona、会话完全独立；前端按上下文区分。

## 3. 总体架构

```text
私人 1:1：
human ──WS──> Comet ──gRPC──> Logic ──ai_trigger──> AI Worker
                                │                      │
                                │                      ├─ 读最近 30 条
                                │                      ├─ AI Companion（private 模式）
                                │                      └─ 流式 → Comet → human
                                └─ Kafka → Job → MySQL（最终 bot 消息）

A-B 气氛组：
A/B ──WS──> Comet ──gRPC──> Logic ──ai_trigger──> AI Worker
                                │                      │
                                │                      ├─ 读激活后最近 30 条
                                │                      ├─ AI Companion（shared 模式）
                                │                      └─ 流式 → Comet → A/B
                                └─ Kafka → Job → MySQL（最终 bot 消息）
```

## 4. 数据模型

### 4.1 bot 用户

`user` 增加：

```sql
ALTER TABLE `user` ADD COLUMN `is_bot` TINYINT NOT NULL DEFAULT 0;
```

预置两个 bot：

- `ai_assistant`：私人助手，`is_bot=1`，不能登录；
- `ai_vibe`：气氛组，`is_bot=1`，不能登录。

bot 的密码哈希为空，登录接口拒绝 `is_bot=1`。

### 4.2 气氛组绑定

```sql
CREATE TABLE `session_bot` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,
  `bot_user_id` BIGINT NOT NULL,
  `status` TINYINT NOT NULL DEFAULT 0,  -- 0 pending,1 active,2 rejected,3 removed
  `invited_by` BIGINT NOT NULL,
  `activated_seq` BIGINT NOT NULL DEFAULT 0,
  `activated_at` TIMESTAMP NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_bot` (`session_id`, `bot_user_id`),
  KEY `idx_session_status` (`session_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

规则：

- A 邀请小桁 → `status=0`，`invited_by=A`；
- B 同意 → `status=1`，`activated_seq = session.last_msg_seq`；
- B 拒绝 → `status=2`；
- 任一方移除 → `status=3`；
- 气氛组只能看到 `msg_seq > activated_seq` 的消息；
- 加入/移除通过 `msg_type=system` 的系统消息展示。

### 4.3 AI 任务与幂等

```sql
CREATE TABLE `ai_job` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `job_id` VARCHAR(64) NOT NULL,
  `session_id` VARCHAR(128) NOT NULL,
  `bot_user_id` BIGINT NOT NULL,
  `mode` VARCHAR(16) NOT NULL,          -- private/shared
  `trigger_message_id` VARCHAR(160) NOT NULL,
  `status` VARCHAR(16) NOT NULL,        -- queued/running/completed/failed/aborted
  `msg_id` VARCHAR(160) DEFAULT '',
  `msg_seq` BIGINT DEFAULT 0,
  `error_code` VARCHAR(64) DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_trigger_bot` (`session_id`, `trigger_message_id`, `bot_user_id`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

用途：

- 防重复回复；
- 记录 msg_id/msg_seq 映射；
- 失败可重试/排查。

## 5. 触发与 mention

### 5.1 结构化 mention

客户端在 `content_json` 中带：

```json
{
  "content": "@小桁 你觉得呢",
  "mentions": [
    {"user_id": 10001, "name": "小桁", "type": "bot"}
  ]
}
```

服务端只检查 `mentions` 中是否有 bot 的 `user_id`；不依赖显示名。

兼容旧客户端：`mentions` 为空时，fallback 检查文本 `@小桁`。

### 5.2 Logic 检测

私人模式：

- 发送消息时，`to_user_id` 命中 bot 缓存 → 投 `ai_trigger`；
- 不解析正文，不读历史。

共享模式：

- 当前消息是文本，且 `mentions` 含 bot id（或 fallback 文本）；
- 查 Redis `session:bot:{session_id}`，确认 `status=active`；
- 投 `ai_trigger`；
- 30 条上下文由 AI Worker 读取，不在 Logic。

bot 自己的消息不触发；媒体消息 P6 不触发。

### 5.3 性能

- 普通消息：只多一次 bot id 缓存判断（私人）或 `mentions` 字段检查（共享）；
- 只有命中才 Redis 查 `session_bot` + Kafka 投递；
- 30 条消息查询、AI 调用、流式推送都在 AI Worker；
- 预计正常消息 QPS 影响 <1~2%；上线前做 A/B 压测。

## 6. 上下文

### 6.1 气氛组（shared）

AI Worker 查询：

```sql
SELECT msg_seq, sender_id, msg_type, content_json, timestamp_ms
FROM message
WHERE session_id = ?
  AND msg_seq > ?
ORDER BY msg_seq DESC
LIMIT 30;
```

- `?` 是 `session_bot.activated_seq`；
- 只取文本；媒体消息转成占位符，例如 `[图片]`；
- 带 sender 显示名；
- 不注入 P4 用户记忆/RAG；
- 不跨会话。

Prompt 形式：

```text
你是“小桁”，A-B 单聊里的气氛组，不是客服或知识助手。
只在被 @ 时说话；回复 1~3 句，轻松、简短，不抢话，不站队。
不要引用任何人的私聊记忆。

参与者：张三、李四、你（小桁）
最近对话：
张三：...
李四：...
张三：@小桁 你觉得呢？
```

### 6.2 私人助手（private）

- 最近 30 条 IM 消息；
- 启用 P4 用户级记忆/RAG（按 human 外部 ID 隔离）；
- 不读取气氛组会话；
- 私人助手可以引用用户自己的记忆，但不能引用 A-B 群里另一个人的信息。

## 7. AI Companion 内部接口

新增：

```text
POST /internal/v1/agent/stream
Authorization: Bearer <INTERNAL_TOKEN>
```

请求：

```json
{
  "request_id": "r_...",
  "mode": "private",
  "user_id": "im:42",
  "session_id": "single:42:10001",
  "bot_user_id": 10001,
  "trigger_message_id": "msg_...",
  "trigger_sender_id": 42,
  "trigger_sender_name": "张三",
  "context_messages": [
    {
      "message_id": "...",
      "sender_id": 42,
      "sender_name": "张三",
      "sender_type": "human",
      "content": "...",
      "timestamp_ms": 1760000000000
    }
  ],
  "persona_id": "xiaoheng_private",
  "idempotency_key": "msg_..."
}
```

- `mode=private`：启用 P4 记忆/RAG；`mode=shared`：只使用 `context_messages`，禁用用户记忆/RAG；
- 响应沿用现有 SSE：`message_start` / `content_delta` / `tool_call` / `tool_result` / `message_end` / `error`；
- 错误必须返回 `error` 事件，带 code/message，便于调试；
- AI 服务不保存 A-B 聊天历史；只记录 runs/tool_calls/retrieval_events。

## 8. 异步与流式

```text
Logic ──Kafka ai_trigger──> AI Worker
                               │
                               ├─ 读上下文
                               ├─ 调 AI Companion SSE
                               ├─ 流式片段 → Comet PushToComet
                               └─ 最终 bot 消息 → Kafka/MySQL
```

- `ai_trigger` 字段：job_id、mode、session_id、bot_user_id、trigger_message_id、trigger_sender_id/name、mention、created_at；
- AI Worker 负责上下文读取、AI 调用、流式推送、最终消息落库；
- 流式片段 30~50ms 合并一次，减少 WS 帧；
- bot 回复在开始时分配 `msg_seq`，最终消息使用同一 `msg_seq`；
- 私人模式推给 human；共享模式推给 A、B；
- 离线用户不推，最终消息重连后拉历史。

## 9. 同意、可见性与权限

### 9.1 气氛组加入

1. A 或 B 在加号菜单点“邀请小桁”；
2. 写入 `session_bot(status=0)`，发系统消息“邀请小桁加入”；
3. 另一位用户同意 → `status=1`，`activated_seq=当前 seq`，系统消息“小桁已加入”；
4. 拒绝 → `status=2`，系统消息“已拒绝”；
5. 邀请前/同意前的消息不进入 AI 上下文；
6. 任一方可移除 → `status=3`，系统消息“小桁已退出”，后续不再触发。

### 9.2 停止

- 共享模式：只有 **mentioner** 可以停止；其他人的停止请求忽略；
- 私人模式：当前用户可停止；
- 停止 → `ai_stop` → Worker 取消 AI 请求 → 已生成部分落库 `aborted`。

### 9.3 错误

- AI 失败/超时/限流 → SSE `error` 事件 → 前端显示错误卡片/提示；
- 同时落一条 system 消息或 ai_job 失败记录，便于调试；
- 后续再改成“小桁刚刚走神了”之类的友好文案。

## 10. UI 入口

加号菜单（上传文件/图片同一入口）：

- **AI 助手**：打开/创建与 `ai_assistant` 的 1:1 会话；
- **邀请小桁**：仅在 A-B 单聊显示；已加入时改为“小桁已在会话中/移除小桁”；
- 私人 bot 会话里不显示“邀请小桁”；
- 气氛组消息显示“小桁”+ bot 标记；支持流式、停止按钮（仅 mentioner 可见/可用）。

## 11. 分阶段

| 阶段 | 内容 | 验收 |
|---|---|---|
| P6.1 | bot 用户、私人 1:1、气氛组邀请/同意、结构化 mention、AI Worker、30 条上下文、流式、最终落库、错误可见 | 两个入口可用 |
| P6.2 | 私人 P4 记忆/RAG、气氛组人设调优、停止权限、限流、幂等 | 私人记忆生效，气氛组不泄露 |
| P6.3 | 群聊 @小桁、群记忆边界 | 群聊可用 |
| P6.4 | 观测/压测/故障注入 | 正常消息 QPS 不明显下降 |

## 12. 测试计划

- Logic：mention 检测、bot 缓存、session_bot 状态、普通消息 QPS；
- AI Worker：30 条边界、激活前消息不可见、停止权限、幂等、错误；
- AI Companion：private 记忆/RAG、shared 禁用、多人说话人格式；
- 集成：邀请/同意/拒绝/移除、@小桁、A/B 同时在线/一方离线、错误返回；
- 性能：有/无 mention 检查的 QPS 对比，AI 流式帧 batching。

## 13. 已确认细节

1. 两个 bot 显示名都叫“小桁”，`user_id`/账号/人设独立。
2. 邀请用普通 `msg_type=system` 消息，`content_json.event=bot_invite`，非邀请方显示“同意/拒绝”按钮。
3. 私人助手 P6.1 先用最近 30 条 + P4 记忆/RAG，不做 IM 历史摘要。
4. 气氛组失败：前端收到 `ai_stream.error` 显示错误；`ai_job` 记录 `error_code/error_message`。离线时只能查 `ai_job`。

## 14. 实现落点（2026-11-23）

### AI Companion

- `app/api/internal.py`：`POST /internal/v1/agent/stream`，Bearer `INTERNAL_TOKEN`。
- `app/orchestrator/im_context.py`：私人/气氛组 prompt 与多说话人上下文。
- `app/prompts/xiaoheng_private.md`、`xiaoheng_shared.md`：两个人设。
- `app/worker/ai_worker.py`：消费 `ai_trigger`、调 AI Companion、批量 `delta`、`finalize`/`fail`；支持 `--trigger-json` 单条调试。
- 迁移：MsgRail `sql/migrations/006_ai_companion.sql`（本次已应用）。

### MsgRail

- `logic/ai_dao.*`：`session_bot`、`ai_job`。
- `logic/ai_service.*`：mention/bot 检测、触发、邀请/同意/拒绝/移除、上下文、流式帧、最终消息、停止。
- `logic/http_handlers_ai.cpp`：`/api/ai/*` 与 `/internal/ai/*`。
- `logic/http_handlers_message.cpp` / `grpc_service.cpp`：发送路径只多一次 bot 判断；命中后投 `ai_trigger`。
- `web_demo/static/index.html`：加号菜单“AI 助手/邀请小桁”、系统卡片、流式气泡、停止按钮。
- `conf/logic.conf`：`kafka_ai_trigger_topic`、两个 bot ID、`ai_internal_token`、`ai_context_message_limit`。

### 验证记录

- 共享模式：邀请 → 同意（`activated_seq=2`）→ 结构化 mention → `ai_job` → Kafka 触发 → 上下文只含 seq>2 → 流式 delta → 最终消息 `msg_seq=4`。
- 私人模式：打开 `single:human:ai_assistant` → 发消息 → `ai_job(private)` → 记忆检索 → 最终消息落库。
- 停止：mentioner 调 `/api/ai/stop` → `ai_job.status=aborted`，双方收到 `stopped` 帧。
- `aiokafka` 未安装时 Worker 会给出安装命令；`--trigger-json` 已完成真实模型端到端验证。

### 后续（P6.2）

- IM 私聊消息进入 P4 记忆抽取（当前只做检索，抽取仍基于 AI Companion 自身会话）。
- Worker 消费 offset 的 at-least-once 重放保护（当前依赖 `ai_job` 状态，仍可能并发重跑）。
- 群聊 @小桁、限流、更多 UI 细节。
