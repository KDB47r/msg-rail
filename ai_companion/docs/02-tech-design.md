# 02 · 技术方案

> 状态：草案，待确认。默认按 **Python FastAPI + OpenAI 兼容接口** 设计；如果更换语言/Provider，模块边界不变，适配层和目录需要调整。

## 1. 技术栈

| 层次 | 选择 | 理由 |
|---|---|---|
| 语言 | Python 3.10+（3.11+ 推荐） | LLM/MCP 生态最全，迭代最快 |
| Web | FastAPI + uvicorn | 原生 async、SSE 简单、Pydantic 校验 |
| 上游模型 | OpenAI 兼容 SDK（DeepSeek/Qwen/GLM） | 国内可直连、成本低、接口通用 |
| HTTP 客户端 | httpx（async） | 超时/取消/连接池可控 |
| 数据库 | PostgreSQL 16 + pgvector | 消息持久化 + 记忆检索一套搞定 |
| 缓存 | Redis 7 | 会话缓存、流缓冲、限流、锁 |
| 迁移 | Alembic | 表结构版本化 |
| 测试 | pytest + pytest-asyncio + respx | 单元/集成/流式 mock |
| 代码质量 | ruff | 快速 lint/format |
| 前端 | 原生 HTML + fetch ReadableStream | 不把学习重点放在前端 |

依赖安装只给命令，不自动执行。

## 2. 目录结构

```text
AI_Companion/
├── CLAUDE.md
├── README.md
├── pyproject.toml
├── alembic.ini
├── app/
│   ├── main.py
│   ├── config.py
│   ├── api/
│   │   ├── chat.py
│   │   ├── conversations.py
│   │   └── health.py
│   ├── orchestrator/
│   │   ├── chat_service.py
│   │   ├── context_builder.py
│   │   ├── agent_loop.py
│   │   └── events.py
│   ├── llm/
│   │   ├── base.py
│   │   ├── openai_compat.py
│   │   └── registry.py
│   ├── mcp/
│   │   ├── client_manager.py
│   │   ├── permissions.py
│   │   └── servers/
│   │       └── time_server.py
│   ├── memory/
│   │   ├── summarizer.py
│   │   ├── extractor.py
│   │   └── retriever.py
│   ├── storage/
│   │   ├── db.py
│   │   ├── models.py
│   │   └── repositories.py
│   ├── prompts/
│   │   └── system.md          # P1 再拆出 persona.yaml
│   ├── security/
│   │   ├── auth.py
│   │   └── moderation.py
│   └── observability/
│       └── logging.py
├── web/
│   └── index.html
├── tests/
│   ├── unit/
│   └── integration/
└── docs/
    ├── 00-handoff.md
    ├── 01-architecture.md
    ├── 02-tech-design.md
    └── 03-integration-contract.md
```

## 3. 公共 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/stream` | 发送消息，SSE 流式返回 |
| GET | `/v1/conversations` | 会话列表 |
| GET | `/v1/conversations/{id}/messages` | 历史消息 |
| POST | `/v1/conversations` | 创建会话（可选） |
| DELETE | `/v1/conversations/{id}` | 归档会话 |
| GET | `/healthz` | 进程存活 |
| GET | `/readyz` | 依赖就绪（DB/Redis） |

鉴权：`Authorization: Bearer <token>`。P0 用固定 dev token，P1 换 JWT。

### 3.1 `POST /v1/chat/stream`

请求头：

```text
Authorization: Bearer <token>
Content-Type: application/json
Accept: text/event-stream
Idempotency-Key: <uuid>
```

请求体：

```json
{
  "conversation_id": "c_01J...",
  "message": "你好",
  "client_message_id": "uuid",
  "persona_id": "default"
}
```

- `conversation_id` 可省略：服务端新建会话，并在 `message_start` 返回。
- `client_message_id` 用于幂等；同一会话内重复请求返回已有结果。
- P0 只支持文本；附件后续再加。

## 4. SSE 协议

### 4.1 响应头

```text
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

### 4.2 事件类型

| event | 方向 | 说明 |
|---|---|---|
| `message_start` | 服务端 | 会话/消息已创建 |
| `content_delta` | 服务端 | 正文增量 |
| `reasoning_delta` | 服务端 | 推理摘要增量（可选） |
| `tool_call` | 服务端 | 模型请求调用工具 |
| `tool_result` | 服务端 | 工具执行结果 |
| `usage` | 服务端 | token 统计（可多次） |
| `message_end` | 服务端 | 终止事件 |
| `error` | 服务端 | 流内错误 |
| `: ping` | 服务端 | 心跳注释行 |

每个事件带递增 `id`，用于断线重连。`message_end` 是正常终止；`error` 之后不再发正文。

### 4.3 示例

```text
id: 1
event: message_start
data: {"conversation_id":"c_01J","message_id":"m_01J","created_at":"2026-11-21T08:00:00Z"}

id: 2
event: content_delta
data: {"delta":"你好呀"}

id: 3
event: tool_call
data: {"call_id":"call_01","name":"get_weather","arguments":{"city":"杭州"}}

id: 4
event: tool_result
data: {"call_id":"call_01","status":"ok","preview":"晴，26℃"}

id: 5
event: usage
data: {"input_tokens":1200,"output_tokens":80,"cached_tokens":900}

id: 6
event: message_end
data: {"finish_reason":"stop","message_id":"m_01J","usage":{"input_tokens":1200,"output_tokens":80}}

: ping
```

错误：

```text
event: error
data: {"code":"UPSTREAM_TIMEOUT","message":"模型响应超时","retryable":true}
```

### 4.4 心跳、重连、取消、背压

- 心跳：每 15s 发送 `: ping\n\n`，防止代理空闲断开。
- 重连：浏览器带 `Last-Event-ID` 重连；服务端从 Redis 缓冲回放。缓冲过期返回 `STREAM_EXPIRED`，客户端用同一 `Idempotency-Key` 发起新请求。
- 取消：客户端 `AbortController` 断开；服务端检测后取消上游任务，把部分结果标为 `aborted`。
- 背压：`StreamingResponse` 逐事件写出；发送缓冲满时等待；超过上限则关闭连接并记录。
- 压缩：SSE 响应禁用 gzip，避免缓冲。

### 4.5 客户端解析

`EventSource` 只支持 GET 且不能带自定义 Header，聊天场景用：

```js
const res = await fetch("/v1/chat/stream", {
  method: "POST",
  headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
  body: JSON.stringify(payload),
  signal: controller.signal,
});
const reader = res.body.getReader();
// 按 \n\n 切事件，解析 event/data/id
```

## 5. 数据模型

> 以下是设计草案，字段可在实现时调整；时间统一 `TIMESTAMPTZ`。

```sql
CREATE TABLE users (
  id            BIGSERIAL PRIMARY KEY,
  external_id   TEXT NOT NULL UNIQUE,
  display_name  TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE conversations (
  id              TEXT PRIMARY KEY,             -- c_xxx
  user_id         BIGINT NOT NULL REFERENCES users(id),
  title           TEXT,
  persona_id      TEXT NOT NULL DEFAULT 'default',
  status          TEXT NOT NULL DEFAULT 'active', -- active/archived
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_message_at TIMESTAMPTZ
);

CREATE TABLE messages (
  id                  TEXT PRIMARY KEY,          -- m_xxx
  conversation_id     TEXT NOT NULL REFERENCES conversations(id),
  role                TEXT NOT NULL,             -- user/assistant/tool/system
  content             TEXT NOT NULL DEFAULT '',
  content_json        JSONB,                     -- 多段内容/工具块
  status              TEXT NOT NULL DEFAULT 'completed',
                                                 -- streaming/completed/aborted/failed
  client_message_id   TEXT,                      -- 幂等
  external_message_id TEXT,                      -- Phase 2：MsgRail 消息 ID
  token_count         INT,
  model               TEXT,
  provider            TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (conversation_id, client_message_id)
);
CREATE INDEX idx_messages_conv_created ON messages(conversation_id, created_at);
CREATE INDEX idx_messages_external ON messages(external_message_id);

CREATE TABLE runs (
  id                   TEXT PRIMARY KEY,         -- r_xxx
  conversation_id      TEXT NOT NULL REFERENCES conversations(id),
  user_message_id      TEXT REFERENCES messages(id),
  assistant_message_id TEXT REFERENCES messages(id),
  status               TEXT NOT NULL,            -- running/completed/aborted/failed
  model                TEXT,
  provider             TEXT,
  prompt_version       TEXT,
  input_tokens         INT,
  output_tokens        INT,
  cached_tokens        INT,
  ttft_ms              INT,
  latency_ms           INT,
  error_code           TEXT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at          TIMESTAMPTZ
);

CREATE TABLE tool_calls (
  id             BIGSERIAL PRIMARY KEY,
  run_id         TEXT NOT NULL REFERENCES runs(id),
  call_id        TEXT NOT NULL,
  tool_name      TEXT NOT NULL,
  server_name    TEXT,
  arguments_json JSONB,
  result_json    JSONB,
  status         TEXT NOT NULL,                  -- pending/success/error/timeout/denied
  latency_ms     INT,
  error_code     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, call_id)
);

CREATE TABLE summaries (
  id               BIGSERIAL PRIMARY KEY,
  conversation_id  TEXT NOT NULL REFERENCES conversations(id),
  up_to_message_id TEXT NOT NULL,
  up_to_created_at TIMESTAMPTZ NOT NULL,
  content          TEXT NOT NULL,
  token_count      INT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_summaries_conv_created
  ON summaries(conversation_id, created_at);

CREATE TABLE memories (
  id                BIGSERIAL PRIMARY KEY,
  user_id           BIGINT NOT NULL REFERENCES users(id),
  kind              TEXT NOT NULL,               -- fact/preference/event
  content           TEXT NOT NULL,
  content_hash      TEXT NOT NULL,               -- 去重
  embedding         vector(1536),                -- 维度随 embedding 模型调整
  source_message_id TEXT,
  confidence        REAL NOT NULL DEFAULT 1.0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at      TIMESTAMPTZ,
  UNIQUE (user_id, content_hash)
);
CREATE INDEX idx_memories_user ON memories(user_id);
CREATE INDEX idx_memories_embedding ON memories
  USING hnsw (embedding vector_cosine_ops);
```

## 6. Redis Key

| Key | 类型 | 用途 | TTL |
|---|---|---|---|
| `chat:conv:{conversation_id}:recent` | list | 最近 N 条消息缓存 | 1h |
| `chat:stream:{message_id}` | list | SSE 事件缓冲（重连回放） | 10m |
| `chat:run:{conversation_id}` | string | 当前 run ID / 会话锁 | 5m |
| `chat:idem:{user_id}:{key}` | string | 幂等结果指针 | 24h |
| `ratelimit:user:{user_id}:{window}` | string | 限流计数 | 60s |
| `mcp:tools:{server_name}` | string | `tools/list` 缓存 | 5m |

## 7. 上下文装配

### 7.1 算法

```text
1. 读取会话、角色卡、最近消息、摘要、记忆
2. 估算 token；超过预算则：
   a. 保留最近 N 轮
   b. 用最近摘要替换更早历史
   c. 仍超则从最旧的记忆/RAG 片段开始裁剪
3. 组装 system（基础规则 + 角色卡 + 工具规则）
4. 组装 messages（摘要作为 system 或 user 前缀，历史按时间）
5. 追加当前用户消息
6. 记录 prompt_version、token 估算
```

### 7.2 Token 预算

见 `01-architecture.md` 的 32K 示例。实现时用 `TokenCounter` 抽象：

- OpenAI 兼容：`tiktoken` 估算。
- Claude：`count_tokens` API（后续接入时）。
- 估算误差按 10% 预留。

### 7.3 摘要

- 触发：历史 token 超过预算的 60%。
- 输入：上次摘要 + 新增的较早消息。
- 输出：结构化摘要（用户事实、偏好、未完成事项、情绪基调）。
- 存储：`summaries`；只保留最新一条有效摘要 + 历史版本可选。
- 异步执行；主链路不等待。

### 7.4 记忆抽取

- 每 6 轮或会话结束时触发异步任务。
- Prompt 要求输出 JSON 数组：`kind`、`content`、`confidence`。
- 去重：`content_hash`；相似内容合并。
- 检索：查询 embedding + top-k + 阈值；按 `last_used_at` 做时间衰减。
- 记忆只作为“背景信息”注入，不允许覆盖 system 规则。

## 8. 工具调用与 MCP

### 8.1 Provider 工具格式

`llm/base.py` 定义统一接口：

```python
class LLMProvider(Protocol):
    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec],
        **kwargs,
    ) -> AsyncIterator[StreamEvent]: ...
```

`StreamEvent`：`text_delta` / `reasoning_delta` / `tool_call` / `usage` / `end`。

`openai_compat.py` 负责把内部格式转换成 OpenAI 的 `tools` / `tool_calls`，并把流式分片拼成完整的 `tool_call`。

### 8.2 工具循环

```python
for step in range(MAX_STEPS):
    async for ev in provider.stream(messages, tools):
        yield ev
    if not ev.tool_calls:
        break
    results = await execute_tools(ev.tool_calls)
    messages.append(assistant_with_tool_calls(ev.tool_calls))
    messages.append(tool_results(results))
```

约束：

- `MAX_STEPS = 5`
- 单工具超时 10s，整个 turn 120s
- 工具结果最大 8KB；超出截断并注明
- 并行调用：`asyncio.gather`；所有结果放同一条 user 消息

### 8.3 MCP 配置

```yaml
mcp:
  servers:
    - name: time
      transport: stdio
      command: python
      args: ["-m", "app.mcp.servers.time_server"]
    - name: weather
      transport: streamable_http
      url: http://localhost:8765/mcp
      headers:
        Authorization: Bearer ${WEATHER_TOKEN}
  permissions:
    default: allow_readonly
    tools:
      delete_note:
        requires_confirmation: true
```

### 8.4 MCP 客户端

- 启动时连接、`initialize`、`tools/list`，缓存工具列表。
- 断线重连：指数退避，最大 30s。
- 工具名冲突：加 `server_name` 前缀。
- 审计：每次 `tools/call` 写 `tool_calls`，含参数、结果、耗时、状态。
- 旧版 HTTP+SSE 传输不实现；远程统一 Streamable HTTP。

## 9. 失败处理、幂等、限流

### 9.1 失败处理

| 场景 | 处理 |
|---|---|
| 上游 429/5xx | 指数退避重试 2 次；仍失败则 SSE `error`，标记可重试 |
| 上游超时 | 取消请求，`UPSTREAM_TIMEOUT`，可重试 |
| 客户端断开 | 取消上游；部分结果 `aborted` 落库 |
| DB 失败 | 请求失败；幂等键保证重试不重复写 |
| Redis 失败 | 降级：跳过缓存/重连缓冲，记录日志 |
| MCP 工具失败 | `tool_result(is_error)`，模型继续；不崩溃 |
| 内容审核命中 | `AI_CONTENT_BLOCKED`，不返回模型内容 |

### 9.2 幂等

- 唯一键：`(conversation_id, client_message_id)`。
- 重复请求：
  - 已完成：直接返回已存 assistant 消息（SSE `message_start` + `content_delta` 全量 + `message_end`，或一次性 `message_end` 带 `replayed=true`）。
  - 进行中：返回 `409` 或等待同一 run 的事件流（P1 实现）。
- Phase 2：`idempotency_key` 用 MsgRail 的消息 ID。

### 9.3 限流

- 每用户每分钟 20 次请求。
- 每日 token 配额按用户/全局配置。
- Redis `INCR` + `EXPIRE`；超限返回 `429`，带 `Retry-After`。
- 上游限流单独处理，不与用户限流混淆。

## 10. 安全

- 鉴权：P0 dev token；P1 JWT；Phase 2 内部 token/mTLS。
- 密钥：环境变量；不写代码/日志/文档。
- 内容审核：输入/输出都过；可先用厂商 moderation，后接本地模型。
- Prompt 注入：外部内容分隔符、工具输出信任级别、system 优先级、拒绝回显。
- PII：日志脱敏；提供删除会话/用户数据接口。
- CORS：开发放开指定 origin；生产同源。
- MCP：默认只读；危险工具确认；不默认开放 shell；server 白名单。

## 11. 性能

- 全链路 async；禁止在事件循环里做同步 IO。
- DB 写入：请求开始写 user message，结束写 assistant + run；不为每个 token 写库。
- Redis 事件缓冲支持重连，不重复调用模型。
- Prompt cache：稳定前缀放前面；记录 `cached_tokens`。
- 连接池：DB 10~20，Redis 10；按部署调整。
- SSE：逐事件 flush；关闭 gzip；心跳 15s。
- 目标：TTFT < 1.5s（不含厂商网络波动）；50 并发流下不 OOM。

## 12. 测试计划

### 12.1 单元测试

- ContextBuilder：预算裁剪、摘要替换、角色卡注入。
- SSE formatter：事件格式、id 递增、JSON 转义。
- 工具 schema 转换：内部格式 ↔ OpenAI `tools`。
- 幂等键：重复请求只产生一条消息。

### 12.2 集成测试

- `FakeProvider` 流式输出固定 chunk，验证 `/v1/chat/stream` 事件序列。
- 工具循环：模型返回 `tool_call` → MCP Server（stdio）→ `tool_result` → 继续输出。
- DB/Redis：Docker Compose 起依赖，测持久化与重连缓冲。
- 取消：客户端断开后上游任务被取消，消息状态 `aborted`。

### 12.3 E2E

- 真实 Provider 冒烟（环境变量开关，默认跳过）。
- 浏览器手动验证：打字机、停止、重试、刷新后历史。

### 12.4 负载与故障

- 50 并发 SSE 流：TTFT、p95、内存。
- 故障注入：上游超时、中途断流、DB 不可用、Redis 不可用、MCP 不可用。
- 验证：错误事件可读、无重复消息、无进程崩溃。

## 13. P0 实现步骤

> 2026-11-21：P0 已完成并验证（9 tests passed，uvicorn 实机 SSE 验证）。
>
> 2026-11-21：P1 代码已完成（SQLAlchemy/Alembic、多轮历史、幂等、历史恢复、前端“新对话”）。
>
> 2026-11-21：P2 已完成并验证。26 tests passed；迁移 `0002_summaries` 已应用；
> token 预算 + 滚动摘要（默认复用 `LLM_MODEL`）已接入，上下文不再按 20 条消息硬截断。
>
> 2026-11-22：P3 第一阶段完成。39 tests passed；迁移 `0003_tool_calls` 已应用；
> AgentLoop、ToolRegistry、本地时间工具、MCP stdio 时间 Server、Open-Meteo 真实天气已接入。
>
> 2026-11-22：P4.1 完成。48 tests passed；迁移 `0004_memories`；
> Qwen text-embedding-v3 + pgvector 0.8.7 + HNSW 的长期记忆检索已接入。
>
> 2026-11-22：P4.2 完成。57 tests passed；迁移 `0005_documents`；
> 文档上传、切块、embedding、top-5 检索与引用已接入。

1. `pyproject.toml`：fastapi、uvicorn、pydantic-settings、httpx、openai、pytest、pytest-asyncio、respx、ruff。
2. `app/config.py`：环境变量配置。
3. `app/main.py`：FastAPI app、`/healthz`、静态文件。
4. `app/api/chat.py`：`POST /v1/chat/stream`，先用 `FakeProvider` 输出固定文本。
5. `app/llm/base.py` + `openai_compat.py`：接真实 Provider。
6. `web/index.html`：fetch + ReadableStream 解析 SSE。
7. `tests/`：SSE formatter、端点事件序列、取消。
8. 验收：`curl -N` 可见事件；浏览器打字机输出；停止按钮生效。

## 14. 开放问题

- 语言是否就用 Python？如果目标是 Java 岗，建议 Spring Boot + Spring AI，架构不变。
- Provider 选 DeepSeek 还是 Qwen？是否同时接 Claude？
- P0 是否直接上 PostgreSQL，还是先用 SQLite 降低环境成本？（建议直接 PostgreSQL，避免迁移）
- 是否在 P1 就引入 JWT，还是继续 dev token？
- 前端是否需要 React？建议先原生，P5 后再考虑。
- 长期记忆的 embedding 模型选型（P4 再定）。
