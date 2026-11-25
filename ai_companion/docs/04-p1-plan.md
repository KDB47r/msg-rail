# 04 · P1 技术方案：多轮对话与持久化

> 状态：已实现并验证。P1 只做“当前会话的短期记忆”；摘要/长期记忆/RAG 在 P2/P4，MCP 在 P3。
>
> 2026-11-22 注：`MAX_HISTORY_MESSAGES` 已在 P2 移除，改为 token 预算 + 滚动摘要。下文关于“最近 20 条”的描述是 P1 的历史行为，保留用于理解，不再代表当前实现。

## 1. 目标与非目标

### 目标

- 会话和消息落 PostgreSQL，刷新页面后能恢复历史。
- 每次请求把最近 N 条历史一起发给模型，实现多轮对话。
- 同一 `client_message_id` 重复请求不产生重复消息。
- 流中断/取消时，已生成部分尽量落库，状态可区分。
- 记录每次 run 的耗时、token、首 token 时间，为 P5 观测打基础。

### 非目标

- token 预算、滚动摘要（P2）
- 长期记忆、pgvector 检索（P4）
- 工具调用、MCP（P3）
- JWT/多用户（P5，P1 固定 dev 用户）
- Redis 缓存/流缓冲（P2/P5；P1 不引入 Redis 依赖）

## 2. 现状与改动点

P0 的 `POST /v1/chat/stream` 每次只发送：

```text
system prompt + 当前用户消息
```

`conversation_id` 虽然会返回给前端，但服务端不保存、不读取历史。P1 改动：

- 新增数据库层和迁移。
- 请求开始时落用户消息、读取最近历史。
- 流结束后落 assistant 消息和 run 统计。
- 前端刷新后按 `conversation_id` 拉取历史并渲染。

## 3. 技术选型

| 组件 | 选择 | 说明 |
|---|---|---|
| 数据库 | PostgreSQL（apt 或 Docker） | P1 不需要 pgvector；P4 再加向量扩展 |
| ORM | SQLAlchemy 2.0 async | 与 FastAPI 的 async 一致 |
| 驱动 | asyncpg | PostgreSQL 异步驱动 |
| 迁移 | Alembic | 表结构版本化 |
| 部署 | apt 优先，Docker 可选 | Docker Hub 拉取超时时直接用 apt |

Redis 当前已在 6379 运行，但 P1 不使用；幂等靠数据库唯一约束。

## 4. 数据模型（P1 迁移）

P1 只建 4 张表，完整字段见 `02-tech-design.md`：

- `users`：P1 固定一个 `dev-user`。
- `conversations`：`id`、`user_id`、`title`、`persona_id`、`status`、时间戳。
- `messages`：`id`、`conversation_id`、`role`、`content`、`status`、`client_message_id`、`token_count`、`model`、`provider`、`created_at`。
- `runs`：`id`、`conversation_id`、`assistant_message_id`、`status`、`model`、`provider`、`prompt_version`、token、`ttft_ms`、`latency_ms`、`error_code`、时间戳。

关键约束/索引：

```sql
CREATE EXTENSION IF NOT EXISTS vector;

UNIQUE (conversation_id, client_message_id) ON messages;
CREATE INDEX idx_messages_conv_created ON messages(conversation_id, created_at);
CREATE INDEX idx_conversations_user_last ON conversations(user_id, last_message_at DESC);
```

迁移中插入 dev 用户：

```sql
INSERT INTO users (external_id, display_name)
VALUES ('dev-user', 'Dev User')
ON CONFLICT (external_id) DO NOTHING;
```

`messages.status`：`streaming` / `completed` / `aborted` / `failed`。
`runs.status`：`running` / `completed` / `aborted` / `failed`。

## 5. API 变更

### 5.1 `POST /v1/chat/stream`

请求体保持兼容：

```json
{
  "conversation_id": "c_...",
  "message": "你好",
  "client_message_id": "uuid",
  "persona_id": "default"
}
```

行为：

1. 鉴权（P1 仍用 dev token）。
2. `conversation_id` 为空 → 新建会话；不为空 → 校验属于 dev 用户且 `status=active`。
3. 幂等检查：按 `(conversation_id, client_message_id)` 查已有消息。
   - assistant 已完成 → 回放已有结果。
   - 已有 user 消息但 assistant 未完成/失败 → 返回 `409 AI_REQUEST_IN_PROGRESS`。
4. 读取最近 N 条 `status=completed` 的历史（user + assistant），不包含当前新消息。
5. 插入当前 user 消息。
6. 创建 run（`status=running`）。
7. 调模型，流式输出；累积 assistant 文本。
8. 结束：写 assistant 消息、更新 run 统计；中断/异常按状态落库。

### 5.2 新增接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/conversations` | 当前 dev 用户的会话列表 |
| GET | `/v1/conversations/{id}/messages` | 历史消息，按时间升序，支持 `limit` |
| POST | `/v1/conversations` | 显式新建会话（可选） |
| DELETE | `/v1/conversations/{id}` | 归档会话（软删除） |

P1 不做分页游标，`limit` 默认 50、上限 200；P2 再补 `before` 游标。

### 5.3 幂等回放

重复请求若已有完整 assistant 消息：

```text
message_start  → 已有 conversation/message/run
content_delta  → 一次性发送完整历史内容
message_end    → 带 replayed=true
```

前端按普通流处理即可，不会重复显示。

## 6. 上下文装配（P1）

```text
system prompt
+ 最近 N 条 completed 历史（user / assistant）
+ 当前 user 消息
```

- `MAX_HISTORY_MESSAGES=20`（配置项）。
- 只取 `status=completed` 的消息；`aborted/failed` 不进入上下文。
- P1 不做 token 计数和裁剪；用消息条数兜底。P2 换成 token 预算 + 摘要。
- 历史顺序按 `created_at ASC`。

### 6.1 P1 的“短暂记忆”原理

核心结论：**模型本身是无状态的，它每次只看到本次请求里带上的文字。P1 的“记忆”是数据库存历史 + 每次请求重放最近 20 条。**

一次请求的链路：

1. 前端带上 `conversation_id` 请求 `POST /v1/chat/stream`。
2. `TurnService.prepare` 读取会话，按 `conversation_id` 从 `messages` 表加载历史：

   ```python
   # app/orchestrator/turn_service.py:117
   history_rows = await messages.list_recent_completed(
       conversation.id,
       self._settings.max_history_messages,
   )
   ```

3. 查询逻辑（`app/storage/repositories.py:168`）：只取 `role in (user, assistant)` 且 `status=completed` 的消息，按 `created_at desc` 取最近 20 条，再反转成时间正序。
4. `build_messages`（`app/orchestrator/context_builder.py:21`）拼成：

   ```text
   [system 人设] + [历史 user/assistant...] + [当前 user 消息]
   ```

5. 整个列表发给模型；SSE 把增量返回给前端。
6. `TurnService.finish`（`turn_service.py:162`）把 assistant 回复、token、耗时落库。
7. 下一轮重复 2~6 步，此时数据库里多了上一轮的 user/assistant，所以模型“记得”。

窗口变化（`MAX_HISTORY_MESSAGES=20`，一轮问答 2 条）：

```text
第 1 轮：system + u1
第 2 轮：system + u1 + a1 + u2
...
第 11 轮：system + 前 10 轮（20 条）+ u11
第 12 轮：最早的第 1 轮被挤出窗口，只剩第 2~11 轮 + u12
```

所以“超过多轮就不记得”不是模型失忆，而是它这次根本没收最早的文本。默认 20 条约等于 10 轮完整问答；到第 12 次请求时开始丢最早的一轮。

为什么不无限重放：

- 模型有上下文窗口上限，超过会报错或截断；
- 输入 token 越多，成本和首 token 延迟越高；
- 旧内容会稀释当前任务的注意力。

三种“记忆”要分清：

| 类型 | 存在哪里 | 怎么影响模型 | 本项目 |
|---|---|---|---|
| 参数记忆 | 模型权重 | 训练时学到的知识，有截止时间 | 改不了，也不能靠多轮对话更新 |
| 上下文/工作记忆 | 本次请求的 prompt | 模型这次能看到的内容 | P1：最近 20 条 |
| 外部记忆 | 数据库/向量库 | 检索后注入 prompt | P2 摘要、P4 长期记忆/RAG |
| 实时信息 | 工具/MCP | 模型调用后拿到结果 | P3 |

刷新页面能恢复历史，是因为前端保存了 `conversation_id`，通过 `GET /v1/conversations/{id}/messages` 从数据库读，不是模型记住了。

P1 的限制就是：窗口按“消息条数”硬截断，超出即丢弃，没有摘要。P2 会把这里升级为 **token 预算 + 滚动摘要**：保留最近几轮原文，更早的对话压缩成摘要注入，从而在有限窗口里记住更久。

## 7. 流式与落库

- 请求开始：插入 user 消息（幂等约束）。
- 创建 run：记录模型、provider、prompt_version、开始时间。
- 首个 `content_delta` 到达时记录 `ttft_ms`。
- 正常结束：插入 assistant 消息（`completed`），更新 run（token、latency、状态）。
- 客户端取消/断连：
  - 取消上游任务；
  - 若已累积文本，插入 assistant 消息 `status=aborted`；
  - run 标 `aborted`。
- 上游异常：run 标 `failed`，记录 `error_code`；已累积文本可存 `failed` 或丢弃（P1 选择存部分，方便排查）。
- 落库在 SSE 生成器的 `finally` 中执行，使用 `asyncio.shield`，避免取消中断写库。

## 8. 前端改动（最小）

- localStorage 保存 `conversation_id`。
- 页面加载：
  - 有 `conversation_id` → `GET /v1/conversations/{id}/messages`，渲染历史；
  - 404/403 → 清掉本地 ID。
- 新增“新对话”按钮：清空消息和 `conversation_id`。
- 发送时带 `client_message_id`；收到 `message_start` 后保存/更新 `conversation_id`。
- P1 暂不做会话侧边栏；P1.5 再加列表。

## 9. 配置新增

```dotenv
DATABASE_URL=postgresql+asyncpg://postgres:ai_companion@127.0.0.1:5432/ai_companion
MAX_HISTORY_MESSAGES=20
DEV_USER_EXTERNAL_ID=dev-user
DB_ECHO=false
```

同步更新 `.env.example`、`env.qwen.example`。

## 10. 启动与迁移

### 10.1 启动 PostgreSQL（用户执行）

方式 A：apt（推荐，避免 Docker Hub 超时）

```bash
sudo apt update
sudo apt install -y postgresql
sudo service postgresql start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'ai_companion';"
sudo -u postgres createdb ai_companion
```

方式 B：Docker

```bash
sudo docker run -d --name ai-pg \
  -e POSTGRES_PASSWORD=ai_companion \
  -e POSTGRES_DB=ai_companion \
  -p 5432:5432 \
  -v ai-pg-data:/var/lib/postgresql/data \
  pgvector/pgvector:pg16
```

验证：

```bash
pg_isready
# 或
sudo docker ps
sudo docker logs ai-pg
```

P1 不需要 pgvector；P4 再加。

### 10.2 安装依赖

```bash
source .venv/bin/activate
pip install "sqlalchemy[asyncio]>=2.0" "asyncpg>=0.29" "alembic>=1.13"
```

### 10.3 执行迁移

```bash
alembic upgrade head
```

## 11. 测试计划

### 单元测试

- `ContextBuilder`：历史按时间顺序、只包含 completed、最多 N 条。
- 幂等：同一 `client_message_id` 只产生一条 user 消息。
- 取消/异常：部分文本落库为 `aborted`/`failed`。

### 集成测试

- 测试使用 SQLite 内存库（`aiosqlite`），测试前 `create_all`，测试后销毁；不依赖本地 PostgreSQL。
- PostgreSQL 迁移通过手动 `alembic upgrade head` + 浏览器/curl 验收。
- `RecordingProvider` 记录收到的 messages：
  - 第一轮“我叫小明”；
  - 第二轮“我叫什么”；
  - 断言第二轮上下文包含第一轮的 user 和 assistant 消息。
- API：历史接口、会话列表、404/403、重复请求回放。

### 手动验收

1. 浏览器发送“我叫小明”，刷新页面，历史仍在。
2. 再发“我叫什么”，模型能答出“小明”（真实 Provider）。
3. 点“新对话”，历史清空；再切回旧会话（P1 先不做列表，可用 localStorage/接口验证）。
4. 停止生成后刷新，部分回复以“已停止”状态保留（若已产生文本）。

## 12. 验收标准

- 刷新页面能恢复当前会话历史。
- 第二轮请求确实携带第一轮上下文（用 RecordingProvider 断言）。
- 重复 `client_message_id` 不产生重复消息。
- 迁移可从空库一键执行；`pytest` 全绿。
- P0 的单轮 SSE、FakeProvider、鉴权、错误事件不回归。

## 13. 任务拆分

1. 依赖 + 配置 + `db.py`（engine/session）。
2. Alembic 初始迁移 + dev 用户。
3. 仓储层：conversations / messages / runs。
4. `ContextBuilder` 支持历史。
5. `ChatService` 持久化、幂等、取消/异常落库。
6. API：聊天改造 + 历史/会话接口。
7. 前端：历史恢复 + 新对话按钮。
8. 测试：单元 + 集成 + 手动验收。
9. 更新 README、技术方案、交接文档。

## 14. 待确认

1. PostgreSQL 用 Docker + sudo 启动，端口 5432，可以吗？
2. P1 前端只做“恢复当前会话 + 新对话”，暂不做会话侧边栏，可以吗？
3. P1 不引入 Redis，幂等只用数据库唯一约束，可以吗？
4. P1 固定 dev 用户，JWT 留到 P5，可以吗？
