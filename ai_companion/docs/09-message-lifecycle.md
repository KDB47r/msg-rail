# 09 · 消息与状态的生命周期

> 本文回答：哪些消息落库、历史窗口怎么选、新会话/刷新/归档分别怎么处理、
> 是否有“20 条容量”的限制。

## 1. 数据表

| 表 | 存什么 | 作用域 |
|---|---|---|
| `conversations` | 会话元数据：id、user_id、title、status、时间 | 单个会话 |
| `messages` | user / assistant 消息正文、状态、client_message_id | 单个会话 |
| `runs` | 每轮模型调用的 token、TTFT、耗时、错误码 | 单个回合 |
| `summaries` | 滚动摘要 + 截止消息/时间 | 单个会话 |
| `tool_calls` | 每次工具调用的参数、结果、耗时、状态 | 单个回合 |
| `memories` | 长期记忆（fact/preference/event）+ embedding | 用户跨会话 |

`messages` 表**不是 20 行容量**，它会一直追加。没有自动删除或 TTL。

## 2. 写入路径

一次正常对话：

```text
prepare：
  1. 找到/创建 conversation
  2. 检查 client_message_id 幂等
  3. 读取最新 summary + 截止点后的 messages
  4. build_context 组装 prompt
  5. 插入 user message（status=completed）
  6. 插入 run（status=running）
  7. commit

流式输出：
  不逐 token 写库

finish：
  1. 插入/更新 assistant message（completed/aborted/failed）
  2. 更新 run：token、TTFT、latency、错误码、finished_at
  3. 必要时触发摘要/记忆抽取
```

特殊情况：

- 客户端取消：已生成部分以 `status=aborted` 落库；没有内容则不写 assistant 消息。
- 模型失败：有部分内容则以 `status=failed` 落库；run 记 `error_code`。
- 工具调用：不写 `messages`，只写 `tool_calls` 审计；最终 assistant 文本写 `messages`。
- 服务端崩溃：user 消息和 run 可能保留；assistant 消息可能缺失；P5 再做恢复/清理。

## 3. 读取路径（不是“读最近 20 条”）

P1 曾经：

```text
读最近 20 条 completed 消息 → 发给模型
```

P2 起改为：

```text
1. 读最新 summary（如果有）
2. 读 summary.up_to_created_at 之后的所有 completed 消息
3. build_context 按 token 预算从新到旧选择
4. 放不下的旧消息标记 needs_summary
5. 回合结束后异步生成滚动摘要
```

所以：

- `MAX_HISTORY_MESSAGES` 已在 P2 移除；
- 实际窗口由 `CONTEXT_WINDOW_TOKENS`、输出预留、system、摘要、工具定义、记忆共同决定；
- `SUMMARY_KEEP_RECENT_MESSAGES=6` 是摘要时保留的最近原文，不是存储容量。

## 4. 新会话 / 刷新 / 归档

### 点“新对话”

前端：

- 清空消息列表；
- 删除 localStorage 里的 `conversation_id`；
- 不断开后端删除接口。

后端：

- 旧 conversation 仍在，`status=active`；
- 旧 messages / runs / summaries 都保留；
- 新会话使用新的 `conversation_id`；
- 新会话不会加载旧会话的消息或摘要；
- 但会检索用户级 `memories`（P4.1）。

### 刷新页面

前端用 localStorage 的 `conversation_id`：

```text
GET /v1/conversations/{id}/messages
```

恢复当前会话历史。没有会话侧边栏，所以旧会话只能通过 ID 或 API 访问。

### 归档会话

```text
DELETE /v1/conversations/{id}
```

只是把 `status` 改成 `archived`：

- 不删除 messages；
- 之后用这个 ID 聊天返回 404；
- 历史数据仍在数据库。

### 当前没有的能力

- 硬删除会话/消息；
- 自动清理旧消息；
- 会话列表 UI；
- 跨会话“上次聊了什么”的情节记忆（P4.1 只做用户画像）。

## 5. 作用域对比

```text
users
  └── memories（跨会话，用户级）

conversations
  ├── messages（会话级，永久追加）
  ├── summaries（会话级，滚动摘要）
  └── runs
        └── tool_calls（回合级审计）
```

因此：

- “我叫什么”可以跨会话记住（memories）；
- “我们上次聊了什么电影”目前不会跨会话记住（没有情节记忆）；
- 当前会话内的多轮上下文由 messages + summary + token 预算提供。

## 6. 常用排查 SQL

```sql
-- 每个会话有多少条消息
SELECT conversation_id, role, status, count(*)
FROM messages
GROUP BY conversation_id, role, status
ORDER BY conversation_id, role;

-- 最近的消息
SELECT id, conversation_id, role, left(content, 80), status, created_at
FROM messages
ORDER BY created_at DESC
LIMIT 20;

-- 会话摘要
SELECT conversation_id, up_to_message_id, left(content, 80), created_at
FROM summaries
ORDER BY created_at DESC
LIMIT 10;

-- 工具审计
SELECT run_id, step, tool_name, status, latency_ms, created_at
FROM tool_calls
ORDER BY id DESC
LIMIT 20;
```

## 7. 一句话总结

> `messages` 是无限追加的消息表；20 条是 P1 的历史读取窗口，P2 已改为 token 预算 + 摘要；新会话不会加载旧消息/摘要，但会加载用户级长期记忆。
