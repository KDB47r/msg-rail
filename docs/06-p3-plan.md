# 06 · P3 技术方案：工具调用与 MCP

> 状态：第一阶段已实现并验证（39 tests passed；PG 迁移 `0003_tool_calls` 已应用）。stdio 时间 Server 和 Open-Meteo 真实天气已验证；Streamable HTTP 是第二阶段。
>
> 目标：让模型能通过统一的工具接口调用本地工具和 MCP Server，解决“只知道训练截止时间”和“不能操作外部数据”的问题。

## 1. 目标与非目标

### 目标

- 手写 Agent 工具循环：模型返回 `tool_call` → 执行 → `tool_result` → 继续生成。
- 统一工具抽象：本地工具 + MCP 工具走同一套注册/权限/审计。
- MCP Client：本地 stdio 和远程 Streamable HTTP 两种传输。
- 示例 MCP Server：时间、天气（Open-Meteo 真实数据）、只读笔记。
- SSE 事件：`tool_call`、`tool_result`，前端能看到工具执行状态。
- 工具审计：每次调用写 `tool_calls` 表。
- 工具 token 预算：工具定义和工具结果也要算进上下文。
- 权限：只读工具自动执行；写/危险工具默认不暴露。
- 测试：工具循环、MCP 调用、超时、错误、截断、权限。

### 非目标

- 完整的“人工确认”交互界面（P3.5）
- 动态工具搜索/相关性选择（工具很多时再做）
- RAG/向量检索（P4）
- 多智能体编排
- 图片/资源类型的工具结果（P3 只处理文本）
- 用户自定义任意 MCP Server（只允许配置白名单）

## 2. 现状

P2 已具备：

- OpenAI 兼容 Provider（qwen-plus），支持文本流式；
- SSE 协议已有 `tool_call` / `tool_result` 事件类型，但尚未产生；
- `Message` 只有 `role` / `content`，没有工具字段；
- 没有 `tool_calls` 表；
- 没有 Agent 循环、MCP Client、工具权限。

## 3. 目标架构

```text
客户端
  │ SSE
  ▼
FastAPI /v1/chat/stream
  │
  ▼
AgentLoop（P3 新增）
  │  1. 组装 messages + tools
  │  2. 调模型，流式输出
  │  3. 有 tool_calls → 执行 → 追加 tool_result → 继续
  ▼
LLM Provider（qwen-plus）
  │ tool_calls
  ▼
ToolRegistry
  ├─ LocalTool（直接函数调用）
  └─ MCPClientManager
        ├─ stdio → 本地 MCP Server
        └─ Streamable HTTP → 本地/远程 MCP Server
  │
  ▼
tool_calls 审计表（PostgreSQL）
```

一次带工具调用的回合：

```text
模型 → tool_call(get_current_time)
  → 执行 MCP 工具
  → 返回 tool_result
  → 模型基于结果生成最终回答
  → message_end
```

## 4. 技术节点与候选方案

### 节点 1：Agent 循环怎么写

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 手写 while 循环（推荐） | 自己控制 tool_call → execute → append → 再调用 | 完全可控，学习价值最高 | 需要处理流式拼接、并行、错误 |
| LangGraph | 用状态图编排 | 生态好、可扩展 | 隐藏循环细节，和“学原理”目标冲突 |
| Agno/LlamaIndex 等 | 框架封装 | 上手快 | 抽象层厚，调试困难 |

**推荐：手写。** 最多 5 步，防止死循环；工具错误作为数据返回给模型，不直接中断。

### 节点 2：工具抽象怎么做

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| MCP-only | 所有工具都走 MCP | 统一协议，接近生产 | 本地简单工具也要起进程，偏重 |
| 本地 + MCP 统一注册（推荐） | `ToolRegistry` 同时管理本地函数和 MCP 工具 | 可以对比两者，渐进式 | 多一层抽象 |
| 框架工具 | 直接用框架的 tool 抽象 | 省事 | 和手写循环目标冲突 |

**推荐：本地 + MCP 统一注册，MCP 作为主路径。** 保留一个本地时间工具作为基线，方便对比“直接调用”和“MCP 调用”的差异。

**关于“后续把本地工具统一到 MCP 协议”**

不需要自己写一套协议。分两个层次：

- **应用层统一（P3 采用）**：本地工具和 MCP 工具都实现同一个 `Tool` 接口（`name` / `description` / `input_schema` / `execute`）。AgentLoop 只调 `tool.execute(args)`，不关心底层是函数还是 MCP。
- **协议层统一（可选，P3.5）**：把本地工具用 FastMCP 包一层，启动成本地 MCP Server，再通过 stdio 连接。官方 SDK 一个装饰器就能做，不需要手写 JSON-RPC。

两种方式可以共存：

- 本地工具直接调用：更快、更好调试、少一个进程；
- MCP 工具：跨应用复用、可远程、协议标准化；
- “统一”指的是给 AgentLoop 同一个接口，不是强迫所有工具都走 MCP 协议。

P3 先做“本地时间工具直接调用 + 时间/天气 MCP Server”，后续再决定是否把本地工具也包成 MCP。

### 节点 3：MCP 传输

| 传输 | 说明 | 优点 | 缺点 | 建议 |
|---|---|---|---|---|
| stdio | 子进程 + stdin/stdout JSON-RPC | 本地简单、无需端口 | 只能本机、进程生命周期要管 | **P3 先做** |
| Streamable HTTP | 单端点、POST/GET、可选 SSE 流 | 远程、可复用、生产常用 | 需要 URL/端口/网络 | **P3 第二阶段** |
| 旧版 HTTP+SSE | 两个端点、旧规范 | 老教程常见 | 已被 Streamable HTTP 取代 | 不实现 |

**推荐：stdio 先跑通，再做本地 Streamable HTTP，最后再考虑公网远程 Server。** 不实现旧版 HTTP+SSE。

### 节点 4：MCP 客户端实现

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 官方 `mcp` Python SDK（推荐） | 官方维护的 ClientSession/传输 | 协议细节不用自己写 | P3 先锁 `mcp>=1.0,<2`；2.x 把 FastMCP 改名为 MCPServer |
| 自己实现 JSON-RPC | 手写 initialize/tools/list/tools/call | 学协议最彻底 | 工作量大、易踩坑 |

**推荐：官方 SDK。** P3 的重点是 Agent 循环和工具设计，不是重写 MCP 协议；协议细节可以通过阅读 SDK 和规范学习。实现前先核对当前 SDK 版本 API。

### 节点 5：工具怎么暴露给模型

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 全量暴露（推荐） | 把所有允许的工具放进 `tools` | 简单、模型选择直观 | 工具多了会占 token、选错 |
| 动态工具搜索 | 先检索相关工具，再注入 | 适合大量工具 | 需要工具索引/检索，P4 再做 |
| 按场景分组 | 不同 persona/场景暴露不同工具 | 降低干扰 | 需要场景配置 |

**推荐：P3 全量暴露 3~5 个工具。** 工具超过 20 个时再讨论动态选择。

### 节点 6：工具权限

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 只读自动 + 写工具不暴露（推荐） | 时间/天气/搜索自动执行，写操作先不做 | 安全、实现简单 | 不能演示“确认后写入” |
| 全部自动 | 所有工具直接执行 | 最省事 | 危险操作风险高 |
| 人工确认 | SSE 推送待确认，客户端 POST 确认 | 安全、体验完整 | 交互/状态机复杂，P3.5 再做 |

**推荐：P3 只做 `readonly_auto`。** 设计好 `permission` 字段，但确认流程留到 P3.5。写/危险工具默认不注册。

### 节点 7：工具结果怎么处理

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 截断（推荐） | 超过 `MAX_TOOL_RESULT_TOKENS` 就截断并注明 | 简单、可控 | 可能丢关键信息 |
| LLM 摘要 | 用小模型压缩结果 | 保留语义 | 多一次调用、增加延迟 |
| 原样返回 | 全部塞进上下文 | 信息完整 | 容易撑爆窗口 |

**推荐：P3 先截断（默认 2000 token），SSE 只给 200 字符预览，审计表存截断后的结果。** 摘要/压缩放 P4/P5。

### 节点 8：并行工具调用

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 串行 | 一个工具执行完再下一个 | 简单、顺序稳定 | 慢 |
| 并行（推荐） | `asyncio.gather`，最多 4 个 | 快 | 要处理顺序/超时/取消 |
| 限制单工具 | 强制一次只调一个 | 最简单 | 浪费模型能力 |

**推荐：并行执行，`TOOL_PARALLEL_LIMIT=4`，结果按调用顺序回填。** 所有 `tool_result` 都返回后再进入下一轮模型调用。

### 节点 9：工具与上下文预算

工具定义和工具结果都会占 token。P2 的 `build_context` 目前只算 system/摘要/历史/当前消息。

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 不算工具（不推荐） | 沿用 P2 预算 | 简单 | 工具一多就超窗口 |
| 预留工具定义预算（推荐） | 从可用预算里扣 `tool_spec_tokens` | 控制风险 | 历史能放的内容变少 |
| 动态预算 | 每轮按工具结果实际 token 重新分配 | 最精确 | 实现复杂 |

**推荐：P3 先预留工具定义 token，并给每个工具结果设上限。** 循环内每次模型调用前估算总 token；超过窗口就停止并返回错误/摘要。

### 节点 10：工具调用要不要持久化/重放

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 审计，不重放（推荐） | `tool_calls` 表记录，后续历史只存最终 assistant 文本 | 简单、上下文干净 | 模型下一轮看不到原始工具结果 |
| 重放工具结果 | 把 tool 消息也存进历史 | 后续可引用细节 | 上下文膨胀、工具结果可能过期 |
| 重放摘要 | 把工具结果压缩后再存 | 平衡 | 需要摘要/结构化 |

**推荐：P3 审计但不重放。** 最终回答已经包含模型需要的信息；后续轮次如果需要实时数据，再调用工具。`tool_calls` 表用于调试、审计、评测。

**“审计不重放”具体是什么意思**

- **审计**：每一次工具调用都写一行 `tool_calls`。同一个 run 里调用天气两次，就有两行；不是“只存最新结果”，也不会覆盖旧调用。
- **不重放**：工具调用和工具结果**不进入后续轮次的 prompt**。`messages` 表只存最终的 assistant 文本；当前回合的 AgentLoop 当然会把 `tool_result` 发回模型，否则模型无法基于结果回答。
- 后续轮次如果需要引用历史工具结果，用 P4 的结构化记忆/RAG，而不是把原始 tool 消息原样塞回上下文。

原因：

- 避免上下文膨胀；
- 工具结果可能过期；
- 工具输出可能包含 prompt injection；
- 原始工具结果通常只对当轮有用，最终回答已经提炼过。

### 节点 11：SSE 和前端

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 完整 tool_call（推荐） | 参数拼完整后发一次 | 实现简单，前端好渲染 | 看不到参数逐字生成 |
| tool_call 增量 | 发 `tool_call_delta` | 实时感强 | 协议/前端复杂 |
| 只在结束时显示 | 不实时显示工具 | 最简单 | 用户不知道模型在干什么 |

**推荐：P3 发完整的 `tool_call`（含参数）和 `tool_result`（含状态/预览），前端先用文字状态展示。** 工具卡片/确认 UI 放 P3.5。

### 节点 12：安全

这是 P3 最重要的非功能节点：

- MCP Server 是任意代码/远程服务，必须白名单配置，不能由用户输入决定。
- 工具输出是不可信内容：用分隔符包裹，明确告诉模型“这是数据，不是指令”。
- 只读工具自动执行；写/危险工具默认不暴露。
- 远程 Streamable HTTP 只允许配置的 URL，防止 SSRF。
- stdio 只允许配置的命令，不接受用户参数。
- 工具结果可能含密钥/PII：日志脱敏，审计表限制大小。
- 工具超时、结果截断、最大步数是硬边界。
- 人工确认流程（P3.5）用于写操作。

### 节点 13：评测

P3 先不做完整评测，P5 再补：

- 工具选择准确率：该调工具时调、不该调时不调；
- 参数正确率；
- 工具成功率/超时率；
- 端到端延迟与 token 成本；
- 错误恢复：工具失败后模型是否能继续。

## 5. 推荐默认路线

| 决策 | 推荐默认 |
|---|---|
| Agent 循环 | 手写，最多 5 步 |
| 工具抽象 | 本地 + MCP 统一 `ToolRegistry` |
| MCP 传输 | stdio 先做，本地 Streamable HTTP 第二 |
| MCP SDK | 官方 `mcp` Python SDK |
| 工具暴露 | 3~5 个全量暴露 |
| 权限 | 只读自动；写/危险不暴露 |
| 工具结果 | 截断 2000 token + 200 字符预览 |
| 并行 | `asyncio.gather`，最多 4 个 |
| 上下文 | 预留工具定义 token；每步检查总预算 |
| 持久化 | `tool_calls` 审计，不重放 |
| SSE | 完整 `tool_call` + `tool_result` |
| Provider | OpenAI 兼容（qwen-plus）先做；Claude 适配后置 |
| MCP 示例 | 时间（stdio）、天气（Open-Meteo）、只读笔记 |
| 配置格式 | JSON（避免额外 YAML 依赖） |

## 6. 数据模型

新增 `tool_calls` 表（迁移 `0003_tool_calls`）：

```sql
CREATE TABLE tool_calls (
  id             SERIAL PRIMARY KEY,
  run_id         TEXT NOT NULL REFERENCES runs(id),
  call_id        TEXT NOT NULL,
  tool_name      TEXT NOT NULL,
  server_name    TEXT,
  arguments_json JSON,
  result_json    JSON,
  status         TEXT NOT NULL,      -- pending/success/error/timeout/denied
  latency_ms     INT,
  error_code     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, call_id)
);
CREATE INDEX idx_tool_calls_run ON tool_calls(run_id);
```

说明：

- `arguments_json` / `result_json` 用通用 `JSON`（SQLite 测试可用，PG 也能存）。
- 结果超过上限时存截断后的内容，避免表膨胀。
- `run_id` 关联到当前回合；一个 run 可以有多次工具调用。
- 不把 tool 消息写入 `messages` 表；后续历史只保留最终 assistant 文本。

## 7. 接口与 SSE

### 7.1 新增事件

`tool_call`：

```json
{
  "run_id": "r_...",
  "step": 1,
  "call_id": "call_...",
  "name": "time__get_current_time",
  "server": "time",
  "arguments": {"timezone": "Asia/Shanghai"}
}
```

`tool_result`：

```json
{
  "run_id": "r_...",
  "step": 1,
  "call_id": "call_...",
  "status": "success",
  "preview": "2026-11-22 10:30:00",
  "latency_ms": 12,
  "error_code": null
}
```

- 工具参数拼完整后再发 `tool_call`；
- `tool_result.preview` 只放前 200 字符；
- 工具失败也发 `tool_result`，`status=error`，模型继续；
- `message_end` 的 `usage` 是多次模型调用的聚合值；
- 循环超限发 `error`，`code=TOOL_LOOP_LIMIT`。

### 7.2 `Message` 扩展

```python
@dataclass
class Message:
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    tool_calls: list[dict] | None = None
    tool_call_id: str | None = None
    name: str | None = None
```

- assistant 消息可以带 `tool_calls`；
- tool 消息带 `tool_call_id` 和工具名；
- context builder 只把 user/assistant 文本放进历史；工具审计单独存。

### 7.3 Provider 扩展

`OpenAICompatProvider` 需要：

- 请求里带 `tools`；
- 流式响应里累积 `delta.tool_calls` 的 `id/name/arguments` 分片；
- 流结束后产出完整 `tool_calls`；
- 把 assistant `tool_calls` 和 tool 消息映射回 OpenAI 格式；
- 工具调用和文本可能同时出现，要分别累积。

`StreamEvent` 需要能携带 `tool_calls` 或单独的 `tool_call` 事件。

## 8. 配置

```dotenv
TOOLS_ENABLED=true
MAX_TOOL_STEPS=5
TOOL_TIMEOUT_SECONDS=10
TOOL_PARALLEL_LIMIT=4
MAX_TOOL_RESULT_TOKENS=2000
TOOL_PERMISSION_MODE=readonly_auto
MCP_CONFIG_PATH=config/mcp_servers.json
```

`config/mcp_servers.json` 示例：

```json
{
  "servers": {
    "time": {
      "transport": "stdio",
      "command": "python",
      "args": ["-m", "app.mcp.servers.time_server"]
    },
    "weather": {
      "transport": "stdio",
      "command": "python",
      "args": ["-m", "app.mcp.servers.weather_server"]
    },
    "weather_http": {
      "transport": "streamable_http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

工具名统一加前缀，例如 `time__get_current_time`，避免不同 Server 重名；OpenAI function name 只允许字母/数字/下划线/短横线，不能直接用点号。

## 9. 代码结构

```text
app/
├── tools/
│   ├── base.py          # ToolSpec / ToolResult / ToolExecutor
│   ├── registry.py      # 本地 + MCP 统一注册、权限、名称前缀
│   └── local.py         # 本地时间工具（基线）
├── mcp/
│   ├── manager.py       # MCP Client Manager（stdio / Streamable HTTP）
│   └── servers/
│       ├── time_server.py
│       ├── weather_server.py
│       └── notes_server.py
├── orchestrator/
│   ├── agent_loop.py    # 工具循环
│   ├── chat_service.py  # 复用流式事件
│   └── context_builder.py
├── llm/
│   ├── base.py          # Message/StreamEvent 扩展
│   └── openai_compat.py # tool_calls 累积与映射
└── storage/
    ├── models.py        # ToolCall
    └── repositories.py  # ToolCallRepository
migrations/versions/0003_tool_calls.py
config/mcp_servers.json
```

## 10. 测试计划

### 单元测试

- 工具名前缀/冲突处理；
- MCP tool schema → OpenAI function schema；
- 参数 JSON 解析失败、工具不存在、权限拒绝；
- 结果截断、预览长度；
- `Message` ↔ OpenAI 格式映射（assistant tool_calls、tool 消息）。

### Provider 测试

- 用假的流式 chunk 模拟 `delta.tool_calls` 分片，断言拼接出完整 tool_call；
- 文本 + tool_call 同时出现；
- 一个响应多个 tool_calls。

### Agent 循环测试

- 单工具调用 → 执行 → 继续 → 最终回答；
- 并行两个工具；
- 工具超时/异常 → tool_result error → 模型继续；
- 超过最大步数 → `TOOL_LOOP_LIMIT`；
- 客户端取消 → 工具任务取消；
- 工具结果超预算 → 截断。

### MCP 测试

- 启动本地 stdio time server，`list_tools` + `call_tool`；
- 本地 Streamable HTTP server（可选，需要端口）；
- MCP Server 不可用 → tool_result error，不影响聊天。

### API 集成测试

- SSE 事件顺序：`message_start → tool_call → tool_result → content_delta → message_end`；
- `tool_calls` 审计行落库；
- P1/P2 测试不回归。

## 11. 验收标准

1. 问“现在几点”，模型调用 `time` 工具并回答正确。
2. 问天气，模型调用 MCP 天气工具（Open-Meteo）并返回真实数据。
3. 工具失败时，模型能说明失败原因并继续对话。
4. 工具调用写入了 `tool_calls` 表。
5. 超过最大步数会被截断并返回明确错误。
6. P1/P2 的 27 个测试保持通过。
7. SSE 前端能看到“调用工具/工具结果”的状态。

## 12. 任务拆分

1. `Message` / `StreamEvent` 工具字段 + OpenAI 流式 tool_call 累积。
2. `ToolRegistry` + 本地时间工具。
3. MCP Client Manager + stdio time server。
4. AgentLoop + SSE tool 事件 + tool_calls 审计。
5. 权限、截断、超时、并行、最大步数。
6. 前端工具状态展示。
7. Streamable HTTP（本地 weather server）。
8. 测试 + 文档 + 交接更新。

## 13. 已确认（2026-11-22）

| # | 决策 | 结论 |
|---|---|---|
| 1 | Agent 循环 | 手写，作为 P3 学习重点 |
| 2 | 工具抽象 | 本地 + MCP 统一 `Tool` 接口；本地工具先直接调用，不手写协议；后续可选 FastMCP 包装 |
| 3 | MCP 传输 | 先 stdio；Streamable HTTP 是当前远程标准，第二阶段做本地 Streamable HTTP |
| 4 | MCP SDK | 官方 `mcp` Python SDK |
| 5 | 工具暴露 | 先 3~5 个全量暴露 |
| 6 | 权限 | 只读自动；写/危险工具不暴露；确认流程 P3.5 |
| 7 | 工具结果 | 先截断（2000 token），后续按需优化 |
| 8 | 并行 | `asyncio.gather`，最多 4 个 |
| 9 | 上下文预算 | 预留工具定义 token；每步检查总预算 |
| 10 | 持久化 | 每次调用都审计，不重放；不是只存最新 |
| 11 | SSE | 完整 `tool_call` + `tool_result` |
| 12 | 安全 | 配置白名单；更细的权限模型后续讨论 |
| 13 | 评测 | P5 再做 |

下一步实现顺序：

1. `Message` / `StreamEvent` 工具字段 + OpenAI 流式 tool_call 累积；
2. `ToolRegistry` + 本地时间工具；
3. MCP Client Manager + stdio 时间 Server；
4. AgentLoop + SSE `tool_call`/`tool_result` + `tool_calls` 审计；
5. 截断/超时/并行/最大步数；
6. 测试 + 文档。
