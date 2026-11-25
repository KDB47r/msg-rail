# AI Companion

一个用于学习大模型后端开发的陪伴型 AI 聊天服务。目标不是做模型训练，而是把 **上下文工程、提示词工程、工具调用/MCP、SSE 流式输出** 这条链路亲手实现一遍。

> 当前状态：P6.1 + MCP v1 已接入 MsgRail（私人 AI 1:1、A-B 气氛组、结构化 mention、Kafka `ai_trigger`、AI Worker、流式推送、最终落库；新增计算器/随机/骰子/节日/冷知识/RSS/网页抓取，搜索 Key 留空）。P4.2 长期记忆 + 文档 RAG 已验证；AI Companion 自身 80 tests passed。

## 学习目标

- 上下文工程：token 预算、滑动窗口、滚动摘要、长期记忆、RAG
- 提示词工程：角色卡分层、版本化、评测
- 工具调用与 MCP：本地 stdio、远程 Streamable HTTP、权限与审计
- 流式输出：SSE 事件协议、心跳、取消、断线重连
- 工程化：幂等、限流、重试/降级、观测、测试

## 非目标

- 模型训练、微调、推理引擎优化
- 一开始就做多模态、语音、复杂多智能体
- 直接改 MsgRail（C++ IM）；Phase 2 才通过接口集成

## 文档

- [交接文档：从这个项目怎么开始](docs/00-handoff.md)
- [架构设计](docs/01-architecture.md)
- [技术方案：API、SSE、表结构、测试](docs/02-tech-design.md)
- [Phase 2 与 MsgRail 的集成契约](docs/03-integration-contract.md)
- [P1 方案：多轮与持久化](docs/04-p1-plan.md)
- [P2 方案：token 预算与摘要](docs/05-p2-plan.md)
- [P3 方案：AgentLoop 与 MCP](docs/06-p3-plan.md)
- [P4 方案：长期记忆与 RAG](docs/07-p4-plan.md)
- [pgvector 安装指南](docs/08-pgvector-install.md)
- [消息与状态的生命周期](docs/09-message-lifecycle.md)
- [P6 MCP 扩展方案：搜索、抓取、知识、气氛组](docs/12-mcp-expansion-plan.md)

## 快速开始

### 1. 启动 PostgreSQL

**方式 A：apt 安装（推荐，避免 Docker Hub 拉取问题）**

```bash
sudo apt update
sudo apt install -y postgresql
sudo service postgresql start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'ai_companion';"
sudo -u postgres createdb ai_companion
```

如果提示数据库已存在，忽略即可。`pg_isready` 应返回 `accepting connections`。

**方式 B：Docker**

```bash
sudo docker run -d --name ai-pg \
  -e POSTGRES_PASSWORD=ai_companion \
  -e POSTGRES_DB=ai_companion \
  -p 5432:5432 \
  -v ai-pg-data:/var/lib/postgresql/data \
  pgvector/pgvector:pg16
```

如果希望免 sudo：`sudo usermod -aG docker $USER && newgrp docker`。

> P1 不需要 pgvector 扩展；P4 做长期记忆时再安装。Docker Hub 拉取超时时，直接用方式 A。

### 2. 安装依赖并迁移

```bash
cd ~/Linux/AI_Companion
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
alembic upgrade head
uvicorn app.asgi:app --reload --port 8000
```

如果 `python3 -m venv` 提示缺少 ensurepip，先执行：

```bash
sudo apt update && sudo apt install -y python3-venv
```

如果 editable install 的构建依赖拉取失败，可以绕过它直接装：

```bash
pip install fastapi "uvicorn[standard]" pydantic-settings httpx openai python-dotenv \
  "sqlalchemy[asyncio]" asyncpg alembic pytest pytest-asyncio respx ruff aiosqlite \
  -i https://pypi.tuna.tsinghua.edu.cn/simple
```

打开 <http://localhost:8000>。默认 `LLM_PROVIDER=fake`，不需要 API Key。

接真实模型（DeepSeek 示例），编辑 `.env`：

```dotenv
LLM_PROVIDER=openai_compat
LLM_BASE_URL=https://api.deepseek.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=deepseek-chat
```

用 curl 验证 SSE：

```bash
curl -N -X POST http://localhost:8000/v1/chat/stream \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"message":"你好"}'
```

跑测试（使用 SQLite 内存库，不需要 PostgreSQL）：

```bash
PYTHONPATH=. python -m pytest -q
```

P1 新增接口：

- `GET /v1/conversations`：会话列表
- `GET /v1/conversations/{id}/messages`：历史消息
- `POST /v1/conversations`：新建会话
- `DELETE /v1/conversations/{id}`：归档会话

`POST /v1/chat/stream` 现在会自动创建/续用 `conversation_id`，并把历史一起发给模型。

## P6：接入 MsgRail

MsgRail 是聊天历史的权威数据源；AI Companion 不保存 A-B 聊天记录。

两个入口：

- **私人 AI 助手**：在 MsgRail 里和 `ai_assistant`（显示名“小桁”）1:1 聊天；启用 P4 记忆/RAG。
- **A-B 气氛组**：邀请 `ai_vibe`（显示名也叫“小桁”）进入 A-B 单聊；双方同意后，只有当前消息结构化 mention 小桁时才回复；只读取加入后的最近 30 条；禁用用户记忆/RAG。

链路：

```text
Logic --Kafka ai_trigger--> AI Worker
AI Worker --HTTP SSE--> AI Companion /internal/v1/agent/stream
AI Worker --HTTP--> Logic /internal/ai/context|delta|finalize|fail
Logic --Kafka push_to_comet--> Job -> Comet -> 浏览器
```

启动 AI Worker（Kafka 客户端是可选依赖，需要手动安装）：

```bash
.venv/bin/pip install "aiokafka>=0.10"   # 或使用你的 PyPI 镜像
scripts/run_ai_worker.sh
```

MCP 工具配置在 `config/mcp_servers.json`；RSS 白名单在 `config/rss_feeds.json`。
搜索 Key 先留空，`web__search` 会明确返回未配置；填 `BOCHA_API_KEY` 后即可使用。
大陆网络无法直连 Wikipedia，默认关闭；需要时设置 `WIKIPEDIA_ENABLED=true`。

没有 Kafka 时也可以手动跑一条触发做本地调试：

```bash
.venv/bin/python -m app.worker.ai_worker --trigger-json '{
  "job_id":"job_demo","mode":"shared",
  "session_id":"single:1:2","bot_user_id":910000000000002,
  "trigger_message_id":"single:1:2-3","trigger_sender_id":1,
  "trigger_sender_name":"A","trigger_content":"@小桁 说句话",
  "target_user_ids":[1,2],"activated_seq":2
}'
```

> `ai_trigger`、`session_bot`、`ai_job` 和两个 bot 用户的迁移在 MsgRail 的 `sql/migrations/006_ai_companion.sql`。MsgRail 的 `conf/logic.conf` 中的 `ai_internal_token` 必须与这里的 `LOGIC_INTERNAL_TOKEN`/`INTERNAL_TOKEN` 一致。

## P2：上下文预算与滚动摘要

默认配置：

```dotenv
CONTEXT_WINDOW_TOKENS=32768
SUMMARY_ENABLED=true
SUMMARY_TRIGGER_RATIO=0.7
SUMMARY_KEEP_RECENT_MESSAGES=6
SUMMARY_MAX_TOKENS=512
SUMMARY_MODEL=
# 空 = LLM_MODEL；需确认该模型在百炼有额度
SUMMARY_TEMPERATURE=0.2
SUMMARY_SYNC=false
TOKEN_SAFETY_MARGIN=1.15
```

- 上下文按 token 预算装配，不再按消息条数硬截断。
- 历史被裁剪，或未摘要 token 超过“可用历史预算”的 70% 时，触发滚动摘要。
- 摘要默认复用 `LLM_MODEL`（当前 qwen-plus），存 `summaries` 表，下一轮自动注入 system prompt；可用 `SUMMARY_MODEL` 单独覆盖。
- 默认后台异步生成；测试可设 `SUMMARY_SYNC=true`。

想快速看到摘要效果，可以临时把窗口调小：

```dotenv
CONTEXT_WINDOW_TOKENS=1000
LLM_MAX_TOKENS=200
SUMMARY_TRIGGER_RATIO=0.2
SUMMARY_KEEP_RECENT_MESSAGES=2
SUMMARY_SYNC=true
```

重启后聊 3~4 轮，然后查库：

```bash
sudo -u postgres psql -d ai_companion \
  -c "SELECT conversation_id, up_to_message_id, left(content, 80), token_count FROM summaries ORDER BY id DESC LIMIT 3;"
```

验证完记得把窗口参数恢复成默认值。

## P3：工具调用与 MCP

P3 第一阶段包含：

- 手写 AgentLoop：模型 → `tool_call` → 执行 → `tool_result` → 再调模型；
- 统一 `ToolRegistry`：本地工具 + MCP 工具；
- 本地时间工具 + MCP stdio 时间 Server；
- MCP stdio 天气 Server（数据来源：Open-Meteo 真实数据）；
- `tool_calls` 审计表；
- 只读工具自动执行，写/危险工具不暴露；
- 工具超时、结果截断、最大步数、并行上限。

安装 MCP SDK：

```bash
source .venv/bin/activate
python -m pip install "mcp>=1.0,<2" -i https://pypi.tuna.tsinghua.edu.cn/simple
```

重启 uvicorn 后，MCP Server 会在 lifespan 中启动。测试：

> 现在几点？

预期 SSE 中出现 `tool_call`、`tool_result`，最终回答包含当前时间。工具调用会写入：

```bash
sudo -u postgres psql -d ai_companion \
  -c "SELECT run_id, tool_name, server_name, status, latency_ms FROM tool_calls ORDER BY id DESC LIMIT 5;"
```

Streamable HTTP 是第二阶段，尚未实现。

## P4.1：长期记忆

P4.1 已实现：

- `memories` 表 + pgvector 0.8.7，`embedding vector(1024)` + HNSW 索引；
- Qwen `text-embedding-v3` 生成 embedding；
- 每 3 轮用 `LLM_MODEL`（当前 qwen-plus）从对话中抽取 fact/preference/event；可用 `MEMORY_EXTRACTION_MODEL` 覆盖；
- 内容 hash + 相似度去重；
- 每轮对话前按当前用户消息检索 top-5（阈值 0.35），注入 system prompt；
- `GET /v1/memories`、`DELETE /v1/memories/{id}`、`DELETE /v1/memories`。

配置默认值见 `.env.example`，可以先用：

```dotenv
MEMORY_ENABLED=true
EMBEDDING_MODEL=text-embedding-v3
EMBEDDING_DIMENSIONS=1024
MEMORY_TOP_K=5
MEMORY_MIN_SIMILARITY=0.35
MEMORY_MAX_TOKENS=600
MEMORY_EXTRACT_EVERY_TURNS=3
MEMORY_EXTRACTION_MODEL=
# 空 = LLM_MODEL；需确认该模型在百炼有额度
MEMORY_SYNC=false
```

验证：

```bash
# 聊 3 轮后查记忆
curl -H "Authorization: Bearer dev-token" http://localhost:8000/v1/memories

# 直接查库
PGPASSWORD=ai_companion psql -h 127.0.0.1 -U postgres -d ai_companion \
  -c "SELECT id, kind, content, confidence, importance FROM memories ORDER BY id DESC LIMIT 10;"
```

关闭记忆：`MEMORY_ENABLED=false`，聊天会跳过 embedding/检索/抽取。

> 注意：开启后每轮对话前会调用一次 embedding，增加少量延迟和 token 成本；抽取每 3 轮一次，默认复用 `LLM_MODEL`。

## P4.2：文档 RAG

已实现：

- `documents` / `document_chunks` 表 + pgvector HNSW；
- 只支持 UTF-8 的 `.txt` / `.md`，单文件默认上限 2 MB；
- 切块：默认 500 token/块，50 token overlap；
- 上传时用 Qwen `text-embedding-v3` 批量生成 embedding；
- 对话前与长期记忆共用一次 query embedding，检索 top-5（阈值 0.35）；
- 注入格式带来源：`[文档：标题#chunk]`；
- `GET /v1/documents`、`DELETE /v1/documents/{id}`。

上传不需要前端 UI，用 curl 直接传原始文本：

```bash
cat > /tmp/test.md <<'EOF'
# 产品手册

星际产品 X100 整机保修期为 36 个月。
电池保修期为 12 个月。
EOF

curl -X POST http://localhost:8000/v1/documents \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: text/markdown" \
  -H "X-Filename: test.md" \
  --data-binary @/tmp/test.md
```

也可以用 JSON：

```bash
curl -X POST http://localhost:8000/v1/documents \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"title":"test.md","content":"保修期为 36 个月。"}'
```

查看 / 删除：

```bash
curl -H "Authorization: Bearer dev-token" http://localhost:8000/v1/documents
curl -X DELETE -H "Authorization: Bearer dev-token" http://localhost:8000/v1/documents/1
```

上传后问“整机保修多久”，回答应引用文档内容；查库：

```bash
PGPASSWORD=ai_companion psql -h 127.0.0.1 -U postgres -d ai_companion \
  -c "SELECT d.id, d.title, count(c.id) AS chunks FROM documents d LEFT JOIN document_chunks c ON c.document_id=d.id GROUP BY d.id, d.title;"
```

关闭 RAG 检索：`RAG_ENABLED=false`。上传接口仍可用，但聊天不会注入文档片段。

