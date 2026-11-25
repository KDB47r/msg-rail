# 07 · P4 技术方案：长期记忆与 RAG

> 状态：P4.1 与 P4.2 均已实现并验证。
>
> 验证记录（2026-11-22）：57 tests passed；迁移 `0005_documents`；真实文档入库/检索/删除验证通过，检索片段带 `[文档：标题#chunk]` 来源；P4.1 语义检索相似度 0.6423。
>
> 目标：让 AI 在“跨会话”和“知识库”两个维度上拥有长期记忆，而不是只靠当前对话摘要。

## 1. 背景与目标

P2 的滚动摘要解决的是“当前会话太长”：

```text
当前会话：最近原文 + 摘要
```

P4 要解决的是另外两个问题：

1. **跨会话记忆**：用户上周说“我叫小明、喜欢猫”，这周新开会话，模型也能想起来。
2. **知识库 RAG**：上传文档后，模型能按问题检索相关片段并回答。

目标：

- 从对话中抽取稳定的用户事实/偏好/事件，存为结构化记忆；
- 用 embedding + 向量检索，在每次对话时注入相关记忆；
- 支持文档上传、切块、embedding、检索（P4.2）；
- 记忆/文档按用户隔离，可删除；
- 记忆注入受 token 预算控制，不挤爆上下文；
- 记忆抽取/检索失败不影响聊天主链路。

非目标：

- 模型微调
- 多租户 JWT（P5）
- 复杂知识图谱
- 多模态文档（图片/扫描 PDF）先不做
- 完整评测体系（P5）

## 2. 三种“记忆”不要混

| 类型 | 作用域 | 内容 | 当前 |
|---|---|---|---|
| 滚动摘要 | 单个会话 | 对话大意、未完成事项 | P2 已有 |
| 结构化长期记忆 | 用户跨会话 | 事实、偏好、关系、事件 | **P4.1** |
| 文档 RAG | 用户/知识库 | 文档切块、按问题检索 | **P4.2** |
| 工具/MCP | 实时外部数据 | 时间、天气、搜索 | P3 已有 |

P4 的 prompt 最终可能长这样：

```text
system（角色卡 + 规则）
+ 会话摘要（P2）
+ 长期记忆（P4.1，按相关性检索）
+ 文档片段（P4.2，按相关性检索）
+ 最近对话原文
+ 当前用户消息
```

## 3. 目标架构

```text
用户消息
  │
  ▼
MemoryRetriever
  ├─ embedding(user message)
  ├─ pgvector: top-k memories（按 user_id 过滤）
  └─ pgvector: top-k document chunks（P4.2）
  │
  ▼
ContextBuilder（system + summary + memories + docs + recent + current）
  │
  ▼
LLM → 回答
  │
  ▼（异步）
MemoryExtractor
  ├─ LLM_MODEL（默认 qwen-plus）抽取 facts/preferences/events
  ├─ 去重/合并
  └─ embedding → memories 表
```

## 4. P4 范围拆分

### P4.1 长期记忆（先做）

- 从对话中抽取结构化记忆；
- 存 `memories` 表，带 embedding；
- 每次对话前检索 top-k 相关记忆；
- 注入 system prompt；
- 提供记忆列表/删除接口；
- 评测：早期事实能否在后续会话答对。

### P4.2 文档 RAG（后做）

- 上传纯文本/Markdown；
- 切块、embedding、存 `document_chunks`；
- 按问题检索 top-k；
- 注入并带来源；
- PDF/网页抓取后置。

**建议先 P4.1，再 P4.2。** 长期记忆是“用户理解”的基础，文档 RAG 是“知识库”能力；两者的检索基础设施可以复用。

## 5. 技术节点与候选方案

### 节点 A：长期记忆的架构

| 方案 | 做法 | 优点 | 缺点 |
|---|---|---|---|
| 只靠摘要 | P2 摘要跨会话复用 | 简单 | 摘要不跨会话，且不按问题检索 |
| 结构化记忆（推荐） | 抽 facts/preferences 存表，按相关性检索 | 精确、可解释、可删除 | 需要抽取/去重/冲突处理 |
| 原始消息 RAG | 所有消息 embedding，按问题检索原文 | 保留原话 | 噪声大、隐私/成本高 |
| 混合（推荐） | 结构化记忆 + 文档 RAG + 摘要 | 覆盖全面 | 组件多，需要预算/评测 |

**推荐：P4.1 结构化记忆 + P2 摘要；P4.2 文档 RAG。** 不建议把所有消息都 embedding 后检索，噪声和成本都高。

### 节点 B：Embedding 模型

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| Qwen embedding API（推荐先做） | DashScope `text-embedding-v3/v4` | 与聊天同 Provider、中文好、接入快 | 按调用收费、数据出境/隐私 |
| 本地 BGE-M3 / bge-small | fastembed/sentence-transformers | 无 API 费、隐私好 | 依赖/模型体积、CPU 推理慢 |
| OpenAI embedding | text-embedding-3-small | 生态成熟 | 网络/成本、和 Qwen 不一致 |

**推荐：P4.1 先用 Qwen embedding API（1024 维），接口抽象成 `EmbeddingProvider`，以后可切本地模型。**

需要实现前确认：

- DashScope OpenAI 兼容模式是否支持 `embeddings`；
- 当前模型名（`text-embedding-v3` / `v4`）和维度；
- 批量输入上限、计费方式。

如果兼容模式不支持，备选：

- 用 DashScope 原生 SDK；
- 用本地 fastembed/BGE。

### 节点 C：向量存储

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| pgvector（推荐） | PostgreSQL 扩展 | 和现有库同库、事务一致、少组件 | 需要安装扩展；PG 14 没有现成 apt 包 |
| Qdrant/Chroma | 独立向量库 | 功能强、易水平扩展 | 多一个服务/依赖 |
| 内存向量检索 | 启动时加载全部向量 | 简单 | 数据量大不可行 |

**推荐：pgvector。** 但当前环境有一个前置问题：

```text
PostgreSQL 14.24
pgvector 未安装
apt-cache search pgvector 无结果
```

可选路径：

1. **源码编译 pgvector（推荐先试）**
   ```bash
   sudo apt install -y postgresql-server-dev-14 build-essential git
   git clone --branch v0.8.1 https://github.com/pgvector/pgvector.git
   cd pgvector && make && sudo make install
   sudo -u postgres psql -d ai_companion -c "CREATE EXTENSION vector;"
   ```
   - 需要 GitHub/网络和编译工具；
   - 版本号以 pgvector 当前 release 为准。

2. **PGDG 安装 PostgreSQL 16 + `postgresql-16-pgvector`**
   - 需要加 PGDG 源、升级/并行安装 PG 16；
   - 更接近生产，但系统改动大。

3. **独立向量库 Qdrant/Chroma**
   - 不依赖 pgvector；
   - 多一个服务，但可以避开编译/网络问题。

4. **先做无向量版 P4.1**
   - 结构化记忆 + 关键词/时间/重要性检索；
   - 先学“抽取/去重/注入”，向量检索延后。
   - 不推荐作为最终形态，但可以作为降级路径。

### 节点 D：记忆抽取

**抽取什么**

- `fact`：名字、职业、城市、经历、家庭/宠物等稳定事实；
- `preference`：喜欢/不喜欢、习惯、沟通偏好；
- `event`：重要事件、计划、截止日期（带时间）；
- 不抽取：天气、问候、一次性指令、敏感密钥。

**何时抽取**

| 方案 | 优点 | 缺点 |
|---|---|---|
| 每轮抽取 | 及时 | 调用多、成本高 |
| 每 N 轮（推荐 3~5 轮） | 成本可控 | 略有延迟 |
| 会话结束/摘要时 | 成本最低 | 实时性差 |
| 用户显式“记住” | 准确 | 需要用户操作 |

**推荐：每 3 轮或摘要触发时异步抽取，默认复用 `LLM_MODEL`（qwen-plus）。** 原方案用 qwen-turbo，但该账号在百炼没有 qwen-turbo 免费额度，导致 429；可用 `MEMORY_EXTRACTION_MODEL` 覆盖。

**抽取输出**

```json
[
  {
    "kind": "preference",
    "content": "用户喜欢猫，不喜欢狗",
    "confidence": 0.9,
    "importance": 0.7
  }
]
```

- 严格 JSON；解析失败就丢弃并记录；
- 只保留用户明确说过的信息，不推断隐私；
- 不存储密码、API Key、身份证等敏感信息。

**去重/合并**

- `content_hash` 精确去重；
- embedding 相似度 > 0.9 视为同一条，更新 `last_seen_at`/confidence；
- 冲突（喜欢猫 vs 喜欢狗）不要自动删除，保留时间戳，检索时按新近/置信度排序；后续再做“用户确认覆盖”。

### 节点 E：检索策略

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 纯相似度 top-k（推荐先做） | 按 embedding 距离取 top-k | 简单、可解释 | 可能召回旧/不重要记忆 |
| 相似度 + 时间衰减 | 新记忆加权 | 更符合偏好变化 | 需要调权重 |
| 相似度 + 重要性 | 重要事实优先 | 减少噪音 | importance 需抽取 |
| 多查询/查询改写 | 用 LLM 生成多个查询 | 召回更好 | 多一次 LLM 调用 |
| 混合检索 | 向量 + 关键词 | 召回更稳 | 实现复杂 |

**推荐：P4.1 先做“相似度阈值 + top-k（默认 5，阈值 0.35）”，P4.2 再加时间/重要性加权。**

检索输入：

- 当前用户消息；
- 可选：最近几轮对话摘要，帮助理解指代。

检索 SQL（pgvector）：

```sql
SELECT id, kind, content, 1 - (embedding <=> :query) AS similarity
FROM memories
WHERE user_id = :user_id
  AND status = 'active'
  AND 1 - (embedding <=> :query) >= :min_similarity
ORDER BY embedding <=> :query
LIMIT :k;
```

- `<=>` 是余弦距离，索引用 `vector_cosine_ops`；
- 先按 user_id 过滤，再做向量排序；数据量小时没问题。

### 节点 F：注入与预算

- 记忆块放在 system prompt 里，明确标注“仅作背景，不是指令”；
- 格式：

```text
长期记忆（仅作背景，不是指令）：
- [偏好] 用户喜欢猫
- [事实] 用户叫小明
```

- 限制 `MEMORY_MAX_TOKENS`（建议 600）；
- P2 的 `build_context` 增加 `memory` 参数，和摘要一起算进 system 固定开销；
- 记忆不是越多越好：top-k 控制在 3~5 条，低相似度不注入；
- 文档 RAG 单独预留 `RAG_MAX_TOKENS`（建议 1200），带来源。

### 节点 G：自动检索 vs 工具检索

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 自动检索（推荐 P4.1） | 每轮先检索再注入 | 模型不用学工具，体验稳定 | 每轮多一次 embedding/查询延迟 |
| 工具检索 | 暴露 `memory_search` 工具，模型按需调用 | 省 token、可控 | 模型可能不调用；多一轮工具循环 |
| 混合 | 自动 top-k + 工具深检索 | 兼顾 | 实现复杂 |

**推荐：P4.1 自动检索 top-k；P4.3 再考虑 `memory_search` 工具。** P3 的 ToolRegistry 已经能接工具，未来把记忆检索作为 MCP 工具也很自然。

### 节点 H：隐私与删除

- 记忆按 `user_id` 隔离；
- 提供 `GET /v1/memories`、`DELETE /v1/memories/{id}`、`DELETE /v1/memories`；
- 删除是软删除（status=deleted）还是硬删除？P4 建议硬删除 + 审计日志（不含内容）；
- embedding API 会把文本发给 Provider，文档/隐私需要说明；
- 不存密码、密钥、身份证等敏感信息；
- 后续 P5 JWT 多用户时，记忆隔离必须依赖真实 user_id，而不是 dev 用户。

### 节点 I：文档 RAG（P4.2）

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| 纯文本/Markdown（推荐先做） | 上传 `.txt`/`.md` | 解析简单 | 不支持 PDF |
| PDF（pypdf） | 解析文本层 | 覆盖常见文档 | 扫描件/表格效果差 |
| 网页抓取 | URL → 正文 | 方便 | 反爬/正文提取复杂 |

切块：

- 建议 500 token/块，50 token overlap；
- 优先按段落/标题切，超长再硬切；
- 每块带 `document_id`、`chunk_index`、`title`、`source`；
- 检索结果注入时带来源：

```text
[文档：产品手册#3] ...
```

文档删除时同步删除 chunks 和 embedding。

### 节点 J：评测

P4 先手动 + 小评测集，P5 系统化：

- **记忆召回**：第 1 轮说“我叫小明”，第 20 轮问“我叫什么”，能否答对；
- **记忆精度**：注入的记忆是否相关，是否干扰回答；
- **冲突/时效**：用户改口后，旧记忆是否还占主导；
- **删除**：删除后不再被检索；
- **RAG 召回/引用**：答案是否来自正确文档片段；
- **成本/延迟**：embedding 调用、向量查询、注入 token。
- A/B：摘要 vs 摘要+记忆 vs 摘要+记忆+文档。

## 6. 推荐默认路线

| 决策 | 推荐 |
|---|---|
| P4 范围 | 先 P4.1 结构化记忆，再 P4.2 文档 RAG |
| Embedding | Qwen `text-embedding-v3`（1024 维，先验证兼容模式） |
| 向量库 | pgvector；先试源码安装 |
| 记忆抽取 | 每 3 轮或摘要触发，复用 `LLM_MODEL`，JSON 输出 |
| 检索 | 相似度阈值 0.35 + top-5 |
| 注入 | system prompt，memory 块 ≤600 token |
| 检索方式 | 自动检索；`memory_search` 工具后续 |
| 隐私 | 按 user_id 隔离；提供列表/删除接口；不存敏感信息 |
| 文档 | 纯文本/Markdown 先做；PDF 后置 |
| 评测 | P4 手动 + 小样本；P5 系统化 |

## 7. 数据模型（迁移 0004）

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE memories (
  id                BIGSERIAL PRIMARY KEY,
  user_id           BIGINT NOT NULL REFERENCES users(id),
  kind              TEXT NOT NULL,              -- fact/preference/event
  content           TEXT NOT NULL,
  content_hash      TEXT NOT NULL,
  embedding         vector(1024),
  source_message_id TEXT,
  confidence        REAL NOT NULL DEFAULT 0.8,
  importance        REAL NOT NULL DEFAULT 0.5,
  status            TEXT NOT NULL DEFAULT 'active', -- active/superseded/deleted
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at      TIMESTAMPTZ,
  UNIQUE (user_id, content_hash)
);
CREATE INDEX idx_memories_user_status ON memories(user_id, status);
CREATE INDEX idx_memories_embedding
  ON memories USING hnsw (embedding vector_cosine_ops);

CREATE TABLE documents (
  id          BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL REFERENCES users(id),
  title       TEXT NOT NULL,
  source      TEXT,
  metadata    JSON,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE document_chunks (
  id           BIGSERIAL PRIMARY KEY,
  document_id  BIGINT NOT NULL REFERENCES documents(id),
  chunk_index  INT NOT NULL,
  content      TEXT NOT NULL,
  token_count  INT,
  embedding    vector(1024),
  metadata     JSON,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_document_chunks_doc ON document_chunks(document_id);
CREATE INDEX idx_document_chunks_embedding
  ON document_chunks USING hnsw (embedding vector_cosine_ops);
```

说明：

- 维度 1024 对应 Qwen embedding；如果模型维度不同，必须改迁移和配置；
- HNSW 索引适合小中规模；IVFFlat 需要训练，P4 不用；
- `memories` 唯一键 `(user_id, content_hash)` 做精确去重；
- 软删除/硬删除策略在实现时定。

## 8. 配置

```dotenv
MEMORY_ENABLED=true
EMBEDDING_PROVIDER=openai_compat      # 先复用 LLM Provider
EMBEDDING_MODEL=text-embedding-v3
EMBEDDING_DIMENSIONS=1024
MEMORY_TOP_K=5
MEMORY_MIN_SIMILARITY=0.35
MEMORY_MAX_TOKENS=600
MEMORY_EXTRACT_EVERY_TURNS=3
MEMORY_EXTRACTION_MODEL=   # 空 = LLM_MODEL；需确认模型有额度

RAG_ENABLED=false
RAG_TOP_K=5
RAG_CHUNK_TOKENS=500
RAG_CHUNK_OVERLAP=50
RAG_MAX_TOKENS=1200
```

## 9. API（建议）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/memories` | 当前用户记忆列表 |
| DELETE | `/v1/memories/{id}` | 删除单条记忆 |
| DELETE | `/v1/memories` | 删除全部记忆 |
| POST | `/v1/documents` | 上传文档（P4.2） |
| GET | `/v1/documents` | 文档列表（P4.2） |
| DELETE | `/v1/documents/{id}` | 删除文档和 chunks（P4.2） |

## 10. 代码结构（建议）

```text
app/
├── memory/
│   ├── embedding.py      # EmbeddingProvider（Qwen/本地）
│   ├── extractor.py      # 对话 → 结构化记忆
│   ├── retriever.py      # 向量检索 + 组装记忆块
│   └── service.py        # 写路径编排
├── rag/
│   ├── chunker.py        # 文档切块
│   ├── ingest.py         # 上传/解析/embedding
│   └── retriever.py      # 文档检索
├── storage/
│   ├── models.py         # Memory/Document/DocumentChunk
│   └── repositories.py   # 查询
├── orchestrator/
│   ├── context_builder.py # 增加 memory/rag 参数
│   └── turn_service.py    # 检索注入
└── api/
    └── memories.py       # 列表/删除
migrations/versions/0004_long_term_memory.py
```

## 11. 测试计划

### 单元测试

- embedding 维度/空文本/批量；
- 记忆抽取 JSON 解析、非法输出丢弃；
- 去重（hash + 相似度）；
- 检索阈值/top-k/用户隔离；
- 注入 token 预算；
- 删除后不再检索。

### 集成测试

- 使用 SQLite + 假 embedding（固定向量）测试检索逻辑；pgvector 测试单独用 PostgreSQL；
- 第 1 轮“我叫小明”，第 N 轮“我叫什么”，断言记忆被检索并注入；
- 删除记忆后不再注入；
- embedding API 失败时聊天继续。

### 手动验收

1. 说“我叫小明，喜欢猫”；
2. 开新会话，问“我叫什么/我喜欢什么”；
3. 检查 `memories` 表和注入日志；
4. 删除记忆，再问，确认不再出现；
5. P4.2 上传一篇 Markdown，问文档里的问题，检查引用。

## 12. 验收标准

- 跨会话能记住稳定事实/偏好；
- 记忆按相关性检索，不把无关记忆塞进上下文；
- 删除记忆后不再被检索；
- 记忆注入不超 token 预算；
- embedding/检索失败不影响聊天；
- P1/P2/P3 测试不回归。

## 13. 前置条件与风险

1. **pgvector 已就绪**：2026-11-22 已源码安装 pgvector 0.8.7，并在 `ai_companion` 库执行 `CREATE EXTENSION vector`，`SELECT '[1,2,3]'::vector` 验证通过。安装记录见 `docs/08-pgvector-install.md`。
2. **embedding API 兼容性**：DashScope OpenAI 兼容模式是否支持 embeddings、模型名/维度，需要先验证。
3. **embedding 成本/隐私**：每次检索和抽取都会调用 API；文档内容会发给 Provider。
4. **记忆质量**：抽取不准会污染长期记忆；需要评测和删除能力。
5. **冲突/时效**：用户偏好会变化，不能简单覆盖；需要时间/置信度策略。
6. **数据迁移**：维度变更需要重建向量列/索引。

## 14. 已确认（2026-11-22）

| # | 决策 | 结论 |
|---|---|---|
| 1 | 架构 | **混合**：结构化记忆 + 文档 RAG；实现顺序仍先 P4.1，再 P4.2，最后在同一 prompt 合并注入 |
| 2 | Embedding | Qwen `text-embedding-v3`，1024 维；接口抽象成 `EmbeddingProvider` |
| 3 | 向量库 | pgvector；**源码编译 v0.8.7**（详见 `docs/08-pgvector-install.md`） |
| 4 | 记忆抽取 | 每 3 轮或摘要触发，复用 `LLM_MODEL`，JSON 输出 |
| 5 | 检索 | 相似度阈值 0.35 + top-5；暂不做时间/重要性加权 |
| 6 | 注入 | system prompt；memory ≤600 token，RAG ≤1200 token |
| 7 | 检索方式 | 自动检索；`memory_search` 工具后置 |
| 8 | 隐私 | 按 `user_id` 隔离；提供列表/删除接口；不存敏感信息 |
| 9 | 文档 | 先只支持 `.txt` / `.md`；PDF 后置 |
| 10 | 评测 | P5 再做；P4 先手动 + 小样本 |

### 实现顺序

```text
共享基础设施：EmbeddingProvider + pgvector + 迁移 0004
  → P4.1 结构化记忆：抽取/去重/检索/注入
  → P4.2 文档 RAG：上传/切块/embedding/检索
  → 混合：同一轮同时检索 memories + document_chunks
```

pgvector 安装细节见 `docs/08-pgvector-install.md`。

## 15. P4.1 实现记录

已新增：

```text
app/memory/embedding.py   # Qwen text-embedding-v3
app/memory/extractor.py   # 对话 → JSON 记忆
app/memory/store.py       # pgvector 原生 SQL 存储/检索
app/memory/service.py     # 抽取/去重/检索/注入
app/api/memories.py       # 列表/删除
migrations/versions/0004_memories.py
```

验证：

- `pytest -q`：48 passed
- `alembic current`：`0004_memories (head)`
- `memories.embedding` 类型：`vector`
- HNSW 索引：`idx_memories_embedding`
- 真实 embedding 检索：`我喜欢什么` → `用户喜欢猫`，相似度 `0.6423`
- 测试数据清理后 `memories` 行数：0

## 16. P4.2 实现记录

已新增：

```text
app/rag/chunker.py      # 500 token 切块 + 50 overlap
app/rag/store.py        # documents/document_chunks + pgvector 检索
app/rag/service.py      # ingest / retrieve / list / delete
app/api/documents.py    # 上传（raw text 或 JSON）、列表、删除
migrations/versions/0005_documents.py
```

验证：

- `pytest -q`：57 passed
- `alembic current`：`0005_documents (head)`
- `documents` / `document_chunks` 表与 HNSW 索引已创建
- 真实文档“整机保修 36 个月”入库 → 检索“整机保修多久”命中并带 `[文档：测试产品手册.md#1]`
- 删除文档后 `documents` / `document_chunks` 行数为 0

上传方式：原始文本 body + `X-Filename`，或 JSON `{title, content}`；不依赖 multipart/前端 UI。
