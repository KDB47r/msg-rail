# 10 · P5 技术方案：可观测性、可靠性与评测

> 状态：草案，待讨论。P5 不再加新的 AI 功能，而是让现有系统“看得见、扛得住、可比较”。
>
> 2026-11-23 更新：qwen-turbo 429 是模型额度问题，已切换 qwen-plus。P5 主线调整为“观测 + 评测”，通用重试/退避/状态化抽取降为稳定性加固（次要）。

## 1. 目标与非目标

### 目标

- 看得见：每次请求的 TTFT、token、工具调用、记忆/RAG 命中、错误都能量化。
- 扛得住：429 / 超时 / embedding 失败 / 后台任务丢失时可降级、可重试。
- 可比较：prompt、模型、上下文策略变更后，有固定评测集判断是否变好。
- 可运营：限流、成本估算、日志脱敏、记忆/文档删除。

### 非目标

- 不做完整监控平台/dashboard
- 不接 Langfuse/OpenTelemetry 也行（可作为 P5.4 可选）
- 不做多租户计费
- 不做模型微调
- 不新增业务场景

## 2. 当前状态与痛点

1. ~~Qwen 429~~ **已解决（2026-11-23）**：原因是 qwen-turbo 在该账号没有免费额度；已切换为 qwen-plus。P5 不再把“修 429”作为主线，但保留通用重试/退避作为稳定性加固。
2. **后台任务丢失**：摘要/记忆抽取是 `asyncio.create_task`，进程重启后不会自动重试。
3. **触发不健壮**：记忆抽取按“每 3 轮取模”，失败后 4/5 轮跳过；只看最近 6 条，早期事实会丢。
4. **没有检索审计**：memory/RAG 命中了什么、相似度多少、注入了多少 token，没有记录。
5. **没有聚合指标**：`runs` 有单轮 token/TTFT，但没有 `/metrics` 或按时间聚合。
6. **没有评测集**：改 prompt、换模型、调 top-k 之后，只能靠感觉。
7. **没有限流/成本控制**：额度容易被聊天 + embedding + 抽取一起打满。
8. **日志是非结构化文本**，排查工具失败/检索为空很费劲。

## 3. P5 分块

### P5.1 观测（优先）

1. **检索审计**
   - 新增 `retrieval_events` 表：run_id、query hash、memory 命中 ID/相似度、RAG 文档/ chunk/相似度、注入 token、耗时。
   - 只存 ID/hash/相似度，避免把敏感原文重复落库。
2. **结构化日志 + 指标**
   - JSON 日志：request_id、conversation_id、run_id、model、prompt_version、tokens、ttft、latency、error。
   - `/metrics`：请求数、错误数（含 429）、token、TTFT、工具调用、memory/RAG 命中。
   - 先做 Prometheus 文本格式或 JSON；不接 dashboard。

### P5.2 评测

- 固定评测集：`evals/cases/*.json`；
- 分类：
  - 画像/人设一致性
  - 多轮上下文
  - 滚动摘要
  - 工具选择与参数
  - 长期记忆召回
  - 文档 RAG 命中/引用
  - 安全/拒绝
- Runner：`python -m app.evals.run --suite core`；
- 评分：
  - 规则断言（包含、工具名、chunk 引用）；
  - 可选 LLM-as-judge（qwen-plus，成本/429 注意）；
- 报告：通过率、TTFT、token、预估成本；
- 用 `PROMPT_VERSION` 对比不同 prompt；
- 默认用 FakeProvider 跑，保证可重复、不烧额度；真实模型评测按需开启。

### P5.3 稳定性、限流、成本与保留（次要）

429 的模型额度问题已经通过 qwen-plus 解决，但通用容错仍值得做，避免未来换模型/换 Provider 时再踩：

- **通用 429 退避**：`Retry-After` / 指数退避 + jitter，最大重试次数；覆盖聊天、摘要、记忆抽取、embedding；
- **状态化抽取**：记录 `last_extracted_message_id`，失败不推进状态，下一轮继续；
- **后台任务可恢复**：进程重启后能从 DB 状态继续，而不是只依赖 asyncio task；
- 用户级 RPM/TPM 限流（先进程内，P5.3 可换 Redis）；
- 每日 token 配额；
- 成本估算：配置每 1M token 单价，按 run 记录；
- 消息/工具审计/检索事件保留策略；
- 会话/消息硬删除、导出；
- 日志脱敏。

### P5.4 可选

- Langfuse / OpenTelemetry trace；
- 简易 dashboard；
- 多用户/多租户。

## 4. 技术节点与候选方案

### 节点 A：可观测性后端

| 方案 | 优点 | 缺点 |
|---|---|---|
| 结构化日志 + PostgreSQL + `/metrics` | 无新服务、可查历史 | 没有现成 trace UI |
| Langfuse | LLM 专用，prompt/trace 友好 | 需要部署/账号 |
| OpenTelemetry + Jaeger/Tempo | 标准、可扩展 | 组件多、学习成本高 |

**推荐先做第一种**，P5.4 再考虑 Langfuse/OTel。

### 节点 B：指标格式

| 方案 | 优点 | 缺点 |
|---|---|---|
| Prometheus 文本（`prometheus-client`） | 标准、易接 Grafana | 多一个依赖 |
| JSON `/metrics` | 简单 | 需要自己写聚合 |

**推荐 `prometheus-client`**；如果不想加依赖，JSON 也可以。

### 节点 C：429 / 后台任务

| 方案 | 优点 | 缺点 |
|---|---|---|
| 进程内重试 | 简单 | 重启丢失 |
| DB 状态 + 下一轮重试 | 可靠、可恢复 | 多一张状态表 |
| Redis/Kafka 队列 | 生产级 | 组件多 |

**推荐 DB 状态 + 下一轮重试**：和现有架构一致，不引入新组件。

> 注：当前 429 是模型额度问题，已切换 qwen-plus；本节作为通用容错设计保留，不再作为 P5 主线。

### 节点 D：记忆抽取触发

| 方案 | 优点 | 缺点 |
|---|---|---|
| 每轮抽取 | 及时 | 429 风险高 |
| 每 N 轮取模 | 成本低 | 失败会跳过，早期事实会丢 |
| 状态化：距上次成功 >= N 轮就抽 | 健壮、可重试 | 多一张状态表 |

**推荐状态化**。

### 节点 E：评测方式

| 方案 | 优点 | 缺点 |
|---|---|---|
| 规则断言 | 快、稳定、便宜 | 只能测确定行为 |
| LLM-as-judge | 可测主观质量 | 成本/429/偏差 |
| 人工评测 | 最准 | 慢 |

**推荐规则断言为主，LLM-as-judge 可选**；FakeProvider 跑核心回归，真实模型跑抽样。

### 节点 F：限流

| 方案 | 优点 | 缺点 |
|---|---|---|
| 进程内令牌桶 | 简单 | 多进程不共享 |
| Redis | 共享、生产常用 | 引入 Redis |

**推荐先进程内**，多进程再 Redis。

## 5. 数据模型（迁移 0006）

```sql
-- 记忆/摘要抽取状态
CREATE TABLE extraction_states (
  id BIGSERIAL PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  kind TEXT NOT NULL,              -- memory / summary
  last_message_id TEXT,
  last_attempt_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  last_error_code TEXT,
  attempt_count INT NOT NULL DEFAULT 0,
  UNIQUE (conversation_id, kind)
);

-- 检索审计
CREATE TABLE retrieval_events (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  query_hash TEXT NOT NULL,
  memory_hits INT NOT NULL DEFAULT 0,
  rag_hits INT NOT NULL DEFAULT 0,
  top_similarity REAL,
  injected_tokens INT,
  latency_ms INT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

可选：给 `runs` 加 `model_calls`、`tool_call_count`、`memory_hits`、`rag_hits`、`estimated_cost`。

## 6. 配置

```dotenv
OBSERVABILITY_ENABLED=true
LOG_JSON=true
METRICS_ENABLED=true
RETRIEVAL_AUDIT_ENABLED=true

RETRY_MAX_ATTEMPTS=3
RETRY_BASE_SECONDS=1.0
RETRY_MAX_SECONDS=60.0

RATE_LIMIT_ENABLED=false
RATE_LIMIT_RPM=20
RATE_LIMIT_DAILY_TOKENS=100000

EVAL_PROVIDER=fake          # fake | real
EVAL_OUTPUT_DIR=evals/reports
```

## 7. 代码结构（建议）

```text
app/observability/
  ├── logging.py        # JSON 日志
  ├── metrics.py        # Prometheus 指标
  └── retrieval.py      # retrieval_events 写入

app/memory/
  └── extraction_state.py

app/evals/
  ├── runner.py
  ├── cases/
  └── report.py

app/api/metrics.py
migrations/versions/0006_observability.py
```

## 8. 测试计划

- 单元：重试退避、状态推进、retrieval_events 写入、指标计数；
- 集成：429 后下一轮重试成功；检索审计行；/metrics 输出；
- 评测：FakeProvider 跑核心 suite，断言通过率；
- 回归：P1~P4 的 57 个测试保持通过。

**推荐顺序：P5.1 观测 → P5.2 评测 → P5.3 稳定性/限流/成本 → P5.4 可选。**

## 9. 验收标准

1. 通用 429/超时不会永久丢抽取；本账号的模型额度问题已通过 qwen-plus 解决。
2. `/metrics` 能看到请求数、错误数、TTFT、token、工具调用、memory/RAG 命中。
3. `retrieval_events` 能查到每次检索命中了什么、相似度、注入 token。
4. `python -m app.evals.run --suite core` 能输出通过率/延迟/成本。
5. prompt 版本变更前后可对比。
6. P1~P4 测试不回归。

## 10. 待你拍板

| # | 决策 | 推荐 |
|---|---|---|
| 1 | P5 先做观测还是评测？ | 先观测，再评测（429 已解决，不再作为主线） |
| 2 | 指标用 `prometheus-client` 还是 JSON？ | Prometheus |
| 3 | 检索审计落 `retrieval_events` 表，还是只写日志？ | 表 + 日志 |
| 4 | 评测默认 FakeProvider，真实模型按需？ | 是 |
| 5 | 限流先进程内，后续再 Redis？ | 是 |
| 6 | Langfuse/OTel 放 P5.4 可选？ | 是 |
| 7 | 消息/工具保留与硬删除现在做还是 P6？ | P5.3/P6 |
