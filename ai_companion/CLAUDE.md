# AI Companion 项目约定

## 项目目标

学习 Agent 工程能力：上下文工程、提示词工程、工具调用/MCP、SSE 流式输出、评测与观测。不做模型训练/微调。

## 技术栈（默认方案，待确认）

- Python 3.10+（3.11+ 推荐），FastAPI + uvicorn
- OpenAI 兼容接口（DeepSeek/Qwen/GLM），通过适配层隔离 Provider
- PostgreSQL + pgvector；Redis
- pytest + pytest-asyncio；ruff
- 前端：极简 HTML + 原生 JS（fetch + ReadableStream）

## 开发流程

1. 新会话先读 `docs/00-handoff.md`，再读相关设计文档。
2. 新功能/方案变更先更新 `docs/`，与用户确认后再编码。
3. 采用能完整满足需求的最小改动；不夹带重构、不改无关代码。
4. 每个阶段跑测试；汇报实际执行的命令和结果，不虚报。
5. 不自动下载/安装依赖；只给命令让用户执行。
6. 不擅自 `git commit` / `git push`。
7. 代码引用使用 `file:line`。

## 目录

- `app/api/`：HTTP/SSE 接口
- `app/orchestrator/`：对话编排、上下文装配、Agent 循环
- `app/llm/`：Provider 适配
- `app/mcp/`：MCP 客户端与本地示例 server
- `app/memory/`：摘要、记忆抽取、检索
- `app/storage/`：数据库与仓储
- `app/prompts/`：角色卡与提示词版本
- `web/`：最小前端
- `tests/`：单元/集成测试
- `docs/`：方案与契约

## 常用命令

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.asgi:app --reload --port 8000
pytest -q
ruff check .
```

## 禁止事项

- 不做模型微调/训练
- 不把 API Key 写入代码、日志或文档
- 不擅自更换 Provider 或引入重型 Agent 框架
- 不在未确认方案前写业务代码
