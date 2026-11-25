#!/usr/bin/env bash
# 启动 MsgRail AI Worker。首次使用需安装 Kafka 客户端可选依赖：
#   .venv/bin/pip install "aiokafka>=0.10"
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" != "--trigger-json" ] &&
   ! .venv/bin/python -c 'import aiokafka' >/dev/null 2>&1; then
  echo "缺少 aiokafka。请先运行：" >&2
  echo '  .venv/bin/pip install "aiokafka>=0.10"' >&2
  exit 1
fi

exec .venv/bin/python -m app.worker.ai_worker "$@"
