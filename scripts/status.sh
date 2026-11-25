#!/bin/bash
# Spark Push 服务状态检查(Kafka KRaft 模式,无 ZooKeeper)
check_port() { ss -tln 2>/dev/null | grep -q ":$2 " && echo "[OK] $1 ($2)" || echo "[NO] $1 ($2)"; }
echo "=== Spark Push Status ==="
check_port "Kafka (KRaft)" 9092
check_port "MySQL" 3306
check_port "PostgreSQL (AI Companion)" 5432
check_port "Redis" 6379
check_port "Logic gRPC" 9100
check_port "Logic HTTP" 9101
check_port "Comet WS" 9200
check_port "Comet gRPC" 9205
check_port "MinIO S3" 9002
check_port "MinIO Console" 9003
check_port "AI Companion" 8000
check_port "Web Demo" 9001
pgrep -x job_server >/dev/null && echo "[OK] Job Server" || echo "[NO] Job Server"
pgrep -x media_worker >/dev/null && echo "[OK] media_worker" || echo "[NO] media_worker"
pgrep -f "app.worker.ai_worker" >/dev/null && echo "[OK] AI Worker" || echo "[NO] AI Worker"
echo ""
ps -eo pid,etime,cmd | grep -E "logic_server|comet_server|job_server|media_worker|web_demo_server|minio|uvicorn app.asgi|app.worker.ai_worker" | grep -v grep
exit 0
