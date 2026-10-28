#!/bin/bash
# Spark Push 服务状态检查(Kafka KRaft 模式,无 ZooKeeper)
check_port() { ss -tln 2>/dev/null | grep -q ":$2 " && echo "[OK] $1 ($2)" || echo "[NO] $1 ($2)"; }
echo "=== Spark Push Status ==="
check_port "Kafka (KRaft)" 9092
check_port "MySQL" 3306
check_port "Redis" 6379
check_port "Logic gRPC" 9100
check_port "Logic HTTP" 9101
check_port "Comet WS" 9200
check_port "Comet gRPC" 9205
check_port "Web Demo" 9001
pgrep -x job_server >/dev/null && echo "[OK] Job Server" || echo "[NO] Job Server"
echo ""
ps -eo pid,etime,cmd | grep -E "logic_server|comet_server|job_server|web_demo_server" | grep -v grep
exit 0
