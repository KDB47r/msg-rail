#!/bin/bash
# Spark Push 一键停止与数据清理脚本(Kafka KRaft 模式,无 ZooKeeper)
#
# 用法:
#   ./scripts/stop_all.sh          # 只停应用服务(logic/comet/job/web_demo)
#   ./scripts/stop_all.sh --all    # 同时停止 Kafka 和 Redis
#   ./scripts/stop_all.sh --clean  # 停止全部服务并清理 Redis/MySQL/Kafka 数据
#
# MySQL 连接参数可通过 MYSQL_HOST、MYSQL_PORT、MYSQL_USER、MYSQL_PASSWORD 覆盖。

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCHEMA_FILE="$PROJECT_ROOT/sql/schema.sql"
REDIS_RDB_FILE="$PROJECT_ROOT/logs/dump.rdb"
KAFKA_HOME="${KAFKA_HOME:-$HOME/software/kafka_2.13-4.1.2}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-123456}"

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

usage() {
    echo "用法: $0 [--all | --clean]"
    echo "  无参数    只停止应用服务"
    echo "  --all     停止应用服务、Kafka 和 Redis"
    echo "  --clean   停止全部服务，清空 Redis 和 Kafka，并重新执行 sql/schema.sql"
}

STOP_INFRA=false
CLEAN_DATA=false
case "${1:-}" in
    "") ;;
    --all) STOP_INFRA=true ;;
    --clean) STOP_INFRA=true; CLEAN_DATA=true ;;
    -h|--help) usage; exit 0 ;;
    *) log_error "未知参数: $1"; usage; exit 2 ;;
esac
if [ "$#" -gt 1 ]; then
    log_error "一次只能指定一个参数"
    usage
    exit 2
fi

# 先发 SIGTERM 优雅退出,超时未退出再 SIGKILL 兜底
# (Comet<->Logic 之间有常驻 gRPC 双向流,优雅关闭可能被阻塞)
stop_process() {
    local name=$1 timeout=${2:-5}
    pgrep -x "$name" >/dev/null || return 0
    log_info "停止 $name..."
    pkill -x "$name" 2>/dev/null || true
    local count=0
    while pgrep -x "$name" >/dev/null; do
        sleep 1
        count=$((count + 1))
        if [ "$count" -ge "$timeout" ]; then
            log_warn "$name 未在 ${timeout}s 内退出,强制结束"
            pkill -9 -x "$name" 2>/dev/null || true
            break
        fi
    done
}

setup_java() {
    if [ -z "${JAVA_HOME:-}" ]; then
        JAVA_HOME=$(ls -d "$HOME"/software/jdk-17* 2>/dev/null | head -1 || true)
    fi
    if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        export JAVA_HOME
        export PATH="$JAVA_HOME/bin:$PATH"
    fi
}

stop_kafka() {
    setup_java
    log_info "停止 Kafka..."
    if [ -x "$KAFKA_HOME/bin/kafka-server-stop.sh" ]; then
        "$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true
    else
        pkill -f 'kafka\.Kafka' 2>/dev/null || true
    fi

    local count=0
    while ss -tln 2>/dev/null | grep -q ':9092 '; do
        sleep 1
        count=$((count + 1))
        if [ "$count" -ge 15 ]; then
            log_warn "Kafka 未在 15s 内退出,强制结束"
            pkill -9 -f 'kafka\.Kafka' 2>/dev/null || true
            sleep 1
            break
        fi
    done

    if ss -tln 2>/dev/null | grep -q ':9092 '; then
        log_error "Kafka 仍在监听 9092 端口"
        return 1
    fi
}

clean_kafka() {
    local props="$KAFKA_HOME/config/server.properties"
    if [ ! -f "$props" ]; then
        log_error "找不到 Kafka 配置文件: $props"
        return 1
    fi

    local configured_dirs data_dir
    configured_dirs=$(awk -F= '/^(log\.dirs|metadata\.log\.dir)=/ {sub(/^[^=]*=/, ""); print}' "$props")
    configured_dirs="${configured_dirs:-/tmp/kraft-combined-logs}"

    while IFS= read -r data_dir; do
        data_dir=$(echo "$data_dir" | xargs)
        [ -z "$data_dir" ] && continue
        case "$data_dir" in
            /*) ;;
            *) log_error "拒绝删除非绝对路径的 Kafka 数据目录: $data_dir"; return 1 ;;
        esac
        case "$data_dir" in
            /|/home|"$HOME") log_error "拒绝删除危险路径: $data_dir"; return 1 ;;
        esac
        log_info "清理 Kafka 数据目录: $data_dir"
        rm -rf -- "$data_dir"
    done < <(echo "$configured_dirs" | tr ',' '\n' | sort -u)
}

stop_redis() {
    local redis_running=false
    if ss -tln 2>/dev/null | grep -q ':6379 '; then
        redis_running=true
    fi

    if [ "$CLEAN_DATA" = true ] && [ "$redis_running" = false ]; then
        if [ -f "$REDIS_RDB_FILE" ]; then
            log_info "Redis 未运行,删除持久化文件: $REDIS_RDB_FILE"
            rm -f -- "$REDIS_RDB_FILE"
        else
            log_info "Redis 未运行且无持久化数据"
        fi
        return 0
    fi

    if ! command -v redis-cli >/dev/null 2>&1; then
        if [ "$redis_running" = true ]; then
            log_error "Redis 正在运行但找不到 redis-cli,无法安全停止或清理"
            return 1
        fi
        log_warn "找不到 redis-cli,跳过停止 Redis"
        return 0
    fi

    if [ "$CLEAN_DATA" = true ]; then
        log_info "清空 Redis 全部数据库..."
        if ! redis-cli FLUSHALL >/dev/null; then
            log_error "Redis 数据清理失败"
            return 1
        fi
        log_info "停止 Redis..."
        if ! redis-cli shutdown save 2>/dev/null; then
            log_error "Redis 停止失败"
            return 1
        fi
        return 0
    fi

    if [ "$redis_running" = true ]; then
        log_info "停止 Redis..."
        redis-cli shutdown nosave 2>/dev/null || true
    fi
}

clean_mysql() {
    if ! command -v mysql >/dev/null 2>&1; then
        log_error "找不到 mysql 客户端,无法重建 MySQL 数据库"
        return 1
    fi
    if [ ! -f "$SCHEMA_FILE" ]; then
        log_error "找不到数据库脚本: $SCHEMA_FILE"
        return 1
    fi

    log_info "重新执行 sql/schema.sql,重建 MySQL 数据库..."
    MYSQL_PWD="$MYSQL_PASSWORD" mysql \
        --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
        < "$SCHEMA_FILE"
}

main() {
    local failed=0 kafka_stopped=true

    stop_process web_demo_server
    stop_process job_server
    stop_process comet_server
    stop_process logic_server

    if [ "$STOP_INFRA" = true ]; then
        stop_redis || failed=1
        if ! stop_kafka; then
            failed=1
            kafka_stopped=false
        fi
    fi

    if [ "$CLEAN_DATA" = true ]; then
        if [ "$kafka_stopped" = true ]; then
            clean_kafka || failed=1
        else
            log_error "Kafka 未完全停止,跳过 Kafka 数据清理"
        fi
        clean_mysql || failed=1
    fi

    echo ""
    if [ "$failed" -ne 0 ]; then
        log_error "服务已停止,但部分数据清理失败"
        exit 1
    fi

    if [ "$CLEAN_DATA" = true ]; then
        log_info "服务已停止,Redis/MySQL/Kafka 数据已清理"
    else
        log_info "服务已停止"
        echo ""
        echo "提示: --all 可停止 Kafka/Redis；--clean 可额外清理 Redis/MySQL/Kafka 数据"
    fi
    echo ""
}

main
