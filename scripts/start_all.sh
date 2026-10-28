#!/bin/bash
# Spark Push 一键启动脚本(Kafka KRaft 模式,无 ZooKeeper)
#
# 用法:
#   ./scripts/start_all.sh
#
# 可通过环境变量覆盖默认路径:
#   KAFKA_HOME  Kafka 安装目录 (默认 ~/software/kafka_2.13-4.1.2)
#   JAVA_HOME   JDK 17 安装目录 (默认自动探测 ~/software/jdk-17*)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
CONF_DIR="$PROJECT_ROOT/conf"
LOG_DIR="$PROJECT_ROOT/logs"

KAFKA_HOME="${KAFKA_HOME:-$HOME/software/kafka_2.13-4.1.2}"
WEB_DEMO_PORT=9001
KAFKA_TOPICS=(push_to_comet broadcast_task)

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

port_open() { ss -tln 2>/dev/null | grep -q ":$1 "; }

check_port() {
    if port_open "$1"; then
        log_info "$2 已在端口 $1 运行"
        return 0
    fi
    return 1
}

wait_for_port() {
    local port=$1 name=$2 max_wait=${3:-30} count=0
    while ! port_open "$port"; do
        sleep 1
        count=$((count + 1))
        if [ "$count" -ge "$max_wait" ]; then
            log_error "$name 启动超时 (端口 $port)"
            return 1
        fi
    done
    log_info "$name 启动成功 (端口 $port)"
}

# 确保 Java 17+ 可用(Kafka 4.x 要求 JDK 17+)
setup_java() {
    if [ -z "${JAVA_HOME:-}" ]; then
        JAVA_HOME=$(ls -d "$HOME"/software/jdk-17* 2>/dev/null | head -1 || true)
    fi
    if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        export JAVA_HOME
        export PATH="$JAVA_HOME/bin:$PATH"
    fi

    if ! command -v java >/dev/null 2>&1; then
        log_error "找不到 Java 17+,请安装课程提供的 JDK 或设置 JAVA_HOME(参考 docs/环境安装.md)"
        exit 1
    fi

    local java_version
    java_version=$(java -version 2>&1 | awk -F '[\".]' '/version/ {print $2; exit}')
    if [ -z "$java_version" ] || [ "$java_version" -lt 17 ]; then
        log_error "Kafka 4.x 需要 Java 17+,当前版本为 ${java_version:-unknown}"
        exit 1
    fi
    log_info "使用 Java $java_version: $(command -v java)"
}

# 启动 Kafka(KRaft 模式,broker+controller 合一,无 ZooKeeper)
start_kafka() {
    if check_port 9092 "Kafka"; then
        return 0
    fi

    if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
        log_error "找不到 Kafka: $KAFKA_HOME(可用 KAFKA_HOME 环境变量指定,参考 docs/环境安装.md)"
        exit 1
    fi

    setup_java

    local props="$KAFKA_HOME/config/server.properties"
    local data_dir
    data_dir=$(grep -E '^log\.dirs=' "$props" | head -1 | cut -d= -f2)
    data_dir="${data_dir:-/tmp/kraft-combined-logs}"

    # KRaft 首次启动前必须格式化存储目录
    if [ ! -f "$data_dir/meta.properties" ]; then
        log_info "首次启动,格式化 KRaft 存储目录: $data_dir"
        local cluster_id
        cluster_id=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
        "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone \
            -t "$cluster_id" -c "$props"
    fi

    log_info "启动 Kafka (KRaft)..."
    # setsid 使 Kafka 脱离当前终端会话,避免随会话退出被回收
    setsid "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$props" >/dev/null 2>&1 < /dev/null
    wait_for_port 9092 "Kafka" 60
}

# 创建项目所需的 topic(幂等)
create_topics() {
    setup_java
    for topic in "${KAFKA_TOPICS[@]}"; do
        "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
            --create --if-not-exists --topic "$topic" \
            --partitions 4 --replication-factor 1 >/dev/null
        log_info "Kafka topic 就绪: $topic"
    done
}

start_redis() {
    if check_port 6379 "Redis"; then
        return 0
    fi
    if ! command -v redis-server >/dev/null 2>&1; then
        log_error "找不到 redis-server,请先安装(参考 docs/环境安装.md)"
        exit 1
    fi
    log_info "启动 Redis..."
    redis-server --daemonize yes --port 6379 --dir "$LOG_DIR"
    wait_for_port 6379 "Redis"
}

check_mysql() {
    if check_port 3306 "MySQL"; then
        return 0
    fi
    log_error "MySQL 未运行,请先启动: sudo systemctl start mysql"
    log_error "并确认已导入表结构: mysql -uroot -p123456 < sql/schema.sql"
    exit 1
}

# 应用服务统一从项目根目录启动,配置文件与 logs/ 目录都用相对路径
start_service() {
    local name=$1 binary=$2 conf=$3
    if pgrep -x "$name" >/dev/null; then
        log_info "$name 已运行"
        return 0
    fi
    log_info "启动 $name..."
    cd "$PROJECT_ROOT"
    # setsid + nohup:脱离当前终端会话,SSH 断开或会话退出后进程仍存活
    setsid nohup "$binary" --config "$conf" > "$LOG_DIR/$name.out" 2>&1 < /dev/null &
}

start_web_demo() {
    if pgrep -x "web_demo_server" >/dev/null; then
        log_info "Web Demo 已运行"
        return 0
    fi
    log_info "启动 Web Demo..."
    cd "$PROJECT_ROOT"
    setsid nohup "$BUILD_DIR/web_demo/web_demo_server" \
        --port "$WEB_DEMO_PORT" \
        --doc-root "$PROJECT_ROOT/web_demo/static" \
        > "$LOG_DIR/web_demo_server.out" 2>&1 < /dev/null &
    wait_for_port "$WEB_DEMO_PORT" "Web Demo"
}

check_build_artifacts() {
    local artifact
    local artifacts=(
        "$BUILD_DIR/logic/logic_server"
        "$BUILD_DIR/comet/comet_server"
        "$BUILD_DIR/job/job_server"
        "$BUILD_DIR/web_demo/web_demo_server"
    )
    for artifact in "${artifacts[@]}"; do
        if [ ! -x "$artifact" ]; then
            log_error "找不到编译产物: $artifact"
            log_error "请先执行: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j\$(nproc)"
            exit 1
        fi
    done
}

main() {
    echo "========================================"
    echo "   Spark Push 一键启动 (Kafka KRaft)"
    echo "========================================"
    echo ""

    check_build_artifacts

    mkdir -p "$LOG_DIR"

    # 基础设施
    check_mysql
    start_redis
    start_kafka
    create_topics

    # 应用服务:Logic -> Comet -> Job
    start_service logic_server "$BUILD_DIR/logic/logic_server" "$CONF_DIR/logic.conf"
    wait_for_port 9100 "Logic gRPC"
    wait_for_port 9101 "Logic HTTP"

    start_service comet_server "$BUILD_DIR/comet/comet_server" "$CONF_DIR/comet.conf"
    wait_for_port 9200 "Comet WebSocket"
    wait_for_port 9205 "Comet gRPC"

    start_service job_server "$BUILD_DIR/job/job_server" "$CONF_DIR/job.conf"
    sleep 2
    if pgrep -x job_server >/dev/null; then
        log_info "Job Server 启动成功"
    else
        log_error "Job Server 启动失败,查看 $LOG_DIR/job_server.out"
        exit 1
    fi

    start_web_demo

    echo ""
    echo "========================================"
    echo "            启动完成!"
    echo "========================================"
    echo ""
    echo "服务端口:"
    echo "  - Web Demo:    http://localhost:$WEB_DEMO_PORT/index.html"
    echo "  - Logic HTTP:  http://localhost:9101  (健康检查 /health)"
    echo "  - Logic gRPC:  localhost:9100"
    echo "  - Comet WS:    ws://localhost:9200"
    echo "  - Comet gRPC:  localhost:9205"
    echo "  - Kafka:       localhost:9092 (KRaft,无 ZooKeeper)"
    echo ""
    echo "日志目录: $LOG_DIR/"
    echo "查看状态: ./scripts/status.sh"
    echo "停止服务: ./scripts/stop_all.sh [--all]"
    echo ""
}

main "$@"
