#pragma once

#include <cstdint>
#include <string>

namespace sparkpush {

struct Config {
    std::string listen_addr;
    int listen_port{0};
    // logic HTTP 端口（如未配置，可回退为 listen_port+1）
    int http_port{0};
    std::string logic_grpc_target;
    std::string kafka_brokers;
    std::string kafka_push_topic;
    std::string kafka_broadcast_topic;
    std::string kafka_consumer_group;

    // Redis/MySQL 配置（连接池参数）
    std::string redis_host;
    int redis_port{6379};
    std::string redis_password;
    int redis_db{0};
    int redis_pool_size{4};

    std::string mysql_host;
    int mysql_port{3306};
    std::string mysql_user;
    std::string mysql_password;
    std::string mysql_db;
    int mysql_pool_size{4};
    int mysql_pool_min_size{0};
    int mysql_pool_max_size{0};
    int mysql_idle_timeout_ms{60000};

    std::string comet_id;
    std::string comet_targets;  // 仅 job 使用，格式：id=addr,id2=addr2

    // comet 进程的 IO 线程数（asio io_context worker 数，>=1）
    int comet_io_threads{4};

    // comet 异步 gRPC 调用线程池大小（用于 VerifyToken、SendUpstreamMessage
    // 等）
    int comet_grpc_pool_size{4};

    // comet 自身提供的 gRPC 服务端口（给 job 调用），如未配置可回退为
    // listen_port+100
    int comet_grpc_port{0};

    // 是否使用 gRPC 双向流模式（性能更高）- Comet -> Logic
    bool use_grpc_stream{false};
    // 双向流连接数量（多流可提升吞吐）
    int grpc_stream_count{1};

    // 是否使用 gRPC 客户端流模式 - Job -> Comet
    bool use_push_stream{false};

    // Job 每个 Kafka consumer 的并行工作线程数与最大在途消息数。
    // 同一 partition 始终串行处理，不同 partition 可并行。
    int job_worker_threads{4};
    int job_max_inflight{1024};

    int redis_max_pool_size{0};
    int redis_connect_timeout_ms{2000};
    int redis_rw_timeout_ms{2000};
    int redis_idle_timeout_ms{60000};

    // Logic HTTP 服务器 IO 线程数（0=单线程，N=N个工作线程）
    int http_threads{4};

    // 预设管理员凭据（默认值等于历史硬编码值，保证不配置时行为完全不变）。
    //   账号：admin，密码明文：admin，MD5：0192023a7bbd73250516f069df18b500
    std::string admin_account;
    std::string admin_password_hash;
    int64_t admin_user_id{900000000000LL};

    // 日志级别（对应 spdlog::level::level_enum，数值与 spdlog 保持一致）：
    //   TRACE=0 DEBUG=1 INFO=2 WARN=3 ERROR=4 FATAL(=critical)=5
    // 默认 2(INFO)；压测/生产建议设为 WARN 以避免热路径日志拖累。
    // 配置文件里既可写数字也可写名称(TRACE/DEBUG/INFO/WARN/ERROR/FATAL,大小写不敏感)。
    int log_level{2};

    // CORS 允许的来源（默认 "*"，行为不变）。
    std::string cors_allow_origin;
};

// 读取 key=value 配置文件并返回解析后的 Config。
Config LoadConfig(const std::string& path);

}  // namespace sparkpush

