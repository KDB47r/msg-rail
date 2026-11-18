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
    // Logic HTTP 业务线程池大小；阻塞 DB/Redis/S3 handler 在这里执行。
    int http_worker_threads{8};

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

    // ---- 媒体对象存储（MinIO / S3，path-style） ----
    // endpoint 供 Logic/worker 内部访问；public_endpoint 供浏览器直传/下载。
    // 二者可以是同一个地址；部署到容器/公网时通常不同。
    std::string s3_endpoint;
    std::string s3_public_endpoint;
    std::string s3_bucket;
    std::string s3_access_key;
    std::string s3_secret_key;
    std::string s3_region;
    int s3_presign_ttl_seconds{900};
    bool s3_path_style{true};

    // 单张图片 / 单个视频 / 单个文件限制与服务端处理参数。
    int64_t media_max_image_bytes{20LL * 1024 * 1024};
    int64_t media_max_video_bytes{100LL * 1024 * 1024};
    int64_t media_max_file_bytes{50LL * 1024 * 1024};
    int media_max_pixels{50000000};
    int media_max_side{12000};
    int64_t media_max_video_duration_ms{10LL * 60 * 1000};
    int media_max_video_pixels{8294400};  // 3840 * 2160
    int media_max_video_side{3840};
    int media_video_poster_max_width{640};
    int media_video_process_timeout_seconds{60};
    int media_video_transcode_timeout_seconds{600};
    int media_video_transcode_max_side{1920};
    int media_video_transcode_crf{23};
    std::string media_video_transcode_preset{"veryfast"};
    std::string media_video_ffprobe_path{"ffprobe"};
    std::string media_video_ffmpeg_path{"ffmpeg"};
    std::string media_video_temp_dir{"/tmp"};
    int media_download_url_ttl_seconds{900};
    // 旧配置：图片/视频共用一个线程池。仅作为兼容 fallback。
    int media_worker_threads{2};
    // 图片和视频独立线程池。
    int media_image_worker_threads{3};
    int media_video_worker_threads{1};
    int media_worker_poll_ms{500};
    int media_task_max_attempts{3};
    // 图片解码的并发内存预算（MiB）。<=0 表示不限制。
    int media_memory_budget_mb{1024};

    // 媒体元数据 Redis 缓存。ready 基本不可变，处理中只缓存极短时间。
    bool media_cache_enabled{true};
    int media_cache_ready_ttl_seconds{600};
    int media_cache_failed_ttl_seconds{60};
    int media_cache_pending_ttl_seconds{1};
    // 缩略图/预览图/视频封面的 CDN 前缀；为空时继续使用预签名 URL。
    // 例：http://127.0.0.1:9002/sparkpush-media 或 https://cdn.example.com/bucket
    std::string media_variant_cdn_base_url;
    // 公开变体上传时写入对象元数据的 Cache-Control。
    std::string media_variant_cache_control{
        "public, max-age=31536000, immutable"};

    // 视频 multipart 上传：分片大小、上传会话 TTL、分片 URL TTL。
    int64_t media_multipart_part_size_bytes{8LL * 1024 * 1024};
    int media_multipart_upload_ttl_seconds{86400};
    int media_upload_url_ttl_seconds{900};
    // media_worker 清理过期上传的周期/批量，以及 init 孤儿媒体 TTL。
    int media_multipart_cleanup_interval_seconds{3600};
    int media_multipart_cleanup_batch{100};
    int media_upload_orphan_ttl_seconds{86400};
};

// 读取 key=value 配置文件并返回解析后的 Config。
Config LoadConfig(const std::string& path);

}  // namespace sparkpush

// 在 sparkpush 命名空间下定义 config 结构体 声明 LoadConfig 函数