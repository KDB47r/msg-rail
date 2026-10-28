#include "config.h"

#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>

#include "logging.h"

namespace sparkpush {

// 去掉字符串首尾空白，供配置文件逐行清洗 key 和 value。
static void Trim(std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos || end == std::string::npos) {
        s.clear();
    } else {
        s = s.substr(start, end - start + 1);
    }
}

// 解析日志级别（数字或名称），非法值回退默认级别并告警。
static int ParseLogLevelOr(const std::string& value, int default_value) {
    std::string v;
    v.reserve(value.size());
    for (char c : value) v.push_back(static_cast<char>(::toupper(c)));
    if (v == "TRACE") return 0;
    if (v == "DEBUG") return 1;
    if (v == "INFO") return 2;
    if (v == "WARN" || v == "WARNING") return 3;
    if (v == "ERROR") return 4;
    if (v == "FATAL") return 5;
    try {
        int n = std::stoi(v);
        if (n >= 0 && n <= 5) return n;
    } catch (const std::exception&) {
    }
    LogWarn("LoadConfig: invalid log_level '" + value + "', fallback to " +
            std::to_string(default_value));
    return default_value;
}

// 解析 int 配置，非法值记录警告并回退默认值，避免脏配置导致启动失败。
static int ParseIntOr(const std::string& key, const std::string& value,
                      int default_value) {
    try {
        size_t idx = 0;
        int result = std::stoi(value, &idx);
        // 允许数字后带单位/注释残留？这里从严：若首字符即非数字会抛异常。
        return result;
    } catch (const std::exception&) {
        LogWarn("LoadConfig: invalid integer for key '" + key + "', value='" +
                value + "', fallback to default " +
                std::to_string(default_value));
        return default_value;
    }
}

// 解析 int64 配置，非法值记录警告并回退默认值。
static int64_t ParseInt64Or(const std::string& key, const std::string& value,
                            int64_t default_value) {
    try {
        size_t idx = 0;
        long long result = std::stoll(value, &idx);
        return static_cast<int64_t>(result);
    } catch (const std::exception&) {
        LogWarn("LoadConfig: invalid integer for key '" + key + "', value='" +
                value + "', fallback to default " +
                std::to_string(default_value));
        return default_value;
    }
}

// 加载 key=value 配置文件，填充默认值并推导未显式配置的端口。
Config LoadConfig(const std::string& path) {
    Config cfg;
    // 默认值
    cfg.listen_addr = "0.0.0.0";
    cfg.listen_port = 9000;
    // logic: 默认 HTTP 端口为 gRPC 端口 + 1
    cfg.http_port = cfg.listen_port + 1;
    cfg.logic_grpc_target = "127.0.0.1:9100";
    cfg.kafka_brokers = "127.0.0.1:9092";
    cfg.kafka_push_topic = "push_to_comet";
    cfg.kafka_broadcast_topic = "broadcast_task";
    cfg.kafka_consumer_group = "spark_push_group";
    cfg.redis_host = "127.0.0.1";
    cfg.redis_port = 6379;
    cfg.redis_password.clear();
    cfg.redis_db = 0;
    cfg.redis_pool_size = 4;
    cfg.redis_max_pool_size = 0;
    cfg.redis_connect_timeout_ms = 2000;
    cfg.redis_rw_timeout_ms = 2000;
    cfg.redis_idle_timeout_ms = 60000;
    cfg.mysql_host = "127.0.0.1";
    cfg.mysql_port = 3306;
    cfg.mysql_user = "root";
    cfg.mysql_password.clear();
    cfg.mysql_db = "spark_push";
    cfg.mysql_pool_size = 4;
    cfg.mysql_pool_min_size = 0;
    cfg.mysql_pool_max_size = 0;
    cfg.mysql_idle_timeout_ms = 60000;
    cfg.comet_id = "comet-1";
    cfg.comet_targets.clear();
    cfg.comet_io_threads = 4;
    cfg.comet_grpc_pool_size = 4;
    // comet: 默认 gRPC 端口为 WebSocket 端口 + 100
    cfg.comet_grpc_port = cfg.listen_port + 100;
    // 预设管理员凭据默认值（与历史硬编码一致）。
    cfg.admin_account = "admin";
    cfg.admin_password_hash = "0192023a7bbd73250516f069df18b500";
    cfg.admin_user_id = 900000000000LL;
    // CORS 默认允许任意来源。
    cfg.cors_allow_origin = "*";
    // 日志级别默认 INFO(2)。
    cfg.log_level = 2;

    std::ifstream fin(path);
    if (!fin) {
        LogError("LoadConfig open file failed: " + path + ", use defaults");
        return cfg;
    }

    // 记录 http_port / comet_grpc_port 是否在配置文件中被显式提供，
    // 只有在未显式提供时才在解析结束后推导默认值（listen_port+1 /
    // listen_port+100）。
    bool http_port_set = false;
    bool comet_grpc_port_set = false;

    std::string line;
    while (std::getline(fin, line)) {
        Trim(line);
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        Trim(key);
        Trim(value);

        if (key == "listen_addr")
            cfg.listen_addr = value;
        else if (key == "listen_port")
            cfg.listen_port = ParseIntOr(key, value, cfg.listen_port);
        else if (key == "http_port") {
            cfg.http_port = ParseIntOr(key, value, cfg.http_port);
            http_port_set = true;
        } else if (key == "logic_grpc_target")
            cfg.logic_grpc_target = value;
        else if (key == "kafka_brokers")
            cfg.kafka_brokers = value;
        else if (key == "kafka_push_topic")
            cfg.kafka_push_topic = value;
        else if (key == "kafka_broadcast_topic")
            cfg.kafka_broadcast_topic = value;
        else if (key == "kafka_consumer_group")
            cfg.kafka_consumer_group = value;
        else if (key == "redis_host")
            cfg.redis_host = value;
        else if (key == "redis_port")
            cfg.redis_port = ParseIntOr(key, value, cfg.redis_port);
        else if (key == "redis_password")
            cfg.redis_password = value;
        else if (key == "redis_db")
            cfg.redis_db = ParseIntOr(key, value, cfg.redis_db);
        else if (key == "redis_pool_size")
            cfg.redis_pool_size = ParseIntOr(key, value, cfg.redis_pool_size);
        else if (key == "redis_max_pool_size")
            cfg.redis_max_pool_size =
                ParseIntOr(key, value, cfg.redis_max_pool_size);
        else if (key == "redis_connect_timeout_ms")
            cfg.redis_connect_timeout_ms =
                ParseIntOr(key, value, cfg.redis_connect_timeout_ms);
        else if (key == "redis_rw_timeout_ms")
            cfg.redis_rw_timeout_ms =
                ParseIntOr(key, value, cfg.redis_rw_timeout_ms);
        else if (key == "redis_idle_timeout_ms")
            cfg.redis_idle_timeout_ms =
                ParseIntOr(key, value, cfg.redis_idle_timeout_ms);
        else if (key == "mysql_host")
            cfg.mysql_host = value;
        else if (key == "mysql_port")
            cfg.mysql_port = ParseIntOr(key, value, cfg.mysql_port);
        else if (key == "mysql_user")
            cfg.mysql_user = value;
        else if (key == "mysql_password")
            cfg.mysql_password = value;
        else if (key == "mysql_db")
            cfg.mysql_db = value;
        else if (key == "mysql_pool_size")
            cfg.mysql_pool_size = ParseIntOr(key, value, cfg.mysql_pool_size);
        else if (key == "mysql_pool_min_size")
            cfg.mysql_pool_min_size =
                ParseIntOr(key, value, cfg.mysql_pool_min_size);
        else if (key == "mysql_pool_max_size")
            cfg.mysql_pool_max_size =
                ParseIntOr(key, value, cfg.mysql_pool_max_size);
        else if (key == "mysql_idle_timeout_ms")
            cfg.mysql_idle_timeout_ms =
                ParseIntOr(key, value, cfg.mysql_idle_timeout_ms);
        else if (key == "comet_id")
            cfg.comet_id = value;
        else if (key == "comet_targets")
            cfg.comet_targets = value;
        else if (key == "comet_io_threads")
            cfg.comet_io_threads = ParseIntOr(key, value, cfg.comet_io_threads);
        else if (key == "comet_grpc_pool_size")
            cfg.comet_grpc_pool_size =
                ParseIntOr(key, value, cfg.comet_grpc_pool_size);
        else if (key == "comet_grpc_port") {
            cfg.comet_grpc_port = ParseIntOr(key, value, cfg.comet_grpc_port);
            comet_grpc_port_set = true;
        } else if (key == "use_grpc_stream")
            cfg.use_grpc_stream = (value == "true" || value == "1");
        else if (key == "grpc_stream_count")
            cfg.grpc_stream_count =
                ParseIntOr(key, value, cfg.grpc_stream_count);
        else if (key == "use_push_stream")
            cfg.use_push_stream = (value == "true" || value == "1");
        else if (key == "job_worker_threads")
            cfg.job_worker_threads =
                ParseIntOr(key, value, cfg.job_worker_threads);
        else if (key == "job_max_inflight")
            cfg.job_max_inflight = ParseIntOr(key, value, cfg.job_max_inflight);
        else if (key == "http_threads")
            cfg.http_threads = ParseIntOr(key, value, cfg.http_threads);
        else if (key == "admin_account")
            cfg.admin_account = value;
        else if (key == "admin_password_hash")
            cfg.admin_password_hash = value;
        else if (key == "admin_user_id")
            cfg.admin_user_id = ParseInt64Or(key, value, cfg.admin_user_id);
        else if (key == "cors_allow_origin")
            cfg.cors_allow_origin = value;
        else if (key == "log_level")
            cfg.log_level = ParseLogLevelOr(value, cfg.log_level);
        else {
            // 未知 key 保持容错：不报错，仅告警后忽略。
            LogWarn("LoadConfig: unknown config key '" + key + "' ignored");
        }
    }

    // 缺省推导：仅当配置文件未显式提供时才根据最终的 listen_port 推导。
    if (!http_port_set) {
        cfg.http_port = cfg.listen_port + 1;
    }
    if (!comet_grpc_port_set) {
        cfg.comet_grpc_port = cfg.listen_port + 100;
    }

    return cfg;
}

}  // namespace sparkpush
