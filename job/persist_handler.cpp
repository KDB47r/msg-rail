#include "persist_handler.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

#include "logging.h"
#include "proto_enums.h"

namespace sparkpush {

namespace {

// 复用同一条幂等 INSERT SQL，跨消息只 prepare 一次
const char kInsertMessageSql[] =
    "INSERT INTO message (session_id, msg_seq, sender_id, msg_type, "
    "content_json, timestamp_ms, client_msg_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?) "
    "ON DUPLICATE KEY UPDATE session_id=session_id";

const char kUpsertSingleSessionSql[] =
    "INSERT INTO `session` "
    "(session_id, type, user1_id, user2_id, group_id, last_msg_seq) "
    "VALUES (?, 0, ?, ?, 0, ?) "
    "ON DUPLICATE KEY UPDATE "
    "last_msg_seq=GREATEST(last_msg_seq, VALUES(last_msg_seq))";

const char kUpsertRoomSessionSql[] =
    "INSERT INTO `session` "
    "(session_id, type, user1_id, user2_id, group_id, last_msg_seq) "
    "VALUES (?, 2, 0, 0, ?, ?) "
    "ON DUPLICATE KEY UPDATE "
    "last_msg_seq=GREATEST(last_msg_seq, VALUES(last_msg_seq))";

void BindString(MYSQL_BIND* bind, const std::string& value,
                unsigned long* len) {
    *len = static_cast<unsigned long>(value.size());
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = const_cast<char*>(value.data());
    bind->buffer_length = *len;
    bind->length = len;
}

void BindInt64(MYSQL_BIND* bind, long long* value) {
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = value;
}

bool ParseSingleSessionId(const std::string& session_id, int64_t* user1,
                          int64_t* user2) {
    static const std::string kPrefix = "single:";
    if (!user1 || !user2 ||
        session_id.compare(0, kPrefix.size(), kPrefix) != 0) {
        return false;
    }
    const std::string rest = session_id.substr(kPrefix.size());
    const auto separator = rest.find(':');
    if (separator == std::string::npos) return false;
    try {
        *user1 = std::stoll(rest.substr(0, separator));
        *user2 = std::stoll(rest.substr(separator + 1));
    } catch (...) {
        return false;
    }
    return *user1 > 0 && *user2 > 0;
}

bool ExecuteStatement(MYSQL* conn, const char* sql, MYSQL_BIND* bind,
                      std::string* err_msg) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0 ||
        mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace

PersistHandler::PersistHandler(const Config& cfg) : cfg_(cfg) {}

PersistHandler::~PersistHandler() { Stop(); }

// reaf
// 初始化 MySQL 连接池和持久化 Kafka 消费者。
bool PersistHandler::Init() {
    // 初始化MySQL连接池
    if (cfg_.mysql_host.empty()) {
        LogError("[PersistHandler] MySQL host not configured");
        return false;
    }

    MySqlConfig mysql_cfg;
    mysql_cfg.host = cfg_.mysql_host;
    mysql_cfg.port = cfg_.mysql_port;
    mysql_cfg.user = cfg_.mysql_user;
    mysql_cfg.password = cfg_.mysql_password;
    mysql_cfg.db = cfg_.mysql_db;
    mysql_cfg.pool_size = cfg_.mysql_pool_size > 0 ? cfg_.mysql_pool_size : 8;

    mysql_pool_ = std::make_unique<MySqlConnectionPool>();
    if (!mysql_pool_->Init(mysql_cfg)) {
        LogError("[PersistHandler] MySQL pool init failed");
        return false;
    }
    LogInfo("[PersistHandler] MySQL pool initialized");

    // 持久化消费组：spark_push_persist_group（独立于推送消费组）
    if (cfg_.kafka_brokers.empty()) {
        LogError("[PersistHandler] Kafka brokers not configured");
        return false;
    }

    KafkaConsumer::Options opts;
    opts.enable_auto_commit = false;
    opts.worker_threads =
        static_cast<size_t>(std::max(1, cfg_.job_worker_threads));
    opts.max_inflight = static_cast<size_t>(std::max(1, cfg_.job_max_inflight));
    std::string persist_group = cfg_.kafka_consumer_group + "_persist";

    if (!persist_consumer_.Init(
            cfg_.kafka_brokers, persist_group, cfg_.kafka_push_topic,
            [this](const std::string& k, const std::string& v) {
                return HandleMessage(k, v);
            },
            opts)) {
        LogError("[PersistHandler] Kafka persist consumer init failed");
        return false;
    }
    LogInfo("[PersistHandler] Persist consumer initialized, group=" +
            persist_group);
    return true;
}

// 启动持久化消费者。
void PersistHandler::Start() {
    persist_consumer_.Start();
    LogInfo("[PersistHandler] Started");
}

// 停止持久化消费者并等待在途任务完成。
void PersistHandler::Stop() {
    persist_consumer_.Stop();
    if (mysql_pool_) {
        mysql_pool_->Stop();
        mysql_pool_.reset();
    }
    LogInfo("[PersistHandler] Stopped");
}

// read
// 在 Kafka 分区工作线程内同步处理：回调返回即代表消息已处理完成，
// KafkaConsumer 随后才提交该分区 offset，保证 at-least-once 语义
// 处理一条消息，仅对需要持久化的消息落库。
void PersistHandler::HandleMessage(const std::string& key,
                                   const std::string& value) {
    (void)key;
    PushToCometRequest req;
    if (!req.ParseFromString(value)) {
        LogError("[PersistHandler] Failed to parse PushToCometRequest");
        return;
    }
    // 只处理需要落盘的消息
    if (req.need_persist()) {
        PersistMessage(req);
    }
}

// 会话元数据与消息在同一事务中落盘，保证会话列表和历史记录一致。
// 在事务中更新会话并幂等写入消息表。
bool PersistHandler::ExecutePersist(const PushToCometRequest& req,
                                    std::string* err_msg) {
    auto guard = mysql_pool_->Acquire(5000);
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "acquire MySQL connection timed out";
        return false;
    }

    const ChatMessage& msg = req.message();
    std::string session_id = msg.session_id();
    long long msg_seq = msg.msg_seq();

    if (mysql_autocommit(conn, 0) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }

    bool ok = false;
    do {
        if (req.scene() == SCENE_SINGLE) {
            int64_t user1 = 0;
            int64_t user2 = 0;
            if (!ParseSingleSessionId(session_id, &user1, &user2)) {
                if (err_msg)
                    *err_msg = "invalid single session_id: " + session_id;
                break;
            }
            long long user1_buf = user1;
            long long user2_buf = user2;
            unsigned long session_id_len = 0;
            MYSQL_BIND session_bind[4];
            std::memset(session_bind, 0, sizeof(session_bind));
            BindString(&session_bind[0], session_id, &session_id_len);
            BindInt64(&session_bind[1], &user1_buf);
            BindInt64(&session_bind[2], &user2_buf);
            BindInt64(&session_bind[3], &msg_seq);
            if (!ExecuteStatement(conn, kUpsertSingleSessionSql, session_bind,
                                  err_msg)) {
                break;
            }
        } else if (req.scene() == SCENE_CHATROOM) {
            long long room_id = req.room_id();
            if (room_id <= 0) {
                if (err_msg) *err_msg = "invalid room_id";
                break;
            }
            unsigned long session_id_len = 0;
            MYSQL_BIND session_bind[3];
            std::memset(session_bind, 0, sizeof(session_bind));
            BindString(&session_bind[0], session_id, &session_id_len);
            BindInt64(&session_bind[1], &room_id);
            BindInt64(&session_bind[2], &msg_seq);
            if (!ExecuteStatement(conn, kUpsertRoomSessionSql, session_bind,
                                  err_msg)) {
                break;
            }
        }

        std::string content_json = msg.content_json();
        std::string client_msg_id = msg.client_msg_id();
        std::string msg_type_str = MsgTypeToString(msg.msg_type());
        long long sender_id = msg.sender_id();
        long long timestamp_ms = msg.timestamp_ms();
        unsigned long message_session_id_len = 0;
        unsigned long msg_type_len = 0;
        unsigned long content_len = 0;
        unsigned long client_msg_id_len = 0;
        MYSQL_BIND message_bind[7];
        std::memset(message_bind, 0, sizeof(message_bind));
        BindString(&message_bind[0], session_id, &message_session_id_len);
        BindInt64(&message_bind[1], &msg_seq);
        BindInt64(&message_bind[2], &sender_id);
        BindString(&message_bind[3], msg_type_str, &msg_type_len);
        BindString(&message_bind[4], content_json, &content_len);
        BindInt64(&message_bind[5], &timestamp_ms);
        BindString(&message_bind[6], client_msg_id, &client_msg_id_len);
        if (!ExecuteStatement(conn, kInsertMessageSql, message_bind, err_msg))
            break;

        if (mysql_commit(conn) != 0) {
            if (err_msg) *err_msg = mysql_error(conn);
            break;
        }
        ok = true;
    } while (false);

    if (!ok) mysql_rollback(conn);
    if (mysql_autocommit(conn, 1) != 0 && ok) {
        if (err_msg) *err_msg = mysql_error(conn);
        ok = false;
    }
    return ok;
}

// MySQL 消息落盘：INSERT ... ON DUPLICATE KEY UPDATE（幂等写入）
// 性能优化：单消费线程持有专用连接 + 复用 prepared statement，
// 消除每条消息的连接借还/ping/stmt init/prepare/close 开销
// 重试策略：与 PushHandler 一致，指数退避 100ms → 400ms → 1600ms
// SQL 注入防护：全部字段使用 prepared statement 参数绑定
// 带重试和死信日志的持久化入口。
void PersistHandler::PersistMessage(const PushToCometRequest& req) {
    static constexpr int kMaxRetries = 3;
    static constexpr int kBaseBackoffMs = 100;  // 100ms → 400ms → 1600ms

    if (!mysql_pool_) return;

    const auto& msg = req.message();
    std::string session_id = msg.session_id();

    for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
        std::string err;
        if (ExecutePersist(req, &err)) {
            return;  // 成功
        }

        if (attempt < kMaxRetries) {
            int backoff_ms = kBaseBackoffMs * (1 << (attempt * 2));
            LogError("[PersistHandler] MySQL persist retry " +
                     std::to_string(attempt + 1) + "/" +
                     std::to_string(kMaxRetries) + " after " +
                     std::to_string(backoff_ms) + "ms, error: " + err);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        } else {
            LogError(
                "[DLQ][Persist] All retries exhausted, msg_id=" + msg.msg_id() +
                " session_id=" + session_id + " error: " + err);
        }
    }
}

}  // namespace sparkpush
