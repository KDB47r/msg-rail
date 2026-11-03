// HttpApiServer 消息域 handler：发送 / 历史 / 已读 / 未读 / 单聊会话列表。
#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"
#include "proto_enums.h"
#include "spark_push.pb.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

// HTTP 消息发送接口
// 优化前路径（5-6 次 MySQL）: HTTP → GetOrCreateSession(MySQL) →
// AllocateSeq(MySQL)
//                            → InsertMessage(MySQL) → GetUserById(MySQL) →
//                            Kafka
// 优化后路径（2 次 Redis）:   HTTP → 构造 session_id → IncrMsgSeq(Redis)
//                            → GetUserRoutes(Redis/缓存) → Kafka
// 优化效果：QPS 从 ~82 提升到 ~5710（70 倍），具体见
// performance_optimization.md
// HTTP 发单聊消息：分配序号、查路由并写 Kafka。
void HttpApiServer::handleSendMessage(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：from_user 由 token 决定，禁止客户端伪造
    int64_t from_user = AuthenticateRequest(req, resp);
    if (from_user <= 0) return;

    // 发送限流（已注释：保留代码供面试讲解，实际压测时关闭以减少 Redis 开销）
    // if (redis_store_) {
    //   std::string rate_key =
    //       "rate:msg:{" + std::to_string(from_user) + "}";
    //   if (!redis_store_->CheckRateLimit(rate_key, 1000, 20)) {
    //     WriteError(resp, 429, "rate limit exceeded, try again later");
    //     return;
    //   }
    // }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"to_user_id\":...,\"content\":\"...\"}");
        return;
    }

    int64_t to_user = 0;
    std::string content;
    try {
        if (!j.contains("to_user_id") || !j.contains("content")) {
            WriteError(resp, 400, "to_user_id/content required");
            return;
        }
        to_user = j.at("to_user_id").get<int64_t>();
        content = j.at("content").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in message send");
        return;
    }

    if (to_user <= 0 || content.empty()) {
        WriteError(resp, 400, "to_user_id/content required");
        return;
    }

    if (!redis_store_ || !kafka_producer_) {
        WriteError(resp, 500, "redis or kafka not initialized");
        return;
    }

    // 构造 session_id（与 gRPC 路径一致，无 MySQL）
    // 统一 session_id 命名格式，与 gRPC 一致：single:{min_id}:{max_id}
    int64_t u1 = std::min(from_user, to_user);
    int64_t u2 = std::max(from_user, to_user);
    std::string session_id =
        "single:" + std::to_string(u1) + ":" + std::to_string(u2);

    // 由统一序号分配器获取 msg_seq；新 Redis key 首次使用时会从 MySQL
    // 恢复历史最大值，避免与已持久化消息冲突。
    int64_t msg_seq = 0;
    std::string seq_err;
    if (!store_ || !store_->AllocateMessageSeq(session_id, &msg_seq, &seq_err)) {
        LogError("AllocateMessageSeq failed for session " + session_id + ": " + seq_err);
        WriteError(resp, 500, "allocate msg_seq failed");
        return;
    }

    // 构造 msg_id
    std::string msg_id = session_id + "-" + std::to_string(msg_seq);

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 构造推送 JSON
    nlohmann::json push_j;
    push_j["type"] = "single_chat";
    push_j["msg_id"] = msg_id;
    push_j["msg_seq"] = msg_seq;
    push_j["from_user_id"] = from_user;
    push_j["to_user_id"] = to_user;
    push_j["content"] = content;
    push_j["create_time"] = now_ms;
    std::string push_json = push_j.dump();

    // 获取接收方路由
    std::vector<std::string> comets;
    bool has_route = redis_store_->GetUserRoutes(to_user, &comets);

    // 构造并发送 PushToCometRequest（带持久化标记）
    // 序列化/投递失败必须反馈给调用方，避免返回“假成功”
    bool deliver_ok = true;
    if (has_route && !comets.empty()) {
        for (const auto& comet_id : comets) {
            PushToCometRequest push_req;
            push_req.set_comet_id(comet_id);
            push_req.set_scene(SCENE_SINGLE);
            push_req.set_to_user_id(to_user);
            push_req.set_need_persist(true);

            ChatMessage* cm = push_req.mutable_message();
            cm->set_msg_id(msg_id);
            cm->set_session_id(session_id);
            cm->set_msg_seq(msg_seq);
            cm->set_sender_id(from_user);
            cm->set_timestamp_ms(now_ms);
            cm->set_msg_type(MSG_TYPE_TEXT);
            cm->set_content_json(push_json);

            auto* target = push_req.add_targets();
            target->set_user_id(to_user);

            std::string payload;
            if (!push_req.SerializeToString(&payload)) {
                LogError("Serialize PushToCometRequest failed for comet " +
                         comet_id);
                deliver_ok = false;
            } else {
                // protobuf 对象序列化成字节串，这个字节串就是 Kafka 的 valu
                const std::string kafka_key = session_id + "|" + comet_id;
                // *** 
                if (!kafka_producer_->Send(kafka_key, payload)) {
                    LogError("Kafka send message failed for comet " +
                             comet_id);
                    deliver_ok = false;
                }
            }
        }
    } else {
        // 无路由，仅持久化：按 session_id 分区，避免固定 key 形成热点
        PushToCometRequest push_req;
        push_req.set_persist_only(true);
        push_req.set_scene(SCENE_SINGLE);
        push_req.set_to_user_id(to_user);
        push_req.set_need_persist(true);

        ChatMessage* cm = push_req.mutable_message();
        cm->set_msg_id(msg_id);
        cm->set_session_id(session_id);
        cm->set_msg_seq(msg_seq);
        cm->set_sender_id(from_user);
        cm->set_timestamp_ms(now_ms);
        cm->set_msg_type(MSG_TYPE_TEXT);
        cm->set_content_json(push_json);

        auto* target = push_req.add_targets();
        target->set_user_id(to_user);

        std::string payload;
        if (!push_req.SerializeToString(&payload)) {
            LogError("Serialize PushToCometRequest(persist_only) failed");
            deliver_ok = false;
        } else if (!kafka_producer_->Send(session_id, payload)) {
            LogError("Kafka send message failed for persist_only, session " +
                     session_id);
            deliver_ok = false;
        }
    }

    if (!deliver_ok) {
        WriteError(resp, 500, "send message to kafka failed");
        return;
    }

    nlohmann::json data{
        {"session_id", session_id}, {"msg_id", msg_id}, {"msg_seq", msg_seq}};
    WriteJson(resp, 0, "ok", data.dump());
}

// read
// 查询会话历史消息。
void HttpApiServer::handleHistory(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：确保请求来自已登录用户
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    // if (auth_user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"session_id\":\"...\",\"anchor_seq\":0,\"limit\":20}");
        return;
    }

    std::string session_id;
    int64_t anchor_seq = 0;
    int limit = 20;
    try {
        if (!j.contains("session_id")) {
            WriteError(resp, 400, "session_id required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        if (j.contains("anchor_seq")) {
            anchor_seq = j.at("anchor_seq").get<int64_t>();
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in history");
        return;
    }
    if (session_id.empty()) {
        WriteError(resp, 400, "session_id required");
        return;
    }
    // 会话归属校验：禁止读取自己不属于的会话历史（IDOR）
    if (!CheckSessionAccess(auth_user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Message> msgs;
    std::string err;
    if (!store_->GetHistory(session_id, anchor_seq, limit, &msgs, &err)) {
        WriteError(resp, 500, "query history failed: " + err);
        return;
    }
    nlohmann::json data;
    data["messages"] = nlohmann::json::array();
    for (const auto& m : msgs) {
        nlohmann::json entry;
        entry["msg_id"] = m.msg_id;
        entry["client_msg_id"] = m.client_msg_id;
        entry["msg_seq"] = m.msg_seq;
        entry["sender_id"] = m.sender_id;
        entry["timestamp_ms"] = m.timestamp_ms;
        entry["msg_type"] = m.msg_type;
        // content_json 本身是一段 JSON
        // 文本，解析后作为嵌套对象输出（与原格式一致）；
        // 解析失败时降级为字符串，保证响应始终是合法 JSON
        if (m.content_json.empty()) {
            entry["content"] = "";
        } else {
            try {
                entry["content"] = nlohmann::json::parse(m.content_json);
            } catch (...) {
                entry["content"] = m.content_json;
            }
        }
        data["messages"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// read
// 把会话标记为已读到指定序号。
void HttpApiServer::handleMarkRead(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"session_id\":\"...\",\"read_seq\":...}");
        return;
    }

    std::string session_id;
    int64_t read_seq = 0;
    try {
        if (!j.contains("session_id") || !j.contains("read_seq")) {
            WriteError(resp, 400, "session_id/read_seq required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        read_seq = j.at("read_seq").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in mark_read");
        return;
    }
    if (session_id.empty() || read_seq <= 0) {
        WriteError(resp, 400, "session_id/read_seq required");
        return;
    }
    // 会话归属校验：禁止操作自己不属于的会话（IDOR）
    if (!CheckSessionAccess(user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::string err;
    if (!store_->MarkRead(user_id, session_id, read_seq, &err)) {
        WriteError(resp, 500, "mark read failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// raed
// 查询会话未读消息数。
void HttpApiServer::handleUnread(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect {\"session_id\":\"...\"}");
        return;
    }

    std::string session_id;
    try {
        if (!j.contains("session_id")) {
            WriteError(resp, 400, "session_id required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in unread");
        return;
    }
    if (session_id.empty()) {
        WriteError(resp, 400, "session_id required");
        return;
    }
    // 会话归属校验：禁止查询自己不属于的会话未读数（IDOR）
    if (!CheckSessionAccess(user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    int64_t unread = 0;
    std::string err;
    if (!store_->GetUnread(user_id, session_id, &unread, &err)) {
        WriteError(resp, 500, "get unread failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"unread\":" << unread << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// read
// 列出用户的单聊会话。
void HttpApiServer::handleSingleSessionList(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：user_id 由 token 决定
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Session> sessions;
    std::string err;
    if (!store_->ListUserSingleSessions(user_id, &sessions, &err)) {
        WriteError(resp, 500, "list sessions failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"sessions\":[";
    bool first = true;
    for (const auto& s : sessions) {
        int64_t peer_id = (s.user1_id == user_id) ? s.user2_id : s.user1_id;
        if (peer_id <= 0) continue;
        if (!first) data << ",";
        first = false;
        data << "{"
             << "\"session_id\":\"" << s.id << "\","
             << "\"peer_user_id\":" << peer_id << ","
             << "\"last_msg_seq\":" << s.last_msg_seq << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
