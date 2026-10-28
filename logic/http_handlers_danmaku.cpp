// HttpApiServer 弹幕域 handler：发送 / 拉取列表。
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
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

// HTTP 发弹幕：写 video_danmaku、消息表并推送房间。
void HttpApiServer::handleDanmakuSend(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    if (!kafka_producer_) {
        WriteError(resp, 500, "kafka producer not initialized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"user_id\":...,\"video_id\":\"...\",\"timeline_ms\":...,"
                   "\"text\":\"...\"}");
        return;
    }

    // 统一鉴权：通过 token 确定 user_id，禁止客户端伪造
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    int64_t timeline_ms = 0;
    std::string video_id;
    std::string text;
    try {
        if (!j.contains("video_id") || !j.contains("timeline_ms") ||
            !j.contains("text")) {
            WriteError(resp, 400, "video_id/timeline_ms/text required");
            return;
        }
        timeline_ms = j.at("timeline_ms").get<int64_t>();
        video_id = j.at("video_id").get<std::string>();
        text = j.at("text").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in danmaku send");
        return;
    }

    if (video_id.empty() || text.empty()) {
        WriteError(resp, 400, "video_id/text required");
        return;
    }

    // 对 demo 而言，用固定的 room_id 承载该视频的弹幕聊天室。
    // 如果未来支持多视频，可约定 video_id 与 room_id 的映射策略。
    const int64_t kDanmakuRoomId = 1001;

    // 构造下行给前端的 JSON 内容（用 nlohmann::json 保证 video_id/text
    // 正确转义）
    nlohmann::json content_j;
    content_j["type"] = "danmaku";
    content_j["video_id"] = video_id;
    content_j["timeline_ms"] = timeline_ms;
    content_j["content"] = {{"text", text}};
    std::string content_json = content_j.dump();

    // 写入 MySQL 的 video_danmaku 表（如果已配置）
    if (danmaku_dao_) {
        int64_t row_id = 0;
        std::string err;
        if (!danmaku_dao_->InsertDanmaku(video_id, timeline_ms, user_id,
                                         content_json, &row_id, &err)) {
            // 中：写库失败只打日志，不中断实时推送
            LogError("InsertDanmaku failed: " + err);
        }
    }

    // 写入内存模型（会话 & 消息），并更新 Redis 中的 last_seq
    Session session;
    std::string err;
    if (!store_->GetOrCreateRoomSession(kDanmakuRoomId, &session, &err)) {
        WriteError(resp, 500, "get room session failed: " + err);
        return;
    }
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    Message msg;
    if (!store_->AppendMessage(session, user_id, "danmaku", content_json,
                               now_ms,
                               "",  // client_msg_id (HTTP 暂不透传)
                               &msg, &err)) {
        WriteError(resp, 500, "append danmaku failed: " + err);
        return;
    }

    // 构造 ChatMessage
    ChatMessage cm;
    cm.set_msg_id(msg.msg_id);
    cm.set_session_id(msg.session_id);
    cm.set_msg_seq(msg.msg_seq);
    cm.set_sender_id(msg.sender_id);
    cm.set_timestamp_ms(msg.timestamp_ms);
    cm.set_msg_type(MsgTypeFromString(msg.msg_type));
    cm.set_content_json(msg.content_json);

    // 计算需要推送到哪些 comet
    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::vector<std::string> room_comets;
    bool use_room_comets = false;
    if (redis_store_ &&
        redis_store_->GetRoomComets(kDanmakuRoomId, &room_comets) &&
        !room_comets.empty()) {
        use_room_comets = true;
    }

    if (use_room_comets) {
        // 仅按 comet 维度 fanout，targets 留空，由 comet 自己按房间成员二次分发
        for (const auto& cid : room_comets) {
            comet_to_users[cid];  // 占位
        }
    } else {
        // 回退：根据房间成员+用户路由做精确 fanout
        std::vector<int64_t> members;
        if (group_member_dao_) {
            if (!group_member_dao_->ListRoomMembers(kDanmakuRoomId, &members,
                                                    &err)) {
                LogError("ListRoomMembers(danmaku) failed: " + err);
            }
        }
        for (int64_t uid : members) {
            std::vector<std::string> comets;
            if (!redis_store_ || !redis_store_->GetUserRoutes(uid, &comets)) {
                continue;
            }
            for (const auto& cid : comets) {
                comet_to_users[cid].push_back(uid);
            }
        }
    }

    if (comet_to_users.empty()) {
        // 没有在线用户，返回成功但不推送
        nlohmann::json data{{"msg_id", msg.msg_id}, {"pushed", false}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }

    // 通过 Kafka 将 PushToCometRequest 写入 push topic
    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;

        PushToCometRequest req_pb;
        req_pb.set_comet_id(comet_id);
        *req_pb.mutable_message() = cm;
        for (int64_t uid : users) {
            auto* t = req_pb.add_targets();
            t->set_user_id(uid);
        }

        std::string payload;
        if (!req_pb.SerializeToString(&payload)) {
            LogError("Serialize PushToCometRequest(danmaku) failed");
            continue;
        }
        const std::string kafka_key = msg.session_id + "|" + comet_id;
        if (!kafka_producer_->Send(kafka_key, payload)) {
            LogError("Kafka send danmaku failed for comet " + comet_id);
        }
    }

    nlohmann::json data{{"msg_id", msg.msg_id}, {"pushed", true}};
    WriteJson(resp, 0, "ok", data.dump());
}

// 按视频和时间范围查询弹幕。
void HttpApiServer::handleDanmakuList(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }

    std::string video_id;
    try {
        if (!j.contains("video_id")) {
            WriteError(resp, 400, "video_id required");
            return;
        }
        video_id = j.at("video_id").get<std::string>();
    } catch (...) {
        WriteError(resp, 400, "invalid json fields");
        return;
    }

    if (video_id.empty()) {
        WriteError(resp, 400, "video_id cannot be empty");
        return;
    }

    // 可选参数：按时间范围和数量限制分页
    int64_t from_ms = 0;
    int64_t to_ms = 0;
    int limit = 2000;  // 单次最多返回 2000 条，避免一次性拉取过多
    try {
        if (j.contains("from_ms")) {
            from_ms = j.at("from_ms").get<int64_t>();
            if (from_ms < 0) from_ms = 0;
        }
        if (j.contains("to_ms")) {
            to_ms = j.at("to_ms").get<int64_t>();
            if (to_ms < 0) to_ms = 0;
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (...) {
        WriteError(resp, 400, "invalid from_ms/to_ms/limit");
        return;
    }

    if (!danmaku_dao_) {
        WriteError(resp, 500, "danmaku dao not initialized");
        return;
    }

    std::vector<DanmakuItem> list;
    std::string err;
    if (!danmaku_dao_->ListDanmaku(video_id, &list, &err, from_ms, to_ms,
                                   limit)) {
        WriteError(resp, 500, "list danmaku failed: " + err);
        return;
    }

    nlohmann::json resp_j;
    resp_j["danmaku"] = nlohmann::json::array();
    for (const auto& item : list) {
        std::string text = "";
        try {
            auto jj = nlohmann::json::parse(item.content_json);
            if (jj.contains("content") && jj["content"].contains("text")) {
                text = jj["content"]["text"].get<std::string>();
            }
        } catch (...) {
            // 单条弹幕 content_json 损坏时降级为空文本（text 保持 ""），
            // 不影响其余弹幕返回；此处刻意不逐条记日志以避免刷屏。
        }

        nlohmann::json entry;
        entry["timeline_ms"] = item.timeline_ms;
        entry["text"] = text;
        resp_j["danmaku"].push_back(entry);
    }

    // 方便前端做增量拉取的简单标记
    resp_j["from_ms"] = from_ms;
    resp_j["to_ms"] = to_ms;
    resp_j["limit"] = limit;
    resp_j["has_more"] = (static_cast<int>(list.size()) >= limit && limit > 0);

    WriteJson(resp, 0, "ok", resp_j.dump());
}

}  // namespace sparkpush
