// HttpApiServer 管理后台域 handler：创建聊天室 / 列出聊天室 / 广播。
#include <chrono>
#include <nlohmann/json.hpp>
#include <random>
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

// 管理员创建聊天室。
void HttpApiServer::handleAdminCreateChatroom(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 管理员鉴权：必须是 admin 用户
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;
    if (!RequireAdmin(auth_user_id, resp)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"name\":\"...\"}");
        return;
    }

    std::string name;
    int64_t owner_id = auth_user_id;  // 创建者即为管理员
    try {
        if (!j.contains("name")) {
            WriteError(resp, 400, "name required");
            return;
        }
        name = j.at("name").get<std::string>();
        // 可选覆盖 owner_id
        if (j.contains("owner_id")) {
            owner_id = j.at("owner_id").get<int64_t>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in admin create chatroom");
        return;
    }
    if (name.empty()) {
        WriteError(resp, 400, "name required");
        return;
    }
    if (!group_dao_) {
        WriteError(resp, 500, "group dao not initialized");
        return;
    }
    int64_t group_id = 0;
    std::string err;
    if (!group_dao_->CreateChatroom(name, owner_id, &group_id, &err)) {
        WriteError(resp, 500, "create chatroom failed: " + err);
        return;
    }
    nlohmann::json data{{"room_id", group_id}, {"name", name}};
    WriteJson(resp, 0, "ok", data.dump());
}

// 管理员列出所有聊天室。
void HttpApiServer::handleAdminListChatroom(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 管理员鉴权
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;
    if (!RequireAdmin(auth_user_id, resp)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect {\"offset\":0,\"limit\":20}");
        return;
    }

    int offset = 0;
    int limit = 20;
    try {
        if (j.contains("offset")) {
            offset = j.at("offset").get<int>();
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in admin list chatroom");
        return;
    }
    if (!group_dao_) {
        WriteError(resp, 500, "group dao not initialized");
        return;
    }
    std::vector<ImGroup> groups;
    std::string err;
    if (!group_dao_->ListChatrooms(offset, limit, &groups, &err)) {
        WriteError(resp, 500, "list chatrooms failed: " + err);
        return;
    }
    nlohmann::json data;
    data["rooms"] = nlohmann::json::array();
    for (const auto& g : groups) {
        data["rooms"].push_back(
            {{"room_id", g.id}, {"name", g.name}, {"owner_id", g.owner_id}});
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 管理员发送全站或房间广播。
void HttpApiServer::handleAdminBroadcast(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 管理员鉴权
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;
    if (!RequireAdmin(auth_user_id, resp)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(
            resp, 400,
            "invalid json body, expect "
            "{\"scope\":\"all|chatroom\",\"room_id\":0,\"text\":\"...\"}");
        return;
    }

    std::string scope = "all";
    int64_t room_id = 0;
    std::string text;
    try {
        if (j.contains("scope")) {
            scope = j.at("scope").get<std::string>();
        }
        if (j.contains("room_id")) {
            room_id = j.at("room_id").get<int64_t>();
        }
        if (!j.contains("text")) {
            WriteError(resp, 400, "text is required");
            return;
        }
        text = j.at("text").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in admin broadcast");
        return;
    }
    if (text.empty()) {
        WriteError(resp, 400, "text is required");
        return;
    }
    if (scope != "all" && scope != "chatroom") {
        WriteError(resp, 400, "scope must be all or chatroom");
        return;
    }
    if (scope == "chatroom" && room_id <= 0) {
        WriteError(resp, 400, "room_id required when scope=chatroom");
        return;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string task_id = [&]() {
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(0, 99999);
        return "bcast-" + std::to_string(now_ms) + "-" +
               std::to_string(dist(rng));
    }();

    std::string real_scope = (scope == "all") ? "all" : "group";
    int64_t group_id = (scope == "chatroom") ? room_id : 0;

    // 构造 JSON 内容：一个简单的系统广播文本（nlohmann 保证 text 正确转义）
    nlohmann::json content_j;
    content_j["type"] = "system_broadcast";
    content_j["scope"] = real_scope;
    if (group_id > 0) {
        content_j["group_id"] = group_id;
    }
    content_j["content"] = {{"text", text}};
    std::string content_json = content_j.dump();

    // 房间广播：与公屏相同的两层扇出——GetRoomComets + push topic + room_id。
    // 全站广播：仍走独立 broadcast_task → Job PushToAll。
    if (scope == "chatroom") {
        if (!kafka_producer_) {
            WriteError(resp, 500, "kafka push producer not initialized");
            return;
        }
        if (!redis_store_) {
            WriteError(resp, 500, "redis store not initialized");
            return;
        }

        std::vector<std::string> room_comets;
        if (!redis_store_->GetRoomComets(group_id, &room_comets)) {
            LogError("GetRoomComets failed for room broadcast room_id=" +
                     std::to_string(group_id));
            room_comets.clear();
        }

        ChatMessage cm;
        cm.set_msg_id(task_id);
        cm.set_session_id("broadcast");
        cm.set_msg_seq(0);
        cm.set_sender_id(0);
        cm.set_timestamp_ms(now_ms);
        cm.set_msg_type(MSG_TYPE_BROADCAST);
        cm.set_content_json(content_json);

        bool deliver_ok = true;
        for (const auto& comet_id : room_comets) {
            PushToCometRequest req_pb;
            req_pb.set_comet_id(comet_id);
            *req_pb.mutable_message() = cm;
            req_pb.set_room_id(group_id);
            req_pb.set_need_persist(false);

            std::string payload;
            if (!req_pb.SerializeToString(&payload)) {
                LogError("Serialize PushToCometRequest(room broadcast) failed");
                deliver_ok = false;
                continue;
            }
            const std::string kafka_key =
                "broadcast|" + std::to_string(group_id) + "|" + comet_id;
            if (!kafka_producer_->Send(kafka_key, payload)) {
                LogError("Kafka send room broadcast failed for comet " +
                         comet_id);
                deliver_ok = false;
            }
        }

        if (!deliver_ok) {
            WriteError(resp, 500, "send room broadcast to kafka failed");
            return;
        }

        nlohmann::json data{{"task_id", task_id},
                            {"scope", real_scope},
                            {"group_id", group_id},
                            {"comet_count",
                             static_cast<int>(room_comets.size())},
                            {"pushed", !room_comets.empty()}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }

    if (!broadcast_producer_) {
        WriteError(resp, 500, "broadcast producer not initialized");
        return;
    }

    BroadcastTaskRequest task;
    task.set_task_id(task_id);
    task.set_scope(BroadcastScopeFromString(real_scope));
    task.set_group_id(group_id);
    task.set_content_json(content_json);

    std::string payload;
    if (!task.SerializeToString(&payload)) {
        WriteError(resp, 500, "serialize BroadcastTaskRequest failed");
        return;
    }
    if (!broadcast_producer_->Send(task_id, payload)) {
        WriteError(resp, 500, "send broadcast task to kafka failed");
        return;
    }

    nlohmann::json data{
        {"task_id", task_id}, {"scope", real_scope}, {"group_id", group_id}};
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
