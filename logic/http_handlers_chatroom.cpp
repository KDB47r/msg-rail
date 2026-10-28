// HttpApiServer 聊天室域 handler：列表 / 加入 / 退出 / 取消订阅 / 在线人数。
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

// 列出用户已加入的聊天室。
void HttpApiServer::handleChatroomList(const HttpRequest& req,
                                       HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：user_id 由 token 决定
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!group_member_dao_) {
        WriteError(resp, 500, "group member dao not initialized");
        return;
    }
    if (!group_dao_) {
        WriteError(resp, 500, "group dao not initialized");
        return;
    }
    std::vector<int64_t> room_ids;
    std::string err;
    if (!group_member_dao_->ListUserChatrooms(user_id, &room_ids, &err)) {
        WriteError(resp, 500, "list user chatrooms failed: " + err);
        return;
    }
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    nlohmann::json data;
    data["rooms"] = nlohmann::json::array();
    for (int64_t room_id : room_ids) {
        Session s;
        if (!store_->GetOrCreateRoomSession(room_id, &s, &err)) {
            LogError("GetOrCreateRoomSession failed: " + err);
            continue;
        }

        ImGroup g;
        std::string g_err;
        std::string name = "Unknown";
        if (group_dao_->GetGroup(room_id, &g, &g_err)) {
            name = g.name;
        }

        data["rooms"].push_back({{"session_id", s.id},
                                 {"room_id", s.group_id},
                                 {"name", name},
                                 {"last_msg_seq", s.last_msg_seq}});
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 校验房间并写入成员关系。
void HttpApiServer::handleChatroomJoin(const HttpRequest& req,
                                       HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"room_id\":...}");
        return;
    }

    int64_t room_id = 0;
    try {
        if (!j.contains("room_id")) {
            WriteError(resp, 400, "room_id required");
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in chatroom join");
        return;
    }
    if (room_id <= 0) {
        WriteError(resp, 400, "room_id required");
        return;
    }
    if (!group_dao_ || !group_member_dao_) {
        WriteError(resp, 500, "group dao not initialized");
        return;
    }
    // 确认聊天室存在且类型正确
    ImGroup g;
    std::string err;
    if (!group_dao_->GetGroup(room_id, &g, &err)) {
        WriteError(resp, 404, "chatroom not found: " + err);
        return;
    }
    if (g.group_type != 1) {
        WriteError(resp, 400, "group is not chatroom");
        return;
    }
    if (!group_member_dao_->AddOrUpdateMember(room_id, user_id, 0, &err)) {
        WriteError(resp, 500, "add member failed: " + err);
        return;
    }

    WriteJson(resp, 0, "ok");
}

// 保留接口，实际离开由 WebSocket 断开触发。
void HttpApiServer::handleChatroomLeave(const HttpRequest& req,
                                        HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"room_id\":...}");
        return;
    }

    int64_t room_id = 0;
    try {
        if (!j.contains("room_id")) {
            WriteError(resp, 400, "room_id required");
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in chatroom leave");
        return;
    }
    if (room_id <= 0) {
        WriteError(resp, 400, "room_id required");
        return;
    }
    // 这里的 leave 仅表示“当前不再接收该房间的实时消息”，
    // 实际的在线状态由 WebSocket + comet/Redis 驱动。
    // 订阅关系交由 /api/chatroom/unsubscribe
    // 维护，这里不修改内存模型中的订阅集合。
    WriteJson(resp, 0, "ok");
}

// 删除用户的房间成员关系。
void HttpApiServer::handleChatroomUnsubscribe(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"room_id\":...}");
        return;
    }

    int64_t room_id = 0;
    try {
        if (!j.contains("room_id")) {
            WriteError(resp, 400, "room_id required");
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in chatroom unsubscribe");
        return;
    }
    if (room_id <= 0) {
        WriteError(resp, 400, "room_id required");
        return;
    }
    if (!group_member_dao_) {
        WriteError(resp, 500, "group member dao not initialized");
        return;
    }
    std::string err;
    if (!group_member_dao_->RemoveMember(room_id, user_id, &err)) {
        WriteError(resp, 500, "remove member failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 查询房间在线人数（Redis 优先，MySQL 兜底）。
void HttpApiServer::handleChatroomOnlineCount(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"room_id\":...}");
        return;
    }

    int64_t room_id = 0;
    try {
        if (!j.contains("room_id")) {
            WriteError(resp, 400, "room_id required");
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in chatroom online_count");
        return;
    }
    if (room_id <= 0) {
        WriteError(resp, 400, "room_id required");
        return;
    }
    int64_t count = 0;
    bool ok = false;
    if (redis_store_) {
        ok = redis_store_->GetRoomOnlineCount(room_id, &count);
    }
    if (!ok && group_member_dao_) {
        std::vector<int64_t> members;
        std::string err;
        if (group_member_dao_->ListRoomMembers(room_id, &members, &err)) {
            count = static_cast<int64_t>(members.size());
            ok = true;
        } else {
            LogError("ListRoomMembers failed when fallback online count: " +
                     err);
        }
    }
    std::ostringstream data;
    data << "{\"room_id\":" << room_id << ",\"online_count\":" << count << "}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
