// 好友申请 / 好友列表 / 打开单聊 handler。
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"
#include "model.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

namespace {

size_t Utf8CharCount(const std::string& value) {
    size_t count = 0;
    for (unsigned char c : value) {
        if ((c & 0xC0) != 0x80) ++count;
    }
    return count;
}

UserBrief FallbackBrief(int64_t user_id) {
    UserBrief brief;
    brief.user_id = user_id;
    return brief;
}

}  // namespace

// 发送好友申请。
void HttpApiServer::handleFriendRequest(const HttpRequest& req,
                                        HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t from_user_id = AuthenticateRequest(req, resp);
    if (from_user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    int64_t to_user_id = 0;
    std::string message;
    try {
        if (!j.contains("to_user_id")) {
            WriteError(resp, 400, "to_user_id required");
            return;
        }
        to_user_id = j["to_user_id"].get<int64_t>();
        if (j.contains("message") && !j["message"].is_null()) {
            if (!j["message"].is_string()) {
                WriteError(resp, 400, "message must be a string");
                return;
            }
            message = j["message"].get<std::string>();
        }
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid friend request fields");
        return;
    }
    if (to_user_id <= 0) {
        WriteError(resp, 400, "to_user_id required");
        return;
    }
    if (to_user_id == from_user_id) {
        WriteError(resp, 400, "cannot add yourself");
        return;
    }
    if (Utf8CharCount(message) > 100) {
        WriteError(resp, 400, "message exceeds 100 characters");
        return;
    }
    if (!user_dao_ || !friend_service_) {
        WriteError(resp, 500, "user service not initialized");
        return;
    }

    User target;
    std::string err;
    if (!user_dao_->GetUserById(to_user_id, &target, &err)) {
        if (err == "user not found") {
            WriteError(resp, 404, "user not found");
        } else {
            WriteError(resp, 500, "query user failed: " + err);
        }
        return;
    }

    std::string relationship;
    int64_t existing_request_id = 0;
    if (!friend_service_->GetRelation(from_user_id, to_user_id, &relationship,
                                      &existing_request_id, &err)) {
        WriteError(resp, 500, "query relationship failed: " + err);
        return;
    }
    if (relationship == "friend") {
        WriteError(resp, 409, "already friends");
        return;
    }
    if (relationship == "pending_out") {
        nlohmann::json data{{"request_id", existing_request_id},
                            {"status", "pending"}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    if (relationship == "pending_in") {
        WriteError(resp, 409, "incoming request pending",
                   nlohmann::json{{"request_id", existing_request_id}}.dump());
        return;
    }

    int64_t request_id = 0;
    std::string status;
    if (!friend_service_->SendRequest(from_user_id, to_user_id, message,
                                      &request_id, &status, &err)) {
        if (err == "incoming request pending") {
            WriteError(resp, 409, "incoming request pending",
                       nlohmann::json{{"request_id", request_id}}.dump());
            return;
        }
        WriteError(resp, 500, "send friend request failed: " + err);
        return;
    }
    nlohmann::json data{{"request_id", request_id}, {"status", status}};
    WriteJson(resp, 0, "ok", data.dump());
}

// 同意/拒绝好友申请。
void HttpApiServer::handleFriendRespond(const HttpRequest& req,
                                        HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    int64_t request_id = 0;
    std::string action;
    try {
        if (!j.contains("request_id") || !j.contains("action")) {
            WriteError(resp, 400, "request_id/action required");
            return;
        }
        request_id = j["request_id"].get<int64_t>();
        action = j["action"].get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid friend respond fields");
        return;
    }
    if (request_id <= 0 || (action != "accept" && action != "reject")) {
        WriteError(resp, 400, "request_id/action required");
        return;
    }
    if (!friend_service_) {
        WriteError(resp, 500, "friend service not initialized");
        return;
    }

    int64_t from_user_id = 0;
    std::string err;
    if (!friend_service_->RespondRequest(user_id, request_id,
                                         action == "accept", &from_user_id,
                                         &err)) {
        if (err == "request not found") {
            WriteError(resp, 404, "request not found");
        } else if (err == "not request recipient") {
            WriteError(resp, 403, "not request recipient");
        } else if (err == "request already handled") {
            WriteError(resp, 409, "request already handled");
        } else {
            WriteError(resp, 500, "respond friend request failed: " + err);
        }
        return;
    }
    nlohmann::json data{
        {"request_id", request_id},
        {"status", action == "accept" ? "accepted" : "rejected"}};
    WriteJson(resp, 0, "ok", data.dump());
}

// 好友列表，按建立时间倒序。
void HttpApiServer::handleFriendList(const HttpRequest& req,
                                     HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!friend_service_) {
        WriteError(resp, 500, "friend service not initialized");
        return;
    }

    std::vector<FriendInfo> friends;
    std::string err;
    if (!friend_service_->ListFriends(user_id, &friends, &err)) {
        WriteError(resp, 500, "list friends failed: " + err);
        return;
    }

    nlohmann::json data;
    data["friends"] = nlohmann::json::array();
    for (const auto& info : friends) {
        User user;
        user.id = info.user_id;
        user.account = info.account;
        user.name = info.name;
        user.avatar_media_id = info.avatar_media_id;
        UserBrief brief;
        BuildUserBrief(media_dao_, media_cache_, s3_client_,
                       media_variant_cdn_base_url_,
                       media_download_url_ttl_seconds_,
                       user, &brief);
        nlohmann::json entry = UserBriefToJson(brief, false);
        entry["created_at"] = info.created_at_ms / 1000;
        data["friends"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 申请列表：incoming 只含 pending，outgoing 也含 pending。
void HttpApiServer::handleFriendRequests(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!friend_service_ || !user_dao_) {
        WriteError(resp, 500, "friend service not initialized");
        return;
    }

    std::vector<FriendRequest> incoming;
    std::vector<FriendRequest> outgoing;
    std::string err;
    if (!friend_service_->ListIncomingRequests(user_id, &incoming, &err)) {
        WriteError(resp, 500, "list incoming requests failed: " + err);
        return;
    }
    if (!friend_service_->ListOutgoingRequests(user_id, &outgoing, &err)) {
        WriteError(resp, 500, "list outgoing requests failed: " + err);
        return;
    }

    // 批量取申请双方资料，避免每个申请单独查一次用户表。
    std::vector<int64_t> user_ids;
    std::unordered_set<int64_t> seen;
    for (const auto& request : incoming) {
        if (seen.insert(request.from_user_id).second) {
            user_ids.push_back(request.from_user_id);
        }
    }
    for (const auto& request : outgoing) {
        if (seen.insert(request.to_user_id).second) {
            user_ids.push_back(request.to_user_id);
        }
    }
    std::vector<User> users;
    if (!user_ids.empty() &&
        !user_dao_->ListUsersByIds(user_ids, &users, &err)) {
        WriteError(resp, 500, "load request users failed: " + err);
        return;
    }
    std::unordered_map<int64_t, User> user_map;
    for (auto& user : users) user_map[user.id] = std::move(user);

    auto brief_for = [&](int64_t id) {
        auto it = user_map.find(id);
        if (it == user_map.end()) return FallbackBrief(id);
        UserBrief brief;
        BuildUserBrief(media_dao_, media_cache_, s3_client_,
                       media_variant_cdn_base_url_,
                       media_download_url_ttl_seconds_,
                       it->second, &brief);
        return brief;
    };

    nlohmann::json data;
    data["incoming"] = nlohmann::json::array();
    for (const auto& request : incoming) {
        nlohmann::json entry;
        entry["request_id"] = request.id;
        entry["from_user"] = UserBriefToJson(
            brief_for(request.from_user_id), false);
        entry["message"] = request.message;
        entry["created_at"] = request.created_at_ms / 1000;
        data["incoming"].push_back(std::move(entry));
    }
    data["outgoing"] = nlohmann::json::array();
    for (const auto& request : outgoing) {
        nlohmann::json entry;
        entry["request_id"] = request.id;
        entry["to_user"] =
            UserBriefToJson(brief_for(request.to_user_id), false);
        entry["status"] = "pending";
        entry["created_at"] = request.created_at_ms / 1000;
        data["outgoing"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 待处理申请数，供侧边栏角标轮询。
void HttpApiServer::handleFriendPendingCount(const HttpRequest& req,
                                             HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!friend_service_) {
        WriteError(resp, 500, "friend service not initialized");
        return;
    }
    int64_t count = 0;
    std::string err;
    if (!friend_service_->CountIncomingRequests(user_id, &count, &err)) {
        WriteError(resp, 500, "count pending requests failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"pending_count", count}}.dump());
}

// 打开/创建单聊。只有好友才能建立会话。
void HttpApiServer::handleOpenSingleSession(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    int64_t peer_user_id = 0;
    try {
        if (!j.contains("peer_user_id")) {
            WriteError(resp, 400, "peer_user_id required");
            return;
        }
        peer_user_id = j["peer_user_id"].get<int64_t>();
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid peer_user_id");
        return;
    }
    if (peer_user_id <= 0) {
        WriteError(resp, 400, "peer_user_id required");
        return;
    }
    if (peer_user_id == user_id) {
        WriteError(resp, 400, "cannot chat with yourself");
        return;
    }
    if (!user_dao_ || !friend_service_ || !store_) {
        WriteError(resp, 500, "service not initialized");
        return;
    }

    User peer;
    std::string err;
    if (!user_dao_->GetUserById(peer_user_id, &peer, &err)) {
        if (err == "user not found") {
            WriteError(resp, 404, "user not found");
        } else {
            WriteError(resp, 500, "query user failed: " + err);
        }
        return;
    }
    bool is_friend = false;
    if (!friend_service_->AreFriends(user_id, peer_user_id, &is_friend,
                                     &err)) {
        WriteError(resp, 500, "check friendship failed: " + err);
        return;
    }
    if (!is_friend) {
        WriteError(resp, 403, "only friends can start a single chat");
        return;
    }

    Session session;
    if (!store_->GetOrCreateSingleSession(user_id, peer_user_id, &session,
                                          &err)) {
        WriteError(resp, 500, "open session failed: " + err);
        return;
    }
    int64_t unread = 0;
    if (!store_->GetUnread(user_id, session.id, &unread, &err)) {
        WriteError(resp, 500, "get unread failed: " + err);
        return;
    }

    UserBrief brief;
    BuildUserBrief(media_dao_, media_cache_, s3_client_,
                       media_variant_cdn_base_url_,
                       media_download_url_ttl_seconds_,
                   peer, &brief);
    nlohmann::json data;
    data["session_id"] = session.id;
    data["peer"] = UserBriefToJson(brief, false);
    data["last_msg_seq"] = session.last_msg_seq;
    data["unread"] = unread;
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
