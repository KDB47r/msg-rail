// 用户资料、头像视图与昵称搜索 handler。
#include <algorithm>
#include <cctype>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"
#include "media_dao.h"
#include "media_id.h"
#include "model.h"
#include "s3_client.h"

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

// 仅去掉首尾 ASCII 空白；昵称内部的普通空格保留。
std::string TrimAscii(const std::string& value) {
    size_t begin = 0;
    size_t end = value.size();
    while (begin < end &&
           std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        ++begin;
    }
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(begin, end - begin);
}

bool HasControlChar(const std::string& value) {
    for (unsigned char c : value) {
        if (c < 0x20 || c == 0x7f) return true;
    }
    return false;
}

bool IsDigits(const std::string& value) {
    if (value.empty()) return false;
    for (unsigned char c : value) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

// 输出当前用户/目标用户的完整资料（不包含 password_hash）。
nlohmann::json BuildProfileJson(const UserBrief& brief, bool is_self,
                                const std::string& relationship) {
    nlohmann::json data = UserBriefToJson(brief, is_self);
    data["is_self"] = is_self;
    data["relationship"] = relationship;
    return data;
}

}  // namespace

namespace detail {

bool BuildUserBrief(MediaDao* media_dao, S3Client* s3, int ttl_seconds,
                    const User& user, UserBrief* out) {
    if (!out) return false;
    out->user_id = user.id;
    out->account = user.account;
    out->name = user.name;
    out->avatar_media_id = user.avatar_media_id;
    out->avatar_url.clear();
    out->avatar_expires_at = 0;

    if (user.avatar_media_id.empty() || !media_dao || !s3 || !s3->ready()) {
        return true;
    }

    MediaObject media;
    std::string err;
    if (!media_dao->GetById(user.avatar_media_id, &media, &err)) {
        LogWarn("avatar media not found: " + user.avatar_media_id + " " + err);
        return true;
    }
    if (media.owner_id != user.id || media.kind != "image" ||
        media.status != "ready") {
        return true;
    }

    std::string object_key = media.object_key;
    std::vector<MediaVariant> variants;
    if (media_dao->ListVariants(media.media_id, &variants, &err)) {
        for (const auto& variant : variants) {
            if (variant.variant == "thumb" && !variant.object_key.empty()) {
                object_key = variant.object_key;
                break;
            }
        }
    }
    if (object_key.empty()) return true;

    out->avatar_url =
        s3->PresignGet(object_key, ttl_seconds, &out->avatar_expires_at);
    return true;
}

nlohmann::json UserBriefToJson(const UserBrief& user, bool include_account) {
    nlohmann::json j;
    j["user_id"] = user.user_id;
    if (include_account) j["account"] = user.account;
    j["name"] = user.name;
    j["avatar_media_id"] = user.avatar_media_id;
    j["avatar_url"] = user.avatar_url;
    j["avatar_expires_at"] = user.avatar_expires_at;
    return j;
}

}  // namespace detail

// 查询用户公开资料。user_id 省略/为 0 时查自己。
void HttpApiServer::handleUserProfileGet(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t viewer_id = AuthenticateRequest(req, resp);
    if (viewer_id <= 0) return;

    int64_t target_id = viewer_id;
    if (!req.body().empty()) {
        nlohmann::json j;
        if (!ParseJsonBody(req.body(), &j)) {
            WriteError(resp, 400, "invalid json body");
            return;
        }
        try {
            if (j.contains("user_id") && !j["user_id"].is_null()) {
                target_id = j["user_id"].get<int64_t>();
            }
        } catch (const nlohmann::json::exception&) {
            WriteError(resp, 400, "invalid user_id");
            return;
        }
        if (target_id <= 0) target_id = viewer_id;
    }

    if (!user_dao_) {
        WriteError(resp, 500, "user dao not initialized");
        return;
    }
    User user;
    std::string err;
    if (!user_dao_->GetUserById(target_id, &user, &err)) {
        if (err == "user not found") {
            WriteError(resp, 404, "user not found");
        } else {
            WriteError(resp, 500, "query user failed: " + err);
        }
        return;
    }

    const bool is_self = (target_id == viewer_id);
    std::string relationship = "self";
    if (!is_self) {
        if (!friend_service_) {
            WriteError(resp, 500, "friend service not initialized");
            return;
        }
        int64_t request_id = 0;
        if (!friend_service_->GetRelation(viewer_id, target_id, &relationship,
                                          &request_id, &err)) {
            WriteError(resp, 500, "query relationship failed: " + err);
            return;
        }
    }

    UserBrief brief;
    BuildUserBrief(media_dao_, s3_client_, media_download_url_ttl_seconds_,
                   user, &brief);
    WriteJson(resp, 0, "ok",
              BuildProfileJson(brief, is_self, relationship).dump());
}

// 修改昵称和/或头像。两个字段至少传一个；未传的字段保持原值。
void HttpApiServer::handleUserProfileUpdate(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j) || !j.is_object()) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("name") && !j.contains("avatar_media_id")) {
        WriteError(resp, 400, "name or avatar_media_id required");
        return;
    }
    if (!user_dao_) {
        WriteError(resp, 500, "user dao not initialized");
        return;
    }

    User user;
    std::string err;
    if (!user_dao_->GetUserById(user_id, &user, &err)) {
        if (err == "user not found") {
            WriteError(resp, 404, "user not found");
        } else {
            WriteError(resp, 500, "query user failed: " + err);
        }
        return;
    }

    std::string new_name = user.name;
    if (j.contains("name")) {
        if (!j["name"].is_string()) {
            WriteError(resp, 400, "name must be a string");
            return;
        }
        new_name = TrimAscii(j["name"].get<std::string>());
        if (new_name.empty()) {
            WriteError(resp, 400, "name cannot be empty");
            return;
        }
        if (Utf8CharCount(new_name) > 32) {
            WriteError(resp, 400, "name exceeds 32 characters");
            return;
        }
        if (HasControlChar(new_name)) {
            WriteError(resp, 400, "name contains control characters");
            return;
        }
    }

    std::string new_avatar = user.avatar_media_id;
    if (j.contains("avatar_media_id")) {
        if (!j["avatar_media_id"].is_string()) {
            WriteError(resp, 400, "avatar_media_id must be a string");
            return;
        }
        new_avatar = j["avatar_media_id"].get<std::string>();
        if (!new_avatar.empty()) {
            if (!IsValidMediaId(new_avatar)) {
                WriteError(resp, 400, "invalid avatar_media_id");
                return;
            }
            if (!media_dao_) {
                WriteError(resp, 500, "media dao not initialized");
                return;
            }
            MediaObject media;
            if (!media_dao_->GetById(new_avatar, &media, &err)) {
                WriteError(resp, 404, "avatar media not found");
                return;
            }
            if (media.owner_id != user_id) {
                WriteError(resp, 403, "forbidden: not avatar owner");
                return;
            }
            if (media.kind != "image") {
                WriteError(resp, 409, "avatar must be an image");
                return;
            }
            if (media.status != "ready") {
                WriteError(resp, 409,
                           "avatar is not ready, current status: " +
                               media.status);
                return;
            }
            if (!media.session_id.empty() || !media.message_id.empty()) {
                WriteError(resp, 409,
                           "avatar media has already been used as a message");
                return;
            }
            if (media_max_image_bytes_ > 0 &&
                media.size_bytes > media_max_image_bytes_) {
                WriteError(resp, 409, "avatar exceeds image size limit");
                return;
            }
        }
    }

    if (!user_dao_->UpdateProfile(user_id, new_name, new_avatar, &err)) {
        WriteError(resp, 500, "update profile failed: " + err);
        return;
    }
    user.name = new_name;
    user.avatar_media_id = new_avatar;

    UserBrief brief;
    BuildUserBrief(media_dao_, s3_client_, media_download_url_ttl_seconds_,
                   user, &brief);
    WriteJson(resp, 0, "ok", BuildProfileJson(brief, true, "self").dump());
}

// 搜索用户：纯数字按 UserID 精确查询，否则按昵称精确匹配。
void HttpApiServer::handleUserSearch(const HttpRequest& req,
                                     HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t viewer_id = AuthenticateRequest(req, resp);
    if (viewer_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string keyword;
    int limit = 20;
    int offset = 0;
    try {
        if (!j.contains("keyword") || !j["keyword"].is_string()) {
            WriteError(resp, 400, "keyword required");
            return;
        }
        keyword = TrimAscii(j["keyword"].get<std::string>());
        if (j.contains("limit") && !j["limit"].is_null()) {
            limit = j["limit"].get<int>();
        }
        if (j.contains("offset") && !j["offset"].is_null()) {
            offset = j["offset"].get<int>();
        }
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid search fields");
        return;
    }
    if (keyword.empty()) {
        WriteError(resp, 400, "keyword cannot be empty");
        return;
    }
    if (Utf8CharCount(keyword) > 64) {
        WriteError(resp, 400, "keyword exceeds 64 characters");
        return;
    }
    if (limit < 1) limit = 1;
    if (limit > 50) limit = 50;
    if (offset < 0) offset = 0;

    if (!user_dao_) {
        WriteError(resp, 500, "user dao not initialized");
        return;
    }
    if (!friend_service_) {
        WriteError(resp, 500, "friend service not initialized");
        return;
    }

    std::vector<User> users;
    std::string match_type = "nickname";
    std::string err;
    if (IsDigits(keyword)) {
        int64_t user_id = 0;
        try {
            user_id = std::stoll(keyword);
        } catch (...) {
            user_id = 0;
        }
        match_type = "user_id";
        if (user_id > 0) {
            User user;
            if (user_dao_->GetUserById(user_id, &user, &err)) {
                users.push_back(std::move(user));
            } else if (err != "user not found") {
                WriteError(resp, 500, "search user failed: " + err);
                return;
            }
        }
    } else {
        if (!user_dao_->SearchUsersByName(keyword, limit, offset, &users,
                                          &err)) {
            WriteError(resp, 500, "search users failed: " + err);
            return;
        }
    }

    nlohmann::json data;
    data["keyword"] = keyword;
    data["match_type"] = match_type;
    data["users"] = nlohmann::json::array();
    for (const auto& user : users) {
        std::string relationship = "none";
        int64_t request_id = 0;
        if (user.id == viewer_id) {
            relationship = "self";
        } else if (!friend_service_->GetRelation(viewer_id, user.id,
                                                 &relationship, &request_id,
                                                 &err)) {
            WriteError(resp, 500, "query relationship failed: " + err);
            return;
        }
        UserBrief brief;
        BuildUserBrief(media_dao_, s3_client_, media_download_url_ttl_seconds_,
                       user, &brief);
        nlohmann::json entry = UserBriefToJson(brief, false);
        entry["relationship"] = relationship;
        entry["request_id"] =
            (relationship == "pending_out" || relationship == "pending_in")
                ? request_id
                : 0;
        data["users"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
