#pragma once

// 内部共享辅助（仅供 HttpApiServer 各翻译单元使用）。
// 这些工具原本以匿名命名空间形式集中在 http_server.cpp 中，为了把庞大的
// handler 实现按业务域拆分到多个 .cpp，这里统一声明在一个内部头里，
// 由 http_server.cpp 提供唯一定义，其余 http_handlers_*.cpp 复用。
//
// 注意：这是实现细节，不属于对外契约，放在 sparkpush::detail 命名空间下。

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>

#include "net/http_server_lite.h"

namespace sparkpush {

class MediaDao;
class S3Client;
struct User;

namespace detail {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;

// 用户公开资料视图：昵称 + 头像签名 URL，绝不包含 password_hash。
struct UserBrief {
    int64_t user_id{0};
    std::string account;
    std::string name;
    std::string avatar_media_id;
    std::string avatar_url;
    int64_t avatar_expires_at{0};
};

// 头像优先使用 thumb 变体，缺失时回退原图；S3 未就绪或媒体不可用时
// 保留 avatar_media_id，avatar_url 为空。
bool BuildUserBrief(MediaDao* media_dao, S3Client* s3, int ttl_seconds,
                    const User& user, UserBrief* out);

nlohmann::json UserBriefToJson(const UserBrief& user, bool include_account);

extern std::string g_cors_allow_origin;

void AddCORSHeaders(HttpResponse* resp);

bool ParseJsonBody(const std::string& body, nlohmann::json* out);

bool ParseJsonAccountPassword(const std::string& body, std::string* account,
                              std::string* password);

bool ParseJsonRegister(const std::string& body, std::string* account,
                       std::string* password, std::string* name);

std::string GenerateSecureToken();

HttpResponse::HttpStatusCode HttpStatusForCode(int code);

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok);

}  // namespace detail
}  // namespace sparkpush
