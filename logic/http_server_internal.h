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
namespace detail {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;

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
