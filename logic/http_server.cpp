#include "http_server.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <nlohmann/json.hpp>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "http_server_internal.h"
#include "logging.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;

// ---- 内部共享辅助实现（声明见 http_server_internal.h） ----
namespace detail {

// CORS 允许来源：默认 "*"（行为不变），由 HttpApiServer 构造时按配置覆盖。
// 仅在启动阶段（IO 线程未开始前）写一次，之后只读，无并发问题。
std::string g_cors_allow_origin = "*";

void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", g_cors_allow_origin);
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    resp->addHeader("Access-Control-Allow-Headers",
                    "Content-Type, Authorization");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

bool ParseJsonBody(const std::string& body, nlohmann::json* out) {
    if (!out) return false;
    if (body.empty()) {
        return false;
    }
    try {
        *out = nlohmann::json::parse(body);
        return true;
    } catch (const nlohmann::json::exception& e) {
        LogError("JSON parse error: " + std::string(e.what()));
        return false;
    } catch (...) {
        return false;
    }
}

bool ParseJsonAccountPassword(const std::string& body, std::string* account,
                              std::string* password) {
    if (!account || !password) return false;
    *account = "";
    *password = "";

    try {
        auto j = nlohmann::json::parse(body);
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        return true;
    } catch (const nlohmann::json::exception& e) {
        LogError("JSON parse error: " + std::string(e.what()));
        return false;
    } catch (...) {
        return false;
    }
}

bool ParseJsonRegister(const std::string& body, std::string* account,
                       std::string* password, std::string* name) {
    if (!account || !password || !name) return false;
    *account = "";
    *password = "";
    *name = "";

    try {
        auto j = nlohmann::json::parse(body);
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        if (j.contains("name") && j["name"].is_string()) {
            *name = j["name"].get<std::string>();
        }
        // 如果没传 name，默认用 account
        if (name->empty()) {
            *name = *account;
        }
        return true;
    } catch (...) {
        return false;
    }
}

// 生成 24 字节随机 token，并转成 48 位十六进制字符串。
std::string GenerateSecureToken() {
    static constexpr int kTokenBytes = 24;  // 192 bit → 32 chars hex
    unsigned char buf[kTokenBytes];
    std::ifstream urandom("/dev/urandom", std::ios::binary);
    if (urandom.good()) {
        urandom.read(reinterpret_cast<char*>(buf), kTokenBytes);
    } else {
        // 降级：使用 std::random_device
        std::random_device rd;
        for (int i = 0; i < kTokenBytes; i += 4) {
            uint32_t val = rd();
            std::memcpy(buf + i, &val, std::min(4, kTokenBytes - i));
        }
    }
    static const char hex[] = "0123456789abcdef";
    std::string token;
    token.reserve(kTokenBytes * 2);
    for (int i = 0; i < kTokenBytes; ++i) {
        token.push_back(hex[buf[i] >> 4]);
        token.push_back(hex[buf[i] & 0x0f]);
    }
    return token;
}

HttpResponse::HttpStatusCode HttpStatusForCode(int code) {
    // 将 body 里的业务 code 映射为语义正确的 HTTP 状态行。
    // http_server_lite 的 setStatusCode 有 int 重载，405/409 直接透传。
    switch (code) {
        case 0:
            return HttpResponse::k200Ok;
        case 400:
            return HttpResponse::k400BadRequest;
        case 401:
            return HttpResponse::k401Unauthorized;
        case 403:
            return HttpResponse::k403Forbidden;
        case 404:
            return HttpResponse::k404NotFound;
        case 500:
            return HttpResponse::k500InternalServerError;
        default:
            return static_cast<HttpResponse::HttpStatusCode>(code);
    }
}

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json,
               HttpResponse::HttpStatusCode http_code) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    // 所有 JSON API 统一打上 CORS 头，方便在 9010/其他端口的前端页面直接调用
    AddCORSHeaders(resp);
    // 使用 nlohmann::json 构造响应体，保证 message 中的引号/反斜杠等被正确转义
    nlohmann::json body;
    body["code"] = code;
    body["message"] = message;
    try {
        // data_json 是调用方构造的一段 JSON 文本，解析后作为嵌套对象输出，
        // 保持对外结构不变（data 仍是对象/数组而非字符串）
        body["data"] = nlohmann::json::parse(data_json);
    } catch (...) {
        // 容错：不是合法 JSON 时降级为字符串输出，避免整个响应损坏
        body["data"] = data_json;
    }
    resp->setBody(body.dump());
}

}  // namespace detail

HttpApiServer::HttpApiServer(uint16_t port, ConversationStore* store,
                             UserDao* user_dao, GroupDao* group_dao,
                             GroupMemberDao* group_member_dao,
                             RedisStore* redis_store,
                             KafkaProducer* kafka_producer,
                             KafkaProducer* broadcast_producer,
                             DanmakuDao* danmaku_dao, const Config& config)
    : store_(store),
      user_dao_(user_dao),
      group_dao_(group_dao),
      group_member_dao_(group_member_dao),
      redis_store_(redis_store),
      kafka_producer_(kafka_producer),
      broadcast_producer_(broadcast_producer),
      danmaku_dao_(danmaku_dao),
      admin_account_(config.admin_account),
      admin_password_hash_(config.admin_password_hash),
      admin_user_id_(config.admin_user_id),
      server_(port, "logic_http_server") {
    detail::g_cors_allow_origin = config.cors_allow_origin;
    server_.setHttpCallback([this](const HttpRequest& req, HttpResponse* resp) {
        this->onRequest(req, resp);
    });
}

// HTTP 路由入口：按路径分发到具体处理器。
void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);

    // 处理浏览器的 CORS 预检请求（OPTIONS），无需走具体业务路由
    if (req.method() == HttpRequest::kOptions) {
        // 对于预检请求，只需要返回 200 + CORS 头即可
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        detail::AddCORSHeaders(resp);
        return;
    }

    // 路由表：从 path 映射到成员函数指针
    static const std::unordered_map<std::string, Handler> kRouteTable = {
        {"/api/login", &HttpApiServer::handleLogin},
        {"/api/register", &HttpApiServer::handleRegister},
        {"/api/message/send", &HttpApiServer::handleSendMessage},
        {"/api/session/history", &HttpApiServer::handleHistory},
        {"/api/session/mark_read", &HttpApiServer::handleMarkRead},
        {"/api/session/unread", &HttpApiServer::handleUnread},
        {"/api/chatroom/join", &HttpApiServer::handleChatroomJoin},
        {"/api/chatroom/leave", &HttpApiServer::handleChatroomLeave},
        {"/api/chatroom/unsubscribe",
         &HttpApiServer::handleChatroomUnsubscribe},
        {"/api/chatroom/online_count",
         &HttpApiServer::handleChatroomOnlineCount},
        {"/api/danmaku/send", &HttpApiServer::handleDanmakuSend},
        {"/api/danmaku/list", &HttpApiServer::handleDanmakuList},
        {"/api/session/list_single", &HttpApiServer::handleSingleSessionList},
        {"/api/chatroom/list", &HttpApiServer::handleChatroomList},
        {"/api/admin/chatroom/create",
         &HttpApiServer::handleAdminCreateChatroom},
        {"/api/admin/chatroom/list", &HttpApiServer::handleAdminListChatroom},
        {"/api/admin/broadcast", &HttpApiServer::handleAdminBroadcast},
    };

    const std::string& path = req.path();

    // ---- 运维端点（无需鉴权） ----
    if (path == "/health") {
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("application/json; charset=utf-8");
        detail::AddCORSHeaders(resp);
        // 检查关键依赖是否可用
        bool redis_ok = (redis_store_ != nullptr);
        bool store_ok = (store_ != nullptr);
        std::string status = (redis_ok && store_ok) ? "ok" : "degraded";
        resp->setBody("{\"status\":\"" + status +
                      "\""
                      ",\"redis\":" +
                      std::string(redis_ok ? "true" : "false") + ",\"store\":" +
                      std::string(store_ok ? "true" : "false") + "}");
        return;
    }
    if (path == "/metrics") {
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        detail::AddCORSHeaders(resp);
        // Prometheus-compatible 简易指标（可扩展）
        std::ostringstream m;
        m << "# HELP sparkpush_up Whether the service is up\n"
          << "# TYPE sparkpush_up gauge\n"
          << "sparkpush_up 1\n";
        resp->setBody(m.str());
        return;
    }

    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        WriteError(resp, 404, "unknown path");
        return;
    }

    Handler handler = it->second;
    (this->*handler)(req, resp);
}

// ---- handler 通用样板辅助 ----
// 输出统一格式的错误 JSON。
void HttpApiServer::WriteError(HttpResponse* resp, int code,
                               const std::string& message,
                               const std::string& data_json) {
    detail::WriteJson(resp, code, message, data_json,
                      detail::HttpStatusForCode(code));
}

// 限制接口只接受 POST 请求。
bool HttpApiServer::RequirePost(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteError(resp, 405, "only POST allowed");
        return false;
    }
    return true;
}

// 校验当前用户是否为管理员。
bool HttpApiServer::RequireAdmin(int64_t uid, HttpResponse* resp) {
    if (uid != admin_user_id_) {
        WriteError(resp, 403, "admin privilege required");
        return false;
    }
    return true;
}

// ---- 统一 Token 鉴权 ----
// 从 Authorization: Bearer <token> 头提取 token，降级到 JSON body 的 "token"
// 字段。 校验成功返回 user_id (>0)；失败则写入 401 响应并返回 0。
// 从请求头或 body 解析 token 并返回 user_id。
int64_t HttpApiServer::AuthenticateRequest(const HttpRequest& req,
                                           HttpResponse* resp) {
    std::string token;

    // 1) 优先从 Authorization 头获取
    std::string auth_header = req.getHeader("Authorization");
    if (auth_header.size() > 7 && auth_header.substr(0, 7) == "Bearer ") {
        token = auth_header.substr(7);
    }

    // 2) 降级：从 JSON body 的 "token" 字段获取
    if (token.empty()) {
        try {
            auto j = nlohmann::json::parse(req.body());
            if (j.contains("token") && j["token"].is_string()) {
                token = j["token"].get<std::string>();
            }
        } catch (...) {
            // body 不是合法 JSON 或无 token 字段，忽略
        }
    }

    if (token.empty()) {
        WriteError(resp, 401,
                   "authentication required: provide Authorization header or "
                   "token field");
        return 0;
    }

    if (!redis_store_) {
        WriteError(resp, 500, "redis store not initialized");
        return 0;
    }

    int64_t user_id = 0;
    if (!redis_store_->GetUserIdByToken(token, &user_id) || user_id <= 0) {
        WriteError(resp, 401, "invalid or expired token");
        return 0;
    }

    return user_id;
}

// ---- 会话归属校验（防止越权 IDOR） ----
// session_id 格式（与消息发送路径保持一致，见 handleSendMessage /
// grpc_service）：
//   single:<a>:<b>   单聊，a/b 为两个 user_id（小的在前）
//   room:<gid>       聊天室，gid 为 im_group.id
//   danmaku:<vid>    弹幕会话，按视频公开
// 检查用户是否有权访问指定会话。
bool HttpApiServer::CheckSessionAccess(int64_t user_id,
                                       const std::string& session_id,
                                       HttpResponse* resp) {
    static const std::string kSinglePrefix = "single:";
    static const std::string kRoomPrefix = "room:";
    static const std::string kDanmakuPrefix = "danmaku:";

    // 单聊：解析出两个 user_id，请求者必须是其中之一
    if (session_id.compare(0, kSinglePrefix.size(), kSinglePrefix) == 0) {
        const std::string rest = session_id.substr(kSinglePrefix.size());
        const auto sep = rest.find(':');
        int64_t a = 0;
        int64_t b = 0;
        bool parsed = false;
        if (sep != std::string::npos) {
            try {
                a = std::stoll(rest.substr(0, sep));
                b = std::stoll(rest.substr(sep + 1));
                parsed = (a > 0 && b > 0);
            } catch (...) {
                parsed = false;
            }
        }
        if (!parsed) {
            WriteError(resp, 400, "invalid session_id format");
            return false;
        }
        if (user_id == a || user_id == b) {
            return true;
        }
        WriteError(resp, 403, "forbidden: not a session member");
        return false;
    }

    // 聊天室：查成员表确认归属关系
    if (session_id.compare(0, kRoomPrefix.size(), kRoomPrefix) == 0) {
        int64_t room_id = 0;
        try {
            room_id = std::stoll(session_id.substr(kRoomPrefix.size()));
        } catch (...) {
            room_id = 0;
        }
        if (room_id <= 0) {
            WriteError(resp, 400, "invalid session_id format");
            return false;
        }
        if (!group_member_dao_) {
            WriteError(resp, 500, "group member dao not initialized");
            return false;
        }
        std::vector<int64_t> members;
        std::string err;
        if (!group_member_dao_->ListRoomMembers(room_id, &members, &err)) {
            WriteError(resp, 500, "check session membership failed: " + err);
            return false;
        }
        if (std::find(members.begin(), members.end(), user_id) !=
            members.end()) {
            return true;
        }
        WriteError(resp, 403, "forbidden: not a session member");
        return false;
    }

    // 弹幕会话：按视频公开，任何已登录用户都可以拉取
    if (session_id.compare(0, kDanmakuPrefix.size(), kDanmakuPrefix) == 0) {
        return true;
    }

    // 未知格式的 session_id：拒绝访问，避免绕过校验
    WriteError(resp, 403, "forbidden: not a session member");
    return false;
}

}  // namespace sparkpush
