// P6 AI Companion HTTP handlers: public bot controls + internal AI Worker API.
#include <nlohmann/json.hpp>
#include <string>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

namespace {

bool ParseSessionId(const nlohmann::json& j, std::string* session_id,
                    HttpResponse* resp) {
    if (!j.contains("session_id") || !j["session_id"].is_string()) {
        WriteJson(resp, 400, "session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return false;
    }
    *session_id = j["session_id"].get<std::string>();
    if (session_id->empty()) {
        WriteJson(resp, 400, "session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return false;
    }
    return true;
}

void WriteAiError(HttpResponse* resp, const std::string& err) {
    WriteJson(resp, 400, err, "{}", HttpResponse::k400BadRequest);
}

}  // namespace

void HttpApiServer::handleAiBots(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json data;
    data["private_bot"] = {
        {"user_id", ai_private_bot_user_id_},
        {"account", "ai_assistant"},
        {"name", "小桁"},
        {"mode", "private"},
    };
    data["shared_bot"] = {
        {"user_id", ai_shared_bot_user_id_},
        {"account", "ai_vibe"},
        {"name", "小桁"},
        {"mode", "shared"},
    };
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAiInvite(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string session_id;
    if (!ParseSessionId(j, &session_id, resp)) return;
    std::string err;
    if (!ai_service_->Invite(session_id, user_id, &err)) {
        WriteAiError(resp, err);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"session_id", session_id},
                             {"status", "pending"}}
                  .dump());
}

void HttpApiServer::handleAiRespond(const HttpRequest& req,
                                    HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string session_id;
    if (!ParseSessionId(j, &session_id, resp)) return;
    if (!j.contains("accept") || !j["accept"].is_boolean()) {
        WriteError(resp, 400, "accept boolean required");
        return;
    }
    const bool accept = j["accept"].get<bool>();
    std::string err;
    if (!ai_service_->Respond(session_id, user_id, accept, &err)) {
        WriteAiError(resp, err);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"session_id", session_id},
                             {"status", accept ? "active" : "rejected"}}
                  .dump());
}

void HttpApiServer::handleAiRemove(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string session_id;
    if (!ParseSessionId(j, &session_id, resp)) return;
    std::string err;
    if (!ai_service_->Remove(session_id, user_id, &err)) {
        WriteAiError(resp, err);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"session_id", session_id},
                             {"status", "removed"}}
                  .dump());
}

void HttpApiServer::handleAiStatus(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string session_id;
    if (!ParseSessionId(j, &session_id, resp)) return;
    nlohmann::json data;
    std::string err;
    if (!ai_service_->GetStatus(session_id, user_id, &data, &err)) {
        WriteAiError(resp, err);
        return;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAiStop(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("job_id") || !j["job_id"].is_string()) {
        WriteError(resp, 400, "job_id required");
        return;
    }
    const std::string job_id = j["job_id"].get<std::string>();
    std::string err;
    if (!ai_service_->Stop(job_id, user_id, &err)) {
        WriteAiError(resp, err);
        return;
    }
    WriteJson(resp, 0, "ok", nlohmann::json{{"job_id", job_id}}.dump());
}

void HttpApiServer::handleInternalAiContext(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    if (!RequireInternalAuth(req, resp)) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("job_id") || !j["job_id"].is_string()) {
        WriteError(resp, 400, "job_id required");
        return;
    }
    nlohmann::json data;
    std::string err;
    if (!ai_service_->GetContext(j["job_id"].get<std::string>(), &data,
                                 &err)) {
        WriteJson(resp, 409, err, "{}", static_cast<HttpResponse::HttpStatusCode>(409));
        return;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleInternalAiDelta(const HttpRequest& req,
                                          HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    if (!RequireInternalAuth(req, resp)) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("job_id") || !j["job_id"].is_string()) {
        WriteError(resp, 400, "job_id required");
        return;
    }
    std::string delta;
    if (j.contains("delta") && j["delta"].is_string()) {
        delta = j["delta"].get<std::string>();
    }
    std::string err;
    if (!ai_service_->PushDelta(j["job_id"].get<std::string>(), delta, &err)) {
        WriteJson(resp, 409, err, "{}", static_cast<HttpResponse::HttpStatusCode>(409));
        return;
    }
    WriteJson(resp, 0, "ok");
}

void HttpApiServer::handleInternalAiFinalize(const HttpRequest& req,
                                             HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    if (!RequireInternalAuth(req, resp)) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("job_id") || !j["job_id"].is_string() ||
        !j.contains("content") || !j["content"].is_string()) {
        WriteError(resp, 400, "job_id/content required");
        return;
    }
    std::string err;
    if (!ai_service_->Finalize(j["job_id"].get<std::string>(),
                               j["content"].get<std::string>(), &err)) {
        WriteJson(resp, 409, err, "{}", static_cast<HttpResponse::HttpStatusCode>(409));
        return;
    }
    WriteJson(resp, 0, "ok");
}

void HttpApiServer::handleInternalAiFail(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    if (!RequireInternalAuth(req, resp)) return;
    if (!ai_service_) {
        WriteError(resp, 500, "ai service not initialized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body");
        return;
    }
    if (!j.contains("job_id") || !j["job_id"].is_string()) {
        WriteError(resp, 400, "job_id required");
        return;
    }
    const std::string code =
        (j.contains("code") && j["code"].is_string())
            ? j["code"].get<std::string>()
            : "AI_FAILED";
    const std::string message =
        (j.contains("message") && j["message"].is_string())
            ? j["message"].get<std::string>()
            : "AI 回复失败";
    std::string err;
    if (!ai_service_->Fail(j["job_id"].get<std::string>(), code, message,
                           &err)) {
        WriteJson(resp, 409, err, "{}", static_cast<HttpResponse::HttpStatusCode>(409));
        return;
    }
    WriteJson(resp, 0, "ok");
}

}  // namespace sparkpush
