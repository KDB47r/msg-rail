// HttpApiServer 认证域 handler：登录 / 注册。
#include <nlohmann/json.hpp>
#include <string>

#include "http_server.h"
#include "http_server_internal.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

// read
// 处理登录：校验账号密码并签发 24 小时 token。
void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
    // 商用接口：只允许 POST，参数通过 JSON body 传递
    if (!RequirePost(req, resp)) return;

    if (req.body().empty()) {
        WriteError(resp, 400, "empty body");
        return;
    }

    std::string account;
    std::string password;
    if (!ParseJsonAccountPassword(req.body(), &account, &password)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"account\":\"...\",\"password\":\"...\"}");
        return;
    }

    // 特殊处理：预设管理员账号走内存校验，不依赖数据库中是否存在该账号。
    if (account == admin_account_) {
        if (password != admin_password_hash_) {
            WriteError(resp, 401, "invalid password");
            return;
        }
        // 生成安全随机 token
        std::string token = GenerateSecureToken();
        if (!redis_store_ ||
            !redis_store_->SetToken(token, admin_user_id_, 24 * 3600)) {
            WriteError(resp, 500, "save token to redis failed");
            return;
        }

        // 把业务结果按统一 JSON 格式填进 HTTP 响应对象 
        // 它不会立刻发网络，只是填好 resp 
        // 等 handler 返回后由 HTTP 服务器统一发送
        nlohmann::json data{{"user_id", admin_user_id_}, {"token", token}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }

    if (account.empty()) {
        WriteError(resp, 400, "account is required");
        return;
    }

    // user_dao_ 不是 HTTP 服务器自己 new 出来的
    // 而是 main.cpp 创建好之后，通过构造函数“注入”进来的

    // 这是一个防御性判空
    if (!user_dao_) {
        WriteError(resp, 500, "user dao not initialized");
        return;
    }

    User user;
    std::string err;
    if (!user_dao_->GetUserByAccount(account, &user, &err)) {
        // 严格登录模式：不存在则提示先注册，而不是自动创建
        WriteError(resp, 404, "user not found, please register first");
        return;
    }

    // 这里约定 password 已经是前端做过 MD5 的
    // hash（示例实现，真实业务还需加盐、迭代等）
    if (user.password_hash != password) {
        WriteError(resp, 401, "invalid password");
        return;
    }

    // 生成安全随机 token
    std::string token = GenerateSecureToken();
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteError(resp, 500, "save token to redis failed");
        return;
    }

    nlohmann::json data{
        {"user_id", user.id}, {"token", token}, {"name", user.name}};
    WriteJson(resp, 0, "ok", data.dump());
}

// read
// 处理注册：创建用户并直接签发 token。
void HttpApiServer::handleRegister(const HttpRequest& req, HttpResponse* resp) {
    // 商用接口：只允许 POST，参数通过 JSON body 传递
    if (!RequirePost(req, resp)) return;

    if (req.body().empty()) {
        WriteError(resp, 400, "empty body");
        return;
    }

    std::string account;
    std::string password;
    std::string name;
    if (!ParseJsonRegister(req.body(), &account, &password, &name)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"account\":\"...\",\"password\":\"...\"}");
        return;
    }

    if (account.empty() || password.empty()) {
        WriteError(resp, 400, "account/password are required");
        return;
    }

    // 预设管理员账号保留，禁止通过注册接口覆盖/创建同名账号。
    if (account == admin_account_) {
        WriteError(resp, 409,
                   "admin account is reserved, please use login directly");
        return;
    }

    if (!user_dao_) {
        WriteError(resp, 500, "user dao not initialized");
        return;
    }

    User existing;
    std::string err;
    if (user_dao_->GetUserByAccount(account, &existing, &err)) {
        WriteError(resp, 409, "account already exists");
        return;
    } else if (!err.empty() && err != "user not found") {
        // 数据库真实错误
        WriteError(resp, 500, "query user failed: " + err);
        return;
    }

    int64_t uid = 0;
    if (!user_dao_->CreateUser(account, name, password, &uid, &err)) {
        WriteError(resp, 500, "register user failed: " + err);
        return;
    }

    User user;
    user.id = uid;
    user.account = account;
    user.name = name;
    user.password_hash = password;

    // 注册成功后直接下发 token，让前端“注册即登录”
    std::string token = GenerateSecureToken();
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteError(resp, 500, "save token to redis failed");
        return;
    }

    nlohmann::json data{
        {"user_id", user.id}, {"token", token}, {"name", user.name}};
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
