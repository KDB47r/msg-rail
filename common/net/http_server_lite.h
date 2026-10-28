#pragma once

// 极简 HTTP/1.1 服务器：asio io_context + tcp::acceptor + llhttp 解析。
// Request/Response 的成员函数与项目历史 HTTP API 同名同义，业务侧 handler
// 无需修改即可接入（req.path()/methodString()/getHeader()/body(),
//  resp->setStatusCode()/setStatusMessage()/setContentType()/addHeader()/
//  setBody()/setCloseConnection()）。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace sparkpush::net {

class HttpRequest {
   public:
    enum Method { kInvalid, kGet, kPost, kHead, kPut, kDelete, kOptions };

    // 返回请求方法。
    Method method() const { return method_; }
    // 返回不含 query 的请求路径。
    const std::string& path() const { return path_; }
    // 返回请求体。
    const std::string& body() const { return body_; }
    // 按 header 名大小写不敏感地读取第一个值。
    std::string getHeader(const std::string& name) const;

    // 内部构造用（由服务器实现填充）。
    // 设置请求方法。
    void set_method(Method m) { method_ = m; }
    // 设置请求路径。
    void set_path(std::string p) { path_ = std::move(p); }
    // 设置请求体。
    void set_body(std::string b) { body_ = std::move(b); }
    // 追加一个请求 header。
    void add_header(std::string name, std::string value) {
        headers_.emplace_back(std::move(name), std::move(value));
    }
    // 清空请求，准备解析下一个请求。
    void clear() {
        method_ = kInvalid;
        path_.clear();
        body_.clear();
        headers_.clear();
    }

   private:
    Method method_{kInvalid};
    std::string path_;
    std::string body_;
    std::vector<std::pair<std::string, std::string>> headers_;
};

class HttpResponse {
   public:
    // 405/409 等未在枚举中的状态码可用 int 重载 setStatusCode(int)。
    enum HttpStatusCode {
        k200Ok = 200,
        k400BadRequest = 400,
        k401Unauthorized = 401,
        k403Forbidden = 403,
        k404NotFound = 404,
        k500InternalServerError = 500,
    };

    // 用枚举设置 HTTP 状态码。
    void setStatusCode(HttpStatusCode c) { status_ = static_cast<int>(c); }
    // 用整数设置 HTTP 状态码，支持枚举外的状态码。
    void setStatusCode(int c) { status_ = c; }
    // 设置响应 Content-Type。
    void setContentType(const std::string& v) { content_type_ = v; }
    // 追加一个响应 header。
    void addHeader(const std::string& name, const std::string& value) {
        headers_.emplace_back(name, value);
    }
    // 设置响应体（拷贝）。
    void setBody(const std::string& b) { body_ = b; }
    // 设置响应体（移动）。
    void setBody(std::string&& b) { body_ = std::move(b); }

    // 返回 HTTP 状态码。
    int status_code() const { return status_; }
    // 返回响应 Content-Type。
    const std::string& content_type() const { return content_type_; }
    // 返回响应体。
    const std::string& body() const { return body_; }
    // 返回全部响应 header。
    const std::vector<std::pair<std::string, std::string>>& headers() const {
        return headers_;
    }

   private:
    int status_{200};
    std::string content_type_;
    std::string body_;
    std::vector<std::pair<std::string, std::string>> headers_;
};

using HttpCallback = std::function<void(const HttpRequest&, HttpResponse*)>;

class HttpServerLite {
   public:
    struct Impl;

    // 创建服务器并绑定监听端口。
    HttpServerLite(uint16_t port, const std::string& name);
    // 析构时停止服务器并关闭监听 socket。
    ~HttpServerLite();

    HttpServerLite(const HttpServerLite&) = delete;
    HttpServerLite& operator=(const HttpServerLite&) = delete;

    // 设置业务请求处理回调。
    void setHttpCallback(HttpCallback cb);
    // 设置 IO 线程数（>=1，包含 accept 线程本身）。
    void setThreadNum(int n);

    // 启动并阻塞在 io_context 上，返回时表示服务已停止。
    void run();
    // 停止接收新连接并停止所有 IO 线程。
    void stop();

   private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace sparkpush::net
