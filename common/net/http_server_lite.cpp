#include "http_server_lite.h"

#include <llhttp.h>

#include <algorithm>
#include <asio.hpp>
#include <atomic>
#include <cstring>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include "logging.h"

namespace sparkpush::net {

namespace {

// 把字符串转成小写，用于 HTTP header 名的大小写不敏感比较。
std::string ToLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return s;
}

// 把 HTTP 状态码转换成原因短语。
const char* ReasonPhrase(int code) {
    switch (code) {
        case 200:
            return "OK";
        case 206:
            return "Partial Content";
        case 301:
            return "Moved Permanently";
        case 400:
            return "Bad Request";
        case 401:
            return "Unauthorized";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 409:
            return "Conflict";
        case 416:
            return "Range Not Satisfiable";
        case 500:
            return "Internal Server Error";
        default:
            return "OK";
    }
}

// 把 llhttp 的方法常量转换成 HttpRequest::Method。
HttpRequest::Method MapMethod(uint8_t m) {
    switch (m) {
        case HTTP_GET:
            return HttpRequest::kGet;
        case HTTP_POST:
            return HttpRequest::kPost;
        case HTTP_HEAD:
            return HttpRequest::kHead;
        case HTTP_PUT:
            return HttpRequest::kPut;
        case HTTP_DELETE:
            return HttpRequest::kDelete;
        case HTTP_OPTIONS:
            return HttpRequest::kOptions;
        default:
            return HttpRequest::kInvalid;
    }
}

}  // namespace

// read
// 按 header 名大小写不敏感地查找并返回第一个值。
std::string HttpRequest::getHeader(const std::string& name) const {
    const std::string lname = ToLower(name);
    for (const auto& kv : headers_) {
        if (ToLower(kv.first) == lname) return kv.second;
    }
    return {};
}

// ---- Session: 单连接读循环 + llhttp 增量解析 + keep-alive ----
struct HttpServerLite::Impl
    : public std::enable_shared_from_this<HttpServerLite::Impl> {
    asio::io_context io_ctx;
    asio::ip::tcp::acceptor acceptor;
    HttpCallback callback;
    std::string name;
    int threads{1};
    std::atomic<bool> stopped{false};

    // 创建监听 socket，绑定端口并开始 listen。
    Impl(uint16_t port, const std::string& n) : acceptor(io_ctx), name(n) {
        asio::ip::tcp::endpoint ep(asio::ip::tcp::v4(), port);
        acceptor.open(ep.protocol());
        acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor.bind(ep);
        acceptor.listen();
    }
};

namespace {

// llhttp 每请求上下文，附着在 llhttp_t::data 上。
struct ParseCtx {
    HttpRequest req;
    std::string cur_header_field;
    std::string cur_header_value;
    bool complete{false};
    bool keep_alive{false};
};

// llhttp 回调：新请求开始，清空上一个请求的解析状态。
int OnMessageBegin(llhttp_t* p) {
    auto* c = static_cast<ParseCtx*>(p->data);
    c->req.clear();
    c->cur_header_field.clear();
    c->cur_header_value.clear();
    c->complete = false;
    return 0;
}

// llhttp 回调：保存请求路径，并去掉 query 部分。
int OnUrl(llhttp_t* p, const char* at, size_t len) {
    auto* c = static_cast<ParseCtx*>(p->data);
    // 只截取 path 部分（去掉 query），logic 现有 handler 只用 path()。
    std::string full(at, len);
    auto qpos = full.find('?');
    if (qpos != std::string::npos) full.resize(qpos);
    c->req.set_path(std::move(full));
    return 0;
}

// llhttp 回调：累积 header 名，遇到新字段时提交上一个 header。
int OnHeaderField(llhttp_t* p, const char* at, size_t len) {
    auto* c = static_cast<ParseCtx*>(p->data);
    if (!c->cur_header_value.empty()) {
        c->req.add_header(c->cur_header_field, c->cur_header_value);
        c->cur_header_field.clear();
        c->cur_header_value.clear();
    }
    c->cur_header_field.append(at, len);
    return 0;
}

// llhttp 回调：累积 header 值。
int OnHeaderValue(llhttp_t* p, const char* at, size_t len) {
    auto* c = static_cast<ParseCtx*>(p->data);
    c->cur_header_value.append(at, len);
    return 0;
}

// llhttp 回调：提交最后一个 header，并设置请求方法。
int OnHeadersComplete(llhttp_t* p) {
    auto* c = static_cast<ParseCtx*>(p->data);
    if (!c->cur_header_field.empty()) {
        c->req.add_header(c->cur_header_field, c->cur_header_value);
        c->cur_header_field.clear();
        c->cur_header_value.clear();
    }
    c->req.set_method(MapMethod(p->method));
    return 0;
}

// llhttp 回调：把请求体片段追加到 HttpRequest。
int OnBody(llhttp_t* p, const char* at, size_t len) {
    auto* c = static_cast<ParseCtx*>(p->data);
    auto b = c->req.body();
    b.append(at, len);
    c->req.set_body(std::move(b));
    return 0;
}

// llhttp 回调：标记请求解析完成，并记录是否保持连接。
int OnMessageComplete(llhttp_t* p) {
    auto* c = static_cast<ParseCtx*>(p->data);
    c->keep_alive = llhttp_should_keep_alive(p) != 0;
    c->complete = true;
    return 0;
}

// 把 HttpResponse 序列化成完整的 HTTP/1.1 响应报文。
std::string BuildResponse(const HttpResponse& resp, bool keep_alive) {
    std::ostringstream oss;
    int code = resp.status_code() ? resp.status_code() : 200;
    oss << "HTTP/1.1 " << code << ' ' << ReasonPhrase(code) << "\r\n";
    if (!resp.content_type().empty()) {
        oss << "Content-Type: " << resp.content_type() << "\r\n";
    }
    bool has_content_length = false;
    for (const auto& kv : resp.headers()) {
        oss << kv.first << ": " << kv.second << "\r\n";
        if (ToLower(kv.first) == "content-length") has_content_length = true;
    }
    if (!has_content_length) {
        oss << "Content-Length: " << resp.body().size() << "\r\n";
    }
    oss << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n";
    oss << "\r\n";
    oss << resp.body();
    return oss.str();
}

class Session : public std::enable_shared_from_this<Session> {
   public:
    // 初始化 llhttp 解析器并绑定各解析回调。
    Session(asio::ip::tcp::socket sock, HttpCallback cb)
        : socket_(std::move(sock)), callback_(std::move(cb)) {
        llhttp_settings_init(&settings_);
        settings_.on_message_begin = OnMessageBegin;
        settings_.on_url = OnUrl;
        settings_.on_header_field = OnHeaderField;
        settings_.on_header_value = OnHeaderValue;
        settings_.on_headers_complete = OnHeadersComplete;
        settings_.on_body = OnBody;
        settings_.on_message_complete = OnMessageComplete;
        llhttp_init(&parser_, HTTP_REQUEST, &settings_);
        parser_.data = &ctx_;
    }

    // 开始读取该连接上的 HTTP 请求。
    void Start() { DoRead(); }

   private:
    // 异步读 socket，把数据交给 llhttp 解析并处理完整请求。
    void DoRead() {
        auto self = shared_from_this();
        socket_.async_read_some(
            asio::buffer(buf_),
            [this, self](std::error_code ec, std::size_t n) {
                if (ec) return;
                auto err = llhttp_execute(&parser_, buf_.data(), n);
                if (err != HPE_OK) {
                    HttpResponse resp;
                    resp.setStatusCode(HttpResponse::k400BadRequest);
                    resp.setContentType("text/plain; charset=utf-8");
                    resp.setBody("Bad Request");
                    WriteAndClose(BuildResponse(resp, /*keep_alive=*/false));
                    return;
                }
                if (ctx_.complete) {
                    HttpResponse resp;
                    //**** 
                    if (callback_) callback_(ctx_.req, &resp);
                    bool ka = ctx_.keep_alive;
                    std::string out = BuildResponse(resp, ka);
                    ctx_.req.clear();
                    ctx_.complete = false;
                    WriteThen(std::move(out), ka);
                    return;
                }
                DoRead();
            });
    }

    // 异步写响应；keep-alive 时继续读下一个请求，否则关闭连接。
    void WriteThen(std::string data, bool keep_alive) {
        auto self = shared_from_this();
        auto buf = std::make_shared<std::string>(std::move(data));
        asio::async_write(
            socket_, asio::buffer(*buf),
            [this, self, buf, keep_alive](std::error_code ec, std::size_t) {
                if (ec || !keep_alive) {
                    std::error_code ig;
                    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ig);
                    return;
                }
                DoRead();
            });
    }

    // 异步写完响应后关闭连接。
    void WriteAndClose(std::string data) {
        auto self = shared_from_this();
        auto buf = std::make_shared<std::string>(std::move(data));
        asio::async_write(socket_, asio::buffer(*buf),
                          [this, self, buf](std::error_code, std::size_t) {
                              std::error_code ig;
                              socket_.shutdown(
                                  asio::ip::tcp::socket::shutdown_both, ig);
                          });
    }

    asio::ip::tcp::socket socket_;
    HttpCallback callback_;
    llhttp_t parser_{};
    llhttp_settings_t settings_{};
    ParseCtx ctx_{};
    std::array<char, 8192> buf_{};
};

// 递归接受新连接，为每个连接创建 Session 并开始读取。
void DoAccept(HttpServerLite::Impl& impl) {
    impl.acceptor.async_accept([&impl](std::error_code ec,
                                       asio::ip::tcp::socket sock) {
        // 有新连接时创建 Session；随后继续接受下一个连接。
        if (!ec) {
            std::make_shared<Session>(std::move(sock), impl.callback)->Start();
        }
        if (!impl.stopped.load(std::memory_order_relaxed)) {
            DoAccept(impl);
        }
    });
}

}  // namespace

// 创建 HTTP 服务器实现并绑定监听端口。
HttpServerLite::HttpServerLite(uint16_t port, const std::string& name)
    : impl_(new Impl(port, name)) {}

// 析构时停止服务器并关闭监听 socket。
HttpServerLite::~HttpServerLite() { stop(); }

// 设置业务请求处理回调。
void HttpServerLite::setHttpCallback(HttpCallback cb) {
    impl_->callback = std::move(cb);
}

// 设置 IO 线程数，至少保留 1 个线程。
void HttpServerLite::setThreadNum(int n) { impl_->threads = n < 1 ? 1 : n; }

// 启动 accept，并在当前线程运行 io_context，直到 stop 被调用。
void HttpServerLite::run() {
    DoAccept(*impl_);
    std::vector<std::thread> workers;
    for (int i = 1; i < impl_->threads; ++i) {
        workers.emplace_back([this] { impl_->io_ctx.run(); });
    }
    impl_->io_ctx.run();
    for (auto& t : workers)
        if (t.joinable()) t.join();
}

// 停止接收新连接，并停止所有 IO 线程。
void HttpServerLite::stop() {
    if (!impl_) return;
    impl_->stopped.store(true, std::memory_order_relaxed);
    std::error_code ec;
    impl_->acceptor.close(ec);
    impl_->io_ctx.stop();
}

}  // namespace sparkpush::net
