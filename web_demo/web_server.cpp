// 基于 asio + llhttp 的静态文件 HTTP Server，用于加载 web_demo/static
// 下的页面。

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>

#include "logging.h"
#include "net/http_server_lite.h"

using sparkpush::net::HttpRequest;
using sparkpush::net::HttpResponse;
using sparkpush::net::HttpServerLite;

namespace {

enum class ArgsParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--port <端口>] [--doc-root <目录>] "
                 "[port] [doc_root]\n"
                 "  -p, --port       指定 HTTP 监听端口 (默认 9001)\n"
                 "  -d, --doc-root   指定静态文件目录 (默认 ./static)\n",
                 prog);
}

bool ParsePortValue(const std::string& value, uint16_t* port) {
    char* end = nullptr;
    long parsed = std::strtol(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0' || parsed < 0 || parsed > 65535) {
        return false;
    }
    *port = static_cast<uint16_t>(parsed);
    return true;
}

ArgsParseResult ParseArgs(int argc, char* argv[], uint16_t* port,
                          std::string* doc_root) {
    bool port_set = false;
    bool doc_root_set = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-p" || arg == "--port") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个端口参数\n", arg.c_str());
                return ArgsParseResult::kError;
            }
            if (!ParsePortValue(argv[++i], port)) {
                std::fprintf(stderr, "非法端口号: %s\n", argv[i]);
                return ArgsParseResult::kError;
            }
            port_set = true;
        } else if (arg == "-d" || arg == "--doc-root") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个目录参数\n", arg.c_str());
                return ArgsParseResult::kError;
            }
            *doc_root = argv[++i];
            doc_root_set = true;
        } else if (arg == "-h" || arg == "--help") {
            return ArgsParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgsParseResult::kError;
        } else {
            if (!port_set) {
                if (!ParsePortValue(arg, port)) {
                    std::fprintf(stderr, "非法端口号: %s\n", arg.c_str());
                    return ArgsParseResult::kError;
                }
                port_set = true;
            } else if (!doc_root_set) {
                *doc_root = arg;
                doc_root_set = true;
            } else {
                std::fprintf(stderr, "多余的位置参数: %s\n", arg.c_str());
                return ArgsParseResult::kError;
            }
        }
    }
    return ArgsParseResult::kOk;
}

std::string GuessContentType(const std::string& path) {
    auto ends_with = [&](const char* s) {
        size_t n = std::strlen(s);
        return path.size() >= n && path.compare(path.size() - n, n, s) == 0;
    };
    if (ends_with(".html")) return "text/html; charset=utf-8";
    if (ends_with(".js")) return "application/javascript; charset=utf-8";
    if (ends_with(".css")) return "text/css; charset=utf-8";
    if (ends_with(".png")) return "image/png";
    if (ends_with(".jpg") || ends_with(".jpeg")) return "image/jpeg";
    if (ends_with(".mp4")) return "video/mp4";
    return "application/octet-stream";
}

bool ParseUnsigned(const std::string& value, uint64_t* out) {
    if (!out || value.empty()) return false;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0') return false;
    *out = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseRangeHeader(const std::string& header, uint64_t file_size,
                      uint64_t* range_start, uint64_t* range_end) {
    static const std::string kPrefix = "bytes=";
    std::string normalized = header;
    normalized.erase(
        normalized.begin(),
        std::find_if(normalized.begin(), normalized.end(),
                     [](unsigned char c) { return !std::isspace(c); }));
    normalized.erase(
        std::find_if(normalized.rbegin(), normalized.rend(),
                     [](unsigned char c) { return !std::isspace(c); })
            .base(),
        normalized.end());
    if (normalized.compare(0, kPrefix.size(), kPrefix) != 0 || file_size == 0) {
        return false;
    }

    const std::string spec = normalized.substr(kPrefix.size());
    if (spec.empty() || spec.find(',') != std::string::npos) return false;
    const auto dash = spec.find('-');
    if (dash == std::string::npos) return false;

    const std::string first = spec.substr(0, dash);
    const std::string last = spec.substr(dash + 1);
    uint64_t start = 0;
    uint64_t end = file_size - 1;

    if (first.empty()) {
        uint64_t suffix_length = 0;
        if (!ParseUnsigned(last, &suffix_length) || suffix_length == 0)
            return false;
        suffix_length = std::min(suffix_length, file_size);
        start = file_size - suffix_length;
    } else {
        if (!ParseUnsigned(first, &start) || start >= file_size) return false;
        if (!last.empty() && (!ParseUnsigned(last, &end) || end < start)) {
            return false;
        }
        end = std::min(end, file_size - 1);
    }

    *range_start = start;
    *range_end = end;
    return true;
}

std::string JoinPath(const std::string& root, const std::string& rel) {
    if (root.empty()) return rel;
    if (rel.empty()) return root;
    if (root.back() == '/') {
        return rel.front() == '/' ? root + rel.substr(1) : root + rel;
    }
    return rel.front() == '/' ? root + rel : root + "/" + rel;
}

}  // namespace

class StaticFileHandler {
   public:
    explicit StaticFileHandler(const std::string& doc_root)
        : doc_root_(doc_root) {}

    void onRequest(const HttpRequest& req, HttpResponse* resp) {
        if (req.method() != HttpRequest::kGet &&
            req.method() != HttpRequest::kHead) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setContentType("text/plain; charset=utf-8");
            resp->setBody("Method Not Allowed");
            return;
        }

        std::string path = req.path();
        if (path.empty() || path == "/") path = "/index.html";

        if (path.find("..") != std::string::npos) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setContentType("text/plain; charset=utf-8");
            resp->setBody("Forbidden");
            return;
        }

        std::string rel = path;
        if (!rel.empty() && rel.front() == '/') rel.erase(0, 1);

        std::string full_path = JoinPath(doc_root_, rel);
        std::ifstream ifs(full_path, std::ios::binary);
        if (!ifs.good()) {
            LOG_WARN << "File not found: " << full_path;
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setContentType("text/plain; charset=utf-8");
            resp->setBody("Not Found");
            return;
        }

        ifs.seekg(0, std::ios::end);
        const std::streamoff file_end = ifs.tellg();
        if (file_end < 0) {
            resp->setStatusCode(HttpResponse::k500InternalServerError);
            resp->setContentType("text/plain; charset=utf-8");
            resp->setBody("Failed to read file");
            return;
        }
        const uint64_t file_size = static_cast<uint64_t>(file_end);
        resp->setContentType(GuessContentType(full_path));
        resp->addHeader("Accept-Ranges", "bytes");

        const std::string range_header = req.getHeader("Range");
        uint64_t range_start = 0;
        uint64_t range_end = file_size == 0 ? 0 : file_size - 1;
        const bool has_range = !range_header.empty();
        if (has_range && !ParseRangeHeader(range_header, file_size,
                                           &range_start, &range_end)) {
            resp->setStatusCode(416);
            resp->addHeader("Content-Range",
                            "bytes */" + std::to_string(file_size));
            return;
        }

        if (has_range) {
            resp->setStatusCode(206);
            resp->addHeader("Content-Range",
                            "bytes " + std::to_string(range_start) + "-" +
                                std::to_string(range_end) + "/" +
                                std::to_string(file_size));
        } else {
            resp->setStatusCode(HttpResponse::k200Ok);
        }

        if (req.method() == HttpRequest::kHead) {
            const uint64_t content_size =
                file_size == 0 ? 0 : range_end - range_start + 1;
            resp->addHeader("Content-Length", std::to_string(content_size));
            return;
        }
        if (file_size == 0) return;

        const uint64_t content_size = range_end - range_start + 1;
        if (content_size >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            resp->setStatusCode(HttpResponse::k500InternalServerError);
            resp->setBody("File is too large");
            return;
        }

        std::string content(static_cast<size_t>(content_size), '\0');
        ifs.clear();
        ifs.seekg(static_cast<std::streamoff>(range_start), std::ios::beg);
        ifs.read(content.data(), static_cast<std::streamsize>(content.size()));
        if (static_cast<size_t>(ifs.gcount()) != content.size()) {
            resp->setStatusCode(HttpResponse::k500InternalServerError);
            resp->setBody("Failed to read file");
            return;
        }
        resp->setBody(std::move(content));
    }

   private:
    std::string doc_root_;
};

int main(int argc, char* argv[]) {
    sparkpush::InitLogging("web_demo", /*also_stdout=*/true);

    uint16_t port = 9001;
    std::string doc_root = "./static";

    ArgsParseResult parse_result = ParseArgs(argc, argv, &port, &doc_root);
    if (parse_result == ArgsParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (parse_result == ArgsParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }

    LOG_INFO << "Starting web_demo_server on port " << port
             << ", doc_root=" << doc_root;

    HttpServerLite server(port, "web_demo_server");
    StaticFileHandler handler(doc_root);
    server.setHttpCallback(
        [&handler](const HttpRequest& req, HttpResponse* resp) {
            handler.onRequest(req, resp);
        });
    server.setThreadNum(2);
    server.run();
    return 0;
}
