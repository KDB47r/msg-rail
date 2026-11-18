#include "s3_client.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <utility>
#include <vector>

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/ssl.h>

#include "logging.h"

namespace sparkpush {

namespace {

using tcp = asio::ip::tcp;

constexpr const char* kAlgorithm = "AWS4-HMAC-SHA256";
constexpr const char* kUnsignedPayload = "UNSIGNED-PAYLOAD";

bool StartsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() &&
           s.compare(0, prefix.size(), prefix) == 0;
}

std::string ToLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return s;
}

std::string Trim(const std::string& s) {
    size_t begin = 0;
    while (begin < s.size() &&
           std::isspace(static_cast<unsigned char>(s[begin]))) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(begin, end - begin);
}

// RFC 3986 percent-encoding。路径保留 '/' 作为分隔符。
std::string UriEncode(const std::string& input, bool keep_slash) {
    static const char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(input.size() * 3);
    for (unsigned char c : input) {
        const bool unreserved =
            (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
            c == '~';
        if (unreserved || (keep_slash && c == '/')) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(kHex[(c >> 4) & 0x0F]);
            out.push_back(kHex[c & 0x0F]);
        }
    }
    return out;
}

// 生成 S3 response-content-disposition 的值。
// ASCII fallback 给老客户端，filename* 保留 UTF-8 原名（RFC 5987）。
std::string AttachmentDisposition(const std::string& name) {
    const std::string effective = name.empty() ? "download.bin" : name;
    std::string fallback;
    fallback.reserve(effective.size());
    for (unsigned char c : effective) {
        if (c >= 32 && c < 127 && c != '"' && c != '\\') {
            fallback.push_back(static_cast<char>(c));
        } else {
            fallback.push_back('_');
        }
    }
    if (fallback.empty()) fallback = "download.bin";
    return "attachment; filename=\"" + fallback + "\"; filename*=UTF-8''" +
           UriEncode(effective, false);
}

void AppendUtf8(std::string& out, unsigned long codepoint) {
    if (codepoint <= 0x7F) {
        out.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0x10FFFF) {
        out.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
}

std::string XmlUnescape(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        if (input[i] != '&') {
            out.push_back(input[i]);
            continue;
        }
        if (input.compare(i, 5, "&amp;") == 0) {
            out.push_back('&');
            i += 4;
        } else if (input.compare(i, 4, "&lt;") == 0) {
            out.push_back('<');
            i += 3;
        } else if (input.compare(i, 4, "&gt;") == 0) {
            out.push_back('>');
            i += 3;
        } else if (input.compare(i, 6, "&quot;") == 0) {
            out.push_back('"');
            i += 5;
        } else if (input.compare(i, 6, "&apos;") == 0) {
            out.push_back('\'');
            i += 5;
        } else if (input.compare(i, 2, "&#") == 0) {
            // MinIO 会把 ETag 的引号编码成 &#34;。
            const size_t end = input.find(';', i + 2);
            if (end != std::string::npos) {
                std::string number = input.substr(i + 2, end - i - 2);
                unsigned long codepoint = 0;
                try {
                    if (!number.empty() &&
                        (number[0] == 'x' || number[0] == 'X')) {
                        codepoint = std::stoul(number.substr(1), nullptr, 16);
                    } else {
                        codepoint = std::stoul(number, nullptr, 10);
                    }
                } catch (...) {
                    codepoint = 0;
                }
                if (codepoint > 0 && codepoint <= 0x10FFFF) {
                    AppendUtf8(out, codepoint);
                    i = end;
                    continue;
                }
            }
            out.push_back('&');
        } else {
            out.push_back('&');
        }
    }
    return out;
}

std::string XmlEscape(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (char c : input) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            default: out.push_back(c); break;
        }
    }
    return out;
}

bool XmlTagValue(const std::string& xml, const std::string& tag,
                 std::string* out) {
    if (!out) return false;
    const std::string open = "<" + tag + ">";
    const std::string close = "</" + tag + ">";
    const size_t begin = xml.find(open);
    if (begin == std::string::npos) return false;
    const size_t value_begin = begin + open.size();
    const size_t end = xml.find(close, value_begin);
    if (end == std::string::npos) return false;
    *out = XmlUnescape(xml.substr(value_begin, end - value_begin));
    return true;
}

std::vector<std::string> XmlBlocks(const std::string& xml,
                                   const std::string& tag) {
    std::vector<std::string> blocks;
    const std::string open = "<" + tag + ">";
    const std::string close = "</" + tag + ">";
    size_t pos = 0;
    while (true) {
        const size_t begin = xml.find(open, pos);
        if (begin == std::string::npos) break;
        const size_t value_begin = begin + open.size();
        const size_t end = xml.find(close, value_begin);
        if (end == std::string::npos) break;
        blocks.push_back(xml.substr(value_begin, end - value_begin));
        pos = end + close.size();
    }
    return blocks;
}

std::string Sha256Hex(const std::string& data) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return {};
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(ctx, data.data(), data.size()) != 1 ||
        EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
        EVP_MD_CTX_free(ctx);
        return {};
    }
    EVP_MD_CTX_free(ctx);
    static const char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(digest_len * 2);
    for (unsigned int i = 0; i < digest_len; ++i) {
        out.push_back(kHex[digest[i] >> 4]);
        out.push_back(kHex[digest[i] & 0x0F]);
    }
    return out;
}

std::array<unsigned char, 32> HmacSha256(const std::string& key,
                                         const std::string& data) {
    std::array<unsigned char, 32> out{};
    unsigned int out_len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         out.data(), &out_len);
    return out;
}

std::string Hex(const unsigned char* data, size_t len) {
    static const char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(kHex[data[i] >> 4]);
        out.push_back(kHex[data[i] & 0x0F]);
    }
    return out;
}

std::string HmacHex(const std::string& key, const std::string& data) {
    auto digest = HmacSha256(key, data);
    return Hex(digest.data(), digest.size());
}

std::array<unsigned char, 32> HmacRaw(const std::string& key,
                                      const std::string& data) {
    return HmacSha256(key, data);
}

std::string ToAmzDate(int64_t epoch_seconds, std::string* date_only) {
    std::time_t t = static_cast<std::time_t>(epoch_seconds);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    if (date_only) {
        std::strftime(buf, sizeof(buf), "%Y%m%d", &tm);
        date_only->assign(buf, 8);
        std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    }
    return std::string(buf);
}

struct ParsedUrl {
    bool tls{false};
    std::string scheme;
    std::string host;
    uint16_t port{80};
    std::string path;
};

bool ParseUrl(const std::string& url, ParsedUrl* out, std::string* err) {
    if (!out) return false;
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        if (err) *err = "URL missing scheme: " + url;
        return false;
    }
    const std::string scheme = ToLower(url.substr(0, scheme_end));
    if (scheme != "http" && scheme != "https") {
        if (err) *err = "unsupported URL scheme: " + scheme;
        return false;
    }
    out->tls = (scheme == "https");
    out->scheme = scheme;
    out->port = out->tls ? 443 : 80;

    size_t authority_begin = scheme_end + 3;
    size_t authority_end = url.find('/', authority_begin);
    if (authority_end == std::string::npos) authority_end = url.size();
    std::string authority = url.substr(authority_begin,
                                       authority_end - authority_begin);
    if (authority.empty()) {
        if (err) *err = "URL missing host";
        return false;
    }

    std::string host_port = authority;
    const auto at = host_port.rfind('@');
    if (at != std::string::npos) host_port = host_port.substr(at + 1);

    if (!host_port.empty() && host_port.front() == '[') {
        if (err) *err = "IPv6 endpoint is not supported";
        return false;
    }
    const auto colon = host_port.rfind(':');
    if (colon != std::string::npos) {
        out->host = host_port.substr(0, colon);
        const std::string port_str = host_port.substr(colon + 1);
        try {
            int p = std::stoi(port_str);
            if (p <= 0 || p > 65535) throw std::out_of_range("port");
            out->port = static_cast<uint16_t>(p);
        } catch (...) {
            if (err) *err = "invalid URL port: " + port_str;
            return false;
        }
    } else {
        out->host = host_port;
    }
    if (out->host.empty()) {
        if (err) *err = "URL missing host";
        return false;
    }
    out->path = authority_end < url.size() ? url.substr(authority_end) : "/";
    if (out->path.empty()) out->path = "/";
    return true;
}

std::string HostHeader(const std::string& host, uint16_t port, bool tls) {
    const bool default_port = (tls && port == 443) || (!tls && port == 80);
    if (default_port) return host;
    return host + ":" + std::to_string(port);
}

struct HttpResponseData {
    int status{0};
    std::map<std::string, std::string> headers;
    std::string body;
};

bool HeaderComplete(const std::string& raw, size_t* header_end) {
    const auto pos = raw.find("\r\n\r\n");
    if (pos == std::string::npos) return false;
    *header_end = pos + 4;
    return true;
}

bool ParseHeaderBlock(const std::string& raw, size_t header_end,
                      HttpResponseData* out) {
    const size_t line_end = raw.find("\r\n");
    if (line_end == std::string::npos || line_end > header_end) return false;
    const std::string status_line = raw.substr(0, line_end);
    // HTTP/1.1 200 OK
    const auto first_space = status_line.find(' ');
    if (first_space == std::string::npos) return false;
    const auto second_space = status_line.find(' ', first_space + 1);
    const std::string code = status_line.substr(
        first_space + 1, (second_space == std::string::npos)
                             ? std::string::npos
                             : second_space - first_space - 1);
    try {
        out->status = std::stoi(code);
    } catch (...) {
        return false;
    }

    size_t pos = line_end + 2;
    while (pos < header_end) {
        const size_t end = raw.find("\r\n", pos);
        if (end == std::string::npos || end > header_end) break;
        const std::string line = raw.substr(pos, end - pos);
        const auto colon = line.find(':');
        if (colon != std::string::npos) {
            out->headers[ToLower(Trim(line.substr(0, colon)))] =
                Trim(line.substr(colon + 1));
        }
        pos = end + 2;
    }
    return true;
}

bool DecodeChunkedBody(const std::string& raw, size_t header_end,
                       std::string* body, size_t* consumed) {
    body->clear();
    size_t pos = header_end;
    while (true) {
        const auto line_end = raw.find("\r\n", pos);
        if (line_end == std::string::npos) return false;
        std::string size_line = raw.substr(pos, line_end - pos);
        const auto semi = size_line.find(';');
        if (semi != std::string::npos) size_line.resize(semi);
        size_line = Trim(size_line);
        unsigned long chunk_size = 0;
        try {
            chunk_size = std::stoul(size_line, nullptr, 16);
        } catch (...) {
            return false;
        }
        pos = line_end + 2;
        if (chunk_size == 0) {
            // 跳过 trailer，直到空行。
            const auto trailer_end = raw.find("\r\n", pos);
            if (trailer_end == std::string::npos) return false;
            if (trailer_end == pos) {
                *consumed = pos + 2;
            } else {
                const auto end2 = raw.find("\r\n\r\n", pos);
                if (end2 == std::string::npos) return false;
                *consumed = end2 + 4;
            }
            return true;
        }
        if (raw.size() < pos + chunk_size + 2) return false;
        body->append(raw, pos, chunk_size);
        pos += chunk_size;
        if (raw.compare(pos, 2, "\r\n") != 0) return false;
        pos += 2;
    }
}

template <typename Socket>
bool ReadHttpResponse(Socket& socket, bool head_request,
                      HttpResponseData* out, std::string* err) {
    std::string raw;
    std::array<char, 8192> buf{};
    size_t header_end = 0;
    while (!HeaderComplete(raw, &header_end)) {
        asio::error_code ec;
        const size_t n = socket.read_some(asio::buffer(buf), ec);
        if (ec) {
            if (err) *err = "read response header failed: " + ec.message();
            return false;
        }
        raw.append(buf.data(), n);
    }
    if (!ParseHeaderBlock(raw, header_end, out)) {
        if (err) *err = "invalid HTTP response header";
        return false;
    }
    if (head_request || out->status == 204 || out->status == 304) {
        return true;
    }

    const auto te_it = out->headers.find("transfer-encoding");
    const bool chunked =
        te_it != out->headers.end() &&
        ToLower(te_it->second).find("chunked") != std::string::npos;

    if (chunked) {
        while (true) {
            size_t consumed = 0;
            std::string decoded;
            if (DecodeChunkedBody(raw, header_end, &decoded, &consumed)) {
                out->body = std::move(decoded);
                return true;
            }
            asio::error_code ec;
            const size_t n = socket.read_some(asio::buffer(buf), ec);
            if (ec) {
                if (err) *err = "read chunked body failed: " + ec.message();
                return false;
            }
            raw.append(buf.data(), n);
        }
    }

    size_t content_length = 0;
    const auto cl_it = out->headers.find("content-length");
    if (cl_it != out->headers.end()) {
        try {
            content_length = static_cast<size_t>(std::stoull(cl_it->second));
        } catch (...) {
            if (err) *err = "invalid Content-Length";
            return false;
        }
    }

    out->body = raw.substr(header_end);
    while (out->body.size() < content_length) {
        asio::error_code ec;
        const size_t n = socket.read_some(asio::buffer(buf), ec);
        if (ec) {
            if (err) *err = "read response body failed: " + ec.message();
            return false;
        }
        out->body.append(buf.data(), n);
    }
    if (out->body.size() > content_length) out->body.resize(content_length);
    return true;
}

bool HttpRoundTrip(const ParsedUrl& url, const std::string& method,
                   const std::vector<std::pair<std::string, std::string>>&
                       headers,
                   const std::string& body, bool head_request,
                   HttpResponseData* out, std::string* err) {
    if (!out) return false;
    try {
        asio::io_context io;
        tcp::resolver resolver(io);
        const auto endpoints =
            resolver.resolve(url.host, std::to_string(url.port));

        std::ostringstream request;
        request << method << " " << url.path << " HTTP/1.1\r\n";
        bool has_host = false;
        bool has_content_length = false;
        for (const auto& kv : headers) {
            if (ToLower(kv.first) == "host") has_host = true;
            if (ToLower(kv.first) == "content-length") has_content_length = true;
            request << kv.first << ": " << kv.second << "\r\n";
        }
        if (!has_host) {
            request << "Host: " << HostHeader(url.host, url.port, url.tls)
                    << "\r\n";
        }
        if (!has_content_length) {
            request << "Content-Length: " << body.size() << "\r\n";
        }
        request << "Connection: close\r\n\r\n";
        request << body;

        if (url.tls) {
            asio::ssl::context ctx(asio::ssl::context::tls_client);
            ctx.set_default_verify_paths();
            asio::ssl::stream<tcp::socket> stream(io, ctx);
            stream.set_verify_mode(asio::ssl::verify_peer);
            stream.set_verify_callback(asio::ssl::host_name_verification(url.host));
            SSL_set_tlsext_host_name(stream.native_handle(), url.host.c_str());
            asio::connect(stream.next_layer(), endpoints);
            stream.handshake(asio::ssl::stream_base::client);
            asio::write(stream, asio::buffer(request.str()));
            return ReadHttpResponse(stream, head_request, out, err);
        }

        tcp::socket socket(io);
        asio::connect(socket, endpoints);
        socket.set_option(tcp::no_delay(true));
        asio::write(socket, asio::buffer(request.str()));
        return ReadHttpResponse(socket, head_request, out, err);
    } catch (const std::exception& e) {
        if (err) *err = std::string("HTTP request failed: ") + e.what();
        return false;
    }
}

template <typename Socket>
void SetSocketRecvTimeout(Socket& socket, int timeout_seconds) {
    if (timeout_seconds <= 0) return;
    struct timeval tv;
    tv.tv_sec = timeout_seconds;
    tv.tv_usec = 0;
    // 失败不致命：没有超时也比把大对象整体读进内存安全。
    setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv,
               sizeof(tv));
}

template <typename Socket>
void SetSocketSendTimeout(Socket& socket, int timeout_seconds) {
    if (timeout_seconds <= 0) return;
    struct timeval tv;
    tv.tv_sec = timeout_seconds;
    tv.tv_usec = 0;
    setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO, &tv,
               sizeof(tv));
}

// 把 S3 响应体流式写入文件。支持 Content-Length 和 chunked 两种编码；
// 超过 max_bytes 立即失败，避免恶意/异常对象把磁盘写满。
template <typename Socket>
bool ReadHttpResponseToFile(Socket& socket, const std::string& file_path,
                            int64_t max_bytes, S3GetFileResult* out,
                            std::string* err) {
    std::string raw;
    std::array<char, 8192> buf{};
    size_t header_end = 0;
    while (!HeaderComplete(raw, &header_end)) {
        asio::error_code ec;
        const size_t n = socket.read_some(asio::buffer(buf), ec);
        if (ec) {
            if (err) *err = "read response header failed: " + ec.message();
            return false;
        }
        raw.append(buf.data(), n);
        if (raw.size() > 64 * 1024) {
            if (err) *err = "response header is too large";
            return false;
        }
    }

    HttpResponseData head;
    if (!ParseHeaderBlock(raw, header_end, &head)) {
        if (err) *err = "invalid HTTP response header";
        return false;
    }
    if (out) {
        const auto ct_it = head.headers.find("content-type");
        if (ct_it != head.headers.end()) out->content_type = ct_it->second;
        const auto etag_it = head.headers.find("etag");
        if (etag_it != head.headers.end()) out->etag = etag_it->second;
    }
    if (head.status < 200 || head.status >= 300) {
        if (err) {
            *err = "S3 GET returned HTTP " + std::to_string(head.status);
            const std::string detail = raw.substr(header_end, 512);
            if (!detail.empty()) *err += ": " + detail;
        }
        return false;
    }

    std::ofstream file(file_path, std::ios::binary | std::ios::trunc);
    if (!file) {
        if (err) *err = "open temp file failed: " + file_path;
        return false;
    }
    int64_t written = 0;
    auto append = [&](const char* data, size_t len) -> bool {
        if (len == 0) return true;
        if (written + static_cast<int64_t>(len) > max_bytes) {
            if (err) *err = "object exceeds byte limit while downloading";
            return false;
        }
        file.write(data, static_cast<std::streamsize>(len));
        if (!file) {
            if (err) *err = "write temp file failed";
            return false;
        }
        written += static_cast<int64_t>(len);
        return true;
    };
    auto fail = [&](const std::string& message) {
        file.close();
        std::remove(file_path.c_str());
        if (err) *err = message;
        return false;
    };

    const auto te_it = head.headers.find("transfer-encoding");
    const bool chunked =
        te_it != head.headers.end() &&
        ToLower(te_it->second).find("chunked") != std::string::npos;
    if (chunked) {
        std::string pending = raw.substr(header_end);
        while (true) {
            const auto line_end = pending.find("\r\n");
            if (line_end == std::string::npos) {
                if (pending.size() > 64 * 1024) {
                    return fail("invalid chunked response");
                }
                asio::error_code ec;
                const size_t n = socket.read_some(asio::buffer(buf), ec);
                if (ec) {
                    return fail("read chunked body failed: " + ec.message());
                }
                pending.append(buf.data(), n);
                continue;
            }
            std::string size_line = pending.substr(0, line_end);
            const auto semi = size_line.find(';');
            if (semi != std::string::npos) size_line.resize(semi);
            size_line = Trim(size_line);
            unsigned long chunk_size = 0;
            try {
                chunk_size = std::stoul(size_line, nullptr, 16);
            } catch (...) {
                return fail("invalid chunk size");
            }
            if (chunk_size > static_cast<unsigned long>(max_bytes) + 1) {
                return fail("chunk exceeds byte limit");
            }
            const size_t need = line_end + 2 + chunk_size + 2;
            if (pending.size() < need) {
                asio::error_code ec;
                const size_t n = socket.read_some(asio::buffer(buf), ec);
                if (ec) {
                    return fail("read chunked body failed: " + ec.message());
                }
                pending.append(buf.data(), n);
                continue;
            }
            if (chunk_size == 0) {
                file.close();
                if (!file) return fail("close temp file failed");
                if (out) out->size_bytes = written;
                return true;
            }
            if (!append(pending.data() + line_end + 2, chunk_size)) {
                return fail(err ? *err : "write temp file failed");
            }
            if (pending.compare(line_end + 2 + chunk_size, 2, "\r\n") != 0) {
                return fail("invalid chunk terminator");
            }
            pending.erase(0, need);
        }
    }

    int64_t content_length = -1;
    const auto cl_it = head.headers.find("content-length");
    if (cl_it != head.headers.end()) {
        try {
            content_length = std::stoll(cl_it->second);
        } catch (...) {
            return fail("invalid Content-Length");
        }
        if (content_length < 0) return fail("invalid Content-Length");
        if (content_length > max_bytes) {
            return fail("object exceeds byte limit");
        }
    }

    size_t initial = raw.size() - header_end;
    if (content_length >= 0 &&
        static_cast<int64_t>(initial) > content_length) {
        initial = static_cast<size_t>(content_length);
    }
    if (!append(raw.data() + header_end, initial)) {
        return fail(err ? *err : "write temp file failed");
    }
    while (content_length < 0 || written < content_length) {
        asio::error_code ec;
        const size_t n = socket.read_some(asio::buffer(buf), ec);
        if (ec) {
            if (content_length < 0 && ec == asio::error::eof) break;
            return fail("read response body failed: " + ec.message());
        }
        size_t take = n;
        if (content_length >= 0 &&
            static_cast<int64_t>(take) > content_length - written) {
            take = static_cast<size_t>(content_length - written);
        }
        if (!append(buf.data(), take)) {
            return fail(err ? *err : "write temp file failed");
        }
        if (content_length < 0 && n == 0) break;
    }
    file.close();
    if (!file) return fail("close temp file failed");
    if (out) out->size_bytes = written;
    return true;
}

bool HttpGetToFile(const std::string& url, const std::string& file_path,
                   int64_t max_bytes, int timeout_seconds,
                   S3GetFileResult* out, std::string* err) {
    ParsedUrl parsed;
    if (!ParseUrl(url, &parsed, err)) return false;
    try {
        asio::io_context io;
        tcp::resolver resolver(io);
        const auto endpoints =
            resolver.resolve(parsed.host, std::to_string(parsed.port));

        std::ostringstream request;
        request << "GET " << parsed.path << " HTTP/1.1\r\n";
        request << "Host: " << HostHeader(parsed.host, parsed.port, parsed.tls)
                << "\r\n";
        request << "Connection: close\r\n\r\n";

        if (parsed.tls) {
            asio::ssl::context ctx(asio::ssl::context::tls_client);
            ctx.set_default_verify_paths();
            asio::ssl::stream<tcp::socket> stream(io, ctx);
            stream.set_verify_mode(asio::ssl::verify_peer);
            stream.set_verify_callback(
                asio::ssl::host_name_verification(parsed.host));
            SSL_set_tlsext_host_name(stream.native_handle(),
                                     parsed.host.c_str());
            asio::connect(stream.next_layer(), endpoints);
            SetSocketRecvTimeout(stream.lowest_layer(), timeout_seconds);
            stream.handshake(asio::ssl::stream_base::client);
            asio::write(stream, asio::buffer(request.str()));
            return ReadHttpResponseToFile(stream, file_path, max_bytes, out,
                                          err);
        }

        tcp::socket socket(io);
        asio::connect(socket, endpoints);
        socket.set_option(tcp::no_delay(true));
        SetSocketRecvTimeout(socket, timeout_seconds);
        asio::write(socket, asio::buffer(request.str()));
        return ReadHttpResponseToFile(socket, file_path, max_bytes, out, err);
    } catch (const std::exception& e) {
        std::remove(file_path.c_str());
        if (err) *err = std::string("HTTP GET failed: ") + e.what();
        return false;
    }
}

// 用预签名 URL 流式上传本地文件。预签名只签 host，Content-Type 可以自由设置；
// Content-Length 必须和文件实际字节数一致，否则 MinIO 会拒绝或留下不完整请求。
bool HttpPutFile(const std::string& url, const std::string& file_path,
                 int64_t content_length, const std::string& content_type,
                 int timeout_seconds, S3PutResult* out, std::string* err) {
    ParsedUrl parsed;
    if (!ParseUrl(url, &parsed, err)) return false;
    if (content_length <= 0) {
        if (err) *err = "upload file is empty";
        return false;
    }
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
        if (err) *err = "open upload file failed: " + file_path;
        return false;
    }

    std::ostringstream header;
    header << "PUT " << parsed.path << " HTTP/1.1\r\n";
    header << "Host: " << HostHeader(parsed.host, parsed.port, parsed.tls)
           << "\r\n";
    if (!content_type.empty()) {
        header << "Content-Type: " << content_type << "\r\n";
    }
    header << "Content-Length: " << content_length << "\r\n";
    header << "Connection: close\r\n\r\n";
    const std::string header_text = header.str();

    auto send = [&](auto& socket) -> bool {
        asio::write(socket, asio::buffer(header_text));
        std::array<char, 64 * 1024> buf{};
        int64_t sent = 0;
        while (file) {
            file.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const std::streamsize n = file.gcount();
            if (n <= 0) break;
            asio::write(socket,
                        asio::buffer(buf.data(), static_cast<size_t>(n)));
            sent += static_cast<int64_t>(n);
            if (sent > content_length) {
                if (err) *err = "upload file grew while sending";
                return false;
            }
        }
        if (sent != content_length) {
            if (err) {
                *err = "upload file size changed while sending";
            }
            return false;
        }

        HttpResponseData resp;
        if (!ReadHttpResponse(socket, false, &resp, err)) return false;
        if (resp.status < 200 || resp.status >= 300) {
            if (err) {
                *err = "S3 PUT returned HTTP " + std::to_string(resp.status) +
                       (resp.body.empty() ? ""
                                          : (": " + resp.body.substr(0, 512)));
            }
            return false;
        }
        if (out) {
            const auto etag = resp.headers.find("etag");
            if (etag != resp.headers.end()) out->etag = etag->second;
        }
        return true;
    };

    try {
        asio::io_context io;
        tcp::resolver resolver(io);
        const auto endpoints =
            resolver.resolve(parsed.host, std::to_string(parsed.port));

        if (parsed.tls) {
            asio::ssl::context ctx(asio::ssl::context::tls_client);
            ctx.set_default_verify_paths();
            asio::ssl::stream<tcp::socket> stream(io, ctx);
            stream.set_verify_mode(asio::ssl::verify_peer);
            stream.set_verify_callback(
                asio::ssl::host_name_verification(parsed.host));
            SSL_set_tlsext_host_name(stream.native_handle(),
                                     parsed.host.c_str());
            asio::connect(stream.next_layer(), endpoints);
            SetSocketRecvTimeout(stream.lowest_layer(), timeout_seconds);
            SetSocketSendTimeout(stream.lowest_layer(), timeout_seconds);
            stream.handshake(asio::ssl::stream_base::client);
            return send(stream);
        }

        tcp::socket socket(io);
        asio::connect(socket, endpoints);
        socket.set_option(tcp::no_delay(true));
        SetSocketRecvTimeout(socket, timeout_seconds);
        SetSocketSendTimeout(socket, timeout_seconds);
        return send(socket);
    } catch (const std::exception& e) {
        if (err) *err = std::string("HTTP PUT failed: ") + e.what();
        return false;
    }
}

std::string NormalizeBasePath(const std::string& path) {
    if (path.empty() || path == "/") return "";
    std::string out = path;
    while (!out.empty() && out.back() == '/') out.pop_back();
    if (out.empty()) return "";
    if (out.front() != '/') out.insert(out.begin(), '/');
    return out;
}

}  // namespace

bool S3Client::ParseEndpoint(const std::string& url, Endpoint* out,
                             std::string* err) {
    if (!out) return false;
    ParsedUrl parsed;
    if (!ParseUrl(url, &parsed, err)) return false;
    out->tls = parsed.tls;
    out->scheme = parsed.scheme;
    out->host = parsed.host;
    out->port = parsed.port;
    out->base_path = NormalizeBasePath(parsed.path);
    return true;
}

bool S3Client::Init(const S3Config& cfg, std::string* err) {
    cfg_ = cfg;
    if (cfg_.endpoint.empty()) {
        if (err) *err = "s3 endpoint is empty";
        return false;
    }
    if (cfg_.bucket.empty()) {
        if (err) *err = "s3 bucket is empty";
        return false;
    }
    if (cfg_.access_key.empty() || cfg_.secret_key.empty()) {
        if (err) *err = "s3 access_key/secret_key is empty";
        return false;
    }
    if (!cfg_.path_style) {
        if (err) *err = "only path-style S3 addressing is supported";
        return false;
    }
    if (cfg_.public_endpoint.empty()) cfg_.public_endpoint = cfg_.endpoint;
    if (cfg_.region.empty()) cfg_.region = "us-east-1";
    if (cfg_.presign_ttl_seconds <= 0) cfg_.presign_ttl_seconds = 900;
    std::string parse_err;
    if (!ParseEndpoint(cfg_.endpoint, &internal_, &parse_err)) {
        if (err) *err = "invalid s3 endpoint: " + parse_err;
        return false;
    }
    if (!ParseEndpoint(cfg_.public_endpoint, &public_, &parse_err)) {
        if (err) *err = "invalid s3 public_endpoint: " + parse_err;
        return false;
    }
    ready_ = true;
    return true;
}

std::string S3Client::ObjectUrlPath(const std::string& object_key) const {
    std::string path = internal_.base_path;
    if (path.empty() || path.back() != '/') path.push_back('/');
    path += UriEncode(cfg_.bucket, false);
    path.push_back('/');
    path += UriEncode(object_key, true);
    return path;
}

std::string S3Client::PresignWithTime(const std::string& method,
                                      const std::string& object_key,
                                      int ttl_seconds, int64_t now_seconds,
                                      int64_t* expires_at,
                                      std::string* err,
                                      const std::string& disposition) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return {};
    }
    return PresignWithTimeForEndpoint(public_, method, object_key, ttl_seconds,
                                      now_seconds, expires_at, err,
                                      disposition);
}

std::string S3Client::PresignPut(const std::string& object_key, int ttl_seconds,
                                 int64_t* expires_at) const {
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    return PresignWithTime("PUT", object_key, ttl_seconds, now, expires_at,
                           nullptr);
}

std::string S3Client::PresignGet(const std::string& object_key, int ttl_seconds,
                                 int64_t* expires_at) const {
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    return PresignWithTime("GET", object_key, ttl_seconds, now, expires_at,
                           nullptr);
}

std::string S3Client::PresignGetDownload(
    const std::string& object_key, int ttl_seconds,
    const std::string& download_name, int64_t* expires_at) const {
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    return PresignWithTime("GET", object_key, ttl_seconds, now, expires_at,
                           nullptr, AttachmentDisposition(download_name));
}

std::string S3Client::PresignWithTimeForEndpoint(
    const Endpoint& ep, const std::string& method,
    const std::string& object_key, int ttl_seconds, int64_t now_seconds,
    int64_t* expires_at, std::string* err,
    const std::string& disposition) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return {};
    }
    if (ttl_seconds <= 0) ttl_seconds = cfg_.presign_ttl_seconds;

    std::string date_only;
    const std::string amz_date = ToAmzDate(now_seconds, &date_only);
    const std::string scope =
        date_only + "/" + cfg_.region + "/s3/aws4_request";

    std::string path = ep.base_path;
    if (path.empty() || path.back() != '/') path.push_back('/');
    path += UriEncode(cfg_.bucket, false);
    path.push_back('/');
    path += UriEncode(object_key, true);

    const std::string host = HostHeader(ep.host, ep.port, ep.tls);
    const std::string credential =
        UriEncode(cfg_.access_key + "/" + scope, false);
    std::ostringstream query;
    query << "X-Amz-Algorithm=" << kAlgorithm
          << "&X-Amz-Credential=" << credential
          << "&X-Amz-Date=" << amz_date
          << "&X-Amz-Expires=" << ttl_seconds
          << "&X-Amz-SignedHeaders=host";
    if (!disposition.empty()) {
        // 参数按字典序排在 X-Amz-* 之后；值需要 URI 编码后参与签名。
        query << "&response-content-disposition="
              << UriEncode(disposition, false);
    }
    const std::string canonical_query = query.str();

    const std::string canonical_headers = "host:" + host + "\n";
    const std::string canonical_request =
        method + "\n" + path + "\n" + canonical_query + "\n" +
        canonical_headers + "\n" + "host" + "\n" + kUnsignedPayload;
    const std::string string_to_sign =
        std::string(kAlgorithm) + "\n" + amz_date + "\n" + scope + "\n" +
        Sha256Hex(canonical_request);

    auto k_date = HmacRaw("AWS4" + cfg_.secret_key, date_only);
    auto k_region = HmacRaw(std::string(reinterpret_cast<char*>(k_date.data()),
                                        k_date.size()),
                            cfg_.region);
    auto k_service = HmacRaw(std::string(reinterpret_cast<char*>(k_region.data()),
                                         k_region.size()),
                             "s3");
    auto k_signing = HmacRaw(std::string(reinterpret_cast<char*>(k_service.data()),
                                         k_service.size()),
                             "aws4_request");
    const std::string signature =
        HmacHex(std::string(reinterpret_cast<char*>(k_signing.data()),
                            k_signing.size()),
                string_to_sign);

    if (expires_at) *expires_at = now_seconds + ttl_seconds;
    std::string url = ep.scheme + "://" + host + path + "?" +
                      canonical_query + "&X-Amz-Signature=" + signature;
    return url;
}

std::string S3Client::PresignUploadPartWithTime(
    const std::string& object_key, const std::string& upload_id,
    int part_number, int ttl_seconds, int64_t now_seconds,
    int64_t* expires_at, std::string* err) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return {};
    }
    if (upload_id.empty() || part_number <= 0) {
        if (err) *err = "invalid upload_id or part number";
        return {};
    }
    if (ttl_seconds <= 0) ttl_seconds = cfg_.presign_ttl_seconds;

    const Endpoint& ep = public_;
    std::string date_only;
    const std::string amz_date = ToAmzDate(now_seconds, &date_only);
    const std::string scope =
        date_only + "/" + cfg_.region + "/s3/aws4_request";

    std::string path = ep.base_path;
    if (path.empty() || path.back() != '/') path.push_back('/');
    path += UriEncode(cfg_.bucket, false);
    path.push_back('/');
    path += UriEncode(object_key, true);

    const std::string host = HostHeader(ep.host, ep.port, ep.tls);
    const std::string credential =
        UriEncode(cfg_.access_key + "/" + scope, false);
    std::ostringstream query;
    query << "X-Amz-Algorithm=" << kAlgorithm
          << "&X-Amz-Credential=" << credential
          << "&X-Amz-Date=" << amz_date
          << "&X-Amz-Expires=" << ttl_seconds
          << "&X-Amz-SignedHeaders=host"
          << "&partNumber=" << part_number
          << "&uploadId=" << UriEncode(upload_id, false);
    const std::string canonical_query = query.str();
    const std::string canonical_headers = "host:" + host + "\n";
    const std::string canonical_request =
        "PUT\n" + path + "\n" + canonical_query + "\n" + canonical_headers +
        "\n" + "host" + "\n" + kUnsignedPayload;
    const std::string string_to_sign =
        std::string(kAlgorithm) + "\n" + amz_date + "\n" + scope + "\n" +
        Sha256Hex(canonical_request);

    auto k_date = HmacRaw("AWS4" + cfg_.secret_key, date_only);
    auto k_region = HmacRaw(std::string(reinterpret_cast<char*>(k_date.data()),
                                        k_date.size()),
                            cfg_.region);
    auto k_service = HmacRaw(std::string(reinterpret_cast<char*>(k_region.data()),
                                         k_region.size()),
                             "s3");
    auto k_signing = HmacRaw(std::string(reinterpret_cast<char*>(k_service.data()),
                                         k_service.size()),
                             "aws4_request");
    const std::string signature =
        HmacHex(std::string(reinterpret_cast<char*>(k_signing.data()),
                            k_signing.size()),
                string_to_sign);

    if (expires_at) *expires_at = now_seconds + ttl_seconds;
    return ep.scheme + "://" + host + path + "?" + canonical_query +
           "&X-Amz-Signature=" + signature;
}

std::string S3Client::PresignUploadPart(const std::string& object_key,
                                        const std::string& upload_id,
                                        int part_number, int ttl_seconds,
                                        int64_t* expires_at) const {
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    return PresignUploadPartWithTime(object_key, upload_id, part_number,
                                     ttl_seconds, now, expires_at, nullptr);
}

bool S3Client::SignedRequest(const std::string& method,
                             const std::string& object_key,
                             const std::string& content_type,
                             const std::string& body,
                             std::string* response_body, int* status_code,
                             std::string* response_etag,
                             std::string* response_content_type,
                             std::string* response_content_length,
                             std::string* err,
                             const std::string& cache_control,
                             const std::string& query) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return false;
    }
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    std::string date_only;
    const std::string amz_date = ToAmzDate(now, &date_only);
    const std::string scope =
        date_only + "/" + cfg_.region + "/s3/aws4_request";

    std::string path = ObjectUrlPath(object_key);
    const std::string host =
        HostHeader(internal_.host, internal_.port, internal_.tls);
    const std::string payload_hash = Sha256Hex(body);
    std::string canonical_headers;
    std::string signed_headers;
    if (!cache_control.empty()) {
        // SigV4 canonical header 必须按字典序，cache-control 在 host 之前。
        canonical_headers += "cache-control:" + cache_control + "\n";
        signed_headers += "cache-control;";
    }
    canonical_headers += "host:" + host + "\n" +
                         "x-amz-content-sha256:" + payload_hash + "\n" +
                         "x-amz-date:" + amz_date + "\n";
    signed_headers += "host;x-amz-content-sha256;x-amz-date";
    const std::string canonical_request =
        method + "\n" + path + "\n" + query + "\n" + canonical_headers +
        "\n" + signed_headers + "\n" + payload_hash;
    const std::string string_to_sign =
        std::string(kAlgorithm) + "\n" + amz_date + "\n" + scope + "\n" +
        Sha256Hex(canonical_request);

    auto k_date = HmacRaw("AWS4" + cfg_.secret_key, date_only);
    auto k_region = HmacRaw(std::string(reinterpret_cast<char*>(k_date.data()),
                                        k_date.size()),
                            cfg_.region);
    auto k_service = HmacRaw(std::string(reinterpret_cast<char*>(k_region.data()),
                                         k_region.size()),
                             "s3");
    auto k_signing = HmacRaw(std::string(reinterpret_cast<char*>(k_service.data()),
                                         k_service.size()),
                             "aws4_request");
    const std::string signature =
        HmacHex(std::string(reinterpret_cast<char*>(k_signing.data()),
                            k_signing.size()),
                string_to_sign);
    const std::string authorization =
        std::string(kAlgorithm) + " Credential=" + cfg_.access_key + "/" +
        scope + ", SignedHeaders=" + signed_headers + ", Signature=" +
        signature;

    ParsedUrl parsed;
    parsed.tls = internal_.tls;
    parsed.scheme = internal_.scheme;
    parsed.host = internal_.host;
    parsed.port = internal_.port;
    parsed.path = query.empty() ? path : (path + "?" + query);

    std::vector<std::pair<std::string, std::string>> headers;
    headers.emplace_back("Host", host);
    headers.emplace_back("x-amz-content-sha256", payload_hash);
    headers.emplace_back("x-amz-date", amz_date);
    headers.emplace_back("Authorization", authorization);
    if (!content_type.empty()) {
        headers.emplace_back("Content-Type", content_type);
    }
    if (!cache_control.empty()) {
        headers.emplace_back("Cache-Control", cache_control);
    }

    HttpResponseData resp;
    if (!HttpRoundTrip(parsed, method, headers, body,
                       method == "HEAD", &resp, err)) {
        return false;
    }
    if (status_code) *status_code = resp.status;
    if (response_body) *response_body = resp.body;
    if (response_etag) {
        const auto it = resp.headers.find("etag");
        if (it != resp.headers.end()) *response_etag = it->second;
    }
    if (response_content_type) {
        const auto it = resp.headers.find("content-type");
        if (it != resp.headers.end()) *response_content_type = it->second;
    }
    if (response_content_length) {
        const auto it = resp.headers.find("content-length");
        if (it != resp.headers.end()) *response_content_length = it->second;
    }
    const bool ok = resp.status >= 200 && resp.status < 300;
    if (!ok && err) {
        *err = "S3 " + method + " returned HTTP " +
               std::to_string(resp.status) +
               (resp.body.empty() ? "" : (": " + resp.body.substr(0, 512)));
    }
    return ok;
}

bool S3Client::HeadObject(const std::string& object_key, S3HeadResult* out,
                          std::string* err) const {
    std::string content_length;
    std::string etag;
    std::string content_type;
    std::string body;
    int status = 0;
    if (!SignedRequest("HEAD", object_key, "", "", &body, &status, &etag,
                       &content_type, &content_length, err)) {
        if (status == 404 && err) *err = "object not found";
        return false;
    }
    if (out) {
        out->etag = etag;
        out->content_type = content_type;
        try {
            out->size_bytes = content_length.empty()
                                  ? 0
                                  : static_cast<int64_t>(std::stoll(
                                        content_length));
        } catch (...) {
            out->size_bytes = 0;
        }
    }
    return true;
}

bool S3Client::GetObject(const std::string& object_key, S3GetResult* out,
                         std::string* err) const {
    std::string body;
    std::string etag;
    std::string content_type;
    std::string content_length;
    int status = 0;
    if (!SignedRequest("GET", object_key, "", "", &body, &status, &etag,
                       &content_type, &content_length, err)) {
        return false;
    }
    if (out) {
        out->body = std::move(body);
        out->etag = std::move(etag);
        out->content_type = std::move(content_type);
    }
    return true;
}

bool S3Client::GetObjectToFile(const std::string& object_key,
                               const std::string& file_path,
                               int64_t max_bytes, int timeout_seconds,
                               S3GetFileResult* out,
                               std::string* err) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return false;
    }
    if (max_bytes <= 0) {
        if (err) *err = "max_bytes must be positive";
        return false;
    }
    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    int64_t expires_at = 0;
    // worker 到 MinIO 走内部 endpoint，避免部署时绕公网。
    const std::string url = PresignWithTimeForEndpoint(
        internal_, "GET", object_key, cfg_.presign_ttl_seconds, now,
        &expires_at, err);
    if (url.empty()) return false;
    return HttpGetToFile(url, file_path, max_bytes, timeout_seconds, out, err);
}

bool S3Client::PutObject(const std::string& object_key,
                         const std::string& body,
                         const std::string& content_type, S3PutResult* out,
                         std::string* err,
                         const std::string& cache_control) const {
    std::string response_body;
    std::string etag;
    std::string response_content_type;
    std::string response_content_length;
    int status = 0;
    if (!SignedRequest("PUT", object_key, content_type, body, &response_body,
                       &status, &etag, &response_content_type,
                       &response_content_length, err, cache_control)) {
        return false;
    }
    if (out) out->etag = etag;
    return true;
}

bool S3Client::PutObjectFromFile(const std::string& object_key,
                                 const std::string& file_path,
                                 const std::string& content_type,
                                 int64_t max_bytes, int timeout_seconds,
                                 S3PutResult* out, std::string* err) const {
    if (!ready_) {
        if (err) *err = "s3 client is not initialized";
        return false;
    }
    if (max_bytes <= 0) {
        if (err) *err = "max_bytes must be positive";
        return false;
    }
    std::error_code ec;
    const auto file_size = std::filesystem::file_size(file_path, ec);
    if (ec) {
        if (err) *err = "stat upload file failed: " + ec.message();
        return false;
    }
    if (file_size <= 0) {
        if (err) *err = "upload file is empty: " + file_path;
        return false;
    }
    if (static_cast<int64_t>(file_size) > max_bytes) {
        if (err) *err = "upload file exceeds byte limit";
        return false;
    }

    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    // 预签名有效期至少覆盖本次上传超时，避免大文件传到一半 URL 过期。
    const int ttl_seconds =
        std::max(cfg_.presign_ttl_seconds,
                 (timeout_seconds > 0 ? timeout_seconds : 0) + 60);
    const std::string url = PresignWithTimeForEndpoint(
        internal_, "PUT", object_key, ttl_seconds, now, nullptr, err);
    if (url.empty()) return false;
    return HttpPutFile(url, file_path, static_cast<int64_t>(file_size),
                       content_type, timeout_seconds, out, err);
}

bool S3Client::DeleteObject(const std::string& object_key,
                            std::string* err) const {
    std::string response_body;
    std::string etag;
    std::string content_type;
    std::string content_length;
    int status = 0;
    return SignedRequest("DELETE", object_key, "", "", &response_body, &status,
                         &etag, &content_type, &content_length, err);
}

bool S3Client::CreateMultipartUpload(const std::string& object_key,
                                     const std::string& content_type,
                                     std::string* upload_id,
                                     std::string* err) const {
    if (!upload_id) {
        if (err) *err = "invalid upload_id output";
        return false;
    }
    upload_id->clear();
    std::string response_body;
    std::string etag;
    std::string response_content_type;
    std::string content_length;
    int status = 0;
    if (!SignedRequest("POST", object_key, content_type, "", &response_body,
                       &status, &etag, &response_content_type,
                       &content_length, err, "", "uploads=")) {
        return false;
    }
    if (status < 200 || status >= 300) {
        if (err) {
            *err = "S3 CreateMultipartUpload returned HTTP " +
                   std::to_string(status) +
                   (response_body.empty() ? "" : (": " + response_body));
        }
        return false;
    }
    if (!XmlTagValue(response_body, "UploadId", upload_id) ||
        upload_id->empty()) {
        if (err) *err = "S3 CreateMultipartUpload response missing UploadId";
        return false;
    }
    return true;
}

bool S3Client::ListParts(const std::string& object_key,
                         const std::string& upload_id,
                         std::vector<S3MultipartPart>* parts,
                         bool* no_such_upload, std::string* err) const {
    if (!parts || upload_id.empty()) {
        if (err) *err = "invalid arguments";
        return false;
    }
    parts->clear();
    if (no_such_upload) *no_such_upload = false;

    std::string response_body;
    std::string etag;
    std::string response_content_type;
    std::string content_length;
    int status = 0;
    const std::string query = "uploadId=" + UriEncode(upload_id, false);
    if (!SignedRequest("GET", object_key, "", "", &response_body, &status,
                       &etag, &response_content_type, &content_length, err,
                       "", query)) {
        return false;
    }
    if (status == 404 ||
        response_body.find("<Code>NoSuchUpload</Code>") != std::string::npos) {
        if (no_such_upload) *no_such_upload = true;
        if (err) *err = "multipart upload not found";
        return false;
    }
    if (status < 200 || status >= 300) {
        if (err) {
            *err = "S3 ListParts returned HTTP " + std::to_string(status) +
                   (response_body.empty() ? "" : (": " + response_body));
        }
        return false;
    }

    for (const auto& block : XmlBlocks(response_body, "Part")) {
        S3MultipartPart part;
        std::string number_str;
        std::string size_str;
        if (!XmlTagValue(block, "PartNumber", &number_str) ||
            !XmlTagValue(block, "ETag", &part.etag)) {
            continue;
        }
        try {
            part.part_number = std::stoi(number_str);
        } catch (...) {
            continue;
        }
        if (XmlTagValue(block, "Size", &size_str)) {
            try {
                part.size_bytes = std::stoll(size_str);
            } catch (...) {
                part.size_bytes = 0;
            }
        }
        parts->push_back(std::move(part));
    }
    std::sort(parts->begin(), parts->end(),
              [](const S3MultipartPart& a, const S3MultipartPart& b) {
                  return a.part_number < b.part_number;
              });
    return true;
}

bool S3Client::CompleteMultipartUpload(
    const std::string& object_key, const std::string& upload_id,
    const std::vector<S3MultipartPart>& parts, S3PutResult* out,
    std::string* err) const {
    if (upload_id.empty() || parts.empty()) {
        if (err) *err = "multipart upload has no parts";
        return false;
    }
    std::ostringstream xml;
    xml << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    for (const auto& part : parts) {
        xml << "<Part><PartNumber>" << part.part_number
            << "</PartNumber><ETag>" << XmlEscape(part.etag)
            << "</ETag></Part>";
    }
    xml << "</CompleteMultipartUpload>";

    std::string response_body;
    std::string etag;
    std::string response_content_type;
    std::string content_length;
    int status = 0;
    const std::string query = "uploadId=" + UriEncode(upload_id, false);
    if (!SignedRequest("POST", object_key, "application/xml", xml.str(),
                       &response_body, &status, &etag, &response_content_type,
                       &content_length, err, "", query)) {
        return false;
    }
    if (status < 200 || status >= 300 ||
        response_body.find("<Error>") != std::string::npos) {
        if (err) {
            *err = "S3 CompleteMultipartUpload returned HTTP " +
                   std::to_string(status) +
                   (response_body.empty() ? "" : (": " + response_body));
        }
        return false;
    }
    if (out) {
        std::string response_etag;
        out->etag = XmlTagValue(response_body, "ETag", &response_etag)
                        ? response_etag
                        : etag;
    }
    return true;
}

bool S3Client::AbortMultipartUpload(const std::string& object_key,
                                    const std::string& upload_id,
                                    std::string* err) const {
    if (upload_id.empty()) {
        if (err) *err = "empty upload_id";
        return false;
    }
    std::string response_body;
    std::string etag;
    std::string response_content_type;
    std::string content_length;
    int status = 0;
    const std::string query = "uploadId=" + UriEncode(upload_id, false);
    if (!SignedRequest("DELETE", object_key, "", "", &response_body, &status,
                       &etag, &response_content_type, &content_length, err,
                       "", query)) {
        return false;
    }
    if ((status >= 200 && status < 300) || status == 404 ||
        response_body.find("<Code>NoSuchUpload</Code>") != std::string::npos) {
        return true;
    }
    if (err) {
        *err = "S3 AbortMultipartUpload returned HTTP " +
               std::to_string(status) +
               (response_body.empty() ? "" : (": " + response_body));
    }
    return false;
}

}  // namespace sparkpush
