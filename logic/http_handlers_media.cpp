// HttpApiServer 媒体域 handler：图片/普通文件直传初始化、确认上传、查询签名 URL。
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
#include "s3_client.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

namespace {

bool IsHexSha256(const std::string& value) {
    if (value.size() != 64) return false;
    for (unsigned char c : value) {
        if (!std::isxdigit(c)) return false;
    }
    return true;
}

// 文件 MIME 不设白名单（浏览器 File.type 可能为空），但必须是
// 形如 type/subtype 的可打印字符串，避免 CR/LF 注入响应头。
bool IsValidMimeType(const std::string& mime) {
    if (mime.empty() || mime.size() > 128) return false;
    bool has_slash = false;
    for (unsigned char c : mime) {
        if (c <= 32 || c == 127 || c == '"' || c == '\\') return false;
        if (c == '/') has_slash = true;
    }
    return has_slash;
}

// 只保留 basename，并去掉控制字符；文件名只用于展示和下载头，
// 不参与对象 key 的路径拼接。
std::string NormalizeFileName(const std::string& raw) {
    const size_t slash = raw.find_last_of("/\\");
    const std::string base =
        slash == std::string::npos ? raw : raw.substr(slash + 1);
    std::string out;
    out.reserve(base.size());
    for (unsigned char c : base) {
        if (c >= 32 && c != 127) out.push_back(static_cast<char>(c));
    }
    return out;
}

// 从文件名提取安全扩展名，限制长度和字符集；没有扩展名时用 bin。
std::string SafeFileExtension(const std::string& filename) {
    const size_t dot = filename.find_last_of('.');
    if (dot == std::string::npos || dot + 1 >= filename.size()) return "bin";
    std::string ext;
    for (size_t i = dot + 1; i < filename.size() && ext.size() < 16; ++i) {
        const unsigned char c = static_cast<unsigned char>(filename[i]);
        if (!std::isalnum(c)) break;
        ext.push_back(static_cast<char>(std::tolower(c)));
    }
    return ext.empty() ? "bin" : ext;
}

}  // namespace

// POST /api/media/upload/init
// 只创建元数据并返回预签名 PUT；图片/视频/文件字节都由浏览器直接写到 MinIO。
void HttpApiServer::handleMediaUploadInit(const HttpRequest& req,
                                          HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!media_dao_ || !s3_client_ || !s3_client_->ready()) {
        WriteError(resp, 503, "media storage is not initialized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"mime_type\":\"image/jpeg\",\"size_bytes\":12345}");
        return;
    }

    std::string kind = "image";
    std::string mime_type;
    std::string filename;
    int64_t size_bytes = 0;
    std::string sha256;
    try {
        if (j.contains("kind")) {
            if (!j["kind"].is_string()) {
                WriteError(resp, 400, "kind must be a string");
                return;
            }
            kind = j["kind"].get<std::string>();
        }
        if (!j.contains("mime_type") || !j["mime_type"].is_string() ||
            !j.contains("size_bytes") || !j["size_bytes"].is_number_integer()) {
            WriteError(resp, 400, "mime_type/size_bytes required");
            return;
        }
        mime_type = j["mime_type"].get<std::string>();
        size_bytes = j["size_bytes"].get<int64_t>();
        if (j.contains("filename") && j["filename"].is_string()) {
            filename = j["filename"].get<std::string>();
        }
        if (j.contains("sha256") && j["sha256"].is_string()) {
            sha256 = j["sha256"].get<std::string>();
        }
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid json fields in media upload init");
        return;
    }

    const bool is_image = (kind == "image");
    const bool is_video = (kind == "video");
    const bool is_file = (kind == "file");
    if (!is_image && !is_video && !is_file) {
        WriteError(resp, 400, "only kind=image/video/file are supported");
        return;
    }
    if (is_image && !IsAllowedImageMime(mime_type)) {
        WriteError(resp, 415, "unsupported image mime_type");
        return;
    }
    if (is_video && !IsAllowedVideoMime(mime_type)) {
        WriteError(resp, 415, "unsupported video mime_type");
        return;
    }
    if (is_file && !IsValidMimeType(mime_type)) {
        WriteError(resp, 415, "invalid file mime_type");
        return;
    }
    int64_t max_bytes = media_max_file_bytes_;
    if (is_image) {
        max_bytes = media_max_image_bytes_;
    } else if (is_video) {
        max_bytes = media_max_video_bytes_;
    }
    if (size_bytes <= 0 || size_bytes > max_bytes) {
        WriteError(resp, 413,
                   is_image ? "image size exceeds limit"
                            : (is_video ? "video size exceeds limit"
                                        : "file size exceeds limit"));
        return;
    }
    if (filename.size() > 255) {
        WriteError(resp, 400, "filename is too long");
        return;
    }
    if (!sha256.empty() && !IsHexSha256(sha256)) {
        WriteError(resp, 400, "sha256 must be 64 hex characters");
        return;
    }
    if (is_image) {
        if (filename.empty()) {
            filename = "upload." + ExtensionForImageMime(mime_type);
        }
    } else if (is_video) {
        filename = NormalizeFileName(filename);
        if (filename.empty()) {
            filename = "video." + ExtensionForVideoMime(mime_type);
        }
    } else {
        filename = NormalizeFileName(filename);
        if (filename.empty()) filename = "file.bin";
    }
    std::string extension;
    if (is_image) {
        extension = ExtensionForImageMime(mime_type);
    } else if (is_video) {
        extension = ExtensionForVideoMime(mime_type);
    } else {
        extension = SafeFileExtension(filename);
    }

    MediaObject media;
    media.media_id = GenerateMediaId();
    media.owner_id = user_id;
    media.kind = kind;
    media.status = "init";
    media.original_name = filename;
    media.mime_type = mime_type;
    media.size_bytes = size_bytes;
    media.sha256 = sha256;
    media.storage_bucket = s3_client_->config().bucket;
    media.object_key = "media/" + media.media_id + "/original." + extension;

    std::string err;
    if (!media_dao_->Create(media, &err)) {
        LogError("create media_object failed: " + err);
        WriteError(resp, 500, "create media metadata failed");
        return;
    }

    int64_t expires_at = 0;
    const int ttl = s3_client_->config().presign_ttl_seconds;
    const std::string upload_url = s3_client_->PresignPut(
        media.object_key, ttl, &expires_at);
    if (upload_url.empty()) {
        LogError("presign PUT failed for media " + media.media_id);
        WriteError(resp, 500, "create upload url failed");
        return;
    }

    nlohmann::json data;
    data["media_id"] = media.media_id;
    data["object_key"] = media.object_key;
    data["status"] = media.status;
    data["upload_method"] = "PUT";
    data["upload_url"] = upload_url;
    data["upload_headers"] = {{"Content-Type", mime_type}};
    data["expires_at"] = expires_at;
    data["max_bytes"] = max_bytes;
    WriteJson(resp, 0, "ok", data.dump());
}

// POST /api/media/upload/complete
// 浏览器 PUT 成功后调用；Logic 用 HEAD 确认对象真的存在。
// 图片/视频入异步处理队列，普通文件直接标记 ready。
void HttpApiServer::handleMediaUploadComplete(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!media_dao_ || !s3_client_ || !s3_client_->ready()) {
        WriteError(resp, 503, "media storage is not initialized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400, "invalid json body, expect {\"media_id\":\"...\"}");
        return;
    }
    std::string media_id;
    try {
        if (!j.contains("media_id") || !j["media_id"].is_string()) {
            WriteError(resp, 400, "media_id required");
            return;
        }
        media_id = j["media_id"].get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid json fields in media upload complete");
        return;
    }
    if (!IsValidMediaId(media_id)) {
        WriteError(resp, 400, "invalid media_id");
        return;
    }

    MediaObject media;
    std::string err;
    if (!media_dao_->GetById(media_id, &media, &err)) {
        WriteError(resp, 404, "media not found");
        return;
    }
    if (media.owner_id != user_id) {
        WriteError(resp, 403, "forbidden: not media owner");
        return;
    }
    if (media.status == "deleted") {
        WriteError(resp, 404, "media not found");
        return;
    }
    if (media.status == "failed") {
        WriteError(resp, 409, "media processing failed");
        return;
    }
    if (media.status == "ready") {
        nlohmann::json data{{"media_id", media_id}, {"status", "ready"}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    if (media.status == "processing") {
        nlohmann::json data{{"media_id", media_id}, {"status", "processing"}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    if (media.kind != "image" && media.kind != "video" &&
        media.kind != "file") {
        WriteError(resp, 400, "unsupported media kind");
        return;
    }
    int64_t max_bytes = media_max_file_bytes_;
    if (media.kind == "image") {
        max_bytes = media_max_image_bytes_;
    } else if (media.kind == "video") {
        max_bytes = media_max_video_bytes_;
    }

    S3HeadResult head;
    if (!s3_client_->HeadObject(media.object_key, &head, &err)) {
        LogWarn("HEAD media object failed for " + media.media_id + ": " + err);
        WriteError(resp, 400, "uploaded object not found; PUT the file first");
        return;
    }
    if (head.size_bytes <= 0) {
        WriteError(resp, 400, "uploaded object is empty");
        return;
    }
    if (head.size_bytes > max_bytes) {
        WriteError(resp, 413, "uploaded object exceeds size limit");
        return;
    }
    if (!head.content_type.empty() &&
        head.content_type != media.mime_type) {
        // MinIO 可能返回 application/octet-stream；这里只告警，真正格式由
        // worker 读取文件头判断，避免仅凭客户端声明的 Content-Type 放行。
        LogWarn("media " + media.media_id + " content-type mismatch: declared=" +
                media.mime_type + ", stored=" + head.content_type);
    }
    if (!media_dao_->MarkUploaded(media.media_id, head.size_bytes, head.etag,
                                  &err)) {
        LogError("MarkUploaded failed for " + media.media_id + ": " + err);
        WriteError(resp, 500, "update media status failed");
        return;
    }

    // 文件不需要转码/缩略图，HEAD 校验通过即可直接标记 ready；
    // 图片/视频进入异步 media_task，由 worker 生成变体。
    std::string final_status = "uploaded";
    if (media.kind == "file") {
        if (!media_dao_->MarkReady(media.media_id, &err)) {
            LogError("MarkReady failed for " + media.media_id + ": " + err);
            WriteError(resp, 500, "update media status failed");
            return;
        }
        final_status = "ready";
    } else {
        const std::string task_type =
            media.kind == "video" ? "process_video" : "process_image";
        if (!media_dao_->EnqueueTask(media.media_id, task_type,
                                     media_task_max_attempts_, &err)) {
            LogError("EnqueueTask failed for " + media.media_id + ": " + err);
            WriteError(resp, 500, "enqueue media task failed");
            return;
        }
    }

    nlohmann::json data{{"media_id", media.media_id},
                        {"status", final_status},
                        {"size_bytes", head.size_bytes}};
    WriteJson(resp, 0, "ok", data.dump());
}

// GET /api/media/{media_id}
// 图片 ready 时返回 original/micro/thumb/preview；视频返回 original+poster；
// 文件返回 original 下载 URL。未 ready 时返回状态，前端轮询。
void HttpApiServer::handleMediaGet(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kGet) {
        WriteError(resp, 405, "only GET allowed");
        return;
    }
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!media_dao_ || !s3_client_ || !s3_client_->ready()) {
        WriteError(resp, 503, "media storage is not initialized");
        return;
    }

    static const std::string kPrefix = "/api/media/";
    const std::string& path = req.path();
    if (path.size() <= kPrefix.size() ||
        path.compare(0, kPrefix.size(), kPrefix) != 0) {
        WriteError(resp, 404, "media not found");
        return;
    }
    const std::string media_id = path.substr(kPrefix.size());
    if (media_id.find('/') != std::string::npos ||
        !IsValidMediaId(media_id)) {
        WriteError(resp, 404, "media not found");
        return;
    }

    MediaObject media;
    std::string err;
    if (!media_dao_->GetById(media_id, &media, &err)) {
        WriteError(resp, 404, "media not found");
        return;
    }

    // owner 永远可读；已绑定消息的媒体允许会话成员读取。
    bool allowed = (media.owner_id == user_id);
    if (!allowed && !media.session_id.empty()) {
        if (!CheckSessionAccess(user_id, media.session_id, resp)) return;
        allowed = true;
    }
    if (!allowed) {
        // 未绑定且不是 owner：用 404 隐藏媒体是否存在。
        WriteError(resp, 404, "media not found");
        return;
    }

    nlohmann::json data;
    data["media_id"] = media.media_id;
    data["kind"] = media.kind;
    data["status"] = media.status;
    data["mime_type"] = media.mime_type;
    data["original_name"] = media.original_name;
    data["size_bytes"] = media.size_bytes;
    data["width"] = media.width;
    data["height"] = media.height;
    data["duration_ms"] = media.duration_ms;
    data["dominant_color"] = media.dominant_color;
    data["blur_hash"] = media.blur_hash;

    if (media.status != "ready") {
        if (media.status == "failed") {
            data["error"] = (user_id == media.owner_id)
                                ? media.error_message
                                : "media processing failed";
        }
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }

    std::vector<MediaVariant> variants;
    if (!media_dao_->ListVariants(media.media_id, &variants, &err)) {
        LogError("ListVariants failed for " + media.media_id + ": " + err);
        WriteError(resp, 500, "list media variants failed");
        return;
    }

    const int ttl = media_download_url_ttl_seconds_ > 0
                        ? media_download_url_ttl_seconds_
                        : 900;
    nlohmann::json variant_json = nlohmann::json::object();
    for (const auto& variant : variants) {
        int64_t expires_at = 0;
        const std::string url =
            s3_client_->PresignGet(variant.object_key, ttl, &expires_at);
        if (url.empty()) {
            WriteError(resp, 503, "create media download url failed");
            return;
        }
        variant_json[variant.variant] = {
            {"url", url},
            {"width", variant.width},
            {"height", variant.height},
            {"size_bytes", variant.size_bytes},
            {"format", variant.format},
            {"expires_at", expires_at},
        };
    }
    // 图片必须有至少一档缩略图；普通文件没有变体，原文件即是最终产物。
    if (variant_json.empty() && media.kind != "file") {
        WriteError(resp, 500, "media has no ready variants");
        return;
    }

    int64_t original_expires_at = 0;
    std::string original_url;
    if (media.kind == "file") {
        // 下载 URL 带 response-content-disposition=attachment，浏览器不会
        // 把用户上传的 HTML/SVG 当作同源页面直接渲染。
        original_url = s3_client_->PresignGetDownload(
            media.object_key, ttl, media.original_name, &original_expires_at);
    } else {
        original_url =
            s3_client_->PresignGet(media.object_key, ttl, &original_expires_at);
    }
    if (original_url.empty()) {
        WriteError(resp, 503, "create original download url failed");
        return;
    }
    data["variants"] = std::move(variant_json);
    data["original"] = {
        {"url", original_url},
        {"width", media.width},
        {"height", media.height},
        {"size_bytes", media.size_bytes},
        {"mime_type", media.mime_type},
        {"download_name", media.original_name},
        {"expires_at", original_expires_at},
    };
    resp->addHeader("Cache-Control", "no-store");
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
