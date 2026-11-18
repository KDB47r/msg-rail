// HttpApiServer 媒体域 handler：图片/普通文件直传初始化、确认上传、查询签名 URL。
#include <algorithm>
#include <cctype>
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"
#include "media_dao.h"
#include "media_id.h"
#include "media_read.h"
#include "s3_client.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

namespace {

constexpr int64_t kMinMultipartPartSize = 5LL * 1024 * 1024;
constexpr int kMaxMultipartParts = 10000;

int64_t NowSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// 计算 S3 multipart 的 part_size / part_count。
bool ComputeMultipartPlan(int64_t size_bytes, int64_t configured_part_size,
                          int64_t* part_size, int* part_count) {
    if (!part_size || !part_count || size_bytes <= 0) return false;
    int64_t size = configured_part_size > 0 ? configured_part_size
                                            : 8LL * 1024 * 1024;
    if (size < kMinMultipartPartSize) size = kMinMultipartPartSize;
    int64_t count = (size_bytes + size - 1) / size;
    if (count > kMaxMultipartParts) {
        size = (size_bytes + kMaxMultipartParts - 1) / kMaxMultipartParts;
        if (size < kMinMultipartPartSize) size = kMinMultipartPartSize;
        count = (size_bytes + size - 1) / size;
    }
    if (count <= 0 || count > kMaxMultipartParts) return false;
    *part_size = size;
    *part_count = static_cast<int>(count);
    return true;
}

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
    bool request_multipart = false;
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
        if (j.contains("multipart")) {
            if (!j["multipart"].is_boolean()) {
                WriteError(resp, 400, "multipart must be a boolean");
                return;
            }
            request_multipart = j["multipart"].get<bool>();
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

    // 视频可以选择 S3 multipart 上传；图片/文件和旧客户端仍走单次 PUT。
    if (is_video && request_multipart) {
        int64_t part_size = 0;
        int part_count = 0;
        if (!ComputeMultipartPlan(size_bytes,
                                  media_multipart_part_size_bytes_,
                                  &part_size, &part_count)) {
            media_dao_->MarkDeleted(media.media_id, &err);
            WriteError(resp, 400, "invalid multipart part size");
            return;
        }

        std::string upload_id;
        if (!s3_client_->CreateMultipartUpload(media.object_key, media.mime_type,
                                               &upload_id, &err)) {
            LogError("CreateMultipartUpload failed for " + media.media_id +
                     ": " + err);
            media_dao_->MarkDeleted(media.media_id, &err);
            WriteError(resp, 503, "create multipart upload failed");
            return;
        }

        MultipartUploadSession session;
        session.media_id = media.media_id;
        session.upload_id = upload_id;
        session.part_size = static_cast<int>(part_size);
        session.parts_total = part_count;
        const int session_ttl =
            media_multipart_upload_ttl_seconds_ > 0
                ? media_multipart_upload_ttl_seconds_
                : 86400;
        session.expires_at = NowSeconds() + session_ttl;
        if (!media_dao_->CreateMultipartSession(session, &err)) {
            LogError("CreateMultipartSession failed for " + media.media_id +
                     ": " + err);
            std::string abort_err;
            s3_client_->AbortMultipartUpload(media.object_key, upload_id,
                                             &abort_err);
            media_dao_->MarkDeleted(media.media_id, &err);
            WriteError(resp, 500, "create multipart upload session failed");
            return;
        }

        nlohmann::json data;
        data["media_id"] = media.media_id;
        data["object_key"] = media.object_key;
        data["status"] = media.status;
        data["upload_method"] = "multipart";
        data["part_size"] = part_size;
        data["part_count"] = part_count;
        data["expires_at"] = session.expires_at;
        data["max_bytes"] = max_bytes;
        WriteJson(resp, 0, "ok", data.dump());
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

    MultipartUploadSession upload;
    bool has_multipart = false;
    if (!media_dao_->GetMultipartSession(media_id, &upload, &has_multipart,
                                         &err)) {
        LogError("GetMultipartSession failed for " + media_id + ": " + err);
        WriteError(resp, 500, "load multipart upload failed");
        return;
    }

    S3HeadResult head;
    if (has_multipart) {
        if (media.kind != "video") {
            WriteError(resp, 409, "multipart upload is only for video");
            return;
        }
        if (upload.expires_at > 0 && upload.expires_at < NowSeconds()) {
            WriteError(resp, 409, "multipart upload expired; init again");
            return;
        }

        std::vector<S3MultipartPart> parts;
        bool no_such_upload = false;
        if (!s3_client_->ListParts(media.object_key, upload.upload_id, &parts,
                                   &no_such_upload, &err)) {
            if (!no_such_upload) {
                LogError("ListParts failed for " + media_id + ": " + err);
                WriteError(resp, 503, "list multipart parts failed");
                return;
            }
            // S3 已完成但 DB 未更新：用 HEAD 恢复，保证 complete 幂等。
            if (!s3_client_->HeadObject(media.object_key, &head, &err)) {
                WriteError(resp, 409,
                           "multipart upload not found; upload again");
                return;
            }
        } else {
            if (static_cast<int>(parts.size()) != upload.parts_total) {
                WriteError(resp, 409,
                           "multipart upload incomplete: uploaded " +
                               std::to_string(parts.size()) + "/" +
                               std::to_string(upload.parts_total));
                return;
            }
            int64_t total = 0;
            const int64_t expected_last =
                media.size_bytes -
                static_cast<int64_t>(upload.part_size) *
                    (upload.parts_total - 1);
            for (size_t i = 0; i < parts.size(); ++i) {
                if (parts[i].part_number != static_cast<int>(i) + 1) {
                    WriteError(resp, 400, "multipart parts are not contiguous");
                    return;
                }
                const int64_t expected =
                    (static_cast<int>(i) == upload.parts_total - 1)
                        ? expected_last
                        : upload.part_size;
                if (expected <= 0 || parts[i].size_bytes != expected) {
                    WriteError(resp, 400,
                               "multipart part size mismatch at part " +
                                   std::to_string(parts[i].part_number));
                    return;
                }
                total += parts[i].size_bytes;
            }
            if (total != media.size_bytes) {
                WriteError(resp, 400, "multipart total size mismatch");
                return;
            }

            S3PutResult complete_result;
            if (!s3_client_->CompleteMultipartUpload(
                    media.object_key, upload.upload_id, parts,
                    &complete_result, &err)) {
                LogError("CompleteMultipartUpload failed for " + media_id +
                         ": " + err);
                WriteError(resp, 503, "complete multipart upload failed");
                return;
            }
            if (!s3_client_->HeadObject(media.object_key, &head, &err)) {
                LogError("HEAD after multipart complete failed for " +
                         media_id + ": " + err);
                WriteError(resp, 503, "verify uploaded object failed");
                return;
            }
        }
    } else if (!s3_client_->HeadObject(media.object_key, &head, &err)) {
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
    if (has_multipart) {
        std::string session_err;
        if (!media_dao_->DeleteMultipartSession(media.media_id, &session_err)) {
            LogWarn("DeleteMultipartSession failed for " + media.media_id +
                    ": " + session_err);
        }
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

// POST /api/media/upload/multipart/part_urls
// 只给 owner 返回指定 part 的短期预签名 UploadPart URL。
void HttpApiServer::handleMediaMultipartPartUrls(const HttpRequest& req,
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
        WriteError(resp, 400, "invalid json body");
        return;
    }
    std::string media_id;
    std::vector<int> part_numbers;
    try {
        if (!j.contains("media_id") || !j["media_id"].is_string() ||
            !j.contains("part_numbers") || !j["part_numbers"].is_array()) {
            WriteError(resp, 400, "media_id/part_numbers required");
            return;
        }
        media_id = j["media_id"].get<std::string>();
        if (j["part_numbers"].size() == 0 || j["part_numbers"].size() > 100) {
            WriteError(resp, 400, "part_numbers must contain 1..100 items");
            return;
        }
        for (const auto& item : j["part_numbers"]) {
            if (!item.is_number_integer()) {
                WriteError(resp, 400, "part_numbers must be integers");
                return;
            }
            const int n = item.get<int>();
            if (n <= 0) {
                WriteError(resp, 400, "part_numbers must be positive");
                return;
            }
            part_numbers.push_back(n);
        }
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid json fields in part_urls");
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
    if (media.status != "init" || media.kind != "video") {
        WriteError(resp, 409, "media is not an active video upload");
        return;
    }

    MultipartUploadSession upload;
    bool found = false;
    if (!media_dao_->GetMultipartSession(media_id, &upload, &found, &err)) {
        LogError("GetMultipartSession failed for " + media_id + ": " + err);
        WriteError(resp, 500, "load multipart upload failed");
        return;
    }
    if (!found) {
        WriteError(resp, 404, "multipart upload not found");
        return;
    }
    if (upload.expires_at > 0 && upload.expires_at < NowSeconds()) {
        WriteError(resp, 409, "multipart upload expired; init again");
        return;
    }

    // 去重，避免同一批重复签名。
    std::sort(part_numbers.begin(), part_numbers.end());
    part_numbers.erase(std::unique(part_numbers.begin(), part_numbers.end()),
                       part_numbers.end());
    for (int n : part_numbers) {
        if (n > upload.parts_total) {
            WriteError(resp, 400, "part_number out of range");
            return;
        }
    }

    const int ttl = media_upload_url_ttl_seconds_ > 0
                        ? media_upload_url_ttl_seconds_
                        : s3_client_->config().presign_ttl_seconds;
    nlohmann::json parts = nlohmann::json::array();
    for (int n : part_numbers) {
        int64_t expires_at = 0;
        const std::string url = s3_client_->PresignUploadPart(
            media.object_key, upload.upload_id, n, ttl, &expires_at);
        if (url.empty()) {
            LogError("PresignUploadPart failed for " + media_id + " part " +
                     std::to_string(n));
            WriteError(resp, 503, "create part upload url failed");
            return;
        }
        parts.push_back({{"part_number", n},
                         {"url", url},
                         {"expires_at", expires_at}});
    }

    nlohmann::json data;
    data["media_id"] = media_id;
    data["parts"] = std::move(parts);
    WriteJson(resp, 0, "ok", data.dump());
}

// POST /api/media/upload/multipart/status
// 返回已上传 part，支持浏览器中断后跳过已完成分片。
void HttpApiServer::handleMediaMultipartStatus(const HttpRequest& req,
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
        WriteError(resp, 400, "invalid json body");
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
        WriteError(resp, 400, "invalid media_id");
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
    if (media.status == "ready" || media.status == "processing" ||
        media.status == "uploaded") {
        nlohmann::json data{{"media_id", media_id},
                            {"status", media.status},
                            {"uploaded_parts", nlohmann::json::array()}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    if (media.status != "init" || media.kind != "video") {
        WriteError(resp, 409, "media is not an active video upload");
        return;
    }

    MultipartUploadSession upload;
    bool found = false;
    if (!media_dao_->GetMultipartSession(media_id, &upload, &found, &err)) {
        LogError("GetMultipartSession failed for " + media_id + ": " + err);
        WriteError(resp, 500, "load multipart upload failed");
        return;
    }
    if (found && upload.expires_at > 0 &&
        upload.expires_at < NowSeconds()) {
        WriteError(resp, 409, "multipart upload expired; init again");
        return;
    }

    if (found) {
        std::vector<S3MultipartPart> parts;
        bool no_such_upload = false;
        if (!s3_client_->ListParts(media.object_key, upload.upload_id, &parts,
                                   &no_such_upload, &err)) {
            if (!no_such_upload) {
                LogError("ListParts failed for " + media_id + ": " + err);
                WriteError(resp, 503, "list multipart parts failed");
                return;
            }
            // upload 已被 S3 合并但 DB 尚未更新：让客户端直接 complete。
            S3HeadResult head;
            if (s3_client_->HeadObject(media.object_key, &head, &err) &&
                head.size_bytes == media.size_bytes) {
                nlohmann::json data{
                    {"media_id", media_id},
                    {"status", "uploaded"},
                    {"uploaded_parts", nlohmann::json::array()}};
                WriteJson(resp, 0, "ok", data.dump());
                return;
            }
            WriteError(resp, 404, "multipart upload not found");
            return;
        }

        nlohmann::json uploaded = nlohmann::json::array();
        for (const auto& part : parts) {
            uploaded.push_back({{"part_number", part.part_number},
                                {"size_bytes", part.size_bytes}});
        }
        nlohmann::json data;
        data["media_id"] = media_id;
        data["status"] = "init";
        data["part_size"] = upload.part_size;
        data["part_count"] = upload.parts_total;
        data["uploaded_parts"] = std::move(uploaded);
        data["expires_at"] = upload.expires_at;
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }

    // 没有会话：兼容 S3 已完成但 DB 没更新的情况。
    S3HeadResult head;
    if (s3_client_->HeadObject(media.object_key, &head, &err) &&
        head.size_bytes == media.size_bytes) {
        nlohmann::json data{{"media_id", media_id},
                            {"status", "uploaded"},
                            {"uploaded_parts", nlohmann::json::array()}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    WriteError(resp, 404, "multipart upload not found");
}

// POST /api/media/upload/abort
// 取消未完成的上传：abort S3 multipart，标记 media deleted。
void HttpApiServer::handleMediaUploadAbort(const HttpRequest& req,
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
        WriteError(resp, 400, "invalid json body");
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
        WriteError(resp, 400, "invalid media_id");
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
        nlohmann::json data{{"media_id", media_id}, {"status", "deleted"}};
        WriteJson(resp, 0, "ok", data.dump());
        return;
    }
    if (media.status != "init") {
        WriteError(resp, 409, "only init upload can be aborted");
        return;
    }

    MultipartUploadSession upload;
    bool found = false;
    if (!media_dao_->GetMultipartSession(media_id, &upload, &found, &err)) {
        LogError("GetMultipartSession failed for " + media_id + ": " + err);
        WriteError(resp, 500, "load multipart upload failed");
        return;
    }
    if (found) {
        if (!s3_client_->AbortMultipartUpload(media.object_key,
                                              upload.upload_id, &err)) {
            LogError("AbortMultipartUpload failed for " + media_id + ": " +
                     err);
            WriteError(resp, 503, "abort multipart upload failed");
            return;
        }
        std::string delete_err;
        if (!media_dao_->DeleteMultipartSession(media_id, &delete_err)) {
            LogWarn("DeleteMultipartSession failed for " + media_id + ": " +
                    delete_err);
        }
    } else {
        // 单次 PUT 的 init 孤儿：对象存在就删掉，不存在也继续。
        std::string delete_err;
        s3_client_->DeleteObject(media.object_key, &delete_err);
    }
    if (!media_dao_->MarkDeleted(media_id, &err)) {
        LogError("MarkDeleted failed for " + media_id + ": " + err);
        WriteError(resp, 500, "mark media deleted failed");
        return;
    }
    nlohmann::json data{{"media_id", media_id}, {"status", "deleted"}};
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

    CachedMedia cached;
    std::string err;
    if (!LoadMediaWithVariants(media_dao_, media_cache_, media_id, &cached,
                               &err)) {
        WriteError(resp, 404, "media not found");
        return;
    }
    MediaObject& media = cached.object;

    // 竞态兜底：缓存里 session_id 为空、请求者不是 owner 时，
    // 可能是“读者在绑定前回源、绑定后写回旧值”。强制回源一次。
    if (media.session_id.empty() && media.owner_id != user_id) {
        CachedMedia fresh;
        std::string refresh_err;
        if (LoadMediaWithVariants(media_dao_, media_cache_, media_id, &fresh,
                                  &refresh_err, true)) {
            cached = std::move(fresh);
        } else {
            LogWarn("refresh media cache before access check failed: " +
                    media_id + " " + refresh_err);
        }
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

    const int ttl = media_download_url_ttl_seconds_ > 0
                        ? media_download_url_ttl_seconds_
                        : 900;
    nlohmann::json variant_json = nlohmann::json::object();
    for (const auto& variant : cached.variants) {
        nlohmann::json entry;
        std::string variant_err;
        if (!BuildVariantJson(variant, media_variant_cdn_base_url_, s3_client_,
                              ttl, &entry, &variant_err)) {
            LogError("build variant url failed for " + media.media_id + "/" +
                     variant.variant + ": " + variant_err);
            WriteError(resp, 503, "create media download url failed");
            return;
        }
        variant_json[variant.variant] = std::move(entry);
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
