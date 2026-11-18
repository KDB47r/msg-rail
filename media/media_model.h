#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace sparkpush {

// media_object：媒体资产的稳定元数据。文件字节在 MinIO/S3，不在 MySQL。
struct MediaObject {
    int64_t id{0};
    std::string media_id;
    int64_t owner_id{0};
    std::string kind;    // image/video/voice/file
    std::string status;  // init/uploaded/processing/ready/failed/deleted
    std::string original_name;
    std::string mime_type;
    int64_t size_bytes{0};
    std::string sha256;
    int width{0};
    int height{0};
    int64_t duration_ms{0};
    std::string blur_hash;
    std::string dominant_color;
    std::string storage_bucket;
    std::string object_key;
    std::string etag;
    std::string session_id;
    std::string message_id;
    std::string error_message;
};

struct MediaVariant {
    std::string media_id;
    std::string variant;    // micro/thumb/preview/poster...
    std::string format;     // webp/jpeg/mp4...
    int width{0};
    int height{0};
    int64_t size_bytes{0};
    int bitrate_kbps{0};
    std::string object_key;
};

struct MediaTask {
    int64_t id{0};
    std::string media_id;
    std::string task_type;
    int attempts{0};
    int max_attempts{3};
};

// 一次 S3 multipart 上传的服务端状态。浏览器不持有 upload_id。
struct MultipartUploadSession {
    std::string media_id;
    std::string upload_id;
    int part_size{0};
    int parts_total{0};
    int64_t expires_at{0};  // Unix 秒
};

}  // namespace sparkpush
