#pragma once

// 极简 S3/MinIO 客户端。
//
// 只实现第一阶段需要的四个能力：
//   - 预签名 PUT / GET URL（浏览器直传、直读）
//   - HEAD Object（complete 时校验对象是否真的存在）
//   - GET Object（media_worker 下载原图）
//   - PUT Object（media_worker 上传缩略图）
//   - PUT Object from file（media_worker 上传转码后的大视频）
//
// 设计约束：
//   * 仅支持 path-style（MinIO 默认），URL 形如
//     http://127.0.0.1:9002/bucket/media/xxx/original.jpg
//   * 不引入 AWS SDK，SigV4 由 OpenSSL 的 HMAC-SHA256 手工实现，
//     便于在面试中讲清楚“预签名 URL 到底签了什么”。
//   * 内部请求支持 http/https；本地开发通常用 http。

#include <cstdint>
#include <string>
#include <vector>

namespace sparkpush {

struct S3Config {
    std::string endpoint;         // 服务端内部访问地址，如 http://127.0.0.1:9002
    std::string public_endpoint;  // 浏览器访问地址；为空时复用 endpoint
    std::string bucket;
    std::string access_key;
    std::string secret_key;
    std::string region{"us-east-1"};
    int presign_ttl_seconds{900};
    bool path_style{true};
};

struct S3HeadResult {
    int64_t size_bytes{0};
    std::string content_type;
    std::string etag;
};

struct S3GetResult {
    std::string body;
    std::string content_type;
    std::string etag;
};

struct S3GetFileResult {
    int64_t size_bytes{0};
    std::string content_type;
    std::string etag;
};

struct S3PutResult {
    std::string etag;
};

struct S3MultipartPart {
    int part_number{0};
    std::string etag;
    int64_t size_bytes{0};
};

class S3Client {
   public:
    S3Client() = default;

    // 校验配置并解析 endpoint。失败时 err 给出可读原因。
    bool Init(const S3Config& cfg, std::string* err);
    bool ready() const { return ready_; }

    // 生成预签名 URL。失败返回空串。
    std::string PresignPut(const std::string& object_key,
                           int ttl_seconds,
                           int64_t* expires_at = nullptr) const;
    std::string PresignGet(const std::string& object_key,
                           int ttl_seconds,
                           int64_t* expires_at = nullptr) const;

    // 生成带 Content-Disposition: attachment 的 GET URL。
    // 用于文件消息下载，避免浏览器把用户上传的 HTML/PDF 直接在当前域打开。
    std::string PresignGetDownload(const std::string& object_key,
                                   int ttl_seconds,
                                   const std::string& download_name,
                                   int64_t* expires_at = nullptr) const;

    // 供单元测试注入固定时间，避免签名结果随当前时间变化。
    std::string PresignWithTime(const std::string& method,
                                const std::string& object_key,
                                int ttl_seconds, int64_t now_seconds,
                                int64_t* expires_at = nullptr,
                                std::string* err = nullptr,
                                const std::string& disposition = "") const;

    bool HeadObject(const std::string& object_key, S3HeadResult* out,
                    std::string* err) const;
    bool GetObject(const std::string& object_key, S3GetResult* out,
                   std::string* err) const;

    // 流式下载到本地文件，避免把大视频整体读进内存。
    // 使用内部 endpoint 的预签名 URL；超过 max_bytes 或超时都会失败并删除半截文件。
    bool GetObjectToFile(const std::string& object_key,
                         const std::string& file_path, int64_t max_bytes,
                         int timeout_seconds, S3GetFileResult* out,
                         std::string* err) const;

    bool PutObject(const std::string& object_key, const std::string& body,
                   const std::string& content_type, S3PutResult* out,
                   std::string* err,
                   const std::string& cache_control = "") const;

    // 流式上传本地文件，避免大视频转码结果整体读进内存。
    // 使用内部 endpoint 的预签名 PUT URL；文件必须非空且不超过 max_bytes。
    bool PutObjectFromFile(const std::string& object_key,
                           const std::string& file_path,
                           const std::string& content_type, int64_t max_bytes,
                           int timeout_seconds, S3PutResult* out,
                           std::string* err) const;

    bool DeleteObject(const std::string& object_key, std::string* err) const;

    // ---- S3 multipart 上传 ----
    bool CreateMultipartUpload(const std::string& object_key,
                               const std::string& content_type,
                               std::string* upload_id, std::string* err) const;
    std::string PresignUploadPart(const std::string& object_key,
                                  const std::string& upload_id,
                                  int part_number, int ttl_seconds,
                                  int64_t* expires_at = nullptr) const;
    // 供单元测试注入固定时间。
    std::string PresignUploadPartWithTime(
        const std::string& object_key, const std::string& upload_id,
        int part_number, int ttl_seconds, int64_t now_seconds,
        int64_t* expires_at = nullptr, std::string* err = nullptr) const;
    bool ListParts(const std::string& object_key, const std::string& upload_id,
                   std::vector<S3MultipartPart>* parts, bool* no_such_upload,
                   std::string* err) const;
    bool CompleteMultipartUpload(
        const std::string& object_key, const std::string& upload_id,
        const std::vector<S3MultipartPart>& parts, S3PutResult* out,
        std::string* err) const;
    bool AbortMultipartUpload(const std::string& object_key,
                              const std::string& upload_id,
                              std::string* err) const;

    const S3Config& config() const { return cfg_; }
    std::string ObjectUrlPath(const std::string& object_key) const;

   private:
    struct Endpoint {
        bool tls{false};
        std::string scheme;
        std::string host;
        uint16_t port{80};
        std::string base_path;  // 不含末尾 '/'
    };

    static bool ParseEndpoint(const std::string& url, Endpoint* out,
                              std::string* err);

    std::string PresignWithTimeForEndpoint(const Endpoint& ep,
                                           const std::string& method,
                                           const std::string& object_key,
                                           int ttl_seconds,
                                           int64_t now_seconds,
                                           int64_t* expires_at,
                                           std::string* err,
                                           const std::string& disposition = "") const;

    bool SignedRequest(const std::string& method,
                       const std::string& object_key,
                       const std::string& content_type,
                       const std::string& body, std::string* response_body,
                       int* status_code,
                       std::string* response_etag,
                       std::string* response_content_type,
                       std::string* response_content_length,
                       std::string* err,
                       const std::string& cache_control = "",
                       const std::string& query = "") const;

    S3Config cfg_;
    Endpoint internal_;
    Endpoint public_;
    bool ready_{false};
};

}  // namespace sparkpush
