#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "media_cache.h"

namespace sparkpush {

class MediaDao;
class S3Client;

// 单张/批量读取媒体元数据 + 变体。先查 Redis，miss 后回源 MySQL 并回填。
// 错误只代表 MySQL 或参数失败；Redis 失败会自动降级到 MySQL。
// bypass_cache=true 时跳过 Redis 读，直接从 MySQL 加载并覆盖缓存；
// 用于媒体绑定前后的竞态兜底。
bool LoadMediaWithVariants(MediaDao* media_dao, MediaCache* cache,
                           const std::string& media_id, CachedMedia* out,
                           std::string* err, bool bypass_cache = false);

bool LoadMediaManyWithVariants(
    MediaDao* media_dao, MediaCache* cache,
    const std::vector<std::string>& media_ids,
    std::unordered_map<std::string, CachedMedia>* out, std::string* err,
    bool bypass_cache = false);

// 是否允许走公开 CDN 的变体。原图/文件/playback 不在此列。
bool IsPublicVariant(const std::string& variant);

// 生成变体 URL。公开变体返回 CDN 稳定 URL，expires_at=0；
// 其余返回预签名 URL 和过期时间。
bool BuildVariantUrl(const MediaVariant& variant,
                     const std::string& cdn_base_url, S3Client* s3,
                     int ttl_seconds, std::string* url,
                     int64_t* expires_at);

// 生成响应里的单个变体 JSON；公开 URL 不写 expires_at。
bool BuildVariantJson(const MediaVariant& variant,
                      const std::string& cdn_base_url, S3Client* s3,
                      int ttl_seconds, nlohmann::json* out, std::string* err);

}  // namespace sparkpush
