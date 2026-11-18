#include "media_read.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "logging.h"
#include "media_dao.h"
#include "s3_client.h"

namespace sparkpush {

namespace {

bool CacheEntryUsable(const CachedMedia& cached) {
    if (cached.object.media_id.empty()) return false;
    // ready 的图片/视频必须有变体；空变体说明缓存条目不可用，回源刷新。
    if (cached.object.status == "ready" && cached.object.kind != "file" &&
        cached.variants.empty()) {
        return false;
    }
    return true;
}

bool BuildVariantUrlInternal(const MediaVariant& variant,
                             const std::string& cdn_base_url, S3Client* s3,
                             int ttl_seconds, std::string* url,
                             int64_t* expires_at) {
    if (!url || variant.object_key.empty()) return false;
    url->clear();
    if (expires_at) *expires_at = 0;

    if (!cdn_base_url.empty() && IsPublicVariant(variant.variant)) {
        std::string base = cdn_base_url;
        while (!base.empty() && base.back() == '/') base.pop_back();
        if (base.empty()) return false;
        *url = base + "/" + variant.object_key;
        return true;
    }

    if (!s3 || !s3->ready()) return false;
    *url = s3->PresignGet(variant.object_key, ttl_seconds, expires_at);
    return !url->empty();
}

}  // namespace

bool LoadMediaManyWithVariants(
    MediaDao* media_dao, MediaCache* cache,
    const std::vector<std::string>& media_ids,
    std::unordered_map<std::string, CachedMedia>* out, std::string* err,
    bool bypass_cache) {
    if (!out) {
        if (err) *err = "invalid output";
        return false;
    }
    out->clear();
    if (media_ids.empty()) return true;
    if (!media_dao) {
        if (err) *err = "media dao is not initialized";
        return false;
    }

    std::unordered_set<std::string> missing(media_ids.begin(),
                                            media_ids.end());
    std::unordered_map<std::string, CachedMedia> cached;
    if (!bypass_cache && cache && cache->GetMany(media_ids, &cached)) {
        for (auto& entry : cached) {
            if (CacheEntryUsable(entry.second)) {
                missing.erase(entry.first);
                out->emplace(entry.first, std::move(entry.second));
            }
        }
    }

    if (missing.empty()) return true;

    std::vector<std::string> missing_ids;
    missing_ids.reserve(missing.size());
    std::unordered_set<std::string> added;
    for (const auto& id : media_ids) {
        if (missing.erase(id) > 0 && added.insert(id).second) {
            missing_ids.push_back(id);
        }
    }

    std::vector<MediaObject> objects;
    if (!media_dao->GetByIds(missing_ids, &objects, err)) return false;

    std::vector<std::string> ready_ids;
    for (const auto& object : objects) {
        if (object.status == "ready") ready_ids.push_back(object.media_id);
    }
    std::vector<MediaVariant> variants;
    if (!ready_ids.empty() &&
        !media_dao->ListVariantsByMediaIds(ready_ids, &variants, err)) {
        return false;
    }

    std::unordered_map<std::string, std::vector<MediaVariant>> by_media;
    for (auto& variant : variants) {
        by_media[variant.media_id].push_back(std::move(variant));
    }

    std::vector<CachedMedia> fill;
    fill.reserve(objects.size());
    for (const auto& object : objects) {
        CachedMedia item;
        item.object = object;
        auto it = by_media.find(object.media_id);
        if (it != by_media.end()) item.variants = it->second;
        fill.push_back(item);
        out->emplace(item.object.media_id, std::move(item));
    }

    if (bypass_cache) {
        // 强制回源通常是在绑定竞态下发生的。此时不能再把刚读到的值写回，
        // 否则可能覆盖绑定方刚做的 DEL；清掉旧 key，让下一次读取重新回源。
        if (cache) {
            for (const auto& item : fill) {
                cache->Invalidate(item.object.media_id);
            }
        }
        return true;
    }
    if (cache && !fill.empty()) cache->PutMany(fill);
    return true;
}

bool LoadMediaWithVariants(MediaDao* media_dao, MediaCache* cache,
                           const std::string& media_id, CachedMedia* out,
                           std::string* err, bool bypass_cache) {
    if (!out) {
        if (err) *err = "invalid output";
        return false;
    }
    std::unordered_map<std::string, CachedMedia> loaded;
    if (!LoadMediaManyWithVariants(media_dao, cache, {media_id}, &loaded, err,
                                   bypass_cache)) {
        return false;
    }
    auto it = loaded.find(media_id);
    if (it == loaded.end()) {
        if (err) *err = "media not found";
        return false;
    }
    *out = std::move(it->second);
    return true;
}

bool IsPublicVariant(const std::string& variant) {
    return variant == "micro" || variant == "thumb" ||
           variant == "preview" || variant == "poster";
}

bool BuildVariantUrl(const MediaVariant& variant,
                     const std::string& cdn_base_url, S3Client* s3,
                     int ttl_seconds, std::string* url,
                     int64_t* expires_at) {
    return BuildVariantUrlInternal(variant, cdn_base_url, s3, ttl_seconds,
                                   url, expires_at);
}

bool BuildVariantJson(const MediaVariant& variant,
                      const std::string& cdn_base_url, S3Client* s3,
                      int ttl_seconds, nlohmann::json* out, std::string* err) {
    if (!out) {
        if (err) *err = "invalid output";
        return false;
    }
    std::string url;
    int64_t expires_at = 0;
    if (!BuildVariantUrlInternal(variant, cdn_base_url, s3, ttl_seconds, &url,
                                 &expires_at)) {
        if (err) *err = "build variant url failed";
        return false;
    }
    nlohmann::json j;
    j["url"] = std::move(url);
    j["width"] = variant.width;
    j["height"] = variant.height;
    j["size_bytes"] = variant.size_bytes;
    j["format"] = variant.format;
    if (expires_at > 0) j["expires_at"] = expires_at;
    *out = std::move(j);
    return true;
}

}  // namespace sparkpush
