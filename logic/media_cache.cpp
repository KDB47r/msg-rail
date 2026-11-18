#include "media_cache.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <nlohmann/json.hpp>
#include <utility>

#include "logging.h"

namespace sparkpush {

namespace {

constexpr size_t kBatchSize = 100;
constexpr int kLogThrottleMs = 5000;

std::string JsonString(const nlohmann::json& j, const char* key) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return {};
    return it->get<std::string>();
}

int64_t JsonInt64(const nlohmann::json& j, const char* key) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_number_integer()) return 0;
    return it->get<int64_t>();
}

int JsonInt(const nlohmann::json& j, const char* key) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_number_integer()) return 0;
    return it->get<int>();
}

}  // namespace

MediaCache::MediaCache(RedisConnectionPool* pool, bool enabled,
                       int ready_ttl_seconds, int failed_ttl_seconds,
                       int pending_ttl_seconds)
    : pool_(pool),
      enabled_(enabled),
      ready_ttl_seconds_(ready_ttl_seconds),
      failed_ttl_seconds_(failed_ttl_seconds),
      pending_ttl_seconds_(pending_ttl_seconds) {}

std::string MediaCache::KeyFor(const std::string& media_id) {
    // hash tag 只包 media_id，后续 Redis Cluster 下每个媒体独立分片。
    return "media:meta:v1:{" + media_id + "}";
}

int MediaCache::TtlFor(const MediaObject& media) const {
    if (media.status == "ready") {
        if (ready_ttl_seconds_ <= 0) return 0;
        // ready 的批量过期会在同一时间回源，加 0~60s 抖动打散。
        const size_t jitter =
            std::hash<std::string>{}(media.media_id) % 61;
        return ready_ttl_seconds_ + static_cast<int>(jitter);
    }
    if (media.status == "failed") return failed_ttl_seconds_;
    if (media.status == "deleted") return 0;
    return pending_ttl_seconds_;
}

bool MediaCache::Serialize(const CachedMedia& media, std::string* out) {
    if (!out || media.object.media_id.empty()) return false;
    try {
        nlohmann::json root;
        root["v"] = 1;

        const MediaObject& obj = media.object;
        nlohmann::json& j = root["obj"];
        j["id"] = obj.id;
        j["media_id"] = obj.media_id;
        j["owner_id"] = obj.owner_id;
        j["kind"] = obj.kind;
        j["status"] = obj.status;
        j["original_name"] = obj.original_name;
        j["mime_type"] = obj.mime_type;
        j["size_bytes"] = obj.size_bytes;
        j["sha256"] = obj.sha256;
        j["width"] = obj.width;
        j["height"] = obj.height;
        j["duration_ms"] = obj.duration_ms;
        j["blur_hash"] = obj.blur_hash;
        j["dominant_color"] = obj.dominant_color;
        j["storage_bucket"] = obj.storage_bucket;
        j["object_key"] = obj.object_key;
        j["etag"] = obj.etag;
        j["session_id"] = obj.session_id;
        j["message_id"] = obj.message_id;
        j["error_message"] = obj.error_message;

        auto variants = nlohmann::json::array();
        for (const auto& variant : media.variants) {
            nlohmann::json v;
            v["variant"] = variant.variant;
            v["format"] = variant.format;
            v["width"] = variant.width;
            v["height"] = variant.height;
            v["size_bytes"] = variant.size_bytes;
            v["bitrate_kbps"] = variant.bitrate_kbps;
            v["object_key"] = variant.object_key;
            variants.push_back(std::move(v));
        }
        root["variants"] = std::move(variants);
        *out = root.dump(-1, ' ', false,
                         nlohmann::json::error_handler_t::replace);
        return true;
    } catch (...) {
        return false;
    }
}

bool MediaCache::Deserialize(const std::string& json, CachedMedia* out) {
    if (!out) return false;
    try {
        const nlohmann::json root = nlohmann::json::parse(json);
        if (!root.is_object()) return false;
        auto version_it = root.find("v");
        if (version_it == root.end() || !version_it->is_number_integer() ||
            version_it->get<int>() != 1) {
            return false;
        }
        auto obj_it = root.find("obj");
        if (obj_it == root.end() || !obj_it->is_object()) return false;

        const nlohmann::json& j = *obj_it;
        CachedMedia cached;
        MediaObject& obj = cached.object;
        obj.id = JsonInt64(j, "id");
        obj.media_id = JsonString(j, "media_id");
        obj.owner_id = JsonInt64(j, "owner_id");
        obj.kind = JsonString(j, "kind");
        obj.status = JsonString(j, "status");
        obj.original_name = JsonString(j, "original_name");
        obj.mime_type = JsonString(j, "mime_type");
        obj.size_bytes = JsonInt64(j, "size_bytes");
        obj.sha256 = JsonString(j, "sha256");
        obj.width = JsonInt(j, "width");
        obj.height = JsonInt(j, "height");
        obj.duration_ms = JsonInt64(j, "duration_ms");
        obj.blur_hash = JsonString(j, "blur_hash");
        obj.dominant_color = JsonString(j, "dominant_color");
        obj.storage_bucket = JsonString(j, "storage_bucket");
        obj.object_key = JsonString(j, "object_key");
        obj.etag = JsonString(j, "etag");
        obj.session_id = JsonString(j, "session_id");
        obj.message_id = JsonString(j, "message_id");
        obj.error_message = JsonString(j, "error_message");
        if (obj.media_id.empty()) return false;

        auto variants_it = root.find("variants");
        if (variants_it != root.end() && variants_it->is_array()) {
            for (const auto& v : *variants_it) {
                if (!v.is_object()) continue;
                MediaVariant variant;
                variant.media_id = obj.media_id;
                variant.variant = JsonString(v, "variant");
                variant.format = JsonString(v, "format");
                variant.width = JsonInt(v, "width");
                variant.height = JsonInt(v, "height");
                variant.size_bytes = JsonInt64(v, "size_bytes");
                variant.bitrate_kbps = JsonInt(v, "bitrate_kbps");
                variant.object_key = JsonString(v, "object_key");
                if (!variant.variant.empty()) {
                    cached.variants.push_back(std::move(variant));
                }
            }
        }
        *out = std::move(cached);
        return true;
    } catch (...) {
        return false;
    }
}

bool MediaCache::GetMany(
    const std::vector<std::string>& media_ids,
    std::unordered_map<std::string, CachedMedia>* out) {
    if (out) out->clear();
    if (!out || !enabled_ || !pool_ || media_ids.empty()) return false;

    for (size_t start = 0; start < media_ids.size(); start += kBatchSize) {
        const size_t count =
            std::min(kBatchSize, media_ids.size() - start);
        auto guard = pool_->Acquire();
        redisContext* ctx = guard.get();
        if (!ctx) {
            LogThrottled("acquire redis connection failed");
            out->clear();
            return false;
        }

        bool append_ok = true;
        for (size_t i = 0; i < count; ++i) {
            const std::string key = KeyFor(media_ids[start + i]);
            if (redisAppendCommand(ctx, "GET %s", key.c_str()) != REDIS_OK) {
                append_ok = false;
                break;
            }
        }
        if (!append_ok) {
            guard.MarkBroken();
            LogThrottled("pipeline GET append failed");
            out->clear();
            return false;
        }

        bool read_ok = true;
        for (size_t i = 0; i < count; ++i) {
            void* raw = nullptr;
            if (redisGetReply(ctx, &raw) != REDIS_OK || !raw) {
                read_ok = false;
                break;
            }
            redisReply* reply = static_cast<redisReply*>(raw);
            if (reply->type == REDIS_REPLY_STRING) {
                CachedMedia cached;
                if (Deserialize(std::string(reply->str, reply->len),
                                &cached) &&
                    cached.object.media_id == media_ids[start + i]) {
                    out->emplace(media_ids[start + i], std::move(cached));
                }
            } else if (reply->type == REDIS_REPLY_ERROR) {
                LogThrottled("GET failed: " +
                             std::string(reply->str ? reply->str : ""));
            }
            freeReplyObject(reply);
        }
        if (!read_ok) {
            guard.MarkBroken();
            LogThrottled("pipeline GET read failed");
            out->clear();
            return false;
        }
    }
    return true;
}

bool MediaCache::PutMany(const std::vector<CachedMedia>& items) {
    if (!enabled_ || !pool_ || items.empty()) return true;

    struct SetItem {
        std::string key;
        int ttl{0};
        std::string json;
    };
    std::vector<SetItem> pending;
    pending.reserve(items.size());
    for (const auto& item : items) {
        const int ttl = TtlFor(item.object);
        if (ttl <= 0) continue;
        std::string json;
        if (!Serialize(item, &json)) continue;
        pending.push_back(
            SetItem{KeyFor(item.object.media_id), ttl, std::move(json)});
    }
    if (pending.empty()) return true;

    for (size_t start = 0; start < pending.size(); start += kBatchSize) {
        const size_t count =
            std::min(kBatchSize, pending.size() - start);
        auto guard = pool_->Acquire();
        redisContext* ctx = guard.get();
        if (!ctx) {
            LogThrottled("acquire redis connection failed");
            return false;
        }

        bool append_ok = true;
        for (size_t i = 0; i < count; ++i) {
            const auto& item = pending[start + i];
            if (redisAppendCommand(ctx, "SETEX %s %d %s", item.key.c_str(),
                                   item.ttl, item.json.c_str()) != REDIS_OK) {
                append_ok = false;
                break;
            }
        }
        if (!append_ok) {
            guard.MarkBroken();
            LogThrottled("pipeline SETEX append failed");
            return false;
        }

        bool read_ok = true;
        for (size_t i = 0; i < count; ++i) {
            void* raw = nullptr;
            if (redisGetReply(ctx, &raw) != REDIS_OK || !raw) {
                read_ok = false;
                break;
            }
            redisReply* reply = static_cast<redisReply*>(raw);
            if (reply->type == REDIS_REPLY_ERROR) {
                LogThrottled("SETEX failed: " +
                             std::string(reply->str ? reply->str : ""));
            }
            freeReplyObject(reply);
        }
        if (!read_ok) {
            guard.MarkBroken();
            LogThrottled("pipeline SETEX read failed");
            return false;
        }
    }
    return true;
}

bool MediaCache::Invalidate(const std::string& media_id) {
    if (!enabled_ || !pool_ || media_id.empty()) return true;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) {
        LogThrottled("acquire redis connection failed");
        return false;
    }
    const std::string key = KeyFor(media_id);
    redisReply* reply =
        static_cast<redisReply*>(redisCommand(ctx, "DEL %s", key.c_str()));
    if (!reply) {
        guard.MarkBroken();
        LogThrottled("DEL failed (null reply)");
        return false;
    }
    const bool ok = (reply->type != REDIS_REPLY_ERROR);
    if (!ok) {
        LogThrottled("DEL failed: " +
                     std::string(reply->str ? reply->str : ""));
    }
    freeReplyObject(reply);
    return ok;
}

void MediaCache::LogThrottled(const std::string& message) {
    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    int64_t previous = last_log_ms_.load(std::memory_order_relaxed);
    if (now - previous < kLogThrottleMs) return;
    if (last_log_ms_.compare_exchange_strong(previous, now)) {
        LogWarn("MediaCache: " + message);
    }
}

}  // namespace sparkpush
