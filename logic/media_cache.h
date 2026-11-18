#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "media_model.h"
#include "redis_pool.h"

namespace sparkpush {

// 媒体读路径的缓存条目：数据库元数据 + 变体列表。
// 不包含预签名 URL；URL 在响应时按当前配置现场生成。
struct CachedMedia {
    MediaObject object;
    std::vector<MediaVariant> variants;
};

// 媒体元数据 Redis 缓存。
//
// Key: media:meta:v1:{<media_id>}
// Value: JSON（CachedMedia）
//
// 设计取舍：
//   - ready 长 TTL（默认 600s），failed 60s，处理中 1s；
//   - 只缓存 DB 字段和 object_key，不缓存会过期的签名 URL；
//   - Redis 失败时返回 false，调用方回退 MySQL；
//   - 批量读用 pipeline，避免逐 key RTT，也兼容未来 Redis Cluster。
class MediaCache {
   public:
    MediaCache(RedisConnectionPool* pool, bool enabled,
               int ready_ttl_seconds, int failed_ttl_seconds,
               int pending_ttl_seconds);

    bool enabled() const { return enabled_; }

    // 批量读取。返回 true 表示 Redis 操作成功（out 里是命中的条目）；
    // 返回 false 表示缓存不可用，调用方应全部回源 MySQL。
    bool GetMany(const std::vector<std::string>& media_ids,
                 std::unordered_map<std::string, CachedMedia>* out);

    // 批量回填。返回 false 仅表示 Redis 操作失败，不影响业务结果。
    bool PutMany(const std::vector<CachedMedia>& items);

    // 媒体绑定/解绑后删除缓存。
    bool Invalidate(const std::string& media_id);

   private:
    static std::string KeyFor(const std::string& media_id);
    static bool Serialize(const CachedMedia& media, std::string* out);
    static bool Deserialize(const std::string& json, CachedMedia* out);
    int TtlFor(const MediaObject& media) const;
    void LogThrottled(const std::string& message);

    RedisConnectionPool* pool_{nullptr};
    bool enabled_{false};
    int ready_ttl_seconds_{600};
    int failed_ttl_seconds_{60};
    int pending_ttl_seconds_{1};
    std::atomic<int64_t> last_log_ms_{0};
};

}  // namespace sparkpush
