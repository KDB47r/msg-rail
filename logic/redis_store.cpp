#include "redis_store.h"

#include <unordered_set>

#include "logging.h"

namespace sparkpush {

namespace {

// Lua 脚本：房间上下线原子操作
// KEYS[1] = room:{<rid>}:comet_count:<cid>
// KEYS[2] = room:{<rid>}:comets
// ARGV[1] = delta (+1 上线 / -1 下线), ARGV[2] = comet_id
// 语义：+1 且计数从 0->1 时 SADD；-1 且计数降到 <=0 时 SREM，负值兜底为 0
constexpr const char* kRoomJoinLeaveLua =
    "local d = tonumber(ARGV[1])\n"
    "local n = redis.call('INCRBY', KEYS[1], d)\n"
    "if d > 0 and n == 1 then\n"
    "  redis.call('SADD', KEYS[2], ARGV[2])\n"
    "elseif d < 0 and n <= 0 then\n"
    "  redis.call('SREM', KEYS[2], ARGV[2])\n"
    "  if n < 0 then redis.call('SET', KEYS[1], 0); n = 0 end\n"
    "end\n"
    "return n";

// 会话消息序号恢复/分配脚本：
// KEYS[1] = session:{<sid>}:seq
// KEYS[2] = session:{<sid>}:seq_initialized
// 常规路径：标记存在时直接 INCR，仍是一次 Redis RTT。
// 初始化路径：上层从 MySQL 获取 persisted_seq 后，以 max(current,persisted) 为下限。
constexpr const char* kIncrSessionSeqLua =
    "if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end\n"
    "return redis.call('INCR', KEYS[1])";

constexpr const char* kInitSessionSeqLua =
    "local floor = tonumber(ARGV[1])\n"
    "if redis.call('EXISTS', KEYS[2]) == 0 then\n"
    "  local current = tonumber(redis.call('GET', KEYS[1]) or '0')\n"
    "  if current < floor then redis.call('SET', KEYS[1], floor) end\n"
    "  redis.call('SET', KEYS[2], '1')\n"
    "end\n"
    "return redis.call('INCR', KEYS[1])";

// Lua 脚本：滑动窗口限流（先判后写，被限流的请求不计入窗口）
// KEYS[1]=rate key, ARGV = now_ms, window_ms, max_count；返回 1 允许 / 0 限流
constexpr const char* kRateLimitLua =
    "local now = tonumber(ARGV[1])\n"
    "local win = tonumber(ARGV[2])\n"
    "local mx  = tonumber(ARGV[3])\n"
    "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - win)\n"
    "local n = redis.call('ZCARD', KEYS[1])\n"
    "if n >= mx then return 0 end\n"
    "redis.call('ZADD', KEYS[1], now, now)\n"
    "redis.call('PEXPIRE', KEYS[1], win + 1000)\n"
    "return 1";

}  // namespace

// 保存 token 到 user_id 的映射并设置过期时间。
bool RedisStore::SetToken(const std::string& token, int64_t user_id,
                          int ttl_seconds) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // hash tag {<token>}：token 本身作为分片键
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SETEX token:{%s} %d %lld", token.c_str(), ttl_seconds,
        static_cast<long long>(user_id));
    if (!reply) {
        LogError("Redis SETEX token failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 用 token 查询对应的 user_id。
bool RedisStore::GetUserIdByToken(const std::string& token, int64_t* user_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    redisReply* reply =
        (redisReply*)redisCommand(ctx, "GET token:{%s}", token.c_str());
    if (!reply) {
        LogError("Redis GET token failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *user_id = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 把一条连接写入用户路由 Hash。
bool RedisStore::AddRoute(int64_t user_id, const std::string& comet_id,
                          const std::string& conn_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    // HASH: route:user:{<uid>} { conn_id => comet_id }
    // 支持同一用户多条连接（多设备/多标签页），每条连接独立记录
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "HSET route:user:{%lld} %s %s", static_cast<long long>(user_id),
        conn_id.c_str(), comet_id.c_str());
    if (!reply) {
        LogError("Redis HSET route failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    if (ok) InvalidateRouteCache(user_id);
    return ok;
}

// 从用户路由 Hash 中删除一条连接。
bool RedisStore::RemoveRoute(int64_t user_id, const std::string& conn_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "HDEL route:user:{%lld} %s", static_cast<long long>(user_id),
        conn_id.c_str());
    if (!reply) {
        LogError("Redis HDEL route failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    if (ok) InvalidateRouteCache(user_id);
    return ok;
}

// 用户路由查询（带本地缓存）：两级缓存（本地 TTL 1s + Redis HASH），
// shared_mutex 读写锁，读操作不互斥。
// 查询用户所在的 Comet 列表（带本地缓存）。
bool RedisStore::GetUserRoutes(int64_t user_id,
                               std::vector<std::string>* comets) {
    if (!comets) return false;

    auto now = std::chrono::steady_clock::now();
    {
        std::shared_lock<std::shared_mutex> lock(route_cache_mutex_);
        auto it = route_cache_.find(user_id);
        if (it != route_cache_.end() && it->second.expire_time > now) {
            *comets = it->second.comets;
            return true;
        }
    }

    std::vector<std::string> redis_comets;
    if (!GetUserRoutesFromRedis(user_id, &redis_comets)) return false;

    {
        std::unique_lock<std::shared_mutex> lock(route_cache_mutex_);
        RouteCacheEntry entry;
        entry.comets = redis_comets;
        entry.expire_time =
            now + std::chrono::milliseconds(route_cache_ttl_ms_);
        route_cache_[user_id] = std::move(entry);
    }
    *comets = std::move(redis_comets);
    return true;
}

// 从 Redis 读取并去重用户所在的 Comet 列表。
bool RedisStore::GetUserRoutesFromRedis(int64_t user_id,
                                        std::vector<std::string>* comets) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "HVALS route:user:{%lld}", static_cast<long long>(user_id));
    if (!reply) {
        LogError("Redis HVALS route failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        // 同一用户多条连接可能落在同一 Comet，需去重
        std::unordered_set<std::string> seen;
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                if (seen.insert(e->str).second) {
                    comets->push_back(e->str);
                }
            }
        }
    }
    freeReplyObject(reply);
    return ok;
}

// 让指定用户的本地路由缓存失效。
void RedisStore::InvalidateRouteCache(int64_t user_id) {
    std::unique_lock<std::shared_mutex> lock(route_cache_mutex_);
    route_cache_.erase(user_id);
}

// last_seq 与 seq 是同一个 key：IncrSessionMsgSeq 生成后无需再单独 SET，
// 原来的 session:last_seq:{sid} 已被合并掉，省一次写 RTT。
// 读取会话当前最大消息序号。
bool RedisStore::GetSessionLastSeq(const std::string& session_id,
                                   int64_t* last_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET session:{%s}:seq", session_id.c_str());
    if (!reply) {
        LogError("Redis GET session seq failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *last_seq = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 保存用户在某会话的已读序号。
bool RedisStore::SetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    // hash tag {<sid>}：与 session:{<sid>}:seq 同 slot，支持 MGET / EVAL
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SET read_seq:{%s}:uid:%lld %lld", session_id.c_str(),
        static_cast<long long>(user_id), static_cast<long long>(read_seq));
    if (!reply) {
        LogError("Redis SET read_seq failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 读取用户在某会话的已读序号。
bool RedisStore::GetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t* read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET read_seq:{%s}:uid:%lld", session_id.c_str(),
        static_cast<long long>(user_id));
    if (!reply) {
        LogError("Redis GET read_seq failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *read_seq = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 一次 MGET 同时取 read_seq 与 session_last_seq：两个 key 都带 {<sid>}
// hash tag，Cluster 下同 slot。未读读路径由 2 次 RTT 压到 1 次。
// -1 约定：key 不存在。
// 批量读取多个会话的已读序号。
bool RedisStore::GetUnreadSeqs(int64_t user_id, const std::string& session_id,
                               int64_t* read_seq, int64_t* last_seq) {
    if (!pool_ || !read_seq || !last_seq) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "MGET read_seq:{%s}:uid:%lld session:{%s}:seq "
        "session:{%s}:seq_initialized",
        session_id.c_str(), static_cast<long long>(user_id),
        session_id.c_str(), session_id.c_str());
    if (!reply) {
        LogError("Redis MGET unread seqs failed");
        guard.MarkBroken();
        return false;
    }

    *read_seq = -1;
    *last_seq = -1;

    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3) {
        // seq_initialized 不存在说明新 key 尚未从 MySQL 恢复，不能相信
        // 当前 seq（它可能是 key 迁移后错误地从 1 开始的旧值）。
        redisReply* initialized = reply->element[2];
        if (initialized && initialized->type == REDIS_REPLY_STRING) {
            ok = true;
            auto parse = [](redisReply* e, int64_t* out) {
                if (e && e->type == REDIS_REPLY_STRING && e->str) {
                    *out = std::stoll(e->str);
                }
            };
            parse(reply->element[0], read_seq);
            parse(reply->element[1], last_seq);
        }
    }
    freeReplyObject(reply);
    return ok;
}

// 查询房间用户分布在哪些 Comet。
bool RedisStore::GetRoomComets(int64_t room_id,
                               std::vector<std::string>* comets) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SMEMBERS room:{%lld}:comets", static_cast<long long>(room_id));
    if (!reply) {
        LogError("Redis SMEMBERS room:comets failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                comets->push_back(e->str);
            }
        }
    }
    freeReplyObject(reply);
    return ok;
}

// 读取房间在线人数。
bool RedisStore::GetRoomOnlineCount(int64_t room_id, int64_t* count) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET room:{%lld}:online_count", static_cast<long long>(room_id));
    if (!reply) {
        LogError("Redis GET room:online_count failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *count = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 原子增减房间在线人数。
bool RedisStore::IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                                     int64_t* new_value) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "INCRBY room:{%lld}:online_count %lld",
        static_cast<long long>(room_id), static_cast<long long>(delta));
    if (!reply) {
        LogError("Redis INCRBY room:online_count failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_value) *new_value = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

// 房间上下线原子操作：EVAL 一次搞定 INCRBY + 条件 SADD/SREM。
// 相比旧的两次 RTT + 应用层判条件：
//   1. RTT 减半
//   2. 消除"并发 INCR 到 1 都触发 SADD / -1 到 0 都触发 SREM"的竞态
// 用 Lua 原子登记用户加入房间及 Comet 计数。
bool RedisStore::RoomJoinAtomic(int64_t room_id, const std::string& comet_id,
                                int64_t* new_count) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    // KEYS 都用 {<room_id>} hash tag，Cluster 下同 slot
    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "EVAL %s 2 room:{%lld}:comet_count:%s room:{%lld}:comets %d %s",
        kRoomJoinLeaveLua, static_cast<long long>(room_id), comet_id.c_str(),
        static_cast<long long>(room_id), 1, comet_id.c_str());
    if (!reply) {
        LogError("Redis EVAL RoomJoin failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_count) *new_count = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

// 用 Lua 原子登记用户离开房间并清理 Comet 计数。
bool RedisStore::RoomLeaveAtomic(int64_t room_id, const std::string& comet_id,
                                 int64_t* new_count) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "EVAL %s 2 room:{%lld}:comet_count:%s room:{%lld}:comets %d %s",
        kRoomJoinLeaveLua, static_cast<long long>(room_id), comet_id.c_str(),
        static_cast<long long>(room_id), -1, comet_id.c_str());
    if (!reply) {
        LogError("Redis EVAL RoomLeave failed");
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_count) *new_count = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

// 会话消息序号生成：初始化标记存在时，单次 EVAL 原子 INCR。
// key 里的 {<sid>} hash tag 让 seq、初始化标记和 read_seq 落在同一 slot。
// 用 Lua 原子递增会话消息序号。
RedisStore::SessionSeqStatus RedisStore::IncrSessionMsgSeq(
    const std::string& session_id, int64_t* new_seq) {
    if (!pool_) return SessionSeqStatus::kError;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return SessionSeqStatus::kError;

    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "EVAL %s 2 session:{%s}:seq session:{%s}:seq_initialized",
        kIncrSessionSeqLua, session_id.c_str(), session_id.c_str());
    if (!reply) {
        guard.MarkBroken();
        return SessionSeqStatus::kError;
    }

    SessionSeqStatus status = SessionSeqStatus::kError;
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == -1) {
            status = SessionSeqStatus::kNeedsInitialization;
        } else if (reply->integer > 0) {
            if (new_seq) *new_seq = reply->integer;
            status = SessionSeqStatus::kOk;
        }
    }
    freeReplyObject(reply);
    return status;
}

// 初始化会话序号下限后再原子递增。
bool RedisStore::InitAndIncrSessionMsgSeq(const std::string& session_id,
                                          int64_t persisted_seq,
                                          int64_t* new_seq) {
    if (!pool_ || persisted_seq < 0) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "EVAL %s 2 session:{%s}:seq session:{%s}:seq_initialized %lld",
        kInitSessionSeqLua, session_id.c_str(), session_id.c_str(),
        static_cast<long long>(persisted_seq));
    if (!reply) {
        guard.MarkBroken();
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER && reply->integer > 0) {
        if (new_seq) *new_seq = reply->integer;
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 滑动窗口限流（Redis Sorted Set + Lua）
// 相比旧的 MULTI + pipeline 版本：
//   1. 语义正确：先判后写，被限流的请求不再计入窗口，避免"越拒越严"
//   2. 单次 EVAL 网络字节数更小
//   3. 保留降级：Redis 不可用时放行
// 调用方保证 key 已带 hash tag（如 rate:msg:{<uid>}）
// 用 Lua 做滑动窗口限流。
bool RedisStore::CheckRateLimit(const std::string& key, int window_ms,
                                int max_count) {
    if (!pool_) return true;

    auto guard = pool_->Acquire(100);
    redisContext* ctx = guard.get();
    if (!ctx) return true;

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();

    redisReply* reply = (redisReply*)redisCommand(
        ctx, "EVAL %s 1 %s %lld %d %d", kRateLimitLua, key.c_str(),
        (long long)now, window_ms, max_count);
    if (!reply) {
        guard.MarkBroken();
        return true;
    }

    bool allowed = true;
    if (reply->type == REDIS_REPLY_INTEGER) {
        allowed = (reply->integer == 1);
    }
    freeReplyObject(reply);
    return allowed;
}

}  // namespace sparkpush
