#pragma once

#include <chrono>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息
//
// Key schema（统一带 hash tag，未来切 Redis Cluster / 客户端分片零成本）：
//   token:{<token>}                          - 登录态
//   route:user:{<uid>}                       - HASH conn_id -> comet_id
//   session:{<sid>}:seq                      - 会话最新 msg_seq（INCR 生成）
//   read_seq:{<sid>}:uid:<uid>               - 用户在会话上的已读 seq
//   room:{<rid>}:comets                      - SET comet_id
//   room:{<rid>}:comet_count:<cid>           - 该 comet 上该房间的用户数
//   room:{<rid>}:online_count                - 房间在线人数
//   rate:msg:{<uid>}                         - 限流 ZSET
//
// 同一分片维度的 key 通过 {...} hash tag 落到同一 slot，
// 使 MGET / MULTI / EVAL 在 Cluster 模式下依然可用。
class RedisStore {
   public:
    explicit RedisStore(RedisConnectionPool* pool,
                        int route_cache_ttl_ms = 1000)
        : pool_(pool), route_cache_ttl_ms_(route_cache_ttl_ms) {}

    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // conn_id 粒度的路由管理：一个 user 可以有多条连接（多设备/多标签页）
    bool AddRoute(int64_t user_id, const std::string& comet_id,
                  const std::string& conn_id);
    bool RemoveRoute(int64_t user_id, const std::string& conn_id);
    // 返回去重后的 comet_id 列表（用于推送路由）
    bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);

    // 会话最新 seq（读的是 IncrSessionMsgSeq 用的同一个 key，无需单独维护）
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);

    // 用户已读 seq
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);

    // 一次 RTT 同时拿 read_seq 与 session_last_seq（未读计算用）
    // 两个 key 共享 {sid} hash tag，Cluster 下同 slot，走 MGET
    // 返回 true 表示 Redis 调用成功；read_seq / last_seq 为 -1 表示 key 不存在
    bool GetUnreadSeqs(int64_t user_id, const std::string& session_id,
                       int64_t* read_seq, int64_t* last_seq);

    // 聊天室房间路由：room_id -> set<comet_id>
    bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);

    // 聊天室在线人数：room_id -> online_count
    bool GetRoomOnlineCount(int64_t room_id, int64_t* count);

    // 使用 INCRBY/DECRBY 维护在线人数计数（delta 可为正/负）
    bool IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                             int64_t* new_value = nullptr);

    // 原子上下线：INCRBY comet_count + 条件 SADD/SREM comets，一次 EVAL 完成
    // 修掉了旧实现里"应用层判后写"的竞态，同时把 2 次 RTT 压到 1 次
    // 返回 new_count：该 comet 上该房间的用户数（>=0）
    bool RoomJoinAtomic(int64_t room_id, const std::string& comet_id,
                        int64_t* new_count);
    bool RoomLeaveAtomic(int64_t room_id, const std::string& comet_id,
                         int64_t* new_count);

    enum class SessionSeqStatus { kOk, kNeedsInitialization, kError };

    // 已完成初始化的会话走单次 EVAL + INCR；初始化标记不存在时不分配序号，
    // 返回 kNeedsInitialization，让上层从持久化存储读取安全下限。
    SessionSeqStatus IncrSessionMsgSeq(const std::string& session_id,
                                       int64_t* new_seq);

    // 首次迁移或 Redis 序号丢失时，将计数器原子提升到 persisted_seq 以上再 INCR。
    // 并发初始化时只有第一个调用负责提升下限，后续调用直接继续 INCR。
    bool InitAndIncrSessionMsgSeq(const std::string& session_id,
                                  int64_t persisted_seq, int64_t* new_seq);

    // 路由变更时主动失效缓存
    void InvalidateRouteCache(int64_t user_id);

    // 滑动窗口限流：判断指定 key 在 window_ms 内是否超过 max_count
    // 返回 true 表示允许通过，false 表示被限流
    // 实现：Lua 脚本 "先判后写"，一次 RTT，原子
    bool CheckRateLimit(const std::string& key, int window_ms, int max_count);

   private:
    // 从Redis查询路由（内部方法）
    bool GetUserRoutesFromRedis(int64_t user_id,
                                std::vector<std::string>* comets);

    RedisConnectionPool* pool_;

    // 本地路由缓存
    struct RouteCacheEntry {
        std::vector<std::string> comets;
        std::chrono::steady_clock::time_point expire_time;
    };
    std::unordered_map<int64_t, RouteCacheEntry> route_cache_;
    mutable std::shared_mutex route_cache_mutex_;  // 读写锁：读不互斥
    int route_cache_ttl_ms_;
};

}  // namespace sparkpush
