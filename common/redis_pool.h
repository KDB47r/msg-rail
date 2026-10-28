#pragma once

#include <hiredis/hiredis.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

namespace sparkpush {

// Redis 连接池配置
// 设计要点：
//   - 支持自动扩容（enable_auto_grow）：空闲连接不足时动态创建，上限
//   max_pool_size
//   - 空闲回收（idle_timeout_ms）：超时未使用的连接自动释放，避免资源泄漏
//   - 热路径优化：Acquire() 跳过 PING
//   验证（减少一次网络往返），连接失效在使用时发现
struct RedisConfig {
    std::string host;
    int port{6379};
    std::string password;
    int db{0};
    int pool_size{4};
    int min_pool_size{0};
    int max_pool_size{0};
    int connect_timeout_ms{2000};
    int rw_timeout_ms{2000};
    int idle_timeout_ms{60000};
    bool enable_auto_grow{true};
};

class RedisConnectionPool;

// RAII 连接守卫：析构时自动归还连接到池（类似 std::unique_lock）
// move-only：拷贝会导致同一 ctx 被 Release 两次
class RedisConnGuard {
   public:
    // 保存连接池和 Redis 上下文，由 Guard 析构时自动归还。
    RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx);
    ~RedisConnGuard();  // 自动归还；被标记为坏的连接则销毁

    RedisConnGuard(const RedisConnGuard&) = delete;
    RedisConnGuard& operator=(const RedisConnGuard&) = delete;
    // 移动构造：转移连接所有权和损坏标记。
    RedisConnGuard(RedisConnGuard&& other) noexcept;
    // 移动赋值：先归还自己的连接，再接管对方的连接。
    RedisConnGuard& operator=(RedisConnGuard&& other) noexcept;

    // 返回裸 redisContext，供业务执行 Redis 命令。
    redisContext* get() const { return ctx_; }

    // 标记连接已损坏（如 redisCommand 返回 nullptr）：
    // 析构时销毁该连接而不归还池，避免坏连接反复污染连接池
    void MarkBroken() { broken_ = true; }

   private:
    RedisConnectionPool* pool_{nullptr};
    redisContext* ctx_{nullptr};
    bool broken_{false};
};

class RedisConnectionPool {
   public:
    // 使用默认构造，连接参数在 Init 中设置。
    RedisConnectionPool() = default;
    // 析构连接池，释放所有空闲连接。
    ~RedisConnectionPool();

    // 初始化连接池并预建最小连接数。
    bool Init(const RedisConfig& cfg);

    // 借出一个连接：优先空闲连接，不足时扩容，最后按超时等待。
    RedisConnGuard Acquire(int timeout_ms = -1);

   private:
    // 建立一个新的 Redis 连接并完成认证/选库。
    redisContext* CreateContextUnlocked();
    // 先回收超时空闲连接，再取出一个空闲连接。
    redisContext* PopIdleUnlocked();
    // 回收超过空闲阈值的连接。
    void CleanupIdleUnlocked();
    // broken=true 时销毁连接并递减计数，而不是归还 idle_
    void Release(redisContext* ctx, bool broken);

    friend class RedisConnGuard;

    std::mutex mutex_;
    std::condition_variable cv_;
    struct ConnEntry {
        redisContext* ctx{nullptr};
        std::chrono::steady_clock::time_point last_used;
    };
    std::deque<ConnEntry> idle_;
    int total_conns_{0};
    bool stopping_{false};
    RedisConfig cfg_;
};

}  // namespace sparkpush
