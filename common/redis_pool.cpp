#include "redis_pool.h"

#include <algorithm>

#include "logging.h"

namespace sparkpush {

// 保存连接池和 Redis 上下文，由 Guard 析构时自动归还。
RedisConnGuard::RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx)
    : pool_(&pool), ctx_(ctx) {}

// 移动构造：转移连接所有权和损坏标记。
RedisConnGuard::RedisConnGuard(RedisConnGuard&& other) noexcept
    : pool_(other.pool_), ctx_(other.ctx_), broken_(other.broken_) {
    other.ctx_ = nullptr;
    other.broken_ = false;
}

// 移动赋值：先归还自己的连接，再接管对方的连接。
RedisConnGuard& RedisConnGuard::operator=(RedisConnGuard&& other) noexcept {
    if (this != &other) {
        if (pool_ && ctx_) {
            pool_->Release(ctx_, broken_);
        }
        pool_ = other.pool_;
        ctx_ = other.ctx_;
        broken_ = other.broken_;
        other.ctx_ = nullptr;
        other.broken_ = false;
    }
    return *this;
}

// 析构时归还连接；坏连接交给连接池销毁。
RedisConnGuard::~RedisConnGuard() {
    if (pool_ && ctx_) {
        pool_->Release(ctx_, broken_);
    }
}

// 析构连接池，释放所有空闲 Redis 连接。
RedisConnectionPool::~RedisConnectionPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    stopping_ = true;
    cv_.notify_all();
    while (!idle_.empty()) {
        redisFree(idle_.front().ctx);
        idle_.pop_front();
    }
    total_conns_ = 0;
}

// 建立 Redis 连接，按配置执行 AUTH、SELECT 并设置读写超时。
redisContext* RedisConnectionPool::CreateContextUnlocked() {
    struct timeval tv;
    tv.tv_sec = cfg_.connect_timeout_ms / 1000;
    tv.tv_usec = (cfg_.connect_timeout_ms % 1000) * 1000;
    redisContext* ctx =
        redisConnectWithTimeout(cfg_.host.c_str(), cfg_.port, tv);
    if (!ctx || ctx->err) {
        if (ctx) {
            LogError(std::string("redisConnect failed: ") + ctx->errstr);
            redisFree(ctx);
        } else {
            LogError("redisConnect failed: null context");
        }
        return nullptr;
    }
    if (!cfg_.password.empty()) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "AUTH %s", cfg_.password.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LogError("redis AUTH failed");
            if (reply) freeReplyObject(reply);
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }
    if (cfg_.db != 0) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "SELECT %d", cfg_.db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LogError("redis SELECT failed");
            if (reply) freeReplyObject(reply);
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }
    struct timeval rw;
    rw.tv_sec = cfg_.rw_timeout_ms / 1000;
    rw.tv_usec = (cfg_.rw_timeout_ms % 1000) * 1000;
    redisSetTimeout(ctx, rw);
    return ctx;
}

// 初始化连接池：保存配置、推导最小/最大连接数并预建最小连接。
bool RedisConnectionPool::Init(const RedisConfig& cfg) {
    std::unique_lock<std::mutex> lock(mutex_);
    cfg_ = cfg;
    if (cfg_.min_pool_size <= 0) {
        cfg_.min_pool_size = cfg_.pool_size > 0 ? cfg_.pool_size : 1;
    }
    if (cfg_.max_pool_size <= 0) {
        cfg_.max_pool_size = std::max(cfg_.min_pool_size, cfg_.pool_size);
    }
    idle_.clear();
    total_conns_ = 0;
    for (int i = 0; i < cfg_.min_pool_size; ++i) {
        redisContext* ctx = CreateContextUnlocked();
        if (!ctx) {
            while (!idle_.empty()) {
                redisFree(idle_.front().ctx);
                idle_.pop_front();
            }
            total_conns_ = 0;
            return false;
        }
        idle_.push_back({ctx, std::chrono::steady_clock::now()});
        ++total_conns_;
    }
    stopping_ = false;
    return true;
}

// 借出一个连接：优先取空闲连接，不足时扩容，最后按超时等待。
RedisConnGuard RedisConnectionPool::Acquire(int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    redisContext* ctx = nullptr;
    auto wait_timeout = std::chrono::milliseconds(timeout_ms);

    while (!stopping_) {
        ctx = PopIdleUnlocked();
        if (ctx) break;
        if (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            ctx = CreateContextUnlocked();
            if (ctx) {
                ++total_conns_;
                break;
            }
        }
        if (timeout_ms < 0) {
            cv_.wait(lock);
        } else if (timeout_ms == 0) {
            return RedisConnGuard(*this, nullptr);
        } else {
            if (cv_.wait_for(lock, wait_timeout) == std::cv_status::timeout) {
                return RedisConnGuard(*this, nullptr);
            }
        }
    }

    // 热路径优化：跳过PING验证直接返回连接
    // 如果连接失效会在实际使用时发现并处理
    if (!ctx || stopping_) {
        return RedisConnGuard(*this, nullptr);
    }
    return RedisConnGuard(*this, ctx);
}

// 先回收超时空闲连接，再从队首取出一个空闲连接。
redisContext* RedisConnectionPool::PopIdleUnlocked() {
    CleanupIdleUnlocked();
    if (idle_.empty()) return nullptr;
    redisContext* ctx = idle_.front().ctx;
    idle_.pop_front();
    return ctx;
}

// 回收超过 idle_timeout 的空闲连接，但不低于最小连接数。
void RedisConnectionPool::CleanupIdleUnlocked() {
    if (cfg_.idle_timeout_ms <= 0) return;
    auto now = std::chrono::steady_clock::now();
    while (!idle_.empty()) {
        auto& entry = idle_.front();
        auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - entry.last_used)
                           .count();
        if (idle_ms < cfg_.idle_timeout_ms ||
            total_conns_ <= cfg_.min_pool_size) {
            break;
        }
        redisFree(entry.ctx);
        idle_.pop_front();
        --total_conns_;
    }
}

// 归还连接；坏连接、错误连接或停止中直接销毁。
void RedisConnectionPool::Release(redisContext* ctx, bool broken) {
    if (!ctx) return;
    std::unique_lock<std::mutex> lock(mutex_);
    // 坏连接（或 hiredis 已置错误标记的连接）销毁而不归还，
    // 递减计数后唤醒等待者，让其按需新建连接
    if (stopping_ || broken || ctx->err) {
        redisFree(ctx);
        if (total_conns_ > 0) --total_conns_;
        cv_.notify_one();
        return;
    }
    idle_.push_back({ctx, std::chrono::steady_clock::now()});
    cv_.notify_one();
}

}  // namespace sparkpush
