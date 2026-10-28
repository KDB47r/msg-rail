#pragma once

#include <mysql/mysql.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

namespace sparkpush {

struct MySqlConfig {
    std::string host;
    int port{3306};
    std::string user;
    std::string password;
    std::string db;
    int pool_size{4};
    int min_pool_size{0};
    int max_pool_size{0};
    int idle_timeout_ms{60000};
    bool enable_auto_grow{true};
    bool ping_on_borrow{true};
    // 连接超时（毫秒）
    int connect_timeout_ms{2000};
    // 读/写超时（毫秒）
    int rw_timeout_ms{5000};
    // 默认字符集
    std::string charset{"utf8mb4"};
};

class MySqlConnectionPool;

class MySqlConnGuard {
   public:
    // 移动构造：转移连接所有权，避免同一连接被重复归还。
    MySqlConnGuard(MySqlConnGuard&& other) noexcept;
    // 移动赋值：先归还自己的连接，再接管对方的连接。
    MySqlConnGuard& operator=(MySqlConnGuard&& other) noexcept;

    MySqlConnGuard(const MySqlConnGuard&) = delete;
    MySqlConnGuard& operator=(const MySqlConnGuard&) = delete;

    // 析构时自动把连接归还给连接池。
    ~MySqlConnGuard();

    // 返回裸 MYSQL 连接，供 DAO 执行 SQL。
    MYSQL* get() const { return conn_; }

   private:
    friend class MySqlConnectionPool;

    // 保存连接池和裸连接，由 Guard 析构时自动归还。
    MySqlConnGuard(MySqlConnectionPool* pool, MYSQL* conn);

    MySqlConnectionPool* pool_{nullptr};
    MYSQL* conn_{nullptr};
};

// MySQL 连接池
// 设计与 RedisConnectionPool 对称：RAII Guard + 自动扩容/回收 + 超时等待
// 区别：MySQL 连接使用 mysql_ping() 做健康检查（ping_on_borrow 可配置）
class MySqlConnectionPool {
   public:
    // 使用默认构造，连接参数在 Init 中设置。
    MySqlConnectionPool() = default;
    // 析构连接池，关闭所有空闲连接。
    ~MySqlConnectionPool();

    // 初始化连接池并预建最小连接数。
    bool Init(const MySqlConfig& cfg);

    // 阻塞获取连接；timeout_ms < 0 表示一直等待，=0 表示立即返回，大于 0
    // 表示等待指定毫秒
    MySqlConnGuard Acquire(int timeout_ms = -1);

    // 优雅停止，拒绝新的获取请求并回收空闲连接
    void Stop();

   private:
    // 创建一个新的 MySQL 连接。
    MYSQL* CreateConnectionUnlocked();
    // 先回收超时空闲连接，再取出一个空闲连接。
    MYSQL* PopIdleConnectionUnlocked();
    // 健康检查，坏连接关闭并置空。
    bool ValidateConnection(MYSQL** conn);
    // 回收超过空闲阈值的连接。
    void CleanupIdleUnlocked();
    // 归还连接；池停止时直接关闭。
    void Release(MYSQL* conn);

    friend class MySqlConnGuard;

    std::mutex mutex_;
    std::condition_variable cv_;
    struct ConnEntry {
        MYSQL* conn{nullptr};
        std::chrono::steady_clock::time_point last_used;
    };
    std::deque<ConnEntry> idle_;
    int total_conns_{0};
    bool stopping_{false};
    bool inited_{false};
    MySqlConfig cfg_;
};

}  // namespace sparkpush
