#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

struct TimerState {
    std::atomic_bool ready{false};
    std::atomic_bool cancelled{false};
};

class TimerWheel {
   public:
    using TimerId = std::uint64_t;
    using Clock = std::chrono::steady_clock;

    // 返回全局唯一的时间轮实例。
    static TimerWheel& instance();

    TimerWheel(const TimerWheel&) = delete;
    TimerWheel& operator=(const TimerWheel&) = delete;

   private:
    friend class LocalTimer;

    struct TimerToken {
        TimerId id{};
        std::shared_ptr<TimerState> state;
    };

    // 校验 tick 并启动时间轮工作线程。
    explicit TimerWheel(
        std::chrono::milliseconds tick = std::chrono::milliseconds(10));
    // 停止工作线程并释放所有节点。
    ~TimerWheel();

    // 注册一个延迟定时器，返回用于查询状态的令牌。
    TimerToken add_timer(std::chrono::milliseconds delay);

    static constexpr std::uint32_t kNearShift = 8;
    static constexpr std::uint32_t kNearSize = 1u << kNearShift;
    static constexpr std::uint32_t kLevelShift = 6;
    static constexpr std::uint32_t kLevelSize = 1u << kLevelShift;
    static constexpr std::uint32_t kNearMask = kNearSize - 1u;
    static constexpr std::uint32_t kLevelMask = kLevelSize - 1u;

    struct TimerNode {
        TimerNode* next{nullptr};
        TimerId id{};
        std::uint32_t expire_tick{};
        std::shared_ptr<TimerState> state;
    };

    struct Bucket {
        TimerNode head;
        TimerNode* tail{&head};

        // 判断桶内是否没有定时器节点。
        bool empty() const { return head.next == nullptr; }
    };

    // 把节点尾插到桶链表。
    static void link(Bucket& bucket, TimerNode* node);
    // 摘下桶里的整条链表并重置桶。
    static TimerNode* clear(Bucket& bucket);

    // 时间轮工作线程：等待到期时间并推进时间轮。
    void worker_loop();
    // 按到期 tick 把节点放入对应时间槽。
    void add_node_locked(TimerNode* node, std::uint32_t base_tick);
    // 以当前 tick 为基准插入节点。
    void add_node_locked(TimerNode* node);
    // 把某层某槽的节点重新插入时间轮。
    void move_list_locked(std::size_t level, std::size_t index);
    // 推进一个 tick 并级联上层时间槽。
    void shift_locked();
    // 收集当前 near 槽里的到期节点。
    void collect_current_slot_locked(std::vector<TimerNode*>& expired);
    // 推进到指定时间并收集沿途到期节点。
    void advance_to_locked(Clock::time_point now,
                           std::vector<TimerNode*>& expired);
    // 把毫秒延迟向上取整成 tick 数。
    std::uint32_t ticks_from_delay(std::chrono::milliseconds delay) const;
    // 把时间点转换成绝对 tick。
    std::uint64_t absolute_tick_from_time(Clock::time_point time) const;
    // 把相对 tick 转换成时间点。
    Clock::time_point tick_to_time(std::uint32_t tick) const;
    // 把绝对 tick 转换成时间点。
    Clock::time_point absolute_tick_to_time(std::uint64_t tick) const;
    // 计算某层某槽下一次级联到 near 的 tick。
    std::uint32_t level_cascade_tick(std::size_t level,
                                     std::size_t index) const;
    // 返回下一次需要唤醒的时间。
    Clock::time_point next_wait_time_locked() const;
    // 重新计算最近的非空时间槽。
    void rebuild_next_wakeup_locked();
    // 标记到期节点并释放节点内存。
    void mark_expired(std::vector<TimerNode*>& expired);

    const std::chrono::milliseconds tick_;
    const Clock::time_point start_time_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};

    Bucket near_[kNearSize];
    Bucket levels_[4][kLevelSize];
    std::uint32_t current_tick_{0};
    std::uint64_t current_absolute_tick_{0};
    std::uint32_t next_wakeup_tick_{0};
    bool has_next_wakeup_{false};

    std::atomic<TimerId> next_id_{1};
    std::thread worker_;
};
