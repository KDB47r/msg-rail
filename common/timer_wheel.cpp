#include "timer_wheel.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

// 返回全局唯一的时间轮实例，函数内静态变量保证线程安全。
TimerWheel& TimerWheel::instance() {
    static TimerWheel wheel;
    return wheel;
}

// 校验 tick 合法并启动时间轮工作线程。
TimerWheel::TimerWheel(std::chrono::milliseconds tick)
    : tick_(tick), start_time_(Clock::now()) {
    if (tick_ <= std::chrono::milliseconds(0)) {
        throw std::invalid_argument("timer tick must be positive");
    }
    worker_ = std::thread(&TimerWheel::worker_loop, this);
}

// 停止工作线程并释放所有时间轮节点。
TimerWheel::~TimerWheel() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_one();
    if (worker_.joinable()) worker_.join();

    // 释放一个桶里的所有定时器节点。
    auto free_bucket = [](Bucket& bucket) {
        TimerNode* current = clear(bucket);
        while (current) {
            TimerNode* next = current->next;
            delete current;
            current = next;
        }
    };
    for (auto& bucket : near_) free_bucket(bucket);
    for (auto& level : levels_)
        for (auto& bucket : level) free_bucket(bucket);
}

// 把节点尾插到桶的链表中。
void TimerWheel::link(Bucket& bucket, TimerNode* node) {
    node->next = nullptr;
    bucket.tail->next = node;
    bucket.tail = node;
}

// 摘下桶里的整条链表并重置桶。
TimerWheel::TimerNode* TimerWheel::clear(Bucket& bucket) {
    TimerNode* result = bucket.head.next;
    bucket.head.next = nullptr;
    bucket.tail = &bucket.head;
    return result;
}

// 创建一个定时器节点，插入时间轮并返回用于查询状态的令牌。
TimerWheel::TimerToken TimerWheel::add_timer(
    std::chrono::milliseconds delay) {
    auto state = std::make_shared<TimerState>();
    const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
    if (delay <= std::chrono::milliseconds(0)) {
        state->ready.store(true, std::memory_order_release);
        return {id, std::move(state)};
    }

    const auto now = Clock::now();
    auto* node = new TimerNode;
    node->id = id;
    node->state = state;
    node->expire_tick = static_cast<std::uint32_t>(
        absolute_tick_from_time(now) + ticks_from_delay(delay));

    bool should_notify = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto old_wait_time = next_wait_time_locked();
        add_node_locked(node);
        rebuild_next_wakeup_locked();
        should_notify = next_wait_time_locked() < old_wait_time;
    }
    if (should_notify) cv_.notify_one();
    return {id, std::move(state)};
}

// 时间轮工作线程：等待最近到期时间，推进时间轮并标记到期节点。
void TimerWheel::worker_loop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        const auto wait_time = next_wait_time_locked();
        if (wait_time == Clock::time_point::max()) {
            cv_.wait(lock, [this] { return stopping_ || has_next_wakeup_; });
        } else {
            cv_.wait_until(lock, wait_time, [this, wait_time] {
                return stopping_ || next_wait_time_locked() < wait_time;
            });
        }
        if (stopping_) break;

        const auto now = Clock::now();
        if (next_wait_time_locked() > now) continue;

        std::vector<TimerNode*> expired;
        advance_to_locked(now, expired);
        rebuild_next_wakeup_locked();
        lock.unlock();
        mark_expired(expired);
        lock.lock();
    }
}

// 按到期 tick 把节点放入 near 或对应层级的时间槽。
void TimerWheel::add_node_locked(TimerNode* node, std::uint32_t base_tick) {
    const auto expire = node->expire_tick;
    if ((expire | kNearMask) == (base_tick | kNearMask)) {
        link(near_[expire & kNearMask], node);
        return;
    }

    std::uint32_t mask = kNearSize << kLevelShift;
    std::size_t level = 0;
    for (; level < 3; ++level) {
        if ((expire | (mask - 1u)) == (base_tick | (mask - 1u))) break;
        mask <<= kLevelShift;
    }
    const auto index =
        (expire >> (kNearShift + level * kLevelShift)) & kLevelMask;
    link(levels_[level][index], node);
}

// 以当前 tick 为基准插入节点。
void TimerWheel::add_node_locked(TimerNode* node) {
    add_node_locked(node, current_tick_);
}

// 把某层某槽的节点重新插入时间轮。
void TimerWheel::move_list_locked(std::size_t level, std::size_t index) {
    TimerNode* current = clear(levels_[level][index]);
    while (current) {
        TimerNode* next = current->next;
        add_node_locked(current);
        current = next;
    }
}

// 推进一个 tick，并在需要时级联上层时间槽。
void TimerWheel::shift_locked() {
    const auto tick = ++current_tick_;
    if (tick == 0) {
        move_list_locked(3, 0);
        return;
    }

    std::uint32_t mask = kNearSize;
    auto time = tick >> kNearShift;
    for (std::size_t level = 0;
         (tick & (mask - 1u)) == 0 && level < 4; ++level) {
        const auto index = time & kLevelMask;
        if (index != 0) {
            move_list_locked(level, index);
            break;
        }
        mask <<= kLevelShift;
        time >>= kLevelShift;
    }
}

// 收集当前 near 槽里的到期节点。
void TimerWheel::collect_current_slot_locked(
    std::vector<TimerNode*>& expired) {
    TimerNode* current = clear(near_[current_tick_ & kNearMask]);
    while (current) {
        TimerNode* next = current->next;
        expired.push_back(current);
        current = next;
    }
}

// 推进到 now 对应的 tick，并收集沿途所有到期节点。
void TimerWheel::advance_to_locked(Clock::time_point now,
                                   std::vector<TimerNode*>& expired) {
    auto target_tick = absolute_tick_from_time(now);
    const auto max_tick =
        current_absolute_tick_ + static_cast<std::uint64_t>(0xffffffffu);
    if (target_tick > max_tick) target_tick = max_tick;

    collect_current_slot_locked(expired);
    while (current_absolute_tick_ < target_tick) {
        shift_locked();
        ++current_absolute_tick_;
        collect_current_slot_locked(expired);
    }
}

// 把毫秒延迟向上取整成 tick 数，至少返回 1 个 tick。
std::uint32_t TimerWheel::ticks_from_delay(
    std::chrono::milliseconds delay) const {
    const auto ticks =
        (delay.count() + tick_.count() - 1) / tick_.count();
    return static_cast<std::uint32_t>(std::max<std::int64_t>(1, ticks));
}

// 把时间点转换成从时间轮起点算起的绝对 tick。
std::uint64_t TimerWheel::absolute_tick_from_time(
    Clock::time_point time) const {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(time - start_time_)
            .count() /
        tick_.count());
}

// 把相对 tick 转换成对应的绝对时间点。
TimerWheel::Clock::time_point TimerWheel::tick_to_time(
    std::uint32_t tick) const {
    const auto distance = tick - current_tick_;
    return absolute_tick_to_time(current_absolute_tick_ + distance);
}

// 把绝对 tick 转换成对应的绝对时间点。
TimerWheel::Clock::time_point TimerWheel::absolute_tick_to_time(
    std::uint64_t tick) const {
    return start_time_ + tick_ * tick;
}

// 计算某层某槽下一次级联到 near 时的 tick。
std::uint32_t TimerWheel::level_cascade_tick(std::size_t level,
                                             std::size_t index) const {
    const auto shift = kNearShift + level * kLevelShift;
    const auto step = std::uint64_t{1} << shift;
    const auto cycle = step * kLevelSize;
    const auto mask = step - 1;
    const auto current = static_cast<std::uint64_t>(current_tick_);
    auto tick = (current & ~mask) |
                (static_cast<std::uint64_t>(index) << shift);
    if (tick <= current) tick += cycle;
    return static_cast<std::uint32_t>(tick);
}

// 返回下一次需要唤醒的时间；没有任务时返回 time_point::max。
TimerWheel::Clock::time_point TimerWheel::next_wait_time_locked() const {
    return has_next_wakeup_ ? tick_to_time(next_wakeup_tick_)
                            : Clock::time_point::max();
}

// 重新扫描并记录最近一个非空时间槽，作为下次唤醒点。
void TimerWheel::rebuild_next_wakeup_locked() {
    has_next_wakeup_ = false;
    next_wakeup_tick_ = 0;

    for (std::uint32_t offset = 1; offset < kNearSize; ++offset) {
        const auto tick = current_tick_ + offset;
        if (!near_[tick & kNearMask].empty()) {
            next_wakeup_tick_ = tick;
            has_next_wakeup_ = true;
            return;
        }
    }

    for (std::size_t level = 0; level < 4; ++level) {
        const auto shift = kNearShift + level * kLevelShift;
        const auto current_index = (current_tick_ >> shift) & kLevelMask;
        for (std::uint32_t offset = 1; offset <= kLevelSize; ++offset) {
            const auto index = (current_index + offset) & kLevelMask;
            if (index == 0 && level < 3) break;
            if (!levels_[level][index].empty()) {
                next_wakeup_tick_ = level_cascade_tick(level, index);
                has_next_wakeup_ = true;
                return;
            }
            if (index == 0) break;
        }
    }
}

// 把到期节点标记为 ready，未取消的定时器随后可执行，最后释放节点。
void TimerWheel::mark_expired(std::vector<TimerNode*>& expired) {
    for (auto* node : expired) {
        if (!node->state->cancelled.load(std::memory_order_acquire)) {
            node->state->ready.store(true, std::memory_order_release);
        }
        delete node;
    }
}
