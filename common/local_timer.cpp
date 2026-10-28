#include "local_timer.h"

#include <utility>

// 获取全局时间轮实例，供本对象注册定时器。
LocalTimer::LocalTimer() : timer_(TimerWheel::instance()) {}

// 析构时取消尚未触发的定时器。
LocalTimer::~LocalTimer() { cancel(); }

// 取消旧定时器，注册新的延迟和回调。
void LocalTimer::schedule(std::chrono::milliseconds delay,
                          std::function<void()> callback) {
    cancel();
    auto token = timer_.add_timer(delay);
    state_ = std::move(token.state);
    callback_ = std::move(callback);
}

// 标记当前定时器取消，并释放回调。
void LocalTimer::cancel() {
    if (state_) {
        state_->cancelled.store(true, std::memory_order_release);
    }
    state_.reset();
    callback_ = nullptr;
}

// 定时器到期且未取消时执行回调，返回本次是否执行。
bool LocalTimer::execute_if_ready() {
    if (!state_) return false;

    if (state_->cancelled.load(std::memory_order_acquire)) {
        state_.reset();
        callback_ = nullptr;
        return false;
    }
    if (!state_->ready.load(std::memory_order_acquire)) return false;

    state_.reset();
    auto callback = std::move(callback_);
    callback_ = nullptr;
    if (callback) callback();
    return true;
}

// 是否还有未处理的定时器。
bool LocalTimer::pending() const { return state_ != nullptr; }
