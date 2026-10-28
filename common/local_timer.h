#pragma once

#include <chrono>
#include <functional>
#include <memory>


#include "timer_wheel.h"

class LocalTimer {
   public:
    // 获取全局时间轮实例，供本对象注册定时器。
    LocalTimer();
    // 析构时取消尚未触发的定时器。
    ~LocalTimer();

    LocalTimer(const LocalTimer&) = delete;
    LocalTimer& operator=(const LocalTimer&) = delete;

    // 取消旧定时器，注册新的延迟和回调。
    void schedule(std::chrono::milliseconds delay,
                  std::function<void()> callback);
    // 标记当前定时器取消，并释放回调。
    void cancel();
    // 到期且未取消时执行回调，返回本次是否执行。
    bool execute_if_ready();
    // 是否还有未处理的定时器。
    bool pending() const;

   private:
    TimerWheel& timer_;
    std::shared_ptr<TimerState> state_;
    std::function<void()> callback_;
};
