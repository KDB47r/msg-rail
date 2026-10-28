#include <atomic>
#include <chrono>
#include <thread>

#include "doctest.h"
#include "local_timer.h"
#include "timer_wheel.h"

using namespace std::chrono_literals;

TEST_CASE("LocalTimer 到期后在调用线程执行回调") {
    LocalTimer timer;
    const auto owner_thread = std::this_thread::get_id();
    std::thread::id callback_thread;
    std::atomic_bool executed{false};

    timer.schedule(20ms, [&] {
        callback_thread = std::this_thread::get_id();
        executed.store(true, std::memory_order_release);
    });

    const auto deadline = TimerWheel::Clock::now() + 1s;
    while (!executed.load(std::memory_order_acquire) &&
           TimerWheel::Clock::now() < deadline) {
        timer.execute_if_ready();
        std::this_thread::sleep_for(1ms);
    }

    CHECK(executed.load(std::memory_order_acquire));
    const bool ran_on_owner_thread = callback_thread == owner_thread;
    CHECK(ran_on_owner_thread);
    CHECK_FALSE(timer.pending());
}

TEST_CASE("LocalTimer 取消后不执行回调") {
    LocalTimer timer;
    std::atomic_bool executed{false};
    timer.schedule(
        20ms, [&] { executed.store(true, std::memory_order_release); });
    timer.cancel();

    const auto deadline = TimerWheel::Clock::now() + 100ms;
    while (TimerWheel::Clock::now() < deadline) {
        timer.execute_if_ready();
        std::this_thread::sleep_for(1ms);
    }

    CHECK_FALSE(executed.load(std::memory_order_acquire));
    CHECK_FALSE(timer.pending());
}

TEST_CASE("LocalTimer 重复调度会替换旧任务") {
    LocalTimer timer;
    int result = 0;

    timer.schedule(20ms, [&] { result = 1; });
    timer.schedule(40ms, [&] { result = 2; });

    const auto deadline = TimerWheel::Clock::now() + 1s;
    while (result == 0 && TimerWheel::Clock::now() < deadline) {
        timer.execute_if_ready();
        std::this_thread::sleep_for(1ms);
    }

    CHECK(result == 2);
    CHECK_FALSE(timer.pending());
}

TEST_CASE("LocalTimer 支持回调完成后重新调度") {
    LocalTimer timer;
    int executions = 0;

    timer.schedule(10ms, [&] { ++executions; });
    const auto first_deadline = TimerWheel::Clock::now() + 1s;
    while (executions == 0 && TimerWheel::Clock::now() < first_deadline) {
        timer.execute_if_ready();
        std::this_thread::sleep_for(1ms);
    }

    REQUIRE(executions == 1);
    timer.schedule(10ms, [&] { ++executions; });
    const auto second_deadline = TimerWheel::Clock::now() + 1s;
    while (executions == 1 && TimerWheel::Clock::now() < second_deadline) {
        timer.execute_if_ready();
        std::this_thread::sleep_for(1ms);
    }

    CHECK(executions == 2);
    CHECK_FALSE(timer.pending());
}
