#pragma once

#include <atomic>
#include <csignal>
#include <functional>

namespace sparkpush {

// 返回全局优雅关闭标志，收到 SIGTERM/SIGINT 后置为 true。
inline std::atomic<bool>& ShutdownRequested() {
    static std::atomic<bool> flag{false};
    return flag;
}

// 注册 SIGTERM/SIGINT：收到信号时置关闭标志并调用可选回调。
inline void InstallSignalHandler(std::function<void()> on_signal = nullptr) {
    // 1. 静态变量 保存回调
    static std::function<void()> s_callback;
    s_callback = std::move(on_signal);

    // 2. 真正的信号处理函数
    // 信号处理函数只做置标志和简单回调，避免在信号上下文做复杂清理。
    auto handler = [](int sig) {
        (void)sig;
        ShutdownRequested().store(true, std::memory_order_relaxed);
        if (s_callback) {
            // 3. 实际调用
            s_callback();
        }
    };

    // 4. 信号触发
    std::signal(SIGTERM, handler); //ctrl+C
    std::signal(SIGINT, handler); //kill / pkill / 容器或 systemd 停止
    //SIGKILL  kill -9 捕获不了，进程直接死，不走优雅关闭(兜底)
}

}  // namespace sparkpush

//