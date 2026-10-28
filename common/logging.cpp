#include "logging.h"

#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sparkpush {

namespace {

std::shared_ptr<spdlog::logger> g_logger;
std::atomic<int> g_level{kLogInfo};  // 未初始化时的默认级别，与旧行为一致
std::mutex g_init_mutex;

// 把项目日志级别转换成 spdlog 的级别枚举。
spdlog::level::level_enum ToSpdLevel(int lvl) {
    switch (lvl) {
        case kLogTrace:
            return spdlog::level::trace;
        case kLogDebug:
            return spdlog::level::debug;
        case kLogInfo:
            return spdlog::level::info;
        case kLogWarn:
            return spdlog::level::warn;
        case kLogError:
            return spdlog::level::err;
        case kLogFatal:
            return spdlog::level::critical;
        default:
            return spdlog::level::info;
    }
}

}  // namespace

// 创建日志目录和轮转文件 sink，初始化全局 logger 与日志级别。
bool InitLogging(const std::string& program, bool also_stdout,
                 size_t max_file_bytes, int log_level) {
    const char* kLogDir = "logs";
    struct stat st;
    if (::stat(kLogDir, &st) != 0) {
        if (::mkdir(kLogDir, 0755) != 0) {
            std::fprintf(stderr, "InitLogging mkdir %s failed: %s\n", kLogDir,
                         std::strerror(errno));
            return false;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        std::fprintf(stderr, "InitLogging: %s exists but is not directory\n",
                     kLogDir);
        return false;
    }

    std::string filename = std::string(kLogDir) + "/" + program + ".log";

    std::lock_guard<std::mutex> lock(g_init_mutex);
    try {
        std::vector<spdlog::sink_ptr> sinks;
        // 保留一份 .1 备份（max_files=1）与旧 rename 轮转对齐。
        // max_file_bytes == 0 时禁用轮转：给一个巨大阈值近似达到效果。
        size_t rotate =
            max_file_bytes > 0 ? max_file_bytes : static_cast<size_t>(-1) / 2;
        sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            filename, rotate, /*max_files=*/1));
        if (also_stdout) {
            sinks.push_back(
                std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
        }
        g_logger = std::make_shared<spdlog::logger>(program, sinks.begin(),
                                                    sinks.end());
        g_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%^%l%$] %v");
        if (log_level >= 0 && log_level <= kLogFatal) {
            g_level.store(log_level, std::memory_order_relaxed);
        }
        g_logger->set_level(
            ToSpdLevel(g_level.load(std::memory_order_relaxed)));
        g_logger->flush_on(spdlog::level::warn);
        spdlog::set_default_logger(g_logger);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "InitLogging spdlog init failed: %s\n", e.what());
        return false;
    }
    return true;
}

// 刷新并销毁全局 logger，释放 spdlog 资源。
void ShutdownLogging() {
    std::lock_guard<std::mutex> lock(g_init_mutex);
    if (g_logger) {
        g_logger->flush();
        g_logger.reset();
    }
    spdlog::shutdown();
}

// 判断当前级别是否允许输出，供日志宏和热路径提前返回。
bool ShouldLog(int level) {
    return level >= g_level.load(std::memory_order_relaxed);
}

// 按指定级别输出一行日志；logger 未初始化时回退到 stderr。
void LogLine(int level, const std::string& msg) {
    auto lg = g_logger;
    if (!lg) {
        // 未初始化时回退到 stderr，行为与旧版一致。
        std::fprintf(stderr, "%s\n", msg.c_str());
        return;
    }
    lg->log(ToSpdLevel(level), "{}", msg);
}

}  // namespace sparkpush
