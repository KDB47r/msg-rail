#pragma once

#include <cstddef>
#include <sstream>
#include <string>

namespace sparkpush {

// 默认单个日志文件轮转阈值：64MB。超过后滚动为 <file>.1 并重开新文件。
constexpr size_t kDefaultLogRotateBytes = 64u * 1024 * 1024;

// 日志级别数值（对外冻结契约，与历史一致）：
//   TRACE=0 DEBUG=1 INFO=2 WARN=3 ERROR=4 FATAL=5
// 大于等于 FATAL 视为关闭日志。
enum LogLevel {
    kLogTrace = 0,
    kLogDebug = 1,
    kLogInfo = 2,
    kLogWarn = 3,
    kLogError = 4,
    kLogFatal = 5,
};

// 初始化日志，输出到 logs/<program>.log（可选同时写 stdout）。
// program 建议传入 "logic" / "job" / "comet" 等组件名。
//
// 参数（保持与迁移前二进制兼容）：
//   also_stdout    : 是否同时写 stdout。
//   max_file_bytes : 按大小轮转阈值，0 表示禁用。
//   log_level      : 若 >=0，则设置全局日志级别（映射到 spdlog level）。
bool InitLogging(const std::string& program, bool also_stdout = false,
                 size_t max_file_bytes = kDefaultLogRotateBytes,
                 int log_level = -1);

// 刷新并销毁全局 logger，释放日志资源。
void ShutdownLogging();

// 判定当前全局级别是否放行给定 level，供热路径 early-return 使用。
bool ShouldLog(int level);

// 底层落盘：将一行文本（不含换行）以指定级别输出。
void LogLine(int level, const std::string& msg);

// 字符串形式的便捷函数（业务代码主要入口）。

// TRACE 级别日志入口，级别不足时直接返回。
inline void LogTrace(const std::string& msg) {
    if (ShouldLog(kLogTrace)) LogLine(kLogTrace, msg);
}
// DEBUG 级别日志入口，级别不足时直接返回。
inline void LogDebug(const std::string& msg) {
    if (ShouldLog(kLogDebug)) LogLine(kLogDebug, msg);
}
// INFO 级别日志入口，级别不足时直接返回。
inline void LogInfo(const std::string& msg) {
    if (ShouldLog(kLogInfo)) LogLine(kLogInfo, msg);
}
// WARN 级别日志入口，级别不足时直接返回。
inline void LogWarn(const std::string& msg) {
    if (ShouldLog(kLogWarn)) LogLine(kLogWarn, msg);
}
// ERROR 级别日志入口，级别不足时直接返回。
inline void LogError(const std::string& msg) {
    if (ShouldLog(kLogError)) LogLine(kLogError, msg);
}
// FATAL 级别日志入口，级别不足时直接返回。
inline void LogFatal(const std::string& msg) {
    if (ShouldLog(kLogFatal)) LogLine(kLogFatal, msg);
}

// 脱敏辅助：记录 token/密钥等敏感字段时只保留前 keep 位。
inline std::string RedactToken(const std::string& token, size_t keep = 6) {
    if (token.empty()) return std::string();
    if (token.size() <= keep) return "***";
    return token.substr(0, keep) + "...";
}

// 流式记录器：兼容旧代码 `LOG_INFO << "a" << b;` 用法。
// 析构时把累积的内容一次性写入
// spdlog；条件判断放在宏里，级别不通过时构造被短路。
class LogStream {
   public:
    // 记录本条日志的级别，内容由 operator<< 逐步累积。
    explicit LogStream(int level) : level_(level) {}
    // 析构时把缓冲区内容一次性写入日志。
    ~LogStream() { LogLine(level_, oss_.str()); }

    // 追加一段内容到流式日志缓冲区。
    template <class T>
    LogStream& operator<<(const T& v) {
        oss_ << v;
        return *this;
    }

   private:
    int level_;
    std::ostringstream oss_;
};

}  // namespace sparkpush

// 流式宏（兼容层）。使用 for-loop 技巧确保每次调用仅构造一个临时 LogStream。
#define SPARKPUSH_LOG_IF(lvl)           \
    if (!::sparkpush::ShouldLog(lvl)) { \
    } else                              \
        ::sparkpush::LogStream(lvl)

#define LOG_TRACE SPARKPUSH_LOG_IF(::sparkpush::kLogTrace)
#define LOG_DEBUG SPARKPUSH_LOG_IF(::sparkpush::kLogDebug)
#define LOG_INFO SPARKPUSH_LOG_IF(::sparkpush::kLogInfo)
#define LOG_WARN SPARKPUSH_LOG_IF(::sparkpush::kLogWarn)
#define LOG_ERROR SPARKPUSH_LOG_IF(::sparkpush::kLogError)
#define LOG_FATAL SPARKPUSH_LOG_IF(::sparkpush::kLogFatal)

//