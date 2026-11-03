#include <cstdio>
#include <string>

#include "app.h"
#include "config.h"
#include "logging.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

 /*
  * kok 参数正常
  * kHelp 输入参数 -h/--help
  * kError 参数错误 如少选未知项 缺少路径 
  */

//打印用法说明。prog 是程序名，比如 ./comet_server，这样提示里能显示正确的命令。
void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/comet.conf。\n",
                 prog);
}

//从命令行里找出配置文件路径
ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/comet.conf";
    *config_path = default_path;
    bool positional_used = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } else {
            if (positional_used) {
                std::fprintf(stderr, "重复的配置文件参数: %s\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = arg;
            positional_used = true;
        }
    }
    return ArgParseResult::kOk;
}

}  // namespace

int main(int argc, char** argv) {
    //解析参数
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    if (result == ArgParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }

    // 先加载配置以拿到日志级别,再初始化日志到独立文件 logs/comet.log。
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::InitLogging("comet",
                           /*also_stdout=*/false,
                           sparkpush::kDefaultLogRotateBytes, cfg.log_level);
    //启动comet
    sparkpush::RunComet(cfg);
    //关闭日志
    sparkpush::ShutdownLogging();
/*
  - flush() 把缓冲区里的日志刷到磁盘；
  - 释放 spdlog 的全局 logger；
  - 调用 spdlog::shutdown()。
*/

    return 0;
}

//