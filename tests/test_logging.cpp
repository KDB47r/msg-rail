// 日志脱敏工具单元测试:确保记录 token 时不泄露完整凭证。
#include <string>

#include "doctest.h"
#include "logging.h"

using namespace sparkpush;

TEST_CASE("RedactToken 只保留前若干位") {
    // 默认保留前 6 位
    CHECK(RedactToken("1a6e36791cfe71044eadedafce089fbb") == "1a6e36...");
    // 自定义保留位数
    CHECK(RedactToken("abcdefghij", 3) == "abc...");
}

TEST_CASE("RedactToken 处理短串与空串") {
    CHECK(RedactToken("") == "");           // 空串原样
    CHECK(RedactToken("short") == "***");   // 长度<=keep(6) 全部隐藏
    CHECK(RedactToken("123456") == "***");  // 恰好等于 keep 也隐藏
}
