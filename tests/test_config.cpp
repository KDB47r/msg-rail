// 配置加载健壮性单元测试:
//   - http_port / comet_grpc_port 缺省推导(未显式配置时 = listen_port+1 / +100)
//   - 显式配置时以配置为准
//   - 非法整数值回退默认而不崩溃
#include <atomic>
#include <cstdio>
#include <fstream>
#include <string>

#include "config.h"
#include "doctest.h"

using namespace sparkpush;

namespace {

// 写一个临时配置文件,返回其路径。测试结束后由调用方删除。
std::string WriteTempConfig(const std::string& content) {
    static std::atomic<int> counter{0};
    std::string path = "/tmp/sparkpush_test_config_" +
                       std::to_string(counter.fetch_add(1)) + ".conf";
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

}  // namespace

TEST_CASE("http_port 未显式配置时按 listen_port+1 推导") {
    std::string path = WriteTempConfig("listen_port=9500\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.listen_port == 9500);
    CHECK(cfg.http_port == 9501);        // 9500 + 1
    CHECK(cfg.comet_grpc_port == 9600);  // 9500 + 100
    std::remove(path.c_str());
}

TEST_CASE("http_port 显式配置时以配置为准,不被推导覆盖") {
    std::string path = WriteTempConfig("listen_port=9500\nhttp_port=8888\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.listen_port == 9500);
    CHECK(cfg.http_port == 8888);  // 显式值,不应被 9501 覆盖
    std::remove(path.c_str());
}

TEST_CASE("http_port 配置行顺序在 listen_port 之前也能正确保留") {
    // 回归:旧实现依赖 key 出现顺序,这里把 http_port 放在 listen_port 之前
    std::string path = WriteTempConfig("http_port=8888\nlisten_port=9500\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.http_port == 8888);
    std::remove(path.c_str());
}

TEST_CASE("非法整数值回退默认且不抛异常") {
    std::string path =
        WriteTempConfig("redis_port=not_a_number\nmysql_port=3307\n");
    Config cfg;
    CHECK_NOTHROW(cfg = LoadConfig(path));
    CHECK(cfg.redis_port == 6379);  // 脏值回退默认
    CHECK(cfg.mysql_port == 3307);  // 合法值正常解析
    std::remove(path.c_str());
}

TEST_CASE("文件不存在时返回全默认配置") {
    Config cfg = LoadConfig("/tmp/definitely_not_exist_sparkpush.conf");
    CHECK(cfg.listen_port == 9000);
    CHECK(cfg.http_port == 9001);  // 9000 + 1
}

TEST_CASE("注释行(#)与空行被忽略,不影响正常字段解析") {
    std::string path = WriteTempConfig(
        "# 这是一行注释\n"
        "\n"
        "   \n"  // 仅空白的行(Trim 后为空)
        "listen_port=9500\n"
        "# redis_port=1111 (注释掉的配置不应生效)\n"
        "redis_host=127.0.0.9\n"
        "\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.listen_port == 9500);
    CHECK(cfg.redis_host == "127.0.0.9");
    CHECK(cfg.redis_port == 6379);  // 被注释掉,保持默认
    std::remove(path.c_str());
}

TEST_CASE("未知 key 被忽略且不影响其他字段") {
    std::string path = WriteTempConfig(
        "totally_unknown_key=some_value\n"
        "listen_port=9700\n"
        "another_unknown=42\n"
        "mysql_port=3310\n");
    Config cfg;
    CHECK_NOTHROW(cfg = LoadConfig(path));
    CHECK(cfg.listen_port == 9700);
    CHECK(cfg.mysql_port == 3310);
    std::remove(path.c_str());
}

TEST_CASE("value 含 '=' 时按首个 '=' 切分,其余归入 value") {
    // config.cpp 使用 line.find('=')(首个等号)切分 key/value,
    // 因此 value 中允许再出现 '='(如密码含 '=')。
    std::string path = WriteTempConfig(
        "redis_password=p=ss=word\n"
        "mysql_password==leading-eq\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.redis_password == "p=ss=word");
    CHECK(cfg.mysql_password ==
          "=leading-eq");  // 首个 '=' 为分隔符,value 以 '=' 开头
    std::remove(path.c_str());
}

TEST_CASE("行首尾空白与 key/value 周围空白被 Trim") {
    std::string path = WriteTempConfig(
        "   listen_port =  9800  \n"
        "\tredis_host\t=\t10.0.0.1\t\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.listen_port == 9800);
    CHECK(cfg.redis_host == "10.0.0.1");
    std::remove(path.c_str());
}
