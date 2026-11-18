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
    CHECK(cfg.media_video_transcode_timeout_seconds == 600);
    CHECK(cfg.media_video_transcode_max_side == 1920);
    CHECK(cfg.media_video_transcode_crf == 23);
    CHECK(cfg.media_video_transcode_preset == "veryfast");
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

TEST_CASE("视频配置项正常解析") {
    std::string path = WriteTempConfig(
        "media_max_video_bytes=209715200\n"
        "media_max_video_duration_ms=300000\n"
        "media_max_video_pixels=2073600\n"
        "media_max_video_side=1920\n"
        "media_video_poster_max_width=480\n"
        "media_video_process_timeout_seconds=30\n"
        "media_video_transcode_timeout_seconds=120\n"
        "media_video_transcode_max_side=1280\n"
        "media_video_transcode_crf=26\n"
        "media_video_transcode_preset=fast\n"
        "media_video_ffprobe_path=/opt/ffmpeg/bin/ffprobe\n"
        "media_video_ffmpeg_path=/opt/ffmpeg/bin/ffmpeg\n"
        "media_video_temp_dir=/var/tmp\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.media_max_video_bytes == 209715200);
    CHECK(cfg.media_max_video_duration_ms == 300000);
    CHECK(cfg.media_max_video_pixels == 2073600);
    CHECK(cfg.media_max_video_side == 1920);
    CHECK(cfg.media_video_poster_max_width == 480);
    CHECK(cfg.media_video_process_timeout_seconds == 30);
    CHECK(cfg.media_video_transcode_timeout_seconds == 120);
    CHECK(cfg.media_video_transcode_max_side == 1280);
    CHECK(cfg.media_video_transcode_crf == 26);
    CHECK(cfg.media_video_transcode_preset == "fast");
    CHECK(cfg.media_video_ffprobe_path == "/opt/ffmpeg/bin/ffprobe");
    CHECK(cfg.media_video_ffmpeg_path == "/opt/ffmpeg/bin/ffmpeg");
    CHECK(cfg.media_video_temp_dir == "/var/tmp");
    std::remove(path.c_str());
}

TEST_CASE("media_memory_budget_mb 默认与解析") {
    Config defaults;
    CHECK(defaults.media_memory_budget_mb == 1024);

    std::string path = WriteTempConfig("media_memory_budget_mb=512\n");
    Config cfg = LoadConfig(path);
    CHECK(cfg.media_memory_budget_mb == 512);
    std::remove(path.c_str());
}

TEST_CASE("媒体缓存与 CDN 配置默认与解析") {
    Config defaults;
    CHECK(defaults.media_cache_enabled);
    CHECK(defaults.media_cache_ready_ttl_seconds == 600);
    CHECK(defaults.media_cache_failed_ttl_seconds == 60);
    CHECK(defaults.media_cache_pending_ttl_seconds == 1);
    CHECK(defaults.media_variant_cdn_base_url.empty());
    CHECK(defaults.media_variant_cache_control ==
          "public, max-age=31536000, immutable");
    CHECK(defaults.media_multipart_part_size_bytes == 8LL * 1024 * 1024);
    CHECK(defaults.media_multipart_upload_ttl_seconds == 86400);
    CHECK(defaults.media_upload_url_ttl_seconds == 900);
    CHECK(defaults.media_multipart_cleanup_interval_seconds == 3600);
    CHECK(defaults.media_multipart_cleanup_batch == 100);
    CHECK(defaults.media_upload_orphan_ttl_seconds == 86400);

    std::string path = WriteTempConfig(
        "media_cache_enabled=false\n"
        "media_cache_ready_ttl_seconds=120\n"
        "media_cache_failed_ttl_seconds=30\n"
        "media_cache_pending_ttl_seconds=2\n"
        "media_variant_cdn_base_url=https://cdn.example.com/bucket\n"
        "media_variant_cache_control=public, max-age=60\n"
        "media_multipart_part_size_bytes=5242880\n"
        "media_multipart_upload_ttl_seconds=3600\n"
        "media_upload_url_ttl_seconds=600\n"
        "media_multipart_cleanup_interval_seconds=60\n"
        "media_multipart_cleanup_batch=20\n"
        "media_upload_orphan_ttl_seconds=7200\n");
    Config cfg = LoadConfig(path);
    CHECK_FALSE(cfg.media_cache_enabled);
    CHECK(cfg.media_cache_ready_ttl_seconds == 120);
    CHECK(cfg.media_cache_failed_ttl_seconds == 30);
    CHECK(cfg.media_cache_pending_ttl_seconds == 2);
    CHECK(cfg.media_variant_cdn_base_url ==
          "https://cdn.example.com/bucket");
    CHECK(cfg.media_variant_cache_control == "public, max-age=60");
    CHECK(cfg.media_multipart_part_size_bytes == 5242880);
    CHECK(cfg.media_multipart_upload_ttl_seconds == 3600);
    CHECK(cfg.media_upload_url_ttl_seconds == 600);
    CHECK(cfg.media_multipart_cleanup_interval_seconds == 60);
    CHECK(cfg.media_multipart_cleanup_batch == 20);
    CHECK(cfg.media_upload_orphan_ttl_seconds == 7200);
    std::remove(path.c_str());
}

TEST_CASE("媒体 worker 线程数：新配置与旧配置兼容") {
    {
        Config cfg;
        CHECK(cfg.media_image_worker_threads == 3);
        CHECK(cfg.media_video_worker_threads == 1);
    }
    {
        std::string path = WriteTempConfig(
            "media_image_worker_threads=4\n"
            "media_video_worker_threads=2\n");
        Config cfg = LoadConfig(path);
        CHECK(cfg.media_image_worker_threads == 4);
        CHECK(cfg.media_video_worker_threads == 2);
        std::remove(path.c_str());
    }
    {
        std::string path = WriteTempConfig("media_worker_threads=2\n");
        Config cfg = LoadConfig(path);
        CHECK(cfg.media_image_worker_threads == 2);
        CHECK(cfg.media_video_worker_threads == 1);
        std::remove(path.c_str());
    }
    {
        std::string path = WriteTempConfig(
            "media_worker_threads=2\n"
            "media_image_worker_threads=4\n"
            "media_video_worker_threads=2\n");
        Config cfg = LoadConfig(path);
        CHECK(cfg.media_image_worker_threads == 4);
        CHECK(cfg.media_video_worker_threads == 2);
        std::remove(path.c_str());
    }
    {
        std::string path = WriteTempConfig(
            "media_image_worker_threads=0\n"
            "media_video_worker_threads=-1\n");
        Config cfg = LoadConfig(path);
        CHECK(cfg.media_image_worker_threads == 1);
        CHECK(cfg.media_video_worker_threads == 1);
        std::remove(path.c_str());
    }
}
