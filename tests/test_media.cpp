// 媒体子系统纯逻辑测试：
//   - media_id 格式/字符集/唯一性
//   - S3 SigV4 预签名 URL 的时间、参数与确定性
//   - 图片文件头识别、libpng 解码、WebP 三档变体生成
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "doctest.h"
#include "image_processor.h"
#include "media_id.h"
#include "memory_limiter.h"
#include "s3_client.h"
#include "video_processor.h"

using namespace sparkpush;

namespace {

std::string Base64Decode(const std::string& input) {
    static const char* kTable =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    int value = 0;
    int bits = -8;
    std::string out;
    for (unsigned char c : input) {
        if (c == '=') break;
        const char* pos = std::strchr(kTable, c);
        if (!pos) continue;
        value = (value << 6) + static_cast<int>(pos - kTable);
        bits += 6;
        if (bits >= 0) {
            out.push_back(static_cast<char>((value >> bits) & 0xFF));
            bits -= 8;
        }
    }
    return out;
}

// 2x2 全红 PNG，由 Python 标准库 zlib 生成，避免测试依赖外部图片文件。
const char* kTinyRedPngBase64 =
    "iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEUlEQVR4nGP4z8DwH4QZYAwA"
    "R8oH+WdZbrcAAAAASUVORK5CYII=";

S3Config TestS3Config() {
    S3Config cfg;
    cfg.endpoint = "http://127.0.0.1:9002";
    cfg.public_endpoint = "http://127.0.0.1:9002";
    cfg.bucket = "sparkpush-media";
    cfg.access_key = "minioadmin";
    cfg.secret_key = "minioadmin";
    cfg.region = "us-east-1";
    cfg.path_style = true;
    return cfg;
}

}  // namespace

TEST_CASE("media_id: 格式、字符集与唯一性") {
    std::set<std::string> ids;
    for (int i = 0; i < 1000; ++i) {
        const std::string id = GenerateMediaId();
        CHECK(id.size() == 28);
        CHECK(id.substr(0, 2) == "m_");
        CHECK(IsValidMediaId(id));
        CHECK(id.find_first_of("ILOU") == std::string::npos);
        ids.insert(id);
    }
    CHECK(ids.size() == 1000);
}

TEST_CASE("media_id: 非法输入被拒绝") {
    CHECK_FALSE(IsValidMediaId(""));
    CHECK_FALSE(IsValidMediaId("m_"));
    CHECK_FALSE(IsValidMediaId("x_01234567890123456789012345"));
    CHECK_FALSE(IsValidMediaId("m_0123456789012345678901234"));   // 26 位
    CHECK_FALSE(IsValidMediaId("m_012345678901234567890123456"));  // 27 位
    CHECK_FALSE(IsValidMediaId("m_0123456789012345678901234I"));   // I 非法
}

TEST_CASE("S3: 预签名 PUT URL 确定且包含 SigV4 参数") {
    S3Client client;
    std::string err;
    REQUIRE(client.Init(TestS3Config(), &err));

    int64_t expires_at = 0;
    const std::string url = client.PresignWithTime(
        "PUT", "media/m_abc/original.jpg", 900, 1700000000, &expires_at,
        &err);
    REQUIRE_FALSE(url.empty());
    CHECK(expires_at == 1700000900);
    CHECK(url.rfind("http://127.0.0.1:9002/sparkpush-media/media/m_abc/"
                    "original.jpg?",
                    0) == 0);
    CHECK(url.find("X-Amz-Algorithm=AWS4-HMAC-SHA256") != std::string::npos);
    CHECK(url.find("X-Amz-Credential=minioadmin%2F20231114%2Fus-east-1%2Fs3%2F"
                   "aws4_request") != std::string::npos);
    CHECK(url.find("X-Amz-Date=20231114T221320Z") != std::string::npos);
    CHECK(url.find("X-Amz-Expires=900") != std::string::npos);
    CHECK(url.find("X-Amz-SignedHeaders=host") != std::string::npos);

    const std::string sig_marker = "X-Amz-Signature=";
    const size_t sig_pos = url.find(sig_marker);
    REQUIRE(sig_pos != std::string::npos);
    const std::string signature = url.substr(sig_pos + sig_marker.size());
    CHECK(signature.size() == 64);

    const std::string again = client.PresignWithTime(
        "PUT", "media/m_abc/original.jpg", 900, 1700000000, nullptr, nullptr);
    CHECK(again == url);

    const std::string get_url = client.PresignWithTime(
        "GET", "media/m_abc/original.jpg", 900, 1700000000, nullptr, nullptr);
    CHECK(get_url != url);
}

TEST_CASE("S3: 文件下载 URL 带 Content-Disposition 且参与签名") {
    S3Client client;
    std::string err;
    REQUIRE(client.Init(TestS3Config(), &err));

    int64_t expires_at = 0;
    const std::string url = client.PresignWithTime(
        "GET", "media/m_abc/original.pdf", 900, 1700000000, &expires_at,
        &err, "attachment; filename=\"report.pdf\"; "
              "filename*=UTF-8''report.pdf");
    REQUIRE_FALSE(url.empty());
    CHECK(expires_at == 1700000900);
    CHECK(url.find("response-content-disposition=attachment%3B%20filename%3D"
                   "%22report.pdf%22%3B%20filename%2A%3DUTF-8%27%27"
                   "report.pdf") != std::string::npos);
    // S3 规范要求 query 按参数名字典序参与签名；response-* 必须排在
    // X-Amz-SignedHeaders 之后、X-Amz-Signature 之前。
    const size_t disposition_pos = url.find("response-content-disposition=");
    const size_t signed_headers_pos = url.find("X-Amz-SignedHeaders=host");
    const size_t signature_pos = url.find("X-Amz-Signature=");
    REQUIRE(disposition_pos != std::string::npos);
    REQUIRE(signed_headers_pos != std::string::npos);
    REQUIRE(signature_pos != std::string::npos);
    CHECK(signed_headers_pos < disposition_pos);
    CHECK(disposition_pos < signature_pos);

    const std::string plain = client.PresignWithTime(
        "GET", "media/m_abc/original.pdf", 900, 1700000000, nullptr, nullptr);
    CHECK(plain.find("response-content-disposition") == std::string::npos);
    CHECK(plain != url);
}

TEST_CASE("S3: multipart UploadPart URL 含 part/uploadId 且参与签名") {
    S3Client client;
    std::string err;
    REQUIRE(client.Init(TestS3Config(), &err));

    int64_t expires_at = 0;
    const std::string url = client.PresignUploadPartWithTime(
        "media/m_abc/original.mp4", "upload/abc+def=", 2, 900, 1700000000,
        &expires_at, &err);
    REQUIRE_FALSE(url.empty());
    CHECK(expires_at == 1700000900);
    CHECK(url.find("partNumber=2") != std::string::npos);
    CHECK(url.find("uploadId=upload%2Fabc%2Bdef%3D") != std::string::npos);
    CHECK(url.find("X-Amz-SignedHeaders=host") != std::string::npos);

    const size_t signed_pos = url.find("X-Amz-SignedHeaders=host");
    const size_t part_pos = url.find("partNumber=2");
    const size_t upload_pos = url.find("uploadId=");
    const size_t sig_pos = url.find("X-Amz-Signature=");
    REQUIRE(signed_pos != std::string::npos);
    REQUIRE(part_pos != std::string::npos);
    REQUIRE(upload_pos != std::string::npos);
    REQUIRE(sig_pos != std::string::npos);
    CHECK(signed_pos < part_pos);
    CHECK(part_pos < upload_pos);
    CHECK(upload_pos < sig_pos);

    const std::string url_part1 = client.PresignUploadPartWithTime(
        "media/m_abc/original.mp4", "upload/abc+def=", 1, 900, 1700000000,
        nullptr, nullptr);
    CHECK(url_part1 != url);
}

TEST_CASE("S3: 非法 endpoint 初始化失败") {
    S3Config cfg = TestS3Config();
    cfg.endpoint = "ftp://127.0.0.1:9002";
    S3Client client;
    std::string err;
    CHECK_FALSE(client.Init(cfg, &err));
    CHECK_FALSE(err.empty());
}

TEST_CASE("图片文件头识别") {
    const std::string png = Base64Decode(kTinyRedPngBase64);
    CHECK(SniffImageMime(png) == "image/png");
    CHECK(SniffImageMime(std::string("\xFF\xD8\xFF\xE0", 4)) == "image/jpeg");
    CHECK(SniffImageMime(std::string("RIFF1234WEBP", 12)) == "image/webp");
    CHECK(SniffImageMime("not an image").empty());
}

TEST_CASE("图片处理: PNG 解码、主色与三档 WebP") {
    const std::string png = Base64Decode(kTinyRedPngBase64);
    ImageLimits limits;
    ProcessedImage processed;
    std::string err;
    REQUIRE(ProcessImage(png, "image/png", limits, &processed, &err));
    CHECK(processed.width == 2);
    CHECK(processed.height == 2);
    CHECK(processed.dominant_color == "#FF0000");
    REQUIRE(processed.variants.size() == 3);
    CHECK(processed.variants[0].variant == "micro");
    CHECK(processed.variants[1].variant == "thumb");
    CHECK(processed.variants[2].variant == "preview");
    for (const auto& variant : processed.variants) {
        CHECK(variant.format == "webp");
        CHECK(variant.width == 2);
        CHECK(variant.height == 2);
        CHECK_FALSE(variant.bytes.empty());
        CHECK(SniffImageMime(variant.bytes) == "image/webp");
    }
}

TEST_CASE("图片处理: 声明的 MIME 与文件头不一致时拒绝") {
    const std::string png = Base64Decode(kTinyRedPngBase64);
    ImageLimits limits;
    ProcessedImage processed;
    std::string err;
    CHECK_FALSE(ProcessImage(png, "image/jpeg", limits, &processed, &err));
    CHECK_FALSE(err.empty());
}

TEST_CASE("图片处理: 像素数/边长超限时拒绝") {
    const std::string png = Base64Decode(kTinyRedPngBase64);
    ImageLimits limits;
    limits.max_pixels = 1;  // 2x2 = 4 > 1
    ProcessedImage processed;
    std::string err;
    CHECK_FALSE(ProcessImage(png, "image/png", limits, &processed, &err));

    limits = ImageLimits{};
    limits.max_side = 1;
    CHECK_FALSE(ProcessImage(png, "image/png", limits, &processed, &err));
}

TEST_CASE("图片处理: 传入 MemoryLimiter 后预算释放") {
    const std::string png = Base64Decode(kTinyRedPngBase64);
    ImageLimits limits;
    ProcessedImage processed;
    MemoryLimiter limiter(1024 * 1024);
    std::string err;
    REQUIRE(ProcessImage(png, "image/png", limits, &processed, &err, &limiter));
    CHECK(processed.width == 2);
    CHECK(processed.height == 2);
    REQUIRE(processed.variants.size() == 3);
    CHECK(limiter.in_use() == 0);
}

TEST_CASE("内存预算: 预算不足时阻塞，释放后继续") {
    MemoryLimiter limiter(100);
    auto first = limiter.Acquire(80);
    CHECK(limiter.in_use() == 80);

    std::atomic<bool> acquired{false};
    std::thread waiter([&]() {
        auto second = limiter.Acquire(80);
        acquired.store(true, std::memory_order_relaxed);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(acquired.load(std::memory_order_relaxed));

    first = MemoryLimiter::Guard();  // 释放
    waiter.join();
    CHECK(acquired.load(std::memory_order_relaxed));
    CHECK(limiter.in_use() == 0);
}

TEST_CASE("内存预算: 超过总预算的任务独占运行") {
    MemoryLimiter limiter(100);
    auto guard = limiter.Acquire(200);
    CHECK(limiter.in_use() == 200);

    guard = MemoryLimiter::Guard();
    auto next = limiter.Acquire(200);
    CHECK(limiter.in_use() == 200);
}

TEST_CASE("内存预算: capacity=0 表示不限制") {
    MemoryLimiter limiter(0);
    auto a = limiter.Acquire(1024);
    auto b = limiter.Acquire(1024);
    CHECK(limiter.in_use() == 0);
}

TEST_CASE("视频 MIME: 只允许 mp4/webm") {
    CHECK(ExtensionForVideoMime("video/mp4") == "mp4");
    CHECK(ExtensionForVideoMime("video/webm") == "webm");
    CHECK(ExtensionForVideoMime("video/quicktime").empty());
    CHECK(IsAllowedVideoMime("video/mp4"));
    CHECK(IsAllowedVideoMime("video/webm"));
    CHECK_FALSE(IsAllowedVideoMime("video/x-matroska"));
    CHECK_FALSE(IsAllowedVideoMime("image/mp4"));
}

TEST_CASE("视频探测: 合法 MP4(H.264/AAC)") {
    const std::string json = R"({
        "streams": [
            {"codec_type":"video","codec_name":"h264","width":1920,
             "height":1080,"duration":"12.500000",
             "disposition":{"attached_pic":0}},
            {"codec_type":"audio","codec_name":"aac"}
        ],
        "format": {"format_name":"mov,mp4,m4a,3gp,3g2,mj2",
                   "duration":"12.500000"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(json, "video/mp4", &info, &err));
    CHECK(info.width == 1920);
    CHECK(info.height == 1080);
    CHECK(info.duration_ms == 12500);
    CHECK(info.container == "mp4");
    CHECK(info.video_codec == "h264");
    CHECK(info.audio_codec == "aac");
    CHECK_FALSE(info.transcode_required);
    CHECK(info.transcode_reason.empty());
}

TEST_CASE("视频探测: 手机竖拍 rotation 交换显示宽高") {
    const std::string json = R"({
        "streams": [
            {"codec_type":"video","codec_name":"h264","width":1920,
             "height":1080,
             "side_data_list":[{"side_data_type":"Display Matrix",
                                "rotation":-90}]}
        ],
        "format": {"format_name":"mp4","duration":"3.0"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(json, "video/mp4", &info, &err));
    CHECK(info.width == 1080);
    CHECK(info.height == 1920);
    CHECK(info.audio_codec.empty());
}

TEST_CASE("视频探测: tags.rotate 同样交换宽高") {
    const std::string json = R"({
        "streams": [
            {"codec_type":"video","codec_name":"h264","width":1080,
             "height":1920,"tags":{"rotate":"90"}}
        ],
        "format": {"format_name":"mp4","duration":"1.0"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(json, "video/mp4", &info, &err));
    CHECK(info.width == 1920);
    CHECK(info.height == 1080);
}

TEST_CASE("视频探测: 合法 WebM(VP9/Opus)") {
    const std::string json = R"({
        "streams": [
            {"codec_type":"video","codec_name":"vp9","width":640,
             "height":360},
            {"codec_type":"audio","codec_name":"opus"}
        ],
        "format": {"format_name":"matroska,webm","duration":"5.25"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(json, "video/webm", &info, &err));
    CHECK(info.container == "webm");
    CHECK(info.duration_ms == 5250);
    CHECK(info.video_codec == "vp9");
    CHECK(info.audio_codec == "opus");
    CHECK_FALSE(info.transcode_required);
}

TEST_CASE("视频探测: 容器与声明 MIME 不一致时拒绝") {
    const std::string json = R"({
        "streams": [{"codec_type":"video","codec_name":"h264",
                     "width":640,"height":360}],
        "format": {"format_name":"mov,mp4,m4a,3gp,3g2,mj2",
                   "duration":"1.0"}
    })";
    VideoProbeInfo info;
    std::string err;
    CHECK_FALSE(ParseVideoProbeJson(json, "video/webm", &info, &err));
    CHECK_FALSE(err.empty());
}

TEST_CASE("视频探测: HEVC MP4 标记为需要转码") {
    const std::string hevc = R"({
        "streams": [{"codec_type":"video","codec_name":"hevc",
                     "width":720,"height":1280},
                    {"codec_type":"audio","codec_name":"aac"}],
        "format": {"format_name":"mp4","duration":"21.958333"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(hevc, "video/mp4", &info, &err));
    CHECK(info.video_codec == "hevc");
    CHECK(info.audio_codec == "aac");
    CHECK(info.transcode_required);
    CHECK(info.transcode_reason == "mp4 hevc");
}

TEST_CASE("视频探测: 不支持的视频编码被拒绝") {
    const std::string av1_webm = R"({
        "streams": [{"codec_type":"video","codec_name":"av1",
                     "width":640,"height":360}],
        "format": {"format_name":"webm","duration":"1.0"}
    })";
    VideoProbeInfo info;
    std::string err;
    CHECK_FALSE(ParseVideoProbeJson(av1_webm, "video/webm", &info, &err));

    const std::string hevc_webm = R"({
        "streams": [{"codec_type":"video","codec_name":"hevc",
                     "width":640,"height":360}],
        "format": {"format_name":"webm","duration":"1.0"}
    })";
    CHECK_FALSE(ParseVideoProbeJson(hevc_webm, "video/webm", &info, &err));
}

TEST_CASE("视频探测: 没有视频流时拒绝") {
    const std::string json = R"({
        "streams": [{"codec_type":"audio","codec_name":"aac"}],
        "format": {"format_name":"mp4","duration":"1.0"}
    })";
    VideoProbeInfo info;
    std::string err;
    CHECK_FALSE(ParseVideoProbeJson(json, "video/mp4", &info, &err));
    CHECK_FALSE(err.empty());
}

TEST_CASE("视频探测: format duration 为 N/A 时回退视频流时长") {
    const std::string json = R"({
        "streams": [{"codec_type":"video","codec_name":"vp8",
                     "width":320,"height":240,"duration":"2.75"}],
        "format": {"format_name":"webm","duration":"N/A"}
    })";
    VideoProbeInfo info;
    std::string err;
    REQUIRE(ParseVideoProbeJson(json, "video/webm", &info, &err));
    CHECK(info.duration_ms == 2750);
}

TEST_CASE("视频探测: 坏 JSON 与缺少时长时拒绝") {
    VideoProbeInfo info;
    std::string err;
    CHECK_FALSE(ParseVideoProbeJson("{not json", "video/mp4", &info, &err));
    const std::string no_duration = R"({
        "streams": [{"codec_type":"video","codec_name":"h264",
                     "width":640,"height":360}],
        "format": {"format_name":"mp4"}
    })";
    CHECK_FALSE(
        ParseVideoProbeJson(no_duration, "video/mp4", &info, &err));
}

TEST_CASE("视频限制: 时长/边长/像素边界") {
    VideoLimits limits;
    VideoProbeInfo info;
    info.width = 3840;
    info.height = 2160;
    info.duration_ms = 10LL * 60 * 1000;
    std::string err;
    CHECK(ValidateVideoLimits(info, limits, &err));  // 恰好等于上限

    info.duration_ms += 1;
    CHECK_FALSE(ValidateVideoLimits(info, limits, &err));
    info.duration_ms = 1000;

    info.width = 4096;  // 4096*2160 > 8294400
    info.height = 2160;
    CHECK_FALSE(ValidateVideoLimits(info, limits, &err));

    info.width = 5000;  // 单边超过 3840
    info.height = 100;
    CHECK_FALSE(ValidateVideoLimits(info, limits, &err));

    info.width = 0;
    info.height = 1080;
    CHECK_FALSE(ValidateVideoLimits(info, limits, &err));
}

TEST_CASE("视频转码: argv 包含 H.264/AAC 与缩放参数") {
    VideoTranscodeOptions options;
    options.max_side = 1280;
    options.crf = 26;
    options.preset = "fast";
    options.timeout_seconds = 120;
    const std::vector<std::string> argv = BuildVideoTranscodeArgv(
        "/tmp/in.mp4", "/tmp/out.mp4", options, "/usr/bin/ffmpeg");
    REQUIRE_FALSE(argv.empty());
    CHECK(argv.front() == "/usr/bin/ffmpeg");
    auto has = [&](const std::string& value) {
        return std::find(argv.begin(), argv.end(), value) != argv.end();
    };
    CHECK(has("libx264"));
    CHECK(has("aac"));
    CHECK(has("yuv420p"));
    CHECK(has("+faststart"));
    CHECK(has("0:a:0?"));
    CHECK(has("128k"));
    CHECK(has("26"));
    CHECK(has("fast"));
    CHECK(has("/tmp/in.mp4"));
    CHECK(has("/tmp/out.mp4"));
    bool has_scale = false;
    for (const auto& arg : argv) {
        if (arg.find("scale=w=min(1280\\,iw)") != std::string::npos) {
            has_scale = true;
            break;
        }
    }
    CHECK(has_scale);
}

TEST_CASE("视频转码: 非法参数被拒绝") {
    VideoTranscodeOptions options;
    std::string err;
    CHECK_FALSE(TranscodeVideoToMp4("", "/tmp/out.mp4", options, "ffmpeg",
                                    &err));
    CHECK_FALSE(TranscodeVideoToMp4("/tmp/in.mp4", "/tmp/in.mp4", options,
                                    "ffmpeg", &err));
    options.crf = 52;
    CHECK_FALSE(TranscodeVideoToMp4("/tmp/in.mp4", "/tmp/out.mp4", options,
                                    "ffmpeg", &err));
}

TEST_CASE("可执行文件查找: PATH 与绝对路径") {
    CHECK(IsExecutableAvailable("sh"));
    CHECK(IsExecutableAvailable("/bin/sh"));
    CHECK_FALSE(IsExecutableAvailable(
        "sparkpush_binary_that_does_not_exist_12345"));
}
