/// 图片处理管线基准测试（只测量，不参与生产逻辑）。
//
// 目的：把 media/image_processor.cpp 的耗时拆成
//   解码 / 缩放 / WebP 编码 / 端到端 ProcessImage
// 并对比 libwebp 不同 method/thread_level/alpha_quality 的编码耗时与输出大小。
//
// 编译目标：image_pipeline_bench
// 运行示例：
//   ./build/tests/image_pipeline_bench /tmp/bench/photo_50mp.jpg
//   ./build/tests/image_pipeline_bench /tmp/bench/photo_50mp.png
//   ./build/tests/image_pipeline_bench /tmp/bench/alpha_12mp.png

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csetjmp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include <jpeglib.h>
#include <png.h>
#include <sys/resource.h>
#include <webp/decode.h>
#include <webp/encode.h>

#include "image_processor.h"

namespace {

struct RgbaImage {
    int width{0};
    int height{0};
    std::vector<uint8_t> pixels;  // RGBA，行优先
};

struct Timing {
    double avg_ms{0.0};
    double min_ms{0.0};
    double max_ms{0.0};
};

struct EncodeStat {
    size_t bytes{0};
    Timing timing;
};

std::string ReadFile(const std::string& path, std::string* err) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (err) *err = "open failed";
        return {};
    }
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    if (size <= 0) {
        if (err) *err = "empty file";
        return {};
    }
    in.seekg(0);
    std::string data(static_cast<size_t>(size), '\0');
    in.read(&data[0], size);
    if (!in) {
        if (err) *err = "read failed";
        return {};
    }
    return data;
}

double NowMs() {
    using Clock = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(
               Clock::now().time_since_epoch())
        .count();
}

template <typename Fn>
Timing Measure(int iterations, Fn&& fn) {
    // 先跑一次做 warm-up，避免首次分配/动态库懒加载污染结果。
    fn();
    Timing t;
    t.min_ms = std::numeric_limits<double>::max();
    double sum = 0.0;
    for (int i = 0; i < iterations; ++i) {
        const double begin = NowMs();
        fn();
        const double cost = NowMs() - begin;
        sum += cost;
        t.min_ms = std::min(t.min_ms, cost);
        t.max_ms = std::max(t.max_ms, cost);
    }
    t.avg_ms = sum / iterations;
    return t;
}

void PrintTiming(const char* label, const Timing& t) {
    std::printf("  %-28s avg=%8.2f ms  min=%8.2f  max=%8.2f\n", label,
                t.avg_ms, t.min_ms, t.max_ms);
}

// ---- JPEG 解码：与 media/image_processor.cpp 相同的完整解码路径 ----

struct JpegErrorManager {
    jpeg_error_mgr pub;
    std::jmp_buf jump;
    char message[JMSG_LENGTH_MAX];
};

extern "C" void BenchJpegErrorExit(j_common_ptr cinfo) {
    auto* err = reinterpret_cast<JpegErrorManager*>(cinfo->err);
    (*cinfo->err->format_message)(cinfo, err->message);
    std::longjmp(err->jump, 1);
}

bool DecodeJpeg(const std::string& input, RgbaImage* out, std::string* err,
                int scale_denom = 1) {
    auto* cinfo = new jpeg_decompress_struct();
    auto* jerr = new JpegErrorManager();
    std::memset(cinfo, 0, sizeof(*cinfo));
    std::memset(jerr, 0, sizeof(*jerr));
    unsigned char* volatile rgb = nullptr;
    volatile bool created = false;

    cinfo->err = jpeg_std_error(&jerr->pub);
    jerr->pub.error_exit = BenchJpegErrorExit;
    if (setjmp(jerr->jump)) {
        std::free(rgb);
        if (created) jpeg_destroy_decompress(cinfo);
        if (err) *err = jerr->message;
        delete cinfo;
        delete jerr;
        return false;
    }

    jpeg_create_decompress(cinfo);
    created = true;
    jpeg_mem_src(cinfo,
                 reinterpret_cast<const unsigned char*>(input.data()),
                 static_cast<unsigned long>(input.size()));
    jpeg_read_header(cinfo, TRUE);
    if (scale_denom != 1 && scale_denom != 2 && scale_denom != 4 &&
        scale_denom != 8) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        if (err) *err = "unsupported jpeg scale denom";
        return false;
    }
    cinfo->scale_num = 1;
    cinfo->scale_denom = scale_denom;
    cinfo->out_color_space = JCS_RGB;
    jpeg_start_decompress(cinfo);
    const int width = static_cast<int>(cinfo->output_width);
    const int height = static_cast<int>(cinfo->output_height);
    if (cinfo->output_components != 3 || width <= 0 || height <= 0) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        if (err) *err = "unsupported jpeg output";
        return false;
    }
    const size_t rgb_size =
        static_cast<size_t>(width) * static_cast<size_t>(height) * 3;
    rgb = static_cast<unsigned char*>(std::malloc(rgb_size));
    if (!rgb) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        if (err) *err = "out of memory";
        return false;
    }
    while (cinfo->output_scanline < cinfo->output_height) {
        JSAMPROW row = rgb + static_cast<size_t>(cinfo->output_scanline) *
                                 static_cast<size_t>(width) * 3;
        jpeg_read_scanlines(cinfo, &row, 1);
    }
    jpeg_finish_decompress(cinfo);
    jpeg_destroy_decompress(cinfo);
    delete cinfo;
    delete jerr;

    out->width = width;
    out->height = height;
    out->pixels.resize(static_cast<size_t>(width) * height * 4);
    for (size_t i = 0, j = 0; i < static_cast<size_t>(width) * height; ++i) {
        out->pixels[j++] = rgb[i * 3 + 0];
        out->pixels[j++] = rgb[i * 3 + 1];
        out->pixels[j++] = rgb[i * 3 + 2];
        out->pixels[j++] = 255;
    }
    std::free(rgb);
    return true;
}

bool DecodePng(const std::string& input, RgbaImage* out, std::string* err) {
    png_image image;
    std::memset(&image, 0, sizeof(image));
    image.version = PNG_IMAGE_VERSION;
    if (!png_image_begin_read_from_memory(
            &image, static_cast<const void*>(input.data()), input.size())) {
        if (err) *err = std::string("png header error: ") + image.message;
        png_image_free(&image);
        return false;
    }
    image.format = PNG_FORMAT_RGBA;
    out->width = static_cast<int>(image.width);
    out->height = static_cast<int>(image.height);
    out->pixels.resize(PNG_IMAGE_SIZE(image));
    if (!png_image_finish_read(&image, nullptr, out->pixels.data(), 0,
                               nullptr)) {
        if (err) *err = std::string("png decode error: ") + image.message;
        png_image_free(&image);
        return false;
    }
    png_image_free(&image);
    return true;
}

bool DecodeWebp(const std::string& input, RgbaImage* out, std::string* err) {
    int width = 0;
    int height = 0;
    if (!WebPGetInfo(reinterpret_cast<const uint8_t*>(input.data()),
                     input.size(), &width, &height)) {
        if (err) *err = "invalid webp header";
        return false;
    }
    out->width = width;
    out->height = height;
    out->pixels.resize(static_cast<size_t>(width) * height * 4);
    uint8_t* decoded = WebPDecodeRGBAInto(
        reinterpret_cast<const uint8_t*>(input.data()), input.size(),
        out->pixels.data(), out->pixels.size(), width * 4);
    if (!decoded) {
        out->pixels.clear();
        if (err) *err = "webp decode failed";
        return false;
    }
    return true;
}

bool DecodeImage(const std::string& input, const std::string& mime,
                 RgbaImage* out, std::string* err) {
    if (mime == "image/jpeg") return DecodeJpeg(input, out, err);
    if (mime == "image/png") return DecodePng(input, out, err);
    if (mime == "image/webp") return DecodeWebp(input, out, err);
    if (err) *err = "unsupported mime: " + mime;
    return false;
}

// ---- 与 media/image_processor.cpp:217-269 相同的缩放实现 ----

void FitMaxSide(int src_width, int src_height, int max_side, int* dst_width,
                int* dst_height) {
    if (src_width <= max_side && src_height <= max_side) {
        *dst_width = src_width;
        *dst_height = src_height;
        return;
    }
    const double scale = std::min(
        static_cast<double>(max_side) / src_width,
        static_cast<double>(max_side) / src_height);
    *dst_width = std::max(1, static_cast<int>(std::lround(src_width * scale)));
    *dst_height = std::max(1, static_cast<int>(std::lround(src_height * scale)));
}

std::vector<uint8_t> ResizeRgba(const std::vector<uint8_t>& src, int src_width,
                                int src_height, int dst_width,
                                int dst_height) {
    std::vector<uint8_t> dst(static_cast<size_t>(dst_width) * dst_height * 4);
    for (int y = 0; y < dst_height; ++y) {
        double sy = (y + 0.5) * src_height / dst_height - 0.5;
        sy = std::max(0.0, std::min(sy, static_cast<double>(src_height - 1)));
        const int y0 = static_cast<int>(sy);
        const int y1 = std::min(y0 + 1, src_height - 1);
        const double fy = sy - y0;
        for (int x = 0; x < dst_width; ++x) {
            double sx = (x + 0.5) * src_width / dst_width - 0.5;
            sx = std::max(0.0, std::min(sx, static_cast<double>(src_width - 1)));
            const int x0 = static_cast<int>(sx);
            const int x1 = std::min(x0 + 1, src_width - 1);
            const double fx = sx - x0;
            size_t di = (static_cast<size_t>(y) * dst_width + x) * 4;
            const size_t p00 = (static_cast<size_t>(y0) * src_width + x0) * 4;
            const size_t p10 = (static_cast<size_t>(y0) * src_width + x1) * 4;
            const size_t p01 = (static_cast<size_t>(y1) * src_width + x0) * 4;
            const size_t p11 = (static_cast<size_t>(y1) * src_width + x1) * 4;
            for (int c = 0; c < 4; ++c) {
                const double top =
                    src[p00 + c] * (1.0 - fx) + src[p10 + c] * fx;
                const double bottom =
                    src[p01 + c] * (1.0 - fx) + src[p11 + c] * fx;
                const double value = top * (1.0 - fy) + bottom * fy;
                dst[di + c] = static_cast<uint8_t>(
                    std::max(0.0, std::min(255.0, value + 0.5)));
            }
        }
    }
    return dst;
}

// ---- 编码对比 ----

EncodeStat EncodeCurrent(const RgbaImage& image, float quality, int iterations) {
    EncodeStat stat;
    uint8_t* output = nullptr;
    size_t last_size = 0;
    stat.timing = Measure(iterations, [&]() {
        if (output) {
            WebPFree(output);
            output = nullptr;
        }
        last_size = WebPEncodeRGBA(image.pixels.data(), image.width,
                                   image.height, image.width * 4, quality,
                                   &output);
    });
    if (output) {
        WebPFree(output);
        output = nullptr;
    }
    stat.bytes = last_size;
    return stat;
}

EncodeStat EncodeLossless(const RgbaImage& image, int iterations) {
    EncodeStat stat;
    uint8_t* output = nullptr;
    size_t last_size = 0;
    stat.timing = Measure(iterations, [&]() {
        if (output) {
            WebPFree(output);
            output = nullptr;
        }
        last_size = WebPEncodeLosslessRGBA(image.pixels.data(), image.width,
                                           image.height, image.width * 4,
                                           &output);
    });
    if (output) {
        WebPFree(output);
        output = nullptr;
    }
    stat.bytes = last_size;
    return stat;
}

// 用 WebPConfig + WebPPicture 复现 WebPEncodeRGBA 的“导入 + 编码”流程，
// 这样可以显式控制 method / thread_level / alpha_quality 等参数。
EncodeStat EncodeConfigured(const RgbaImage& image, float quality, int method,
                            int thread_level, int alpha_quality,
                            bool use_sharp_yuv, int iterations) {
    EncodeStat stat;
    stat.timing = Measure(iterations, [&]() {
        WebPConfig config;
        if (!WebPConfigInit(&config)) return;
        config.quality = quality;
        config.lossless = 0;
        config.method = method;
        config.thread_level = thread_level;
        if (alpha_quality >= 0) config.alpha_quality = alpha_quality;
        config.use_sharp_yuv = use_sharp_yuv ? 1 : 0;

        WebPPicture picture;
        if (!WebPPictureInit(&picture)) return;
        picture.width = image.width;
        picture.height = image.height;
        if (!WebPPictureImportRGBA(&picture, image.pixels.data(),
                                   image.width * 4)) {
            WebPPictureFree(&picture);
            return;
        }
        WebPMemoryWriter writer;
        WebPMemoryWriterInit(&writer);
        picture.writer = WebPMemoryWrite;
        picture.custom_ptr = &writer;
        const int ok = WebPEncode(&config, &picture);
        if (ok) {
            stat.bytes = writer.size;
        }
        WebPMemoryWriterClear(&writer);
        WebPPictureFree(&picture);
    });
    return stat;
}

std::string FormatBytes(size_t bytes) {
    char buf[64];
    if (bytes >= 1024 * 1024) {
        std::snprintf(buf, sizeof(buf), "%.2f MiB",
                      static_cast<double>(bytes) / (1024.0 * 1024.0));
    } else if (bytes >= 1024) {
        std::snprintf(buf, sizeof(buf), "%.1f KiB",
                      static_cast<double>(bytes) / 1024.0);
    } else {
        std::snprintf(buf, sizeof(buf), "%zu B", bytes);
    }
    return buf;
}

long PeakRssMb() {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) return -1;
    return usage.ru_maxrss / 1024;  // Linux: KB -> MB
}

bool g_process_only = false;
bool g_legacy_preview = false;
std::string g_save_variants_dir;

bool EncodeConfiguredToFile(const RgbaImage& image, float quality, int method,
                            const std::string& path, std::string* err) {
    WebPConfig config;
    if (!WebPConfigInit(&config)) {
        if (err) *err = "webp config init failed";
        return false;
    }
    config.quality = quality;
    config.lossless = 0;
    config.method = method;
    config.thread_level = 0;
    config.alpha_quality = 100;

    WebPPicture picture;
    if (!WebPPictureInit(&picture)) {
        if (err) *err = "webp picture init failed";
        return false;
    }
    picture.width = image.width;
    picture.height = image.height;
    if (!WebPPictureImportRGBA(&picture, image.pixels.data(),
                               image.width * 4)) {
        WebPPictureFree(&picture);
        if (err) *err = "webp import rgba failed";
        return false;
    }

    WebPMemoryWriter writer;
    WebPMemoryWriterInit(&writer);
    picture.writer = WebPMemoryWrite;
    picture.custom_ptr = &writer;
    if (!WebPEncode(&config, &picture) || writer.size == 0 || !writer.mem) {
        WebPMemoryWriterClear(&writer);
        WebPPictureFree(&picture);
        if (err) *err = "webp encode failed";
        return false;
    }
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        WebPMemoryWriterClear(&writer);
        WebPPictureFree(&picture);
        if (err) *err = "open output failed: " + path;
        return false;
    }
    file.write(reinterpret_cast<const char*>(writer.mem),
               static_cast<std::streamsize>(writer.size));
    WebPMemoryWriterClear(&writer);
    WebPPictureFree(&picture);
    return true;
}

void SaveVariants(const sparkpush::ProcessedImage& processed,
                  const std::string& source_path) {
    if (g_save_variants_dir.empty()) return;
    std::error_code ec;
    std::filesystem::create_directories(g_save_variants_dir, ec);
    const std::string base =
        std::filesystem::path(source_path).filename().string();
    for (const auto& variant : processed.variants) {
        const std::string out = g_save_variants_dir + "/" + base + "." +
                                variant.variant + "." + variant.format;
        std::ofstream file(out, std::ios::binary);
        if (!file) {
            std::fprintf(stderr, "save variant failed: %s\n", out.c_str());
            continue;
        }
        file.write(variant.bytes.data(),
                   static_cast<std::streamsize>(variant.bytes.size()));
    }
}

bool RunOne(const std::string& path, int iterations) {
    std::string err;
    const std::string input = ReadFile(path, &err);
    if (input.empty()) {
        std::fprintf(stderr, "read %s failed: %s\n", path.c_str(), err.c_str());
        return false;
    }
    const std::string mime = sparkpush::SniffImageMime(input);
    if (mime.empty()) {
        std::fprintf(stderr, "%s: unsupported image header\n", path.c_str());
        return false;
    }

    std::printf("\n=== %s ===\n", path.c_str());
    std::printf("mime=%s  compressed=%s (%zu bytes)\n", mime.c_str(),
                FormatBytes(input.size()).c_str(), input.size());

    if (g_legacy_preview) {
        if (g_save_variants_dir.empty()) {
            std::fprintf(stderr, "--legacy-preview 需要 --save-variants <dir>\n");
            return false;
        }
        RgbaImage full;
        if (!DecodeImage(input, mime, &full, &err)) {
            std::fprintf(stderr, "legacy decode failed: %s\n", err.c_str());
            return false;
        }
        int width = 0;
        int height = 0;
        FitMaxSide(full.width, full.height, 1280, &width, &height);
        RgbaImage preview;
        preview.width = width;
        preview.height = height;
        if (width != full.width || height != full.height) {
            preview.pixels = ResizeRgba(full.pixels, full.width, full.height,
                                        width, height);
        } else {
            preview.pixels = full.pixels;
        }
        std::error_code ec;
        std::filesystem::create_directories(g_save_variants_dir, ec);
        const std::string base =
            std::filesystem::path(path).filename().string();
        const std::string out =
            g_save_variants_dir + "/" + base + ".legacy_preview.webp";
        if (!EncodeConfiguredToFile(preview, 80.0f, 2, out, &err)) {
            std::fprintf(stderr, "legacy encode failed: %s\n", err.c_str());
            return false;
        }
        std::printf("  legacy preview saved: %s (%dx%d)\n", out.c_str(),
                    width, height);
        return true;
    }

    if (g_process_only) {
        // 只跑 worker 实际调用的 ProcessImage，peak RSS 不受 benchmark
        // 自己持有的解码副本影响。
        sparkpush::ImageLimits limits;
        sparkpush::ProcessedImage processed;
        Timing t = Measure(1, [&]() {
            sparkpush::ProcessedImage tmp;
            if (sparkpush::ProcessImage(input, mime, limits, &tmp, &err)) {
                processed = std::move(tmp);
            }
        });
        std::printf("  ProcessImage only           avg=%8.2f ms  original=%dx%d\n",
                    t.avg_ms, processed.width, processed.height);
        size_t variant_bytes = 0;
        for (const auto& variant : processed.variants) {
            variant_bytes += variant.bytes.size();
            std::printf("  variant=%-8s %dx%d  %s\n", variant.variant.c_str(),
                        variant.width, variant.height,
                        FormatBytes(variant.bytes.size()).c_str());
        }
        std::printf("  total variant bytes=%s  peak RSS=%ld MiB\n",
                    FormatBytes(variant_bytes).c_str(), PeakRssMb());
        SaveVariants(processed, path);
        return true;
    }

    RgbaImage decoded;
    Timing decode_timing = Measure(iterations, [&]() {
        RgbaImage tmp;
        DecodeImage(input, mime, &tmp, &err);
    });
    if (!DecodeImage(input, mime, &decoded, &err)) {
        std::fprintf(stderr, "decode failed: %s\n", err.c_str());
        return false;
    }
    const int64_t pixels =
        static_cast<int64_t>(decoded.width) * decoded.height;
    std::printf("dimensions=%dx%d  pixels=%.2f MP  RGBA=%.2f MiB\n",
                decoded.width, decoded.height,
                static_cast<double>(pixels) / 1e6,
                static_cast<double>(decoded.pixels.size()) / (1024.0 * 1024.0));
    PrintTiming("decode", decode_timing);

    if (mime == "image/jpeg") {
        std::printf("  JPEG DCT scale sweep (1/1, 1/2, 1/4, 1/8)\n");
        for (int denom : {1, 2, 4, 8}) {
            Timing t = Measure(3, [&]() {
                RgbaImage tmp;
                DecodeJpeg(input, &tmp, &err, denom);
            });
            RgbaImage scaled;
            if (DecodeJpeg(input, &scaled, &err, denom)) {
                std::printf("  scale 1/%-2d avg=%8.2f ms  %dx%d  RGBA=%.2f MiB\n",
                            denom, t.avg_ms, scaled.width, scaled.height,
                            static_cast<double>(scaled.pixels.size()) /
                                (1024.0 * 1024.0));
            } else {
                std::printf("  scale 1/%-2d failed: %s\n", denom, err.c_str());
            }
        }
    }

    // 三档缩放耗时。
    struct VariantSpec {
        const char* name;
        int max_side;
        float quality;
    };
    const VariantSpec specs[] = {
        {"preview(1280)", 1280, 80.0f},
        {"thumb(320)", 320, 82.0f},
        {"micro(32)", 32, 50.0f},
    };

    std::vector<RgbaImage> resized(3);
    for (int i = 0; i < 3; ++i) {
        int w = 0;
        int h = 0;
        FitMaxSide(decoded.width, decoded.height, specs[i].max_side, &w, &h);
        resized[i].width = w;
        resized[i].height = h;
        Timing t = Measure(iterations, [&]() {
            if (w != decoded.width || h != decoded.height) {
                resized[i].pixels = ResizeRgba(decoded.pixels, decoded.width,
                                               decoded.height, w, h);
            } else {
                resized[i].pixels = decoded.pixels;
            }
        });
        char label[64];
        std::snprintf(label, sizeof(label), "resize %s", specs[i].name);
        PrintTiming(label, t);
        std::printf("  %-28s output=%dx%d  %.2f MiB\n", "", w, h,
                    static_cast<double>(resized[i].pixels.size()) /
                        (1024.0 * 1024.0));
    }

    // 编码：当前实现 vs 不同 method / thread_level / alpha_quality。
    for (int i = 0; i < 3; ++i) {
        const auto& image = resized[i];
        std::printf("\n encode %s q=%.0f (%dx%d)\n", specs[i].name,
                    specs[i].quality, image.width, image.height);

        EncodeStat current =
            EncodeCurrent(image, specs[i].quality, iterations);
        std::printf("  %-28s avg=%8.2f ms  size=%s\n", "WebPEncodeRGBA(default)",
                    current.timing.avg_ms, FormatBytes(current.bytes).c_str());

        for (int method = 0; method <= 6; ++method) {
            EncodeStat s = EncodeConfigured(image, specs[i].quality, method, 0,
                                            100, false, iterations);
            char label[64];
            std::snprintf(label, sizeof(label), "method=%d", method);
            std::printf("  %-28s avg=%8.2f ms  size=%s\n", label,
                        s.timing.avg_ms, FormatBytes(s.bytes).c_str());
        }

        EncodeStat threaded = EncodeConfigured(
            image, specs[i].quality, 4, 1, 100, false, iterations);
        std::printf("  %-28s avg=%8.2f ms  size=%s\n", "method=4,thread=1",
                    threaded.timing.avg_ms, FormatBytes(threaded.bytes).c_str());

        EncodeStat alpha80 = EncodeConfigured(image, specs[i].quality, 4, 0,
                                              80, false, iterations);
        std::printf("  %-28s avg=%8.2f ms  size=%s\n", "method=4,alpha_q=80",
                    alpha80.timing.avg_ms, FormatBytes(alpha80.bytes).c_str());

        if (i == 0) {  // 只有 preview 做无损对照，避免输出过长
            EncodeStat lossless = EncodeLossless(image, iterations);
            std::printf("  %-28s avg=%8.2f ms  size=%s\n",
                        "WebPEncodeLosslessRGBA",
                        lossless.timing.avg_ms,
                        FormatBytes(lossless.bytes).c_str());
        }
    }

    // 端到端：与 worker 调用完全一致的 ProcessImage。
    sparkpush::ImageLimits limits;
    sparkpush::ProcessedImage processed;
    Timing end_to_end = Measure(1, [&]() {
        sparkpush::ProcessedImage tmp;
        if (sparkpush::ProcessImage(input, mime, limits, &tmp, &err)) {
            processed = std::move(tmp);
        }
    });
    std::printf("\n end-to-end ProcessImage\n");
    PrintTiming("decode+resize+encode+color", end_to_end);
    size_t variant_bytes = 0;
    for (const auto& variant : processed.variants) {
        variant_bytes += variant.bytes.size();
        std::printf("  variant=%-8s %dx%d  %s\n", variant.variant.c_str(),
                    variant.width, variant.height,
                    FormatBytes(variant.bytes.size()).c_str());
    }
    std::printf("  total variant bytes=%s\n",
                FormatBytes(variant_bytes).c_str());
    std::printf("  peak RSS(final, includes benchmark copies)=%ld MiB\n",
                PeakRssMb());
    SaveVariants(processed, path);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::vector<std::string> paths;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--process-only") {
            g_process_only = true;
        } else if (arg == "--legacy-preview") {
            g_legacy_preview = true;
        } else if (arg == "--save-variants") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "--save-variants 需要一个目录\n");
                return 1;
            }
            g_save_variants_dir = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            std::printf(
                "用法: %s [--process-only] [--legacy-preview] "
                "[--save-variants <dir>] <image> [image...]\n"
                "  --process-only  只跑 worker 实际调用的 ProcessImage，\n"
                "                  用于测量接近生产的耗时和峰值 RSS。\n"
                "  --legacy-preview 用旧 RGBA 全量解码 + 双线性缩放生成 preview，\n"
                "                  需要配合 --save-variants 使用，用于质量 A/B。\n"
                "  --save-variants 把 micro/thumb/preview 变体写到目录。\n"
                "支持 JPEG/PNG/WebP；建议一次只传一张。\n",
                argv[0]);
            return 0;
        } else {
            paths.push_back(arg);
        }
    }
    if (paths.empty()) {
        std::fprintf(stderr,
                     "用法: %s [--process-only] <image> [image...]\n",
                     argv[0]);
        return 1;
    }
    const int iterations = 3;
    bool ok = true;
    for (const auto& path : paths) {
        ok = RunOne(path, iterations) && ok;
    }
    return ok ? 0 : 1;
}
