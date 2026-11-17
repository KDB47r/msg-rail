#include "image_processor.h"

#include <algorithm>
#include <csetjmp>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include <jpeglib.h>
#include <png.h>
#include <webp/decode.h>
#include <webp/encode.h>

#include "memory_limiter.h"

namespace sparkpush {

namespace {

constexpr int kMicroMaxSide = 32;
constexpr int kThumbMaxSide = 320;
constexpr int kPreviewMaxSide = 1280;
constexpr size_t kMemoryEstimateMargin = 16 * 1024 * 1024;

struct RgbaImage {
    int width{0};           // 解码后的实际尺寸（JPEG 可能已 DCT 缩放）
    int height{0};
    int original_width{0};  // 文件头里的原始尺寸
    int original_height{0};
    std::vector<uint8_t> pixels;  // RGBA，行优先
};

bool ValidateDimensions(int width, int height, const ImageLimits& limits,
                        std::string* err) {
    if (width <= 0 || height <= 0) {
        if (err) *err = "invalid image dimensions";
        return false;
    }
    if (width > limits.max_side || height > limits.max_side) {
        if (err) {
            *err = "image side exceeds limit: " + std::to_string(width) + "x" +
                   std::to_string(height);
        }
        return false;
    }
    if (static_cast<int64_t>(width) * height > limits.max_pixels) {
        if (err) {
            *err = "image pixels exceed limit: " +
                   std::to_string(static_cast<int64_t>(width) * height);
        }
        return false;
    }
    return true;
}

// libjpeg 的默认 error_exit 会直接 exit；这里改成 longjmp 回到调用点。
struct JpegErrorManager {
    jpeg_error_mgr pub;
    std::jmp_buf jump;
    char message[JMSG_LENGTH_MAX];
};

extern "C" void JpegErrorExit(j_common_ptr cinfo) {
    auto* err = reinterpret_cast<JpegErrorManager*>(cinfo->err);
    (*cinfo->err->format_message)(cinfo, err->message);
    std::longjmp(err->jump, 1);
}

bool DecodeJpeg(const std::string& input, const ImageLimits& limits,
                int scale_denom, RgbaImage* out, std::string* err) {
    // cinfo/jerr 放到堆上：longjmp 之后栈上被修改过的对象值不再可靠。
    auto* cinfo = new jpeg_decompress_struct();
    auto* jerr = new JpegErrorManager();
    std::memset(cinfo, 0, sizeof(*cinfo));
    std::memset(jerr, 0, sizeof(*jerr));
    unsigned char* volatile rgb = nullptr;
    volatile bool created = false;

    cinfo->err = jpeg_std_error(&jerr->pub);
    jerr->pub.error_exit = JpegErrorExit;
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
    const int original_width = static_cast<int>(cinfo->image_width);
    const int original_height = static_cast<int>(cinfo->image_height);
    if (!ValidateDimensions(original_width, original_height, limits, err)) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        return false;
    }
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
    const int channels = cinfo->output_components;
    if (channels != 3 || width <= 0 || height <= 0) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        if (err) *err = "unsupported jpeg output format";
        return false;
    }

    const size_t rgb_size =
        static_cast<size_t>(width) * static_cast<size_t>(height) * 3;
    rgb = static_cast<unsigned char*>(std::malloc(rgb_size));
    if (!rgb) {
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        if (err) *err = "out of memory while decoding jpeg";
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
    out->original_width = original_width;
    out->original_height = original_height;
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

bool DecodePng(const std::string& input, const ImageLimits& limits,
               RgbaImage* out, std::string* err) {
    png_image image;
    std::memset(&image, 0, sizeof(image));
    image.version = PNG_IMAGE_VERSION;
    if (!png_image_begin_read_from_memory(
            &image, static_cast<const void*>(input.data()), input.size())) {
        if (err) *err = std::string("png header error: ") + image.message;
        png_image_free(&image);
        return false;
    }
    if (!ValidateDimensions(static_cast<int>(image.width),
                            static_cast<int>(image.height), limits, err)) {
        png_image_free(&image);
        return false;
    }
    image.format = PNG_FORMAT_RGBA;
    out->width = static_cast<int>(image.width);
    out->height = static_cast<int>(image.height);
    out->original_width = out->width;
    out->original_height = out->height;
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

bool DecodeWebp(const std::string& input, const ImageLimits& limits,
                RgbaImage* out, std::string* err) {
    int width = 0;
    int height = 0;
    if (!WebPGetInfo(reinterpret_cast<const uint8_t*>(input.data()),
                     input.size(), &width, &height)) {
        if (err) *err = "invalid webp header";
        return false;
    }
    if (!ValidateDimensions(width, height, limits, err)) return false;
    // 直接解码到预分配缓冲，避免 WebPDecodeRGBA 的临时 malloc + copy。
    const size_t stride = static_cast<size_t>(width) * 4;
    out->width = width;
    out->height = height;
    out->original_width = width;
    out->original_height = height;
    out->pixels.resize(static_cast<size_t>(width) * height * 4);
    uint8_t* decoded = WebPDecodeRGBAInto(
        reinterpret_cast<const uint8_t*>(input.data()), input.size(),
        out->pixels.data(), out->pixels.size(), static_cast<int>(stride));
    if (!decoded) {
        out->pixels.clear();
        if (err) *err = "webp decode failed";
        return false;
    }
    return true;
}

bool DecodeImage(const std::string& input, const std::string& mime,
                 const ImageLimits& limits, int jpeg_scale_denom,
                 RgbaImage* out, std::string* err) {
    bool ok = false;
    if (mime == "image/jpeg") {
        ok = DecodeJpeg(input, limits, jpeg_scale_denom, out, err);
    } else if (mime == "image/png") {
        ok = DecodePng(input, limits, out, err);
    } else if (mime == "image/webp") {
        ok = DecodeWebp(input, limits, out, err);
    } else {
        if (err) *err = "unsupported image mime: " + mime;
        return false;
    }
    if (!ok) return false;
    // 尺寸上限按原图校验；JPEG 的输出可能已被 DCT 缩小。
    return ValidateDimensions(out->original_width, out->original_height,
                              limits, err);
}

struct ImageDimensions {
    int width{0};
    int height{0};
    // 渐进式 JPEG 需要整幅系数/缓冲，DCT 缩放不能按比例降低这部分内存。
    bool progressive{false};
    // 仅 WebP 使用：format=1 为 lossy，format=2 为 lossless。
    bool webp_lossy{false};
    bool webp_has_alpha{false};
};

// 只读文件头拿原始尺寸，用于选择 DCT 缩放比例和估算内存。
bool ProbeImageDimensions(const std::string& input, const std::string& mime,
                          ImageDimensions* out, std::string* err) {
    if (!out) return false;

    if (mime == "image/jpeg") {
        auto* cinfo = new jpeg_decompress_struct();
        auto* jerr = new JpegErrorManager();
        std::memset(cinfo, 0, sizeof(*cinfo));
        std::memset(jerr, 0, sizeof(*jerr));
        volatile bool created = false;

        cinfo->err = jpeg_std_error(&jerr->pub);
        jerr->pub.error_exit = JpegErrorExit;
        if (setjmp(jerr->jump)) {
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
        out->width = static_cast<int>(cinfo->image_width);
        out->height = static_cast<int>(cinfo->image_height);
        out->progressive = cinfo->progressive_mode != 0;
        jpeg_destroy_decompress(cinfo);
        delete cinfo;
        delete jerr;
        return out->width > 0 && out->height > 0;
    }

    if (mime == "image/png") {
        png_image image;
        std::memset(&image, 0, sizeof(image));
        image.version = PNG_IMAGE_VERSION;
        if (!png_image_begin_read_from_memory(
                &image, static_cast<const void*>(input.data()),
                input.size())) {
            if (err) *err = std::string("png header error: ") + image.message;
            png_image_free(&image);
            return false;
        }
        out->width = static_cast<int>(image.width);
        out->height = static_cast<int>(image.height);
        png_image_free(&image);
        return out->width > 0 && out->height > 0;
    }

    if (mime == "image/webp") {
        WebPBitstreamFeatures features;
        std::memset(&features, 0, sizeof(features));
        if (WebPGetFeatures(reinterpret_cast<const uint8_t*>(input.data()),
                            input.size(), &features) != VP8_STATUS_OK) {
            if (err) *err = "invalid webp header";
            return false;
        }
        out->width = features.width;
        out->height = features.height;
        out->webp_lossy = (features.format == 1);
        out->webp_has_alpha = (features.has_alpha != 0);
        return out->width > 0 && out->height > 0;
    }

    if (err) *err = "unsupported image mime: " + mime;
    return false;
}

// 选“仍能让最长边 >= target 的最小解码尺寸”。
// libjpeg 支持 1/1、1/2、1/4、1/8 这几档 DCT 缩放。
int ChooseJpegScaleDenom(int width, int height, int target_max_side) {
    if (target_max_side <= 0) return 1;
    const int long_side = std::max(width, height);
    if (long_side <= 0) return 1;
    const int candidates[] = {8, 4, 2};
    for (int denom : candidates) {
        if (long_side / denom >= target_max_side) return denom;
    }
    return 1;
}

void FitMaxSide(int src_width, int src_height, int max_side, int* dst_width,
                int* dst_height);

size_t EstimateImageMemoryBytes(const std::string& mime, int original_width,
                                int original_height, int jpeg_scale_denom,
                                bool progressive, bool webp_yuv,
                                bool webp_lossless, bool png_downscale,
                                size_t input_size) {
    if (mime == "image/png" && png_downscale) {
        int preview_width = 0;
        int preview_height = 0;
        FitMaxSide(original_width, original_height, kPreviewMaxSide,
                   &preview_width, &preview_height);
        const int64_t preview_pixels =
            static_cast<int64_t>(preview_width) * preview_height;
        const int64_t total = preview_pixels * 4 +
                              static_cast<int64_t>(original_width) * 4 +
                              static_cast<int64_t>(input_size) +
                              static_cast<int64_t>(kMemoryEstimateMargin);
        return total > 0 ? static_cast<size_t>(total) : 0;
    }
    if (mime == "image/webp" && webp_yuv) {
        // lossy WebP YUV 路径：全量 YUV420（1.5B/px）+ preview RGBA。
        int preview_width = 0;
        int preview_height = 0;
        FitMaxSide(original_width, original_height, kPreviewMaxSide,
                   &preview_width, &preview_height);
        const int64_t original_pixels =
            static_cast<int64_t>(original_width) * original_height;
        const int64_t preview_pixels =
            static_cast<int64_t>(preview_width) * preview_height;
        const int64_t total = original_pixels * 3 / 2 + preview_pixels * 4 +
                              static_cast<int64_t>(input_size) +
                              static_cast<int64_t>(kMemoryEstimateMargin);
        return total > 0 ? static_cast<size_t>(total) : 0;
    }

    const int denom = (mime == "image/jpeg") ? jpeg_scale_denom : 1;
    const int64_t decoded_width =
        (static_cast<int64_t>(original_width) + denom - 1) / denom;
    const int64_t decoded_height =
        (static_cast<int64_t>(original_height) + denom - 1) / denom;
    const int64_t pixels = decoded_width * decoded_height;

    int64_t bytes_per_pixel = 4;
    if (mime == "image/jpeg") {
        bytes_per_pixel = 7;  // RGB 3 + RGBA 4
    } else if (mime == "image/webp") {
        // WebPDecodeRGBAInto 写入一块 RGBA；lossless 解码器还有较大的
        // 内部缓冲（45MP 实测峰值约 379MiB），按 8B/px 保守估算。
        bytes_per_pixel = webp_lossless ? 8 : 4;
    }

    int64_t total = pixels * bytes_per_pixel +
                    static_cast<int64_t>(input_size) +
                    static_cast<int64_t>(kMemoryEstimateMargin);
    if (mime == "image/jpeg" && progressive) {
        // 渐进式 JPEG 需要整幅系数数组和全尺寸缓冲；实测约 6 字节/像素，
        // 这部分不会因为 DCT 缩放而按比例下降。
        total += static_cast<int64_t>(original_width) *
                 static_cast<int64_t>(original_height) * 6;
    }
    return total > 0 ? static_cast<size_t>(total) : 0;
}

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

uint8_t ClampByte(int value) {
    return static_cast<uint8_t>(std::max(0, std::min(255, value)));
}

// 对 8-bit 平面做 area/box 降采样。只用于缩小；
// 每个目标像素取源图对应矩形内所有像素的平均值，避免大倍率缩小的混叠。
void DownscalePlaneBox(const uint8_t* src, int src_width, int src_height,
                       int src_stride, uint8_t* dst, int dst_width,
                       int dst_height) {
    for (int y = 0; y < dst_height; ++y) {
        const int sy0 = static_cast<int>(
            static_cast<int64_t>(y) * src_height / dst_height);
        int sy1 = static_cast<int>(
            static_cast<int64_t>(y + 1) * src_height / dst_height);
        if (sy1 <= sy0) sy1 = sy0 + 1;
        for (int x = 0; x < dst_width; ++x) {
            const int sx0 = static_cast<int>(
                static_cast<int64_t>(x) * src_width / dst_width);
            int sx1 = static_cast<int>(
                static_cast<int64_t>(x + 1) * src_width / dst_width);
            if (sx1 <= sx0) sx1 = sx0 + 1;
            uint32_t sum = 0;
            uint32_t count = 0;
            for (int sy = sy0; sy < sy1; ++sy) {
                const uint8_t* row =
                    src + static_cast<size_t>(sy) * src_stride;
                for (int sx = sx0; sx < sx1; ++sx) {
                    sum += row[sx];
                    ++count;
                }
            }
            dst[static_cast<size_t>(y) * dst_width + x] =
                count == 0
                    ? 0
                    : static_cast<uint8_t>((sum + count / 2) / count);
        }
    }
}

// lossy WebP：解码到 YUV420，在 YUV 域缩小到 preview，再只对 preview 做
// YUV->RGBA。VP8 解码仍是全量；省的是全尺寸 RGB 转换/上采样和全尺寸 RGBA。
bool DecodeWebpYuvDownscaled(const std::string& input, int source_width,
                             int source_height, RgbaImage* out,
                             std::string* err) {
    int width = 0;
    int height = 0;
    uint8_t* u = nullptr;
    uint8_t* v = nullptr;
    int stride = 0;
    int uv_stride = 0;
    uint8_t* y = WebPDecodeYUV(
        reinterpret_cast<const uint8_t*>(input.data()), input.size(), &width,
        &height, &u, &v, &stride, &uv_stride);
    if (!y || !u || !v || width <= 0 || height <= 0 || stride <= 0 ||
        uv_stride <= 0) {
        if (y) WebPFree(y);
        if (err) *err = "webp yuv decode failed";
        return false;
    }

    const int src_w = width;
    const int src_h = height;
    if (src_w != source_width || src_h != source_height) {
        WebPFree(y);
        if (err) *err = "webp yuv dimensions mismatch";
        return false;
    }

    int dst_w = 0;
    int dst_h = 0;
    FitMaxSide(src_w, src_h, kPreviewMaxSide, &dst_w, &dst_h);
    if (dst_w <= 0 || dst_h <= 0) {
        WebPFree(y);
        if (err) *err = "webp yuv invalid preview size";
        return false;
    }

    const int src_uv_w = (src_w + 1) / 2;
    const int src_uv_h = (src_h + 1) / 2;
    const int dst_uv_w = (dst_w + 1) / 2;
    const int dst_uv_h = (dst_h + 1) / 2;

    std::vector<uint8_t> y_plane(
        static_cast<size_t>(dst_w) * dst_h);
    std::vector<uint8_t> u_plane(
        static_cast<size_t>(dst_uv_w) * dst_uv_h);
    std::vector<uint8_t> v_plane(
        static_cast<size_t>(dst_uv_w) * dst_uv_h);

    DownscalePlaneBox(y, src_w, src_h, stride, y_plane.data(), dst_w, dst_h);
    DownscalePlaneBox(u, src_uv_w, src_uv_h, uv_stride, u_plane.data(),
                      dst_uv_w, dst_uv_h);
    DownscalePlaneBox(v, src_uv_w, src_uv_h, uv_stride, v_plane.data(),
                      dst_uv_w, dst_uv_h);
    WebPFree(y);

    out->width = dst_w;
    out->height = dst_h;
    out->original_width = src_w;
    out->original_height = src_h;
    out->pixels.resize(static_cast<size_t>(dst_w) * dst_h * 4);
    for (int yy = 0; yy < dst_h; ++yy) {
        const int cy = std::min(yy / 2, dst_uv_h - 1);
        for (int xx = 0; xx < dst_w; ++xx) {
            const int cx = std::min(xx / 2, dst_uv_w - 1);
            const int Y = y_plane[static_cast<size_t>(yy) * dst_w + xx];
            const int U = static_cast<int>(
                              u_plane[static_cast<size_t>(cy) * dst_uv_w + cx]) -
                          128;
            const int V = static_cast<int>(
                              v_plane[static_cast<size_t>(cy) * dst_uv_w + cx]) -
                          128;
            const int C = Y - 16;
            // limited-range BT.601，和 libwebp 的 RGBA 输出一致。
            const size_t di =
                (static_cast<size_t>(yy) * dst_w + xx) * 4;
            out->pixels[di + 0] = ClampByte((298 * C + 409 * V + 128) >> 8);
            out->pixels[di + 1] =
                ClampByte((298 * C - 100 * U - 208 * V + 128) >> 8);
            out->pixels[di + 2] = ClampByte((298 * C + 516 * U + 128) >> 8);
            out->pixels[di + 3] = 255;
        }
    }
    return true;
}

// ---- PNG 逐行解码 + box 降采样 ----
// 用 C 风格实现，避免 libpng 的 longjmp 跨过 C++ 对象析构。

struct PngMemoryReader {
    const std::string* data{nullptr};
    size_t offset{0};
};

void PngReadFromMemory(png_structp png, png_bytep out, png_size_t count) {
    auto* reader = static_cast<PngMemoryReader*>(png_get_io_ptr(png));
    if (!reader || !reader->data ||
        reader->offset + count > reader->data->size()) {
        png_error(png, "unexpected end of png data");
        return;
    }
    std::memcpy(out, reader->data->data() + reader->offset, count);
    reader->offset += count;
}

void PngErrorHandler(png_structp png, png_const_charp message) {
    auto* err = static_cast<std::string*>(png_get_error_ptr(png));
    if (err) *err = message ? message : "png decode error";
    std::longjmp(png_jmpbuf(png), 1);
}

// 逐行读取 PNG，按目标尺寸做 2D box 平均，只保留目标 RGBA 和一行累加器。
bool DecodePngDownscaledRows(const std::string& input, int expected_width,
                             int expected_height, int dst_width,
                             int dst_height, uint8_t* dst, size_t dst_size,
                             std::string* err) {
    if (!dst || dst_width <= 0 || dst_height <= 0 ||
        static_cast<size_t>(dst_width) * dst_height * 4 > dst_size) {
        if (err) *err = "invalid png downscale arguments";
        return false;
    }

    png_structp png = png_create_read_struct(PNG_LIBPNG_VER_STRING, err,
                                             PngErrorHandler, nullptr);
    if (!png) {
        if (err) *err = "png_create_read_struct failed";
        return false;
    }

    png_infop volatile info = nullptr;
    uint8_t* volatile row = nullptr;
    uint32_t* volatile acc = nullptr;
    int* volatile h_count = nullptr;
    // libpng 的 error handler 会 longjmp；必须先在 read struct 上建立
    // setjmp，再调用可能出错的 png_create_info_struct/read_info。
    if (setjmp(png_jmpbuf(png))) {
        std::free((void*)row);
        std::free((void*)acc);
        std::free((void*)h_count);
        png_infop info_local = info;
        png_destroy_read_struct(&png, info_local ? &info_local : nullptr,
                                nullptr);
        if (err && err->empty()) *err = "png decode failed";
        return false;
    }

    info = png_create_info_struct(png);
    if (!info) {
        png_destroy_read_struct(&png, nullptr, nullptr);
        if (err) *err = "png_create_info_struct failed";
        return false;
    }

    PngMemoryReader reader{&input, 0};
    png_set_read_fn(png, &reader, PngReadFromMemory);
    png_read_info(png, info);

    png_uint_32 width = 0;
    png_uint_32 height = 0;
    int bit_depth = 0;
    int color_type = 0;
    int interlace_type = 0;
    int compression_type = 0;
    int filter_type = 0;
    png_get_IHDR(png, info, &width, &height, &bit_depth, &color_type,
                 &interlace_type, &compression_type, &filter_type);
    if (static_cast<int>(width) != expected_width ||
        static_cast<int>(height) != expected_height) {
        if (err) *err = "png dimensions changed during decode";
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
        return false;
    }
    if (interlace_type != PNG_INTERLACE_NONE) {
        if (err) *err = "interlaced png downscale is not supported";
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
        return false;
    }

    if (color_type == PNG_COLOR_TYPE_PALETTE) {
        png_set_palette_to_rgb(png);
    }
    if (color_type == PNG_COLOR_TYPE_GRAY && bit_depth < 8) {
        png_set_expand_gray_1_2_4_to_8(png);
    }
    if (png_get_valid(png, info, PNG_INFO_tRNS)) {
        png_set_tRNS_to_alpha(png);
    }
    if (bit_depth == 16) {
        png_set_strip_16(png);
    }
    if (color_type == PNG_COLOR_TYPE_GRAY ||
        color_type == PNG_COLOR_TYPE_GRAY_ALPHA) {
        png_set_gray_to_rgb(png);
    }
    if (!(color_type & PNG_COLOR_MASK_ALPHA) &&
        !png_get_valid(png, info, PNG_INFO_tRNS)) {
        png_set_add_alpha(png, 0xff, PNG_FILLER_AFTER);
    }
    png_read_update_info(png, info);

    const png_size_t rowbytes = png_get_rowbytes(png, info);
    const int channels = png_get_channels(png, info);
    if (channels != 4 ||
        rowbytes < static_cast<png_size_t>(width) * 4) {
        if (err) *err = "png did not expand to rgba";
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
        return false;
    }

    row = static_cast<uint8_t*>(std::malloc(rowbytes));
    acc = static_cast<uint32_t*>(
        std::calloc(static_cast<size_t>(dst_width) * 4, sizeof(uint32_t)));
    h_count = static_cast<int*>(
        std::malloc(static_cast<size_t>(dst_width) * sizeof(int)));
    if (!row || !acc || !h_count) {
        if (err) *err = "out of memory for png downscale";
        std::free((void*)row);
        std::free((void*)acc);
        std::free((void*)h_count);
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
        return false;
    }

    for (int dx = 0; dx < dst_width; ++dx) {
        const int sx0 = static_cast<int>(
            static_cast<int64_t>(dx) * width / dst_width);
        int sx1 = static_cast<int>(
            static_cast<int64_t>(dx + 1) * width / dst_width);
        if (sx1 <= sx0) sx1 = sx0 + 1;
        h_count[dx] = sx1 - sx0;
    }

    int sy = 0;
    for (int dy = 0; dy < dst_height; ++dy) {
        int sy1 = static_cast<int>(
            static_cast<int64_t>(dy + 1) * height / dst_height);
        if (sy1 <= sy) sy1 = sy + 1;
        std::memset((void*)acc, 0,
                    static_cast<size_t>(dst_width) * 4 * sizeof(uint32_t));
        int source_rows = 0;
        for (; sy < sy1; ++sy) {
            png_read_row(png, reinterpret_cast<png_bytep>(row), nullptr);
            const uint8_t* src = (const uint8_t*)row;
            for (int dx = 0; dx < dst_width; ++dx) {
                const int sx0 = static_cast<int>(
                    static_cast<int64_t>(dx) * width / dst_width);
                int sx1 = static_cast<int>(
                    static_cast<int64_t>(dx + 1) * width / dst_width);
                if (sx1 <= sx0) sx1 = sx0 + 1;
                const uint8_t* p = src + static_cast<size_t>(sx0) * 4;
                uint32_t r = 0, g = 0, b = 0, a = 0;
                for (int sx = sx0; sx < sx1; ++sx, p += 4) {
                    r += p[0];
                    g += p[1];
                    b += p[2];
                    a += p[3];
                }
                const size_t ai = static_cast<size_t>(dx) * 4;
                acc[ai + 0] += r;
                acc[ai + 1] += g;
                acc[ai + 2] += b;
                acc[ai + 3] += a;
            }
            ++source_rows;
        }
        if (source_rows <= 0) {
            if (err) *err = "png downscale produced empty row";
            std::free((void*)row);
            std::free((void*)acc);
            std::free((void*)h_count);
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
            return false;
        }
        for (int dx = 0; dx < dst_width; ++dx) {
            const uint32_t count =
                static_cast<uint32_t>(source_rows * h_count[dx]);
            const size_t ai = static_cast<size_t>(dx) * 4;
            const size_t di =
                (static_cast<size_t>(dy) * dst_width + dx) * 4;
            for (int c = 0; c < 4; ++c) {
                dst[di + c] =
                    count == 0
                        ? 0
                        : static_cast<uint8_t>((acc[ai + c] + count / 2) /
                                               count);
            }
        }
    }

    png_read_end(png, nullptr);
    std::free((void*)row);
    std::free((void*)acc);
    std::free((void*)h_count);
        png_infop info_local = info;
        png_destroy_read_struct(&png, &info_local, nullptr);
    return true;
}

bool DecodePngDownscaled(const std::string& input, int expected_width,
                         int expected_height, RgbaImage* out,
                         std::string* err) {
    int dst_width = 0;
    int dst_height = 0;
    FitMaxSide(expected_width, expected_height, kPreviewMaxSide, &dst_width,
               &dst_height);
    if (dst_width >= expected_width && dst_height >= expected_height) {
        return false;
    }
    out->pixels.resize(static_cast<size_t>(dst_width) * dst_height * 4);
    if (!DecodePngDownscaledRows(input, expected_width, expected_height,
                                 dst_width, dst_height, out->pixels.data(),
                                 out->pixels.size(), err)) {
        out->pixels.clear();
        return false;
    }
    out->width = dst_width;
    out->height = dst_height;
    out->original_width = expected_width;
    out->original_height = expected_height;
    return true;
}

// 双线性缩放，够用且实现简单；只用于缩略图，不追求高质量重采样。
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
            const size_t p00 =
                (static_cast<size_t>(y0) * src_width + x0) * 4;
            const size_t p10 =
                (static_cast<size_t>(y0) * src_width + x1) * 4;
            const size_t p01 =
                (static_cast<size_t>(y1) * src_width + x0) * 4;
            const size_t p11 =
                (static_cast<size_t>(y1) * src_width + x1) * 4;
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

bool EncodeWebp(const std::vector<uint8_t>& rgba, int width, int height,
                float quality, int method, std::string* out,
                std::string* err) {
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
    if (!WebPValidateConfig(&config)) {
        if (err) *err = "webp config is invalid";
        return false;
    }

    WebPPicture picture;
    if (!WebPPictureInit(&picture)) {
        if (err) *err = "webp picture init failed";
        return false;
    }
    picture.width = width;
    picture.height = height;
    if (!WebPPictureImportRGBA(&picture, rgba.data(), width * 4)) {
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
    out->assign(reinterpret_cast<const char*>(writer.mem), writer.size);
    WebPMemoryWriterClear(&writer);
    WebPPictureFree(&picture);
    return true;
}

std::string ComputeDominantColor(const std::vector<uint8_t>& rgba,
                                 int width, int height) {
    const size_t total = static_cast<size_t>(width) * height;
    const size_t step = std::max<size_t>(1, total / 4096);
    uint64_t r = 0;
    uint64_t g = 0;
    uint64_t b = 0;
    uint64_t alpha_sum = 0;
    for (size_t i = 0; i < total; i += step) {
        const uint32_t a = rgba[i * 4 + 3];
        r += static_cast<uint64_t>(rgba[i * 4 + 0]) * a;
        g += static_cast<uint64_t>(rgba[i * 4 + 1]) * a;
        b += static_cast<uint64_t>(rgba[i * 4 + 2]) * a;
        alpha_sum += a;
    }
    if (alpha_sum == 0) return "#FFFFFF";
    char color[8];
    std::snprintf(color, sizeof(color), "#%02X%02X%02X",
                  static_cast<unsigned>(r / alpha_sum),
                  static_cast<unsigned>(g / alpha_sum),
                  static_cast<unsigned>(b / alpha_sum));
    return color;
}

bool AddVariant(const RgbaImage& image, const std::string& name,
                int max_side, float quality, int method, ProcessedImage* out,
                std::string* err) {
    int width = 0;
    int height = 0;
    FitMaxSide(image.width, image.height, max_side, &width, &height);
    std::vector<uint8_t> pixels;
    const std::vector<uint8_t>* source = &image.pixels;
    if (width != image.width || height != image.height) {
        pixels = ResizeRgba(image.pixels, image.width, image.height, width,
                            height);
        source = &pixels;
    }
    ImageVariantData variant;
    variant.variant = name;
    variant.format = "webp";
    variant.width = width;
    variant.height = height;
    if (!EncodeWebp(*source, width, height, quality, method, &variant.bytes,
                    err)) {
        return false;
    }
    out->variants.push_back(std::move(variant));
    return true;
}

}  // namespace

std::string SniffImageMime(const std::string& bytes) {
    const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
    const size_t size = bytes.size();
    if (size >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF) {
        return "image/jpeg";
    }
    static const unsigned char kPngMagic[8] = {0x89, 0x50, 0x4E, 0x47,
                                               0x0D, 0x0A, 0x1A, 0x0A};
    if (size >= sizeof(kPngMagic) &&
        std::memcmp(data, kPngMagic, sizeof(kPngMagic)) == 0) {
        return "image/png";
    }
    if (size >= 12 && std::memcmp(data, "RIFF", 4) == 0 &&
        std::memcmp(data + 8, "WEBP", 4) == 0) {
        return "image/webp";
    }
    return {};
}

bool ProcessImage(const std::string& input, const std::string& mime,
                  const ImageLimits& limits, ProcessedImage* out,
                  std::string* err, MemoryLimiter* limiter) {
    if (!out) {
        if (err) *err = "output is null";
        return false;
    }
    const std::string actual_mime = SniffImageMime(input);
    if (actual_mime.empty()) {
        if (err) *err = "unsupported or corrupted image header";
        return false;
    }
    if (!mime.empty() && mime != actual_mime) {
        if (err) {
            *err = "declared mime " + mime + " does not match file content " +
                   actual_mime;
        }
        return false;
    }

    ImageDimensions source;
    if (!ProbeImageDimensions(input, actual_mime, &source, err)) return false;
    if (!ValidateDimensions(source.width, source.height, limits, err)) {
        return false;
    }

    const int jpeg_scale_denom =
        actual_mime == "image/jpeg"
            ? ChooseJpegScaleDenom(source.width, source.height,
                                   kPreviewMaxSide)
            : 1;

    // 大尺寸 lossy WebP 走 YUV 域缩小；大尺寸 PNG 走逐行降采样。
    // 小图/alpha/lossless 走零拷贝或全量 RGBA。
    const int long_side = std::max(source.width, source.height);
    const bool use_webp_yuv =
        actual_mime == "image/webp" && source.webp_lossy &&
        !source.webp_has_alpha && long_side >= kPreviewMaxSide * 2;
    const bool use_png_downscale =
        actual_mime == "image/png" && long_side >= kPreviewMaxSide * 2;

    // 从解码开始前就占用预算，覆盖解码、主色、缩放和编码。
    const size_t estimated_bytes = EstimateImageMemoryBytes(
        actual_mime, source.width, source.height, jpeg_scale_denom,
        source.progressive, use_webp_yuv, !source.webp_lossy,
        use_png_downscale, input.size());
    auto memory_guard =
        limiter ? limiter->Acquire(estimated_bytes) : MemoryLimiter::Guard();

    RgbaImage image;
    auto full_decode_with_budget = [&]() {
        // 缩小路径异常时回退全量解码；先释放缩小路径预算，再按全量预算
        // 重新获取，避免低估内存。
        memory_guard = MemoryLimiter::Guard();
        const size_t fallback_bytes = EstimateImageMemoryBytes(
            actual_mime, source.width, source.height, jpeg_scale_denom,
            source.progressive, false, !source.webp_lossy, false,
            input.size());
        memory_guard =
            limiter ? limiter->Acquire(fallback_bytes)
                    : MemoryLimiter::Guard();
        return DecodeImage(input, actual_mime, limits, jpeg_scale_denom,
                           &image, err);
    };

    bool decoded = false;
    if (use_webp_yuv) {
        decoded = DecodeWebpYuvDownscaled(
            input, source.width, source.height, &image, err);
        if (!decoded) decoded = full_decode_with_budget();
    } else if (use_png_downscale) {
        decoded = DecodePngDownscaled(input, source.width, source.height,
                                      &image, err);
        if (!decoded) decoded = full_decode_with_budget();
    } else {
        decoded = DecodeImage(input, actual_mime, limits, jpeg_scale_denom,
                              &image, err);
    }
    if (!decoded) return false;
    out->width = image.original_width;
    out->height = image.original_height;
    out->dominant_color =
        ComputeDominantColor(image.pixels, image.width, image.height);
    out->variants.clear();
    if (!AddVariant(image, "micro", kMicroMaxSide, 50.0f, 0, out, err)) {
        return false;
    }
    if (!AddVariant(image, "thumb", kThumbMaxSide, 82.0f, 0, out, err)) {
        return false;
    }
    if (!AddVariant(image, "preview", kPreviewMaxSide, 80.0f, 2, out, err)) {
        return false;
    }
    return true;
}

}  // namespace sparkpush
