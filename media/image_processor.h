#pragma once

#include <string>
#include <vector>

namespace sparkpush {

class MemoryLimiter;

struct ImageLimits {
    int max_pixels{50000000};
    int max_side{12000};
};

struct ImageVariantData {
    std::string variant;  // micro / thumb / preview
    std::string format;   // webp
    int width{0};
    int height{0};
    std::string bytes;
};

struct ProcessedImage {
    int width{0};
    int height{0};
    std::string dominant_color;  // #RRGGBB
    std::vector<ImageVariantData> variants;
};

// 只信任文件头，不信任客户端声明的 Content-Type。
// 目前支持 JPEG / PNG / WebP，其他返回空串。
std::string SniffImageMime(const std::string& bytes);

// 解码原图，按最长边生成 micro(32) / thumb(320) / preview(1280)
// 三档 WebP；同时计算主色。JPEG 会按 preview 尺寸做 DCT 缩放解码，
// PNG/WebP 仍是完整解码。任何解码/尺寸超限都会返回 false。
// limiter 为空时不限制并发内存。
bool ProcessImage(const std::string& input, const std::string& mime,
                  const ImageLimits& limits, ProcessedImage* out,
                  std::string* err, MemoryLimiter* limiter = nullptr);

}  // namespace sparkpush
