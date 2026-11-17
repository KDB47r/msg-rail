#pragma once

#include <string>

namespace sparkpush {

// 生成对外 media_id：m_ + 26 位 Crockford Base32。
// 前 48 bit 是毫秒时间戳，后 80 bit 来自 /dev/urandom；不暴露数据库自增 ID。
std::string GenerateMediaId();

// 校验 m_ + 26 位 Crockford Base32（不含 I/L/O/U）。
bool IsValidMediaId(const std::string& media_id);

// 根据 MIME 返回对象存储扩展名；不认识的 MIME 返回空串。
std::string ExtensionForImageMime(const std::string& mime_type);

bool IsAllowedImageMime(const std::string& mime_type);

// 视频只支持 MP4 / WebM；不认识的 MIME 返回空串。
std::string ExtensionForVideoMime(const std::string& mime_type);

bool IsAllowedVideoMime(const std::string& mime_type);

}  // namespace sparkpush
