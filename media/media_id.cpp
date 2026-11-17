#include "media_id.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <random>

namespace sparkpush {

namespace {

// Crockford Base32：去掉容易混淆的 I、L、O、U。
constexpr char kCrockford[] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

bool IsCrockford(char c) {
    for (char x : kCrockford) {
        if (c == x) return true;
    }
    return false;
}

int BitAt(const std::array<uint8_t, 16>& bytes, int bit) {
    // bit=0 是最低位；数组按大端解释。
    const int byte_index = 15 - bit / 8;
    const int bit_index = bit % 8;
    return (bytes[static_cast<size_t>(byte_index)] >> bit_index) & 1;
}

}  // namespace

std::string GenerateMediaId() {
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now()
                                   .time_since_epoch())
                               .count();
    std::array<uint8_t, 16> bytes{};
    for (int i = 0; i < 6; ++i) {
        bytes[static_cast<size_t>(i)] = static_cast<uint8_t>(
            (static_cast<uint64_t>(now_ms) >> (8 * (5 - i))) & 0xFF);
    }

    std::ifstream urandom("/dev/urandom", std::ios::binary);
    if (urandom.good()) {
        urandom.read(reinterpret_cast<char*>(bytes.data() + 6), 10);
    } else {
        std::random_device rd;
        for (int i = 6; i < 16; ++i) {
            bytes[static_cast<size_t>(i)] = static_cast<uint8_t>(rd() & 0xFF);
        }
    }

    std::string id = "m_";
    id.reserve(28);
    for (int i = 25; i >= 0; --i) {
        int value = 0;
        for (int b = 0; b < 5; ++b) {
            value |= BitAt(bytes, i * 5 + b) << b;
        }
        id.push_back(kCrockford[value]);
    }
    return id;
}

bool IsValidMediaId(const std::string& media_id) {
    if (media_id.size() != 28) return false;
    if (media_id[0] != 'm' || media_id[1] != '_') return false;
    for (size_t i = 2; i < media_id.size(); ++i) {
        if (!IsCrockford(media_id[i])) return false;
    }
    return true;
}

std::string ExtensionForImageMime(const std::string& mime_type) {
    if (mime_type == "image/jpeg") return "jpg";
    if (mime_type == "image/png") return "png";
    if (mime_type == "image/webp") return "webp";
    return {};
}

bool IsAllowedImageMime(const std::string& mime_type) {
    return !ExtensionForImageMime(mime_type).empty();
}

std::string ExtensionForVideoMime(const std::string& mime_type) {
    if (mime_type == "video/mp4") return "mp4";
    if (mime_type == "video/webm") return "webm";
    return {};
}

bool IsAllowedVideoMime(const std::string& mime_type) {
    return !ExtensionForVideoMime(mime_type).empty();
}

}  // namespace sparkpush
