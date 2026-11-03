#include "websocket_utils.h"

#include <limits>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

namespace sparkpush {
namespace {

bool SafeGetInt64(const nlohmann::json& j, const std::string& key,
                  int64_t* out) {
    auto it = j.find(key);
    if (it == j.end()) return false;
    if (it->is_number_unsigned()) {
        uint64_t value = it->get<uint64_t>();
        if (value >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return false;
        }
        *out = static_cast<int64_t>(value);
        return true;
    }
    if (it->is_number_integer()) {
        *out = it->get<int64_t>();
        return true;
    }
    if (it->is_string()) {
        try {
            const std::string value = it->get<std::string>();
            size_t parsed = 0;
            long long number = std::stoll(value, &parsed);
            if (parsed != value.size()) return false;
            *out = static_cast<int64_t>(number);
            return true;
        } catch (const std::invalid_argument&) {
            return false;
        } catch (const std::out_of_range&) {
            return false;
        }
    }
    return false;
}

bool SafeGetString(const nlohmann::json& j, const std::string& key,
                   std::string* out) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return false;
    *out = it->get<std::string>();
    return true;
}

}  // namespace

bool ParseUpstreamMessage(const std::string& json_str,
                          UpstreamMessageMeta* meta) {
    if (!meta) return false;
    try {
        const auto j = nlohmann::json::parse(json_str);
        if (!j.is_object()) return false;

        UpstreamMessageMeta parsed;
        SafeGetString(j, "type", &parsed.type);
        SafeGetString(j, "client_msg_id", &parsed.client_msg_id);
        SafeGetInt64(j, "to_user_id", &parsed.to_user_id);
        if (!SafeGetInt64(j, "group_id", &parsed.group_id)) {
            SafeGetInt64(j, "room_id", &parsed.group_id);
        }
        SafeGetString(j, "video_id", &parsed.video_id);
        SafeGetInt64(j, "timeline_ms", &parsed.timeline_ms);

        if (parsed.type.empty() && parsed.to_user_id <= 0 &&
            parsed.group_id <= 0 && parsed.video_id.empty()) {
            return false;
        }
        *meta = std::move(parsed);
        return true;
    } catch (const nlohmann::json::parse_error&) {
        return false;
    }
}

}  // namespace sparkpush

// 从json解析字段 tpye字段存meta 不碰不解析msg
