#pragma once

#include <cstdint>
#include <string>

namespace sparkpush {

// 上行消息中用于路由与控制决策的字段。
struct UpstreamMessageMeta {
    std::string type;
    int64_t to_user_id{0};
    int64_t group_id{0};
    std::string client_msg_id;
    std::string video_id;
    int64_t timeline_ms{0};
};

// 解析上行 JSON。兼容 group_id / room_id 以及字符串形式的整数；
// 至少包含 type 或一个有效路由字段时返回 true。
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta);

}  // namespace sparkpush

//
