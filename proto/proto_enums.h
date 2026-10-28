// SparkPush - proto 枚举 与 外部字符串 的双向映射（header-only）
//
// 背景：浏览器 WS/HTTP 的 JSON、以及 MySQL 的 message.msg_type 列都使用字符串，
// 属于系统的外部边界，取值不能变。内部 proto 使用强类型 enum。
// 本头文件仅在“字符串边界 <-> proto 内部枚举”转换处使用。
#pragma once

#include <string>

#include "spark_push.pb.h"

namespace sparkpush {

// ---------- Scene ----------
inline Scene SceneFromString(const std::string& s) {
    if (s == "single") return SCENE_SINGLE;
    if (s == "chatroom") return SCENE_CHATROOM;
    if (s == "danmaku") return SCENE_DANMAKU;
    return SCENE_UNSPECIFIED;
}

inline const char* SceneToString(Scene v) {
    switch (v) {
        case SCENE_SINGLE:
            return "single";
        case SCENE_CHATROOM:
            return "chatroom";
        case SCENE_DANMAKU:
            return "danmaku";
        default:
            return "";
    }
}

// ---------- MsgType ----------
inline MsgType MsgTypeFromString(const std::string& s) {
    if (s == "text") return MSG_TYPE_TEXT;
    if (s == "system") return MSG_TYPE_SYSTEM;
    if (s == "danmaku") return MSG_TYPE_DANMAKU;
    if (s == "broadcast") return MSG_TYPE_BROADCAST;
    return MSG_TYPE_UNSPECIFIED;
}

inline const char* MsgTypeToString(MsgType v) {
    switch (v) {
        case MSG_TYPE_TEXT:
            return "text";
        case MSG_TYPE_SYSTEM:
            return "system";
        case MSG_TYPE_DANMAKU:
            return "danmaku";
        case MSG_TYPE_BROADCAST:
            return "broadcast";
        default:
            return "";
    }
}

// ---------- BroadcastScope ----------
inline BroadcastScope BroadcastScopeFromString(const std::string& s) {
    if (s == "all") return BROADCAST_SCOPE_ALL;
    if (s == "group") return BROADCAST_SCOPE_GROUP;
    return BROADCAST_SCOPE_UNSPECIFIED;
}

inline const char* BroadcastScopeToString(BroadcastScope v) {
    switch (v) {
        case BROADCAST_SCOPE_ALL:
            return "all";
        case BROADCAST_SCOPE_GROUP:
            return "group";
        default:
            return "";
    }
}

}  // namespace sparkpush
