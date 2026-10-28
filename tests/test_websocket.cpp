// WebSocket 上行业务消息解析单元测试。
// RFC6455 握手、mask 与帧长边界由 websocketpp 负责，并在集成 Gate 中验证。
#include <cstdint>
#include <limits>
#include <string>

#include "doctest.h"
#include "websocket_utils.h"

using namespace sparkpush;

TEST_CASE("ParseUpstreamMessage 解析单聊") {
    UpstreamMessageMeta meta;
    CHECK(ParseUpstreamMessage(
        R"({"type":"single_chat","to_user_id":123,"client_msg_id":"c1"})",
        &meta));
    CHECK(meta.type == "single_chat");
    CHECK(meta.to_user_id == 123);
    CHECK(meta.client_msg_id == "c1");
}

TEST_CASE("ParseUpstreamMessage 解析聊天室与 room_id 兼容") {
    UpstreamMessageMeta with_group;
    CHECK(ParseUpstreamMessage(
        R"({"type":"chatroom","group_id":456,"room_id":789})", &with_group));
    CHECK(with_group.group_id == 456);

    UpstreamMessageMeta with_room;
    CHECK(ParseUpstreamMessage(R"({"type":"chatroom_leave","room_id":789})",
                               &with_room));
    CHECK(with_room.group_id == 789);
}

TEST_CASE("ParseUpstreamMessage 解析弹幕字段") {
    UpstreamMessageMeta meta;
    CHECK(ParseUpstreamMessage(
        R"({"type":"danmaku","video_id":"vid_888","group_id":456,"timeline_ms":12345})",
        &meta));
    CHECK(meta.type == "danmaku");
    CHECK(meta.video_id == "vid_888");
    CHECK(meta.group_id == 456);
    CHECK(meta.timeline_ms == 12345);
}

TEST_CASE("ParseUpstreamMessage 兼容字符串形式整数") {
    UpstreamMessageMeta meta;
    CHECK(ParseUpstreamMessage(
        R"({"type":"single_chat","to_user_id":"321","timeline_ms":"42"})",
        &meta));
    CHECK(meta.to_user_id == 321);
    CHECK(meta.timeline_ms == 42);
}

TEST_CASE("ParseUpstreamMessage 允许 type 或 video_id 单独作为有效字段") {
    UpstreamMessageMeta only_type;
    CHECK(ParseUpstreamMessage(R"({"type":"chatroom_join"})", &only_type));
    CHECK(only_type.type == "chatroom_join");

    UpstreamMessageMeta only_video;
    CHECK(ParseUpstreamMessage(R"({"video_id":"vid_1"})", &only_video));
    CHECK(only_video.video_id == "vid_1");
}

TEST_CASE("ParseUpstreamMessage 拒绝非法或无路由 JSON") {
    UpstreamMessageMeta meta;
    CHECK_FALSE(ParseUpstreamMessage("not a json", &meta));
    CHECK_FALSE(ParseUpstreamMessage("{}", &meta));
    CHECK_FALSE(ParseUpstreamMessage("[]", &meta));
    CHECK_FALSE(ParseUpstreamMessage("null", &meta));
    CHECK_FALSE(ParseUpstreamMessage("{}", nullptr));
}

TEST_CASE("ParseUpstreamMessage 拒绝脏数字字符串并保留其他有效字段") {
    UpstreamMessageMeta meta;
    CHECK(ParseUpstreamMessage(
        R"({"type":"single_chat","to_user_id":"123abc"})", &meta));
    CHECK(meta.type == "single_chat");
    CHECK(meta.to_user_id == 0);
}

TEST_CASE("ParseUpstreamMessage 防止无符号整数溢出") {
    UpstreamMessageMeta meta;
    const std::string json =
        R"({"type":"single_chat","to_user_id":18446744073709551615})";
    CHECK(ParseUpstreamMessage(json, &meta));
    CHECK(meta.type == "single_chat");
    CHECK(meta.to_user_id == 0);
}
