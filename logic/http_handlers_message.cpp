// HttpApiServer 消息域 handler：发送 / 历史 / 已读 / 未读 / 单聊会话列表。
#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "http_server.h"
#include "http_server_internal.h"
#include "logging.h"
#include "media_cache.h"
#include "media_dao.h"
#include "media_id.h"
#include "media_read.h"
#include "model.h"
#include "proto_enums.h"
#include "s3_client.h"
#include "spark_push.pb.h"

namespace sparkpush {

using HttpRequest = ::sparkpush::net::HttpRequest;
using HttpResponse = ::sparkpush::net::HttpResponse;
using namespace detail;

namespace {

// 按 UTF-8 码点计数：一个中文字符算 1 个，而不是 3 个字节。
size_t Utf8CharCount(const std::string& value) {
    size_t count = 0;
    for (unsigned char c : value) {
        if ((c & 0xC0) != 0x80) ++count;
    }
    return count;
}

struct MediaBrief {
    int64_t owner_id{0};
    std::string kind;
    std::string status;
    std::string session_id;
    std::string thumb_url;
    int64_t thumb_expires_at{0};
    std::string original_url;
    int64_t original_expires_at{0};
    nlohmann::json data;
};

std::string ExtractMediaIdFromContent(const std::string& content_json) {
    if (content_json.empty()) return {};
    try {
        const auto root = nlohmann::json::parse(content_json);
        if (!root.is_object()) return {};
        if (root.contains("media_id") && root["media_id"].is_string()) {
            return root["media_id"].get<std::string>();
        }
        if (root.contains("content") && root["content"].is_object()) {
            const auto& inner = root["content"];
            if (inner.contains("media_id") && inner["media_id"].is_string()) {
                return inner["media_id"].get<std::string>();
            }
        }
    } catch (const nlohmann::json::exception&) {
    }
    return {};
}

// 一次批量加载媒体元数据，并预先签好变体/原图 URL。
// 调用方负责访问控制：history 只挂 session_id 匹配的媒体，
// 头像只使用 owner_id 匹配的 ready 图片。
bool LoadMediaBriefs(MediaDao* media_dao, MediaCache* media_cache, S3Client* s3,
                     const std::string& cdn_base_url,
                     const std::string& expected_session_id, int ttl_seconds,
                     const std::vector<std::string>& media_ids,
                     std::unordered_map<std::string, MediaBrief>* out,
                     std::string* err) {
    if (!media_dao || !out) {
        if (err) *err = "invalid arguments";
        return false;
    }
    out->clear();
    if (media_ids.empty()) return true;
    if (!s3 || !s3->ready()) {
        if (err) *err = "s3 client is not ready";
        return false;
    }

    std::unordered_map<std::string, CachedMedia> loaded;
    if (!LoadMediaManyWithVariants(media_dao, media_cache, media_ids, &loaded,
                                   err)) {
        return false;
    }

    // 历史消息的场景下，ready 媒体的 session_id 必须等于当前会话。
    // 空 session 可能是“绑定前回源、绑定后写回”的旧缓存，强制回源一次。
    if (!expected_session_id.empty()) {
        std::vector<std::string> stale_ids;
        for (const auto& item : loaded) {
            if (item.second.object.session_id.empty()) {
                stale_ids.push_back(item.first);
            }
        }
        if (!stale_ids.empty()) {
            std::unordered_map<std::string, CachedMedia> refreshed;
            std::string refresh_err;
            if (LoadMediaManyWithVariants(media_dao, media_cache, stale_ids,
                                          &refreshed, &refresh_err, true)) {
                for (auto& entry : refreshed) {
                    loaded[entry.first] = std::move(entry.second);
                }
            } else {
                LogWarn("refresh media cache for history failed: " +
                        refresh_err);
            }
        }
    }

    for (const auto& item : loaded) {
        const MediaObject& media = item.second.object;
        MediaBrief brief;
        brief.owner_id = media.owner_id;
        brief.kind = media.kind;
        brief.status = media.status;
        brief.session_id = media.session_id;

        nlohmann::json data;
        data["media_id"] = media.media_id;
        data["kind"] = media.kind;
        data["status"] = media.status;
        data["mime_type"] = media.mime_type;
        data["size_bytes"] = media.size_bytes;
        data["width"] = media.width;
        data["height"] = media.height;
        data["duration_ms"] = media.duration_ms;
        data["dominant_color"] = media.dominant_color;
        data["blur_hash"] = media.blur_hash;
        data["original_name"] = media.original_name;

        nlohmann::json variants_json = nlohmann::json::object();
        if (media.status == "ready") {
            for (const auto& variant : item.second.variants) {
                nlohmann::json entry;
                std::string variant_err;
                if (!BuildVariantJson(variant, cdn_base_url, s3, ttl_seconds,
                                      &entry, &variant_err)) {
                    continue;
                }
                variants_json[variant.variant] = entry;
                if (variant.variant == "thumb") {
                    brief.thumb_url = entry.value("url", "");
                    brief.thumb_expires_at = entry.value("expires_at", 0LL);
                }
            }

            int64_t original_expires_at = 0;
            const std::string original_url =
                media.kind == "file"
                    ? s3->PresignGetDownload(media.object_key, ttl_seconds,
                                             media.original_name,
                                             &original_expires_at)
                    : s3->PresignGet(media.object_key, ttl_seconds,
                                     &original_expires_at);
            if (!original_url.empty()) {
                brief.original_url = original_url;
                brief.original_expires_at = original_expires_at;
                data["original"] = {
                    {"url", original_url},
                    {"width", media.width},
                    {"height", media.height},
                    {"size_bytes", media.size_bytes},
                    {"mime_type", media.mime_type},
                    {"download_name", media.original_name},
                    {"expires_at", original_expires_at},
                };
            }
        }
        data["variants"] = std::move(variants_json);
        brief.data = std::move(data);
        out->emplace(media.media_id, std::move(brief));
    }
    return true;
}

}  // namespace

// HTTP 消息发送接口
// 优化前路径（5-6 次 MySQL）: HTTP → GetOrCreateSession(MySQL) →
// AllocateSeq(MySQL)
//                            → InsertMessage(MySQL) → GetUserById(MySQL) →
//                            Kafka
// 优化后路径（2 次 Redis）:   HTTP → 构造 session_id → IncrMsgSeq(Redis)
//                            → GetUserRoutes(Redis/缓存) → Kafka
// 优化效果：QPS 从 ~82 提升到 ~5710（70 倍），具体见
// performance_optimization.md
// HTTP 发单聊消息：分配序号、查路由并写 Kafka。
void HttpApiServer::handleSendMessage(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：from_user 由 token 决定，禁止客户端伪造
    int64_t from_user = AuthenticateRequest(req, resp);
    if (from_user <= 0) return;

    // 发送限流（已注释：保留代码供面试讲解，实际压测时关闭以减少 Redis 开销）
    // if (redis_store_) {
    //   std::string rate_key =
    //       "rate:msg:{" + std::to_string(from_user) + "}";
    //   if (!redis_store_->CheckRateLimit(rate_key, 1000, 20)) {
    //     WriteError(resp, 429, "rate limit exceeded, try again later");
    //     return;
    //   }
    // }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"to_user_id\":...,\"content\":\"...\"}");
        return;
    }

    int64_t to_user = 0;
    std::string msg_type = "text";
    std::string content;
    std::string media_id;
    std::string caption;
    nlohmann::json mentions;
    bool has_mentions = false;
    try {
        if (!j.contains("to_user_id")) {
            WriteError(resp, 400, "to_user_id required");
            return;
        }
        to_user = j.at("to_user_id").get<int64_t>();
        if (j.contains("msg_type") && j["msg_type"].is_string()) {
            msg_type = j["msg_type"].get<std::string>();
        }
        if (msg_type == "image" || msg_type == "file" ||
            msg_type == "video") {
            if (!j.contains("media_id") || !j["media_id"].is_string()) {
                WriteError(resp, 400,
                           "media_id required for image/video/file message");
                return;
            }
            media_id = j["media_id"].get<std::string>();
            if (j.contains("caption") && j["caption"].is_string()) {
                caption = j["caption"].get<std::string>();
            }
        } else if (msg_type == "text") {
            if (!j.contains("content") || !j["content"].is_string()) {
                WriteError(resp, 400, "content required for text message");
                return;
            }
            content = j["content"].get<std::string>();
        } else {
            WriteError(resp, 400, "unsupported msg_type: " + msg_type);
            return;
        }
        if (j.contains("mentions") && j["mentions"].is_array()) {
            mentions = j["mentions"];
            has_mentions = true;
        }
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in message send");
        return;
    }

    const bool is_image = (msg_type == "image");
    const bool is_video = (msg_type == "video");
    const bool is_file = (msg_type == "file");
    const bool is_media = is_image || is_video || is_file;
    if (to_user <= 0) {
        WriteError(resp, 400, "to_user_id required");
        return;
    }
    if (!is_media && content.empty()) {
        WriteError(resp, 400, "content required");
        return;
    }
    if (is_media && media_id.empty()) {
        WriteError(resp, 400, "media_id required");
        return;
    }
    if (Utf8CharCount(caption) > 500) {
        WriteError(resp, 400, "caption exceeds 500 characters");
        return;
    }

    if (ai_service_ && ai_service_->IsSharedBot(to_user)) {
        WriteError(resp, 400, "shared bot cannot be used as a 1:1 peer");
        return;
    }
    const bool private_bot_target =
        ai_service_ && ai_service_->IsPrivateBot(to_user);

    // 单聊只允许好友；私人 AI 助手是固定 bot 联系人，不需要好友关系。
    if (!private_bot_target) {
        if (!friend_service_) {
            WriteError(resp, 500, "friend service not initialized");
            return;
        }
        bool is_friend = false;
        std::string friend_err;
        if (!friend_service_->AreFriends(from_user, to_user, &is_friend,
                                         &friend_err)) {
            WriteError(resp, 500, "check friendship failed: " + friend_err);
            return;
        }
        if (!is_friend) {
            WriteError(resp, 403, "only friends can send single chat messages");
            return;
        }
    }

    if (!redis_store_ || !kafka_producer_) {
        WriteError(resp, 500, "redis or kafka not initialized");
        return;
    }

    // 构造 session_id（与 gRPC 路径一致，无 MySQL）
    // 统一 session_id 命名格式，与 gRPC 一致：single:{min_id}:{max_id}
    int64_t u1 = std::min(from_user, to_user);
    int64_t u2 = std::max(from_user, to_user);
    std::string session_id =
        "single:" + std::to_string(u1) + ":" + std::to_string(u2);

    // 媒体消息在分配 seq 之前先校验归属和状态，避免无效请求消耗序号。
    MediaObject media;
    if (is_media) {
        if (!media_dao_) {
            WriteError(resp, 500, "media dao not initialized");
            return;
        }
        if (!IsValidMediaId(media_id)) {
            WriteError(resp, 400, "invalid media_id");
            return;
        }
        std::string media_err;
        if (!media_dao_->GetById(media_id, &media, &media_err)) {
            WriteError(resp, 404, "media not found");
            return;
        }
        if (media.owner_id != from_user) {
            WriteError(resp, 403, "forbidden: not media owner");
            return;
        }
        if (media.kind != msg_type) {
            WriteError(resp, 400,
                       "media kind does not match msg_type: " + media.kind);
            return;
        }
        if (media.status != "ready") {
            WriteError(resp, 409,
                       "media is not ready, current status: " + media.status);
            return;
        }
        if (!media.session_id.empty() || !media.message_id.empty()) {
            WriteError(resp, 409, "media has already been used");
            return;
        }
    }

    // 由统一序号分配器获取 msg_seq；新 Redis key 首次使用时会从 MySQL
    // 恢复历史最大值，避免与已持久化消息冲突。
    int64_t msg_seq = 0;
    std::string seq_err;
    if (!store_ || !store_->AllocateMessageSeq(session_id, &msg_seq, &seq_err)) {
        LogError("AllocateMessageSeq failed for session " + session_id + ": " + seq_err);
        WriteError(resp, 500, "allocate msg_seq failed");
        return;
    }

    // 构造 msg_id
    std::string msg_id = session_id + "-" + std::to_string(msg_seq);

    // 先绑定媒体再投 Kafka：否则接收方可能先收到消息、后查询媒体，
    // 在绑定完成前看到 404。条件 UPDATE 同时保证一个媒体只能发一次。
    if (is_media) {
        bool bound = false;
        std::string bind_err;
        if (!media_dao_->BindToMessage(media.media_id, from_user, session_id,
                                       msg_id, &bound, &bind_err)) {
            LogError("BindToMessage failed for " + media.media_id + ": " +
                     bind_err);
            WriteError(resp, 500, "bind media to message failed");
            return;
        }
        if (!bound) {
            WriteError(resp, 409, "media has already been used");
            return;
        }
        // 绑定后 session_id 变化，读缓存必须失效，否则接收方可能读到旧的空 session。
        if (media_cache_) media_cache_->Invalidate(media.media_id);
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 构造推送 JSON
    nlohmann::json push_j;
    push_j["type"] = "single_chat";
    push_j["msg_id"] = msg_id;
    push_j["msg_seq"] = msg_seq;
    push_j["from_user_id"] = from_user;
    push_j["to_user_id"] = to_user;
    if (is_image) {
        // 图片消息的 content 是对象；width/height/dominant_color 由服务端
        // 从 media_object 注入，客户端不能伪造。
        nlohmann::json image_content;
        image_content["type"] = "image";
        image_content["media_id"] = media.media_id;
        image_content["width"] = media.width;
        image_content["height"] = media.height;
        image_content["dominant_color"] = media.dominant_color;
        image_content["blur_hash"] = media.blur_hash;
        image_content["mime_type"] = media.mime_type;
        image_content["size_bytes"] = media.size_bytes;
        image_content["caption"] = caption;
        push_j["content"] = std::move(image_content);
        push_j["msg_type"] = "image";
    } else if (is_video) {
        // 视频宽高/时长以服务端 ffprobe 结果为准，客户端不能伪造。
        nlohmann::json video_content;
        video_content["type"] = "video";
        video_content["media_id"] = media.media_id;
        video_content["width"] = media.width;
        video_content["height"] = media.height;
        video_content["duration_ms"] = media.duration_ms;
        video_content["mime_type"] = media.mime_type;
        video_content["size_bytes"] = media.size_bytes;
        video_content["caption"] = caption;
        push_j["content"] = std::move(video_content);
        push_j["msg_type"] = "video";
    } else if (is_file) {
        // 文件名/大小/MIME 均以服务端 media_object 为准，避免客户端伪造。
        nlohmann::json file_content;
        file_content["type"] = "file";
        file_content["media_id"] = media.media_id;
        file_content["name"] = media.original_name;
        file_content["mime_type"] = media.mime_type;
        file_content["size_bytes"] = media.size_bytes;
        file_content["caption"] = caption;
        push_j["content"] = std::move(file_content);
        push_j["msg_type"] = "file";
    } else {
        push_j["content"] = content;
        push_j["msg_type"] = "text";
        if (has_mentions) push_j["mentions"] = mentions;
    }
    push_j["create_time"] = now_ms;
    std::string push_json = push_j.dump();

    // 获取接收方路由
    std::vector<std::string> comets;
    bool has_route = redis_store_->GetUserRoutes(to_user, &comets);

    // 构造并发送 PushToCometRequest（带持久化标记）
    // 序列化/投递失败必须反馈给调用方，避免返回“假成功”
    bool deliver_ok = true;
    int delivered = 0;
    if (has_route && !comets.empty()) {
        for (const auto& comet_id : comets) {
            PushToCometRequest push_req;
            push_req.set_comet_id(comet_id);
            push_req.set_scene(SCENE_SINGLE);
            push_req.set_to_user_id(to_user);
            push_req.set_need_persist(true);

            ChatMessage* cm = push_req.mutable_message();
            cm->set_msg_id(msg_id);
            cm->set_session_id(session_id);
            cm->set_msg_seq(msg_seq);
            cm->set_sender_id(from_user);
            cm->set_timestamp_ms(now_ms);
            cm->set_msg_type(MsgTypeFromString(msg_type));
            cm->set_content_json(push_json);

            auto* target = push_req.add_targets();
            target->set_user_id(to_user);

            std::string payload;
            if (!push_req.SerializeToString(&payload)) {
                LogError("Serialize PushToCometRequest failed for comet " +
                         comet_id);
                deliver_ok = false;
            } else {
                // protobuf 对象序列化成字节串，这个字节串就是 Kafka 的 valu
                const std::string kafka_key = session_id + "|" + comet_id;
                // ***
                if (!kafka_producer_->Send(kafka_key, payload)) {
                    LogError("Kafka send message failed for comet " +
                             comet_id);
                    deliver_ok = false;
                } else {
                    ++delivered;
                }
            }
        }
    } else {
        // 无路由，仅持久化：按 session_id 分区，避免固定 key 形成热点
        PushToCometRequest push_req;
        push_req.set_persist_only(true);
        push_req.set_scene(SCENE_SINGLE);
        push_req.set_to_user_id(to_user);
        push_req.set_need_persist(true);

        ChatMessage* cm = push_req.mutable_message();
        cm->set_msg_id(msg_id);
        cm->set_session_id(session_id);
        cm->set_msg_seq(msg_seq);
        cm->set_sender_id(from_user);
        cm->set_timestamp_ms(now_ms);
        cm->set_msg_type(MsgTypeFromString(msg_type));
        cm->set_content_json(push_json);

        auto* target = push_req.add_targets();
        target->set_user_id(to_user);

        std::string payload;
        if (!push_req.SerializeToString(&payload)) {
            LogError("Serialize PushToCometRequest(persist_only) failed");
            deliver_ok = false;
        } else if (!kafka_producer_->Send(session_id, payload)) {
            LogError("Kafka send message failed for persist_only, session " +
                     session_id);
            deliver_ok = false;
        } else {
            ++delivered;
        }
    }

    if (!deliver_ok) {
        // 全部投递失败才撤销绑定，允许客户端用同一 media_id 重试。
        // 只要有一个 Comet 成功，接收方就可能已经看到消息，此时保留绑定。
        if (is_media && delivered == 0 && media_dao_) {
            std::string unbind_err;
            if (!media_dao_->UnbindMessage(media.media_id, from_user, msg_id,
                                           &unbind_err)) {
                LogError("UnbindMessage failed for " + media.media_id + ": " +
                         unbind_err);
            } else if (media_cache_) {
                media_cache_->Invalidate(media.media_id);
            }
        }
        WriteError(resp, 500, "send message to kafka failed");
        return;
    }

    // 用户消息已经进入发送链路，异步触发 AI。失败只记录，不回滚用户消息。
    if (!is_media && ai_service_) {
        if (private_bot_target) {
            if (!ai_service_->TriggerPrivate(session_id, from_user, to_user,
                                             msg_id, push_json)) {
                LogError("AI private trigger failed session=" + session_id);
            }
        } else if (msg_type == "text") {
            if (!ai_service_->MaybeTriggerShared(session_id, from_user, to_user,
                                                 msg_id, push_json)) {
                LogError("AI shared trigger failed session=" + session_id);
            }
        }
    }

    nlohmann::json data{
        {"session_id", session_id}, {"msg_id", msg_id}, {"msg_seq", msg_seq}};
    if (is_media) {
        data["media_id"] = media.media_id;
        data["msg_type"] = msg_type;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// POST /api/message/forward
// 单聊消息转发：校验源会话权限，复制源消息内容；媒体消息复用对象和变体，
// 不重新上传/解码，只创建新的 media_object 和 media_variant。
void HttpApiServer::handleForwardMessage(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    const int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!store_ || !media_dao_) {
        WriteError(resp, 500, "conversation store or media dao not initialized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"source_session_id\":\"...\",\"source_msg_seq\":1,"
                   "\"to_user_id\":2}");
        return;
    }

    std::string source_session_id;
    int64_t source_msg_seq = 0;
    int64_t to_user = 0;
    try {
        if (!j.contains("source_session_id") ||
            !j["source_session_id"].is_string() ||
            !j.contains("source_msg_seq") ||
            !j["source_msg_seq"].is_number_integer() ||
            !j.contains("to_user_id") ||
            !j["to_user_id"].is_number_integer()) {
            WriteError(resp, 400,
                       "source_session_id/source_msg_seq/to_user_id required");
            return;
        }
        source_session_id = j["source_session_id"].get<std::string>();
        source_msg_seq = j["source_msg_seq"].get<int64_t>();
        to_user = j["to_user_id"].get<int64_t>();
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 400, "invalid json fields in message forward");
        return;
    }

    if (source_session_id.rfind("single:", 0) != 0) {
        WriteError(resp, 400, "only single chat messages can be forwarded");
        return;
    }
    if (source_msg_seq <= 0 || to_user <= 0) {
        WriteError(resp, 400, "source_msg_seq/to_user_id must be positive");
        return;
    }
    if (!CheckSessionAccess(user_id, source_session_id, resp)) return;

    Message source;
    std::string err;
    if (!store_->GetMessageBySeq(source_session_id, source_msg_seq, &source,
                                 &err)) {
        WriteError(resp, 404, "source message not found");
        return;
    }
    if (source.msg_type != "text" && source.msg_type != "image" &&
        source.msg_type != "video" && source.msg_type != "file") {
        WriteError(resp, 400, "unsupported source message type");
        return;
    }

    nlohmann::json source_root;
    try {
        source_root = nlohmann::json::parse(source.content_json);
    } catch (const nlohmann::json::exception&) {
        WriteError(resp, 500, "source message content is invalid");
        return;
    }
    const nlohmann::json* source_content = nullptr;
    auto content_it = source_root.find("content");
    if (content_it != source_root.end()) source_content = &(*content_it);
    if (source_content && source_content->is_object() &&
        source_content->contains("content") &&
        (*source_content)["content"].is_object()) {
        source_content = &(*source_content)["content"];
    }

    std::string caption;
    std::string new_media_id;
    bool clone_created = false;

    if (source.msg_type == "text") {
        if (!source_content || !source_content->is_string()) {
            WriteError(resp, 400, "source text content is invalid");
            return;
        }
    } else {
        const std::string source_media_id =
            ExtractMediaIdFromContent(source.content_json);
        if (source_media_id.empty()) {
            WriteError(resp, 400, "source message has no media");
            return;
        }
        MediaObject media;
        if (!media_dao_->GetById(source_media_id, &media, &err)) {
            WriteError(resp, 404, "source media not found");
            return;
        }
        if (media.session_id != source_session_id ||
            media.message_id != source.msg_id) {
            WriteError(resp, 409, "source media does not belong to message");
            return;
        }
        if (media.kind != source.msg_type) {
            WriteError(resp, 400, "source media kind mismatch");
            return;
        }
        if (media.status != "ready") {
            WriteError(resp, 409, "source media is not ready");
            return;
        }
        std::vector<MediaVariant> variants;
        if (!media_dao_->ListVariants(media.media_id, &variants, &err)) {
            LogError("ListVariants failed for forward " + media.media_id +
                     ": " + err);
            WriteError(resp, 500, "load source media variants failed");
            return;
        }
        if (source_content && source_content->is_object() &&
            source_content->contains("caption") &&
            (*source_content)["caption"].is_string()) {
            caption = (*source_content)["caption"].get<std::string>();
        }

        int64_t asset_id = 0;
        bool has_asset = false;
        if (!media_dao_->GetAssetIdForMedia(media.media_id, &asset_id,
                                            &has_asset, &err)) {
            LogError("GetAssetIdForMedia failed for " + media.media_id + ": " +
                     err);
            WriteError(resp, 500, "load source media asset failed");
            return;
        }
        new_media_id = GenerateMediaId();
        if (!media_dao_->CreateClone(media, new_media_id, user_id, variants,
                                     has_asset ? asset_id : 0, &err)) {
            LogError("CreateClone failed for " + media.media_id + ": " + err);
            WriteError(resp, 500, "clone source media failed");
            return;
        }
        clone_created = true;
    }

    // 构造一个等价于普通 /api/message/send 的请求，复用发送核心：
    // 好友校验、分配 seq、绑定媒体、Kafka 投递、失败回滚都在原逻辑里。
    nlohmann::json send_j;
    send_j["to_user_id"] = to_user;
    send_j["msg_type"] = source.msg_type;
    if (source.msg_type == "text") {
        send_j["content"] = *source_content;
    } else {
        send_j["media_id"] = new_media_id;
        if (!caption.empty()) send_j["caption"] = caption;
    }
    HttpRequest send_req = req;
    send_req.set_body(send_j.dump());
    handleSendMessage(send_req, resp);

    if (clone_created && resp->status_code() != 200) {
        std::string delete_err;
        if (!media_dao_->DeleteClone(new_media_id, user_id, &delete_err)) {
            LogWarn("DeleteClone after forward failure failed for " +
                    new_media_id + ": " + delete_err);
        }
    }
}

// read
// 查询会话历史消息。
void HttpApiServer::handleHistory(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：确保请求来自已登录用户
    int64_t auth_user_id = AuthenticateRequest(req, resp);
    if (auth_user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"session_id\":\"...\",\"anchor_seq\":0,\"limit\":20}");
        return;
    }

    std::string session_id;
    int64_t anchor_seq = 0;
    int limit = 20;
    try {
        if (!j.contains("session_id")) {
            WriteError(resp, 400, "session_id required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        if (j.contains("anchor_seq")) {
            anchor_seq = j.at("anchor_seq").get<int64_t>();
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in history");
        return;
    }
    if (session_id.empty()) {
        WriteError(resp, 400, "session_id required");
        return;
    }
    // 会话归属校验：禁止读取自己不属于的会话历史（IDOR）
    if (!CheckSessionAccess(auth_user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Message> msgs;
    std::string err;
    if (!store_->GetHistory(session_id, anchor_seq, limit, &msgs, &err)) {
        WriteError(resp, 500, "query history failed: " + err);
        return;
    }
    // 历史消息统一展示“当前昵称/头像”：批量查发送者，避免逐条查库。
    std::vector<int64_t> sender_ids;
    std::unordered_set<int64_t> seen_senders;
    for (const auto& m : msgs) {
        if (m.sender_id > 0 && seen_senders.insert(m.sender_id).second) {
            sender_ids.push_back(m.sender_id);
        }
    }
    std::unordered_map<int64_t, UserBrief> sender_views;
    if (!sender_ids.empty() && user_dao_) {
        std::vector<User> senders;
        std::string user_err;
        if (!user_dao_->ListUsersByIds(sender_ids, &senders, &user_err)) {
            WriteError(resp, 500, "load message senders failed: " + user_err);
            return;
        }
        for (const auto& sender : senders) {
            UserBrief brief;
            BuildUserBrief(media_dao_, media_cache_, s3_client_,
                           media_variant_cdn_base_url_,
                           media_download_url_ttl_seconds_, sender, &brief);
            sender_views[sender.id] = std::move(brief);
        }
    }

    // 历史消息里的媒体元数据一次批量查询，避免前端逐张
    // GET /api/media/{id}。失败时退回旧行为，前端仍有单张 fallback。
    std::vector<std::string> msg_media_ids(msgs.size());
    std::vector<std::string> unique_media_ids;
    std::unordered_set<std::string> seen_media_ids;
    for (size_t i = 0; i < msgs.size(); ++i) {
        if (msgs[i].msg_type != "image" && msgs[i].msg_type != "video" &&
            msgs[i].msg_type != "file") {
            continue;
        }
        const std::string media_id =
            ExtractMediaIdFromContent(msgs[i].content_json);
        if (media_id.empty()) continue;
        msg_media_ids[i] = media_id;
        if (seen_media_ids.insert(media_id).second) {
            unique_media_ids.push_back(media_id);
        }
    }
    std::unordered_map<std::string, MediaBrief> media_briefs;
    if (!unique_media_ids.empty()) {
        std::string media_err;
        if (!LoadMediaBriefs(media_dao_, media_cache_, s3_client_,
                             media_variant_cdn_base_url_, session_id,
                             media_download_url_ttl_seconds_,
                             unique_media_ids, &media_briefs, &media_err)) {
            LogWarn("batch load media briefs failed: " + media_err);
            media_briefs.clear();
        }
    }

    nlohmann::json data;
    data["messages"] = nlohmann::json::array();
    for (size_t idx = 0; idx < msgs.size(); ++idx) {
        const auto& m = msgs[idx];
        nlohmann::json entry;
        entry["msg_id"] = m.msg_id;
        entry["client_msg_id"] = m.client_msg_id;
        entry["msg_seq"] = m.msg_seq;
        entry["sender_id"] = m.sender_id;
        entry["timestamp_ms"] = m.timestamp_ms;
        entry["msg_type"] = m.msg_type;
        auto sender_it = sender_views.find(m.sender_id);
        if (sender_it != sender_views.end()) {
            entry["sender"] = UserBriefToJson(sender_it->second, false);
        } else {
            // 用户被删除等异常数据：保留可读占位，不让整页历史失败。
            UserBrief fallback;
            fallback.user_id = m.sender_id;
            fallback.name = "User " + std::to_string(m.sender_id);
            entry["sender"] = UserBriefToJson(fallback, false);
        }
        // content_json 本身是一段 JSON
        // 文本，解析后作为嵌套对象输出（与原格式一致）；
        // 解析失败时降级为字符串，保证响应始终是合法 JSON
        if (m.content_json.empty()) {
            entry["content"] = "";
        } else {
            try {
                entry["content"] = nlohmann::json::parse(m.content_json);
            } catch (...) {
                entry["content"] = m.content_json;
            }
        }
        if (!msg_media_ids[idx].empty()) {
            const auto media_it = media_briefs.find(msg_media_ids[idx]);
            if (media_it != media_briefs.end() &&
                media_it->second.session_id == session_id &&
                media_it->second.status != "deleted") {
                entry["media"] = media_it->second.data;
            }
        }
        data["messages"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// read
// 把会话标记为已读到指定序号。
void HttpApiServer::handleMarkRead(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect "
                   "{\"session_id\":\"...\",\"read_seq\":...}");
        return;
    }

    std::string session_id;
    int64_t read_seq = 0;
    try {
        if (!j.contains("session_id") || !j.contains("read_seq")) {
            WriteError(resp, 400, "session_id/read_seq required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        read_seq = j.at("read_seq").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in mark_read");
        return;
    }
    if (session_id.empty() || read_seq <= 0) {
        WriteError(resp, 400, "session_id/read_seq required");
        return;
    }
    // 会话归属校验：禁止操作自己不属于的会话（IDOR）
    if (!CheckSessionAccess(user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::string err;
    int64_t last_seq = 0;
    if (!store_->GetLastSeq(session_id, &last_seq, &err)) {
        if (err == "session not found") {
            // 会话尚未创建（从未发过消息），没有可标记的已读位置。
            WriteJson(resp, 0, "ok");
            return;
        }
        WriteError(resp, 500, "get session last seq failed: " + err);
        return;
    }
    // 防止客户端传入超过会话最新序号的游标，导致长期 0 未读。
    if (read_seq > last_seq) read_seq = last_seq;
    if (read_seq <= 0) {
        WriteJson(resp, 0, "ok");
        return;
    }
    if (!store_->MarkRead(user_id, session_id, read_seq, &err)) {
        WriteError(resp, 500, "mark read failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// raed
// 查询会话未读消息数。
void HttpApiServer::handleUnread(const HttpRequest& req, HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteError(resp, 400,
                   "invalid json body, expect {\"session_id\":\"...\"}");
        return;
    }

    std::string session_id;
    try {
        if (!j.contains("session_id")) {
            WriteError(resp, 400, "session_id required");
            return;
        }
        session_id = j.at("session_id").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteError(resp, 400, "invalid json fields in unread");
        return;
    }
    if (session_id.empty()) {
        WriteError(resp, 400, "session_id required");
        return;
    }
    // 会话归属校验：禁止查询自己不属于的会话未读数（IDOR）
    if (!CheckSessionAccess(user_id, session_id, resp)) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    int64_t unread = 0;
    std::string err;
    if (!store_->GetUnread(user_id, session_id, &unread, &err)) {
        WriteError(resp, 500, "get unread failed: " + err);
        return;
    }
    int64_t last_seq = 0;
    if (!store_->GetLastSeq(session_id, &last_seq, &err)) {
        if (err != "session not found") {
            WriteError(resp, 500, "get session last seq failed: " + err);
            return;
        }
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"unread", unread},
                             {"last_msg_seq", last_seq}}
                  .dump());
}

// read
// 列出用户的单聊会话。
void HttpApiServer::handleSingleSessionList(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;

    // 统一鉴权：user_id 由 token 决定
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!store_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Session> sessions;
    std::string err;
    if (!store_->ListUserSingleSessions(user_id, &sessions, &err)) {
        WriteError(resp, 500, "list sessions failed: " + err);
        return;
    }
    // 最近有消息的会话优先展示。
    std::sort(sessions.begin(), sessions.end(),
              [](const Session& a, const Session& b) {
                  return a.last_msg_seq > b.last_msg_seq;
              });

    std::vector<int64_t> peer_ids;
    std::unordered_set<int64_t> seen_peers;
    for (const auto& s : sessions) {
        int64_t peer_id = (s.user1_id == user_id) ? s.user2_id : s.user1_id;
        if (peer_id > 0 && seen_peers.insert(peer_id).second) {
            peer_ids.push_back(peer_id);
        }
    }
    std::vector<User> peers;
    if (!peer_ids.empty()) {
        if (!user_dao_) {
            WriteError(resp, 500, "user dao not initialized");
            return;
        }
        if (!user_dao_->ListUsersByIds(peer_ids, &peers, &err)) {
            WriteError(resp, 500, "load session peers failed: " + err);
            return;
        }
    }
    std::unordered_map<int64_t, User> peer_map;
    for (auto& peer : peers) peer_map[peer.id] = std::move(peer);

    // 头像 media 一次批量加载；加载失败时回退旧的逐头像查询。
    std::vector<std::string> avatar_media_ids;
    std::unordered_set<std::string> seen_avatar_ids;
    // peers 已经被 move 进 peer_map，这里必须从 peer_map 读取。
    for (const auto& entry : peer_map) {
        const User& peer = entry.second;
        if (!peer.avatar_media_id.empty() &&
            seen_avatar_ids.insert(peer.avatar_media_id).second) {
            avatar_media_ids.push_back(peer.avatar_media_id);
        }
    }
    std::unordered_map<std::string, MediaBrief> avatar_briefs;
    bool avatar_batch_ok = true;
    if (!avatar_media_ids.empty()) {
        std::string avatar_err;
        if (!LoadMediaBriefs(media_dao_, media_cache_, s3_client_,
                             media_variant_cdn_base_url_, "",
                             media_download_url_ttl_seconds_,
                             avatar_media_ids, &avatar_briefs, &avatar_err)) {
            LogWarn("batch load avatar media failed: " + avatar_err);
            avatar_batch_ok = false;
        }
    }

    nlohmann::json data;
    data["sessions"] = nlohmann::json::array();
    for (const auto& s : sessions) {
        int64_t peer_id = (s.user1_id == user_id) ? s.user2_id : s.user1_id;
        if (peer_id <= 0) continue;
        int64_t unread = 0;
        if (!store_->GetUnread(user_id, s.id, &unread, &err)) {
            WriteError(resp, 500, "get unread failed: " + err);
            return;
        }
        UserBrief brief;
        auto peer_it = peer_map.find(peer_id);
        if (peer_it != peer_map.end()) {
            const User& peer = peer_it->second;
            brief.user_id = peer.id;
            brief.account = peer.account;
            brief.name = peer.name;
            brief.avatar_media_id = peer.avatar_media_id;
            if (!peer.avatar_media_id.empty()) {
                bool filled = false;
                if (avatar_batch_ok) {
                    const auto avatar_it =
                        avatar_briefs.find(peer.avatar_media_id);
                    if (avatar_it != avatar_briefs.end() &&
                        avatar_it->second.owner_id == peer.id &&
                        avatar_it->second.kind == "image" &&
                        avatar_it->second.status == "ready") {
                        if (!avatar_it->second.thumb_url.empty()) {
                            brief.avatar_url = avatar_it->second.thumb_url;
                            brief.avatar_expires_at =
                                avatar_it->second.thumb_expires_at;
                            filled = true;
                        } else if (!avatar_it->second.original_url.empty()) {
                            brief.avatar_url =
                                avatar_it->second.original_url;
                            brief.avatar_expires_at =
                                avatar_it->second.original_expires_at;
                            filled = true;
                        }
                    }
                }
                if (!filled && !avatar_batch_ok) {
                    BuildUserBrief(media_dao_, media_cache_, s3_client_,
                                   media_variant_cdn_base_url_,
                                   media_download_url_ttl_seconds_, peer,
                                   &brief);
                }
            }
        } else {
            brief.user_id = peer_id;
            brief.name = "User " + std::to_string(peer_id);
        }
        nlohmann::json entry;
        entry["session_id"] = s.id;
        entry["peer_user_id"] = peer_id;
        entry["last_msg_seq"] = s.last_msg_seq;
        entry["unread"] = unread;
        entry["peer_name"] = brief.name;
        entry["peer_avatar_media_id"] = brief.avatar_media_id;
        entry["peer_avatar_url"] = brief.avatar_url;
        entry["peer_avatar_expires_at"] = brief.avatar_expires_at;
        data["sessions"].push_back(std::move(entry));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 全局未读：单聊 + 已加入聊天室。只读，不创建任何 session。
void HttpApiServer::handleUnreadSummary(const HttpRequest& req,
                                        HttpResponse* resp) {
    if (!RequirePost(req, resp)) return;
    int64_t user_id = AuthenticateRequest(req, resp);
    if (user_id <= 0) return;
    if (!store_ || !group_member_dao_) {
        WriteError(resp, 500, "conversation store not initialized");
        return;
    }

    std::string err;
    int64_t total = 0;
    nlohmann::json data;
    data["sessions"] = nlohmann::json::array();

    std::vector<Session> singles;
    if (!store_->ListUserSingleSessions(user_id, &singles, &err)) {
        WriteError(resp, 500, "list single sessions failed: " + err);
        return;
    }
    for (const auto& session : singles) {
        int64_t unread = 0;
        if (!store_->GetUnread(user_id, session.id, &unread, &err)) {
            WriteError(resp, 500, "get single unread failed: " + err);
            return;
        }
        total += unread;
        data["sessions"].push_back(
            nlohmann::json{{"session_id", session.id},
                           {"type", 0},
                           {"unread", unread},
                           {"last_msg_seq", session.last_msg_seq}});
    }

    std::vector<int64_t> room_ids;
    if (!group_member_dao_->ListUserChatrooms(user_id, &room_ids, &err)) {
        WriteError(resp, 500, "list user chatrooms failed: " + err);
        return;
    }
    for (int64_t room_id : room_ids) {
        std::string session_id = "room:" + std::to_string(room_id);
        Session session;
        if (!store_->GetSessionById(session_id, &session, &err)) {
            // 从未产生过消息的房间没有 session 行，自然没有未读。
            if (err == "session not found") continue;
            WriteError(resp, 500, "get room session failed: " + err);
            return;
        }
        int64_t unread = 0;
        if (!store_->GetUnread(user_id, session_id, &unread, &err)) {
            WriteError(resp, 500, "get room unread failed: " + err);
            return;
        }
        total += unread;
        data["sessions"].push_back(
            nlohmann::json{{"session_id", session_id},
                           {"type", 2},
                           {"room_id", room_id},
                           {"unread", unread},
                           {"last_msg_seq", session.last_msg_seq}});
    }

    data["total"] = total;
    WriteJson(resp, 0, "ok", data.dump());
}

}  // namespace sparkpush
