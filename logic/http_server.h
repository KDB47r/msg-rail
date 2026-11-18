#pragma once

#include <memory>

#include "config.h"
#include "conversation_store.h"
#include "danmaku_dao.h"
#include "friend_service.h"
#include "group_dao.h"
#include "kafka_producer.h"
#include "net/http_server_lite.h"
#include "redis_store.h"
#include "thread_pool.h"
#include "user_dao.h"

namespace sparkpush {

class MediaDao;
class MediaCache;
class S3Client;

class HttpApiServer {
   public:
    using HttpRequest = ::sparkpush::net::HttpRequest;
    using HttpResponse = ::sparkpush::net::HttpResponse;

    HttpApiServer(uint16_t port, ConversationStore* store, UserDao* user_dao,
                  FriendService* friend_service, GroupDao* group_dao,
                  GroupMemberDao* group_member_dao, RedisStore* redis_store,
                  KafkaProducer* kafka_producer,
                  KafkaProducer* broadcast_producer, DanmakuDao* danmaku_dao,
                  MediaDao* media_dao, S3Client* s3_client,
                  MediaCache* media_cache, const Config& config);
    ~HttpApiServer();

    void setThreadNum(int numThreads) { server_.setThreadNum(numThreads); }
    void run() { server_.run(); }
    void stop() { server_.stop(); }

   private:
    void onRequest(const HttpRequest& req, HttpResponse* resp);
    // 异步入口：IO 线程只复制请求并投递，真正的 handler 在业务线程池执行。
    void onRequestAsync(const HttpRequest& req, HttpResponse* resp,
                        std::function<void()> done);

    // 账号体系：登录 / 注册
    void handleLogin(const HttpRequest& req, HttpResponse* resp);
    void handleRegister(const HttpRequest& req, HttpResponse* resp);

    void handleSendMessage(const HttpRequest& req, HttpResponse* resp);
    void handleHistory(const HttpRequest& req, HttpResponse* resp);
    void handleMarkRead(const HttpRequest& req, HttpResponse* resp);
    void handleUnread(const HttpRequest& req, HttpResponse* resp);

    // 用户资料 / 昵称搜索 / 好友
    void handleUserProfileGet(const HttpRequest& req, HttpResponse* resp);
    void handleUserProfileUpdate(const HttpRequest& req, HttpResponse* resp);
    void handleUserSearch(const HttpRequest& req, HttpResponse* resp);
    void handleFriendRequest(const HttpRequest& req, HttpResponse* resp);
    void handleFriendRespond(const HttpRequest& req, HttpResponse* resp);
    void handleFriendList(const HttpRequest& req, HttpResponse* resp);
    void handleFriendRequests(const HttpRequest& req, HttpResponse* resp);
    void handleFriendPendingCount(const HttpRequest& req, HttpResponse* resp);

    // 会话 / 房间列表
    void handleSingleSessionList(const HttpRequest& req, HttpResponse* resp);
    void handleOpenSingleSession(const HttpRequest& req, HttpResponse* resp);
    void handleUnreadSummary(const HttpRequest& req, HttpResponse* resp);
    void handleChatroomList(const HttpRequest& req, HttpResponse* resp);
    void handleChatroomJoin(const HttpRequest& req, HttpResponse* resp);
    void handleChatroomLeave(const HttpRequest& req, HttpResponse* resp);
    void handleChatroomUnsubscribe(const HttpRequest& req, HttpResponse* resp);
    void handleChatroomOnlineCount(const HttpRequest& req, HttpResponse* resp);

    // 媒体：初始化直传 / 确认上传 / 查询元数据与签名 URL
    void handleMediaUploadInit(const HttpRequest& req, HttpResponse* resp);
    void handleMediaUploadComplete(const HttpRequest& req, HttpResponse* resp);
    void handleMediaMultipartPartUrls(const HttpRequest& req,
                                      HttpResponse* resp);
    void handleMediaMultipartStatus(const HttpRequest& req,
                                    HttpResponse* resp);
    void handleMediaUploadAbort(const HttpRequest& req, HttpResponse* resp);
    void handleMediaGet(const HttpRequest& req, HttpResponse* resp);

    // 弹幕
    void handleDanmakuSend(const HttpRequest& req, HttpResponse* resp);
    void handleDanmakuList(const HttpRequest& req, HttpResponse* resp);

    // 管理后台
    void handleAdminCreateChatroom(const HttpRequest& req, HttpResponse* resp);
    void handleAdminListChatroom(const HttpRequest& req, HttpResponse* resp);
    void handleAdminBroadcast(const HttpRequest& req, HttpResponse* resp);

    // ---- handler 通用样板辅助 ----
    void WriteError(HttpResponse* resp, int code, const std::string& message,
                    const std::string& data_json = "{}");
    bool RequirePost(const HttpRequest& req, HttpResponse* resp);
    bool RequireAdmin(int64_t uid, HttpResponse* resp);

    // 从 Authorization: Bearer <token> 头或 JSON body 的 token 字段提取并校验，
    // 失败自动写 401 响应并返回 0。
    int64_t AuthenticateRequest(const HttpRequest& req, HttpResponse* resp);

    // 会话归属校验（防止越权 IDOR）。失败自动写 403/400/500。
    bool CheckSessionAccess(int64_t user_id, const std::string& session_id,
                            HttpResponse* resp);

    ConversationStore* store_;
    UserDao* user_dao_;
    FriendService* friend_service_;
    GroupDao* group_dao_;
    GroupMemberDao* group_member_dao_;
    RedisStore* redis_store_;
    KafkaProducer* kafka_producer_;
    KafkaProducer* broadcast_producer_;
    DanmakuDao* danmaku_dao_;
    MediaDao* media_dao_;
    S3Client* s3_client_;
    MediaCache* media_cache_;

    std::string admin_account_;
    std::string admin_password_hash_;
    int64_t admin_user_id_;
    int64_t media_max_image_bytes_;
    int64_t media_max_video_bytes_;
    int64_t media_max_file_bytes_;
    int media_download_url_ttl_seconds_;
    int media_task_max_attempts_;
    std::string media_variant_cdn_base_url_;
    int64_t media_multipart_part_size_bytes_;
    int media_multipart_upload_ttl_seconds_;
    int media_upload_url_ttl_seconds_;

    std::unique_ptr<ThreadPool> worker_pool_;
    ::sparkpush::net::HttpServerLite server_;
};

}  // namespace sparkpush

//