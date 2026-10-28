#include "comet_server.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "proto_enums.h"
#include "websocket_utils.h"

namespace sparkpush {

namespace {

// 从 WebSocket 握手路径中提取 token=... 参数（不处理 URL 解码）。
std::string ExtractTokenFromResource(const std::string& resource) {
    // 找到 token= 的位置；找不到说明客户端没带 token。
    auto qpos = resource.find("token=");
    if (qpos == std::string::npos) return {};
    qpos += 6;

    // 截取 token= 后面的内容，遇到 & 表示后面还有别的查询参数。
    std::string token = resource.substr(qpos);
    auto amp = token.find('&');
    if (amp != std::string::npos) token = token.substr(0, amp);
    return token;
}

int64_t SteadyNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

CometServer::CometServer(const Config& cfg)
    : cfg_(cfg),
      grpc_pool_(cfg.comet_grpc_pool_size > 0 ? cfg.comet_grpc_pool_size : 4,
                 "comet_grpc_pool"),
      use_stream_(cfg.use_grpc_stream),
      stream_count_(cfg.grpc_stream_count > 0 ? cfg.grpc_stream_count : 1) {
    comet_id_ = cfg.comet_id;
    channel_ = grpc::CreateChannel(cfg.logic_grpc_target,
                                   grpc::InsecureChannelCredentials());
    logic_stub_ = sparkpush::LogicService::NewStub(channel_);
    stream_client_ =
        std::make_unique<LogicStreamClient>(logic_stub_.get(), stream_count_);

    ws_.clear_access_channels(websocketpp::log::alevel::all);
    ws_.clear_error_channels(websocketpp::log::elevel::all);
    ws_.set_error_channels(websocketpp::log::elevel::fatal |
                           websocketpp::log::elevel::rerror);
    ws_.init_asio();
    ws_.set_reuse_addr(true);

    ws_.set_validate_handler(
        [this](ConnHandle hdl) { return OnValidate(hdl); });
    ws_.set_open_handler([this](ConnHandle hdl) { OnOpen(hdl); });
    ws_.set_close_handler([this](ConnHandle hdl) { OnClose(hdl); });
    ws_.set_message_handler([this](ConnHandle hdl, WsServer::message_ptr msg) {
        OnMessage(hdl, msg);
    });
    ws_.set_pong_handler(
        [this](ConnHandle hdl, std::string s) { OnPong(hdl, std::move(s)); });
}

CometServer::~CometServer() { Stop(); }

void CometServer::SetThreadNum(int n) { io_threads_ = n < 1 ? 1 : n; }

// 启动 gRPC 调用池、WebSocket 监听、IO 线程和心跳。
void CometServer::Start() {
    grpc_pool_.Start();
    if (use_stream_) {
        if (!stream_client_->Start()) use_stream_ = false;
    }

    std::error_code ec;
    ws_.listen(static_cast<uint16_t>(cfg_.listen_port), ec);
    if (ec) {
        LogError("CometServer listen failed on port " +
                 std::to_string(cfg_.listen_port) + ": " + ec.message());
        return;
    }
    ws_.start_accept();

    for (int i = 1; i < io_threads_; ++i) {
        io_workers_.emplace_back([this] { ws_.run(); });
    }
    ScheduleHeartbeat();
}

// 阻塞运行事件循环，直到服务停止。
void CometServer::Run() {
    ws_.run();
    for (auto& t : io_workers_)
        if (t.joinable()) t.join();
}

// 停止心跳、监听和所有连接，并回收 IO 线程。
void CometServer::Stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) return;

    StopHeartbeat();

    std::error_code ec;
    ws_.stop_listening(ec);
    for (const auto& h : registry_.AllConns()) {
        std::error_code cec;
        ws_.close(h, websocketpp::close::status::going_away, "server stop",
                  cec);
    }
    ws_.stop();
    for (auto& t : io_workers_)
        if (t.joinable()) t.join();
    io_workers_.clear();

    if (stream_client_) stream_client_->Stop();
    LogInfo("CometServer stopped");
}

// WebSocket 握手校验 token 并生成 conn_id。
bool CometServer::OnValidate(const ConnHandle& hdl) {
    // 1. 取出本次 WebSocket 握手对应的连接对象和请求路径。
    auto conn = ws_.get_con_from_hdl(hdl);
    const std::string resource = conn->get_resource();

    // 2. 从 /ws?token=xxx 中截出 token；没有 token 直接拒绝握手。
    const std::string token = ExtractTokenFromResource(resource);
    if (token.empty()) {
        LogError("No token in WebSocket handshake resource=" + resource);
        return false;
    }

    // 3. Logic 的 gRPC stub 没初始化时无法校验 token，直接拒绝。
    if (!logic_stub_) {
        LogError("Logic stub not initialized");
        return false;
    }

    // 4. 每条连接分配独立的 ConnContext，并用原子计数器生成 conn_id。
    //    conn_id 形如 comet-1:7，用于后续路由和断线清理。
    auto ctx = std::make_shared<ConnContext>();
    const uint64_t sequence =
        conn_id_counter_.fetch_add(1, std::memory_order_relaxed);
    ctx->conn_id = comet_id_ + ":" + std::to_string(sequence);
    ctx->handle = hdl;

    // 5. 按连接序号把连接分到 64 个心跳桶之一，避免一次扫描所有连接。
    ctx->heartbeat_bucket =
        static_cast<uint8_t>(sequence % kHeartbeatBucketCount);

    // 6. 把 token、Comet 编号、连接编号发给 Logic 做真正的校验。
    VerifyTokenRequest vreq;
    vreq.set_token(token);
    vreq.set_comet_id(comet_id_);
    vreq.set_conn_id(ctx->conn_id);
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;

    //同步阻塞 阻塞该asio_io线程 
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);

    // 7. 同时检查两层结果：gRPC 传输成功 + 业务错误码为 0。
    //    Logic 对无效 token 通常返回 gRPC OK，但 error.code()=401。
    if (!status.ok() || vrep.error().code() != 0) {
        LogError("VerifyToken failed: " + (status.ok()
                                               ? vrep.error().message()
                                               : status.error_message()));
        return false;
    }

    // 8. 校验通过：保存 user_id，记录最后活跃时间，并把上下文挂到连接上。
    //    随后 websocketpp 会调用 OnOpen，把连接加入本机注册表和心跳桶。
    ctx->user_id = vrep.user_id();
    ctx->last_pong_ms.store(SteadyNowMs(), std::memory_order_relaxed);
    conn->context = std::move(ctx);
    return true;
}

// 握手成功后把连接加入本机注册表和心跳桶。
void CometServer::OnOpen(const ConnHandle& hdl) {
    auto ctx = GetConnectionContext(hdl);
    if (!ctx || ctx->user_id <= 0) return;
    registry_.AddConn(ctx->user_id, hdl);
    AddToHeartbeatBucket(ctx);
    LogInfo("WebSocket handshake done, user_id=" +
            std::to_string(ctx->user_id) + " conn_id=" + ctx->conn_id);
}

// 连接关闭后清理注册表并通知 Logic 下线。
void CometServer::OnClose(const ConnHandle& hdl) {
    auto ctx = GetConnectionContext(hdl);
    if (ctx) ctx->closed.store(true, std::memory_order_release);

    int64_t user_id = 0;
    std::vector<int64_t> left_rooms;
    registry_.RemoveConn(hdl, &user_id, &left_rooms);
    const std::string conn_id = ctx ? ctx->conn_id : std::string{};
    if (user_id > 0) {
        NotifyUserOffline(user_id, conn_id);
        for (int64_t rid : left_rooms) NotifyRoomLeave(rid, user_id);
    }
    LogInfo("Connection closed conn_id=" + conn_id);
}

// 收到 WebSocket 消息后交给文本消息处理。
void CometServer::OnMessage(const ConnHandle& hdl, WsServer::message_ptr msg) {
    if (msg->get_opcode() != websocketpp::frame::opcode::text) return;
    auto ctx = GetConnectionContext(hdl);
    if (!ctx || ctx->closed.load(std::memory_order_acquire) ||
        ctx->user_id <= 0) {
        CloseConn(hdl);
        return;
    }
    HandleTextMessage(hdl, *ctx, msg->get_payload());
}

// __COMET_SERVER_APPEND__

// 从 WebSocket 句柄取出连接上下文。
std::shared_ptr<ConnContext> CometServer::GetConnectionContext(
    const ConnHandle& hdl) {
    std::error_code ec;
    auto conn = ws_.get_con_from_hdl(hdl, ec);
    return ec || !conn ? nullptr : conn->context;
}

// 收到 pong 时刷新最后活跃时间。
void CometServer::UpdatePong(const ConnHandle& hdl) {
    auto ctx = GetConnectionContext(hdl);
    if (ctx && !ctx->closed.load(std::memory_order_acquire)) {
        ctx->last_pong_ms.store(SteadyNowMs(), std::memory_order_relaxed);
    }
}

// 向指定连接发送一条文本消息。
void CometServer::SendText(const ConnHandle& hdl, const std::string& payload) {
    std::error_code ec;
    ws_.send(hdl, payload, websocketpp::frame::opcode::text, ec);
    (void)ec;
}

// 正常关闭指定 WebSocket 连接。
void CometServer::CloseConn(const ConnHandle& hdl) {
    std::error_code ec;
    ws_.close(hdl, websocketpp::close::status::normal, "close", ec);
}

// 把连接放进对应的心跳扫描桶。
void CometServer::AddToHeartbeatBucket(
    const std::shared_ptr<ConnContext>& ctx) {
    auto& bucket = heartbeat_buckets_[ctx->heartbeat_bucket];
    std::lock_guard<std::mutex> lock(bucket.mutex);
    bucket.entries.push_back({ctx->handle, ctx});
}

// 扫描一个心跳桶，超时就关闭连接。
void CometServer::ScanNextHeartbeatBucket() {
    struct HeartbeatAction {
        ConnHandle handle;
        bool close{false};
    };

    const size_t index = next_heartbeat_bucket_++ % kHeartbeatBucketCount;
    auto& bucket = heartbeat_buckets_[index];
    const int64_t now_ms = SteadyNowMs();
    const int64_t timeout_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kHeartbeatTimeout)
            .count();
    std::vector<HeartbeatAction> actions;

    {
        std::lock_guard<std::mutex> lock(bucket.mutex);
        actions.reserve(bucket.entries.size());
        auto it = bucket.entries.begin();
        while (it != bucket.entries.end()) {
            auto ctx = it->context.lock();
            if (!ctx || ctx->closed.load(std::memory_order_acquire)) {
                it = bucket.entries.erase(it);
                continue;
            }
            const int64_t last_pong =
                ctx->last_pong_ms.load(std::memory_order_relaxed);
            actions.push_back({it->handle, now_ms - last_pong > timeout_ms});
            ++it;
        }
    }

    if (actions.empty() || stopped_.load(std::memory_order_acquire)) return;
    ws_.get_io_service().post([this, actions = std::move(actions)]() {
        if (stopped_.load(std::memory_order_acquire)) return;
        for (const auto& action : actions) {
            std::error_code ec;
            if (action.close) {
                ws_.close(action.handle, websocketpp::close::status::normal,
                          "heartbeat timeout", ec);
            } else {
                ws_.ping(action.handle, "ping", ec);
            }
        }
    });
}

// 安排下一次心跳扫描。
void CometServer::ScheduleHeartbeat() {
    heartbeat_stopping_.store(false, std::memory_order_release);
    heartbeat_thread_ = std::make_unique<std::thread>([this] {
        LocalTimer timer;
        timer.schedule(kHeartbeatTick,
                       [this] { ScanNextHeartbeatBucket(); });

        std::unique_lock<std::mutex> lock(heartbeat_wait_mutex_);
        while (!heartbeat_stopping_.load(std::memory_order_acquire)) {
            heartbeat_wait_cv_.wait_for(lock, std::chrono::milliseconds(10),
                                        [this] {
                                            return heartbeat_stopping_.load(
                                                std::memory_order_acquire);
                                        });
            if (heartbeat_stopping_.load(std::memory_order_acquire)) break;
            lock.unlock();
            if (timer.execute_if_ready()) {
                timer.schedule(kHeartbeatTick,
                               [this] { ScanNextHeartbeatBucket(); });
            }
            lock.lock();
        }
    });
}

// 停止心跳定时器并回收心跳线程。
void CometServer::StopHeartbeat() {
    heartbeat_stopping_.store(true, std::memory_order_release);
    heartbeat_wait_cv_.notify_all();
    if (heartbeat_thread_ && heartbeat_thread_->joinable()) {
        heartbeat_thread_->join();
    }
    heartbeat_thread_.reset();
}

// 把用户加入本机房间表并上报 Logic。
void CometServer::AddUserToRoom(int64_t room_id, int64_t user_id) {
    if (registry_.JoinRoom(room_id, user_id)) NotifyRoomJoin(room_id, user_id);
}

// 把用户移出本机房间表并上报 Logic。
void CometServer::RemoveUserFromRoom(int64_t room_id, int64_t user_id) {
    if (registry_.LeaveRoom(room_id, user_id))
        NotifyRoomLeave(room_id, user_id);
}

// 异步通知 Logic 该连接已下线。
void CometServer::NotifyUserOffline(int64_t user_id,
                                    const std::string& conn_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    grpc_pool_.Submit([stub, user_id, comet_id, conn_id]() {
        UserOfflineRequest req;
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        req.set_conn_id(conn_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->UserOffline(&ctx, req, &rep);
        if (!status.ok()) {
            LogError("UserOffline RPC failed for user " +
                     std::to_string(user_id) + ": " + status.error_message());
        } else if (rep.error().code() != 0) {
            LogError("UserOffline logic error for user " +
                     std::to_string(user_id) + ": " + rep.error().message());
        }
    });
}

// 异步通知 Logic 用户加入房间。
void CometServer::NotifyRoomJoin(int64_t room_id, int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    grpc_pool_.Submit([stub, room_id, user_id, comet_id]() {
        RoomReportRequest req;
        req.set_room_id(room_id);
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        stub->ReportRoomJoin(&ctx, req, &rep);
    });
}

// 异步通知 Logic 用户离开房间。
void CometServer::NotifyRoomLeave(int64_t room_id, int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    grpc_pool_.Submit([stub, room_id, user_id, comet_id]() {
        RoomReportRequest req;
        req.set_room_id(room_id);
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        stub->ReportRoomLeave(&ctx, req, &rep);
    });
}

// __COMET_SERVER_APPEND2__

// 解析上行 JSON 并转成 gRPC 请求发给 Logic。
void CometServer::HandleTextMessage(const ConnHandle& hdl,
                                    const ConnContext& ctx,
                                    const std::string& payload) {
    UpstreamMessageMeta meta;
    if (!ParseUpstreamMessage(payload, &meta)) {
        SendText(hdl, R"({"type":"error","message":"invalid message format"})");
        return;
    }
    const std::string& type = meta.type;
    std::string scene;
    if (type == "single_chat") {
        if (meta.to_user_id <= 0) {
            SendText(
                hdl,
                R"({"type":"error","message":"to_user_id must be positive"})");
            return;
        }
        scene = "single";
    } else if (type == "chatroom" || type == "chatroom_msg") {
        if (meta.group_id <= 0) {
            SendText(
                hdl,
                R"({"type":"error","message":"group_id(room_id) must be positive"})");
            return;
        }
        scene = "chatroom";
    } else if (type == "danmaku") {
        if (meta.video_id.empty()) {
            SendText(
                hdl,
                R"({"type":"error","message":"video_id required for danmaku"})");
            return;
        }
        if (meta.group_id <= 0) {
            SendText(
                hdl,
                R"({"type":"error","message":"group_id(room_id) required for danmaku"})");
            return;
        }
        scene = "danmaku";
    } else if (type == "chatroom_join" || type == "chatroom_leave") {
        if (meta.group_id <= 0) {
            SendText(
                hdl,
                R"({"type":"error","message":"group_id(room_id) must be positive"})");
            return;
        }
        if (type == "chatroom_join") {
            AddUserToRoom(meta.group_id, ctx.user_id);
            SendText(hdl, R"({"type":"ack","op":"chatroom_join"})");
        } else {
            RemoveUserFromRoom(meta.group_id, ctx.user_id);
            SendText(hdl, R"({"type":"ack","op":"chatroom_leave"})");
        }
        return;
    } else {
        SendText(hdl, R"({"type":"error","message":"unsupported type"})");
        return;
    }

    if (!logic_stub_) {
        SendText(hdl, R"({"type":"error","message":"logic not available"})");
        return;
    }

    int64_t received_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    UpstreamMessageRequest req;
    req.set_from_user_id(ctx.user_id);
    req.set_scene(SceneFromString(scene));
    req.set_client_msg_id(meta.client_msg_id);
    req.set_comet_id(comet_id_);
    req.set_conn_id(ctx.conn_id);
    req.set_received_time_ms(received_time_ms);
    if (scene == "single") {
        req.set_to_user_id(meta.to_user_id);
    } else if (scene == "chatroom") {
        req.set_group_id(meta.group_id);
    } else if (scene == "danmaku") {
        req.set_group_id(meta.group_id);
        req.set_video_id(meta.video_id);
        req.set_timeline_ms(meta.timeline_ms);
    }
    req.set_content_json(payload);

    const std::string client_msg_id = req.client_msg_id();
    ConnHandle h = hdl;
    if (use_stream_ && stream_client_ && stream_client_->Running()) {
        StreamMessage stream_msg;
        *stream_msg.mutable_upstream() = req;
        stream_client_->Send(
            std::move(stream_msg),
            [this, h, client_msg_id](const StreamResponse& resp) {
                nlohmann::json result;
                result["client_msg_id"] = client_msg_id;
                if (resp.error().code() != 0) {
                    result["type"] = "error";
                    result["message"] = "send failed";
                } else {
                    result["type"] = "ack";
                    result["msg_id"] =
                        resp.upstream_reply().message().msg_id();
                }
                SendText(h, result.dump());
            });
    } else {
        auto* stub = logic_stub_.get();
        grpc_pool_.Submit([this, h, stub, req, client_msg_id]() {
            UpstreamMessageReply rep;
            grpc::ClientContext rpc_ctx;
            auto status = stub->SendUpstreamMessage(&rpc_ctx, req, &rep);
            nlohmann::json result;
            result["client_msg_id"] = client_msg_id;
            if (!status.ok() || rep.error().code() != 0) {
                std::string msg = status.ok() ? rep.error().message()
                                              : status.error_message();
                LogError("SendUpstreamMessage failed: " + msg);
                result["type"] = "error";
                result["message"] = "send failed";
            } else {
                result["type"] = "ack";
                result["msg_id"] = rep.message().msg_id();
            }
            SendText(h, result.dump());
        });
    }
}

static std::string PayloadFor(const ChatMessage& msg) {
    return msg.content_json().empty()
               ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
               : msg.content_json();
}

// 向指定用户的所有本机连接推送消息。
void CometServer::PushToUsers(const ChatMessage& msg,
                              const std::vector<int64_t>& user_ids) {
    const std::string payload = PayloadFor(msg);
    for (const auto& h : registry_.ConnsForUsers(user_ids))
        SendText(h, payload);
}

// 向本机房间内所有连接推送消息。
void CometServer::PushToRoom(const ChatMessage& msg, int64_t room_id) {
    const std::string payload = PayloadFor(msg);
    for (const auto& h : registry_.ConnsInRoom(room_id)) SendText(h, payload);
}

// 向本机所有连接广播消息。
void CometServer::PushToAll(const ChatMessage& msg) {
    const std::string payload = PayloadFor(msg);
    for (const auto& h : registry_.AllConns()) SendText(h, payload);
}

}  // namespace sparkpush
