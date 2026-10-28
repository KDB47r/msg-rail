#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "config.h"
#include "conn_registry.h"
#include "local_timer.h"
#include "logging.h"
#include "logic_stream_client.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"

// grpcpp 必须先包含，避免 asio 的宏与其内部符号冲突
#include <grpcpp/grpcpp.h>

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

namespace sparkpush {

// 每条 WebSocket 连接的会话上下文，由 websocketpp connection 持有。
struct ConnContext {
    int64_t user_id{0};
    std::string conn_id;  // comet_id:seq
    ConnHandle handle;
    std::atomic<int64_t> last_pong_ms{0};
    std::atomic<bool> closed{false};
    uint8_t heartbeat_bucket{0};
};

struct WsConnectionBase {
    std::shared_ptr<ConnContext> context;
};

struct WsConfig : public websocketpp::config::asio {
    using connection_base = WsConnectionBase;
};

using WsServer = websocketpp::server<WsConfig>;

// CometServer：WebSocket 网关，管理长连接、与 logic 通信、承接 job 的下行推送。
// 网络栈基于 standalone asio + websocketpp
class CometServer {
   public:
    explicit CometServer(const Config& cfg);
    ~CometServer();

    CometServer(const CometServer&) = delete;
    CometServer& operator=(const CometServer&) = delete;

    void SetThreadNum(int thread_num);
    void Start();  // 启动 acceptor 与 io 线程池
    void Run();    // 阻塞运行（直到 Stop）
    void Stop();   // 优雅关停，幂等

    // 供 gRPC CometService 调用：将消息推送给若干用户 / 房间 / 全网
    void PushToUsers(const ChatMessage& msg,
                     const std::vector<int64_t>& user_ids);
    void PushToRoom(const ChatMessage& msg, int64_t room_id);
    void PushToAll(const ChatMessage& msg);

   private:
    // websocketpp 回调
    bool OnValidate(const ConnHandle& hdl);  // 握手时鉴权
    void OnOpen(const ConnHandle& hdl);      // 握手完成
    void OnClose(const ConnHandle& hdl);     // 连接关闭
    void OnMessage(const ConnHandle& hdl,
                   WsServer::message_ptr msg);  // 收到帧
    void OnPong(const ConnHandle& hdl, std::string) { UpdatePong(hdl); }

    void HandleTextMessage(const ConnHandle& hdl, const ConnContext& ctx,
                           const std::string& payload);

    void SendText(const ConnHandle& hdl, const std::string& payload);
    void CloseConn(const ConnHandle& hdl);

    std::shared_ptr<ConnContext> GetConnectionContext(const ConnHandle& hdl);
    void UpdatePong(const ConnHandle& hdl);
    void AddToHeartbeatBucket(const std::shared_ptr<ConnContext>& ctx);
    void ScanNextHeartbeatBucket();
    void ScheduleHeartbeat();
    void StopHeartbeat();

    void NotifyUserOffline(int64_t user_id, const std::string& conn_id);
    void AddUserToRoom(int64_t room_id, int64_t user_id);
    void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
    void NotifyRoomJoin(int64_t room_id, int64_t user_id);
    void NotifyRoomLeave(int64_t room_id, int64_t user_id);

    Config cfg_;
    WsServer ws_;
    ConnRegistry registry_;

    int io_threads_{1};
    std::vector<std::thread> io_workers_;
    std::unique_ptr<std::thread> heartbeat_thread_;
    std::mutex heartbeat_wait_mutex_;
    std::condition_variable heartbeat_wait_cv_;
    std::atomic<bool> heartbeat_stopping_{false};
    std::atomic<bool> stopped_{false};

    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    std::string comet_id_;
    ThreadPool grpc_pool_;

    bool use_stream_{false};
    int stream_count_{1};
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<LogicStreamClient> stream_client_;

    std::atomic<uint64_t> conn_id_counter_{0};

    // 心跳分桶：64 个桶轮流扫描，每 500ms 扫一个，32s 轮完一圈；
    // 60s 超时约等于两圈，避免一次扫描延迟就误杀连接。
    static constexpr size_t kHeartbeatBucketCount = 64;
    static constexpr auto kHeartbeatTick = std::chrono::milliseconds(500);
    static constexpr auto kHeartbeatTimeout = std::chrono::seconds(60);

    struct HeartbeatEntry {
        ConnHandle handle;
        std::weak_ptr<ConnContext> context;
    };
    struct HeartbeatBucket {
        std::mutex mutex;
        std::vector<HeartbeatEntry> entries;
    };

    std::array<HeartbeatBucket, kHeartbeatBucketCount> heartbeat_buckets_;
    size_t next_heartbeat_bucket_{0};
};

}  // namespace sparkpush
