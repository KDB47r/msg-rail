#include "push_handler.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

#include "logging.h"
#include "proto_enums.h"

namespace sparkpush {

PushHandler::PushHandler(const Config& cfg)
    : cfg_(cfg), use_stream_(cfg.use_push_stream) {}

PushHandler::~PushHandler() { Stop(); }

// 初始化推送/广播两个 Kafka 消费者和流连接。
bool PushHandler::Init() {
    ParseCometTargets();

    if (cfg_.kafka_brokers.empty()) {
        LogError("[PushHandler] Kafka brokers not configured");
        return false;
    }

    // 推送消费组：spark_push_push_group
    KafkaConsumer::Options opts;
    opts.enable_auto_commit = false;
    opts.worker_threads =
        static_cast<size_t>(std::max(1, cfg_.job_worker_threads));
    opts.max_inflight = static_cast<size_t>(std::max(1, cfg_.job_max_inflight));
    std::string push_group = cfg_.kafka_consumer_group + "_push";

    if (!push_consumer_.Init(
            cfg_.kafka_brokers, push_group, cfg_.kafka_push_topic,
            [this](const std::string& k, const std::string& v) {
                return HandleMessage(k, v);
            },
            opts)) {
        LogError("[PushHandler] Kafka push consumer init failed");
        return false;
    }
    LogInfo("[PushHandler] Push consumer initialized, group=" + push_group);

    // 广播消费组
    if (!broadcast_consumer_.Init(
            cfg_.kafka_brokers, cfg_.kafka_consumer_group,
            cfg_.kafka_broadcast_topic,
            [this](const std::string& k, const std::string& v) {
                return HandleBroadcastTask(k, v);
            },
            opts)) {
        LogError("[PushHandler] Kafka broadcast consumer init failed");
        return false;
    }

    // 如果启用流模式，初始化流连接
    if (use_stream_) {
        InitStreams();
    }

    return true;
}

// 为每个 Comet 建立一条推送流。
void PushHandler::InitStreams() {
    stream_running_ = true;

    for (const auto& kv : comet_addrs_) {
        const std::string& comet_id = kv.first;
        const std::string& addr = kv.second;

        auto state = std::make_unique<StreamState>();
        state->channel =
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        state->stub = CometService::NewStub(state->channel);
        state->ctx = std::make_unique<grpc::ClientContext>();

        state->writer =
            state->stub->PushStream(state->ctx.get(), &state->response);

        if (!state->writer) {
            LogError("[PushHandler] Failed to create stream to comet: " +
                     comet_id);
            continue;
        }

        // 先将state添加到map中，再启动线程（避免竞态）
        streams_[comet_id] = std::move(state);

        // 启动写入线程
        streams_[comet_id]->writer_thread =
            std::thread(&PushHandler::StreamWriterLoop, this, comet_id);

        LogInfo("[PushHandler] Stream initialized for comet: " + comet_id);
    }
}

// 重建与指定 Comet 的流连接。
bool PushHandler::ReconnectStream(const std::string& comet_id,
                                  StreamState* state) {
    auto addr_it = comet_addrs_.find(comet_id);
    if (addr_it == comet_addrs_.end()) return false;

    // 关闭旧流
    if (state->writer) {
        state->writer->WritesDone();
        state->writer.reset();
    }
    state->ctx.reset();

    // 重建 channel/stub/stream
    state->channel = grpc::CreateChannel(addr_it->second,
                                         grpc::InsecureChannelCredentials());
    state->stub = CometService::NewStub(state->channel);
    state->ctx = std::make_unique<grpc::ClientContext>();
    state->response = PushStreamReply{};
    state->writer = state->stub->PushStream(state->ctx.get(), &state->response);

    if (!state->writer) {
        LogError("[PushHandler] Reconnect stream failed for comet: " +
                 comet_id);
        return false;
    }
    LogInfo("[PushHandler] Stream reconnected for comet: " + comet_id);
    return true;
}

// 流模式写入线程：每个 Comet 对应一个独立的写入线程
// 从消息队列中取出请求，通过 gRPC ClientWriter 流式发送
// 断线时自动重连（指数退避 500ms → 8s），并重试当前消息
// 从队列取消息写入 Comet 流，失败时重连重试。
void PushHandler::StreamWriterLoop(const std::string& comet_id) {
    auto it = streams_.find(comet_id);
    if (it == streams_.end()) return;

    StreamState* state = it->second.get();
    static constexpr int kMaxReconnectBackoffMs = 8000;
    int reconnect_backoff_ms = 500;

    while (stream_running_) {
        PushToCometRequest req;
        {
            std::unique_lock<std::mutex> lock(state->queue_mutex);
            state->queue_cv.wait(lock, [&]() {
                return !state->send_queue.empty() || !stream_running_;
            });

            if (!stream_running_ && state->send_queue.empty()) break;
            if (state->send_queue.empty()) continue;

            req = std::move(state->send_queue.front());
            state->send_queue.pop();
        }

        bool ok = state->writer && state->writer->Write(req);
        if (ok) {
            reconnect_backoff_ms = 500;  // 写成功，重置退避
            continue;
        }

        // 写失败 → 重连
        LogError("[PushHandler] Stream write failed for comet: " + comet_id +
                 ", reconnecting...");

        while (stream_running_) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(reconnect_backoff_ms));
            if (!stream_running_) break;

            if (ReconnectStream(comet_id, state)) {
                // 重连成功，重试发送当前消息
                if (state->writer->Write(req)) {
                    reconnect_backoff_ms = 500;
                    break;
                }
            }
            // 指数退避
            reconnect_backoff_ms =
                std::min(reconnect_backoff_ms * 2, kMaxReconnectBackoffMs);
        }
    }

    // 关闭流
    if (state->writer) {
        state->writer->WritesDone();
    }
}

// 把推送任务放入对应 Comet 的发送队列。
void PushHandler::SendToStream(const std::string& comet_id,
                               PushToCometRequest req) {
    auto it = streams_.find(comet_id);
    if (it == streams_.end()) {
        // 流不存在，回退到同步调用
        LogInfo("[PushHandler] No stream for comet=" + comet_id +
                ", fallback to sync RPC");
        ProcessPushRequest(req);
        return;
    }

    StreamState* state = it->second.get();
    {
        std::lock_guard<std::mutex> lock(state->queue_mutex);
        state->send_queue.push(std::move(req));
    }
    state->queue_cv.notify_one();
}

// 启动 Kafka 消费者和流线程。
void PushHandler::Start() {
    push_consumer_.Start();
    broadcast_consumer_.Start();
    LogInfo("[PushHandler] Started, stream_mode=" +
            std::string(use_stream_ ? "true" : "false"));
}

// 停止消费者和流线程，并等待在途任务完成。
void PushHandler::Stop() {
    // 先停止消费并排空所有分区工作任务，避免任务仍访问已销毁的流状态。
    push_consumer_.Stop();
    broadcast_consumer_.Stop();

    stream_running_ = false;
    for (auto& kv : streams_) {
        kv.second->queue_cv.notify_all();
        if (kv.second->writer_thread.joinable()) {
            kv.second->writer_thread.join();
        }
    }
    streams_.clear();

    LogInfo("[PushHandler] Stopped");
}

// 解析配置中的 comet_id=host:port 列表。
void PushHandler::ParseCometTargets() {
    comet_addrs_.clear();
    if (cfg_.comet_targets.empty()) return;
    std::stringstream ss(cfg_.comet_targets);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        std::string id = item.substr(0, pos);
        std::string addr = item.substr(pos + 1);
        if (!id.empty() && !addr.empty()) {
            comet_addrs_[id] = addr;
            LogInfo("[PushHandler] Configured comet: " + id + " -> " + addr);
        }
    }
}

// gRPC Stub 懒初始化：首次访问某个 Comet 时创建 channel + stub 并缓存
// gRPC HTTP/2 channel 支持多路复用，一个 stub 可以并发多个 RPC 调用
// 懒加载并复用指定 Comet 的 gRPC stub。
CometService::Stub* PushHandler::GetStub(const std::string& comet_id) {
    std::lock_guard<std::mutex> lock(stub_mu_);
    auto it = comet_stubs_.find(comet_id);
    if (it != comet_stubs_.end()) return it->second.get();
    auto addr_it = comet_addrs_.find(comet_id);
    if (addr_it == comet_addrs_.end()) return nullptr;
    auto channel = grpc::CreateChannel(addr_it->second,
                                       grpc::InsecureChannelCredentials());
    auto stub = CometService::NewStub(channel);
    auto* ptr = stub.get();
    comet_stubs_[comet_id] = std::move(stub);
    return ptr;
}

// 处理一条 Kafka 推送任务，按模式选择流或一元推送。
void PushHandler::HandleMessage(const std::string& key,
                                const std::string& value) {
    (void)key;

    PushToCometRequest req;
    if (!req.ParseFromString(value)) {
        LogError("[PushHandler] Failed to parse PushToCometRequest");
        return;
    }

    const std::string& comet_id = req.comet_id();

    // persist_only 消息不需要推送
    if (req.persist_only()) {
        return;
    }

    if (use_stream_ && stream_running_) {
        // 流模式：直接发送到流队列
        SendToStream(comet_id, std::move(req));
    } else {
        // 传统模式：在 Kafka 分区工作线程内同步完成推送（含重试），
        // 回调返回后 KafkaConsumer 才提交该分区 offset，保证 at-least-once 语义
        ProcessPushRequest(req);
    }
}

// 单条推送：带指数退避的重试机制
// 退避策略：100ms → 400ms → 1600ms（base * 4^attempt）
// 超时控制：每次 RPC 设置 3 秒 deadline，避免长时间阻塞
// 死信处理：所有重试耗尽后记录 [DLQ] 日志，可接入告警或补偿队列
// 一元推送到目标 Comet，失败按退避重试。
void PushHandler::ProcessPushRequest(const PushToCometRequest& req) {
    static constexpr int kMaxRetries = 3;
    static constexpr int kBaseBackoffMs = 100;  // 100ms → 400ms → 1600ms

    CometService::Stub* stub = GetStub(req.comet_id());
    if (!stub) {
        LogError("[DLQ][Push] No stub for comet_id=" + req.comet_id() +
                 " msg_id=" + req.message().msg_id());
        return;
    }

    for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
        PushToCometReply reply;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(3));
        auto status = stub->PushToComet(&ctx, req, &reply);
        if (status.ok()) {
            return;  // 成功
        }

        if (attempt < kMaxRetries) {
            int backoff_ms = kBaseBackoffMs * (1 << (attempt * 2));  // 指数退避
            LogError("[PushHandler] PushToComet retry " +
                     std::to_string(attempt + 1) + "/" +
                     std::to_string(kMaxRetries) + " after " +
                     std::to_string(backoff_ms) +
                     "ms, error: " + status.error_message());
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        } else {
            // 所有重试用尽，记录死信日志
            LogError("[DLQ][Push] All retries exhausted, comet_id=" +
                     req.comet_id() + " msg_id=" + req.message().msg_id() +
                     " session_id=" + req.message().session_id() +
                     " error: " + status.error_message());
        }
    }
}

// 处理广播任务，向所有配置的 Comet 推送。
void PushHandler::HandleBroadcastTask(const std::string& key,
                                      const std::string& value) {
    (void)key;
    BroadcastTaskRequest task;
    if (!task.ParseFromString(value)) {
        LogError("[PushHandler] Failed to parse BroadcastTaskRequest");
        return;
    }

    ChatMessage msg;
    msg.set_msg_id(task.task_id());
    msg.set_session_id("broadcast");
    msg.set_msg_seq(0);
    msg.set_sender_id(0);
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    msg.set_timestamp_ms(now_ms);
    msg.set_msg_type(MSG_TYPE_BROADCAST);
    msg.set_content_json(task.content_json());

    // scope=group：禁止 PushToAll。无 Redis 时仍遍历配置内 Comet，但带
    // room_id，由 Comet PushToRoom；本机无成员则空扇出。主路径上 Logic
    // 已把房间广播改走 push topic，此处兜底残留/旧任务。
    const bool room_scoped =
        task.scope() == BROADCAST_SCOPE_GROUP && task.group_id() > 0;

    for (const auto& kv : comet_addrs_) {
        const std::string& comet_id = kv.first;
        CometService::Stub* stub = GetStub(comet_id);
        if (!stub) continue;

        PushToCometRequest req;
        req.set_comet_id(comet_id);
        *req.mutable_message() = msg;
        if (room_scoped) {
            req.set_room_id(task.group_id());
        }

        PushToCometReply reply;
        grpc::ClientContext ctx;
        stub->PushToComet(&ctx, req, &reply);
    }
}

}  // namespace sparkpush
