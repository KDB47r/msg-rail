#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "spark_push.grpc.pb.h"

namespace sparkpush {

// LogicStreamClient：封装与 Logic 的 gRPC 双向流（MessageStream）客户端。
//
// 从 CometServer 中抽出的独立组件，负责：
//   - 建立 N 条持久双向流（多路复用，提高吞吐）；
//   - 每条流各有一个 writer 线程（发送）与 reader 线程（接收）；
//   - 通过 request_id 关联请求与响应，回调在所有流之间共享一张映射表；
//   - 轮询（round-robin）把请求分配到不同的流。
//
// 生命周期：构造后调用 Start() 建流并启动读写线程；Stop() 优雅关停
// （唤醒 writer、WritesDone + TryCancel 让 reader 的 Read() 返回、join 全部
// 线程、清空 pending 回调）。Stop() 幂等，可被显式调用与析构分别触发。
class LogicStreamClient {
   public:
    // stub 必须在本对象整个生命周期内保持有效（由 CometServer 持有）。
    LogicStreamClient(sparkpush::LogicService::Stub* stub, int stream_count);
    ~LogicStreamClient();

    LogicStreamClient(const LogicStreamClient&) = delete;
    LogicStreamClient& operator=(const LogicStreamClient&) = delete;

    // 建立所有流并启动读写线程。成功返回 true；任一流创建失败返回 false，
    // 此时不启动线程，Running() 保持 false。
    bool Start();

    // 优雅关停：唤醒并 join 所有读写线程，清空待处理回调。幂等。
    void Stop();

    // 是否处于可用（已成功启动且未关停）状态。
    bool Running() const { return stream_running_.load(); }

    // 通过流发送一条消息。内部会为其分配唯一 request_id（覆盖入参中的字段），
    // 并在收到对应响应时回调 callback。
    void Send(StreamMessage msg,
              std::function<void(const StreamResponse&)> callback);

   private:
    // 单条流的发送请求。
    struct PendingRequest {
        StreamMessage msg;
        std::function<void(const StreamResponse&)> callback;
    };

    // 单条流的状态（context / 流对象 / 读写线程 / 发送队列）。
    struct StreamState {
        std::unique_ptr<grpc::ClientContext> ctx;
        std::unique_ptr<grpc::ClientReaderWriter<StreamMessage, StreamResponse>>
            stream;
        std::thread writer_thread;
        std::thread reader_thread;
        std::queue<PendingRequest> send_queue;
        std::mutex send_queue_mutex;
        std::condition_variable send_queue_cv;
        std::unordered_map<uint64_t, std::function<void(const StreamResponse&)>>
            pending_callbacks_;
        std::mutex pending_callbacks_mutex;
    };

    void WriterLoop(int stream_idx);
    void ReaderLoop(int stream_idx);
    uint64_t NextRequestId();

    sparkpush::LogicService::Stub* stub_;  // 非拥有
    int stream_count_;

    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<bool> stream_running_{false};
    std::atomic<bool> stopped_{false};  // Stop() 防重入标志

    std::atomic<uint64_t> request_id_counter_{0};
};

}  // namespace sparkpush

//