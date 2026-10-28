#include "logic_stream_client.h"

#include <string>
#include <utility>

#include "logging.h"

namespace sparkpush {

LogicStreamClient::LogicStreamClient(sparkpush::LogicService::Stub* stub,
                                     int stream_count)
    : stub_(stub), stream_count_(stream_count > 0 ? stream_count : 1) {}

LogicStreamClient::~LogicStreamClient() { Stop(); }

// 生成自增的请求编号。
uint64_t LogicStreamClient::NextRequestId() {
    return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

// 创建到 Logic 的多条双向流及读写线程。
bool LogicStreamClient::Start() {
    if (!stub_) {
        LogError("LogicStreamClient::Start called with null stub");
        return false;
    }

    streams_.resize(stream_count_);

    for (int i = 0; i < stream_count_; ++i) {
        streams_[i] = std::make_unique<StreamState>();
        streams_[i]->ctx = std::make_unique<grpc::ClientContext>();
        streams_[i]->stream = stub_->MessageStream(streams_[i]->ctx.get());

        if (!streams_[i]->stream) {
            LogError("Failed to create gRPC stream " + std::to_string(i));
            streams_.clear();
            return false;
        }
    }

    stream_running_ = true;

    // 启动每个流的读写线程
    for (int i = 0; i < stream_count_; ++i) {
        streams_[i]->writer_thread =
            std::thread(&LogicStreamClient::WriterLoop, this, i);
        streams_[i]->reader_thread =
            std::thread(&LogicStreamClient::ReaderLoop, this, i);
    }

    LogInfo("gRPC bidirectional streams initialized, count=" +
            std::to_string(stream_count_));
    return true;
}

// 停止流、唤醒阻塞读并回收线程。
void LogicStreamClient::Stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) {
        return;
    }

    stream_running_ = false;

    // 唤醒所有阻塞中的读写线程：
    //  - notify_all 唤醒在 send_queue_cv 上等待的 writer；
    //  - WritesDone + TryCancel 让 reader 阻塞的 Read() 返回 false。
    for (auto& state : streams_) {
        if (!state) continue;
        state->send_queue_cv.notify_all();
        if (state->stream) {
            state->stream->WritesDone();
        }
        if (state->ctx) {
            state->ctx->TryCancel();
        }
    }

    for (auto& state: streams_) {
        if (!state) continue;
        if (state->writer_thread.joinable()) {
            state->writer_thread.join();
        }
        if (state->reader_thread.joinable()) {
            state->reader_thread.join();
        }
    }

    streams_.clear();
}

// 从发送队列取请求写入指定流。
void LogicStreamClient::WriterLoop(int stream_idx) {
    auto& state = streams_[stream_idx];
    if (!state || !state->stream) return;

    while (stream_running_) {
        PendingRequest req;
        {
            std::unique_lock<std::mutex> lock(state->send_queue_mutex);
            state->send_queue_cv.wait(lock, [&state, this] () {
                return !state->send_queue.empty() || !stream_running_;
            });

            if (!stream_running_) break;
            if (state->send_queue.empty()) continue;

            req = std::move(state->send_queue.front());
            state->send_queue.pop();
        }

        if (req.callback) {
            std::lock_guard<std::mutex> lock(state->pending_callbacks_mutex);
            state->pending_callbacks_[req.msg.request_id()] = std::move(req.callback);
        }

        if (!state->stream->Write(req.msg)) {
            LogError("Stream " + std::to_string(stream_idx) + " write failed");
            break;
        }
    }
}

// 读取 Logic 响应并按 request_id 回调。
void LogicStreamClient::ReaderLoop(int stream_idx) {
    auto& state = streams_[stream_idx];
    if (!state || !state->stream) return;
    StreamResponse resp;

    while (stream_running_ && state->stream->Read(&resp)) {
        
        std::function<void(const StreamResponse&)> callback;
        {
            std::lock_guard<std::mutex> lock(state->pending_callbacks_mutex);
            auto it = state->pending_callbacks_.find(resp.request_id());
            if (it != state->pending_callbacks_.end()) {
                callback = std::move(it->second);
                state->pending_callbacks_.erase(it);
            }
        }

        if (callback) {
            callback(resp);
        }
    }

    if (stream_running_) {
        LogError("Stream " + std::to_string(stream_idx) +
                 " read loop ended unexpectedly");
    }
}

// 轮询选择一条流发送请求并登记回调。
void LogicStreamClient::Send(
    StreamMessage msg, std::function<void(const StreamResponse&)> callback) {
    uint64_t req_id = NextRequestId();
    msg.set_request_id(req_id);

    // 轮询分配到不同流
    int stream_idx = req_id % stream_count_;
    auto& state = streams_[stream_idx];

    {
        std::lock_guard<std::mutex> lock(state->send_queue_mutex);
        state->send_queue.push({std::move(msg), std::move(callback)});
    }
    state->send_queue_cv.notify_one();
}

}  // namespace sparkpush
