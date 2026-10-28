#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "thread_pool.h"

namespace sparkpush {

class KafkaConsumer {
   public:
    struct Options {
        bool enable_auto_commit{false};
        int auto_commit_interval_ms{5000};
        int max_poll_interval_ms{300000};
        int session_timeout_ms{45000};
        std::string auto_offset_reset{"earliest"};
        size_t worker_threads{1};
        size_t max_inflight{1024};
    };

    // 使用默认构造，consumer 和 worker 池在 Init/Start 中创建。
    KafkaConsumer() = default;
    ~KafkaConsumer();

    // 消息在有界工作池中执行。同一 partition 严格串行，不同 partition
    // 可并行；处理完成后由 Kafka 消费线程按 partition 顺序提交 offset。
    bool Init(
        const std::string& brokers, const std::string& group_id,
        const std::string& topic,
        std::function<void(const std::string&, const std::string&)> callback,
        const Options& options);

    // 启动 worker 池和消费线程。
    void Start();
    // 停止消费并等待在途任务完成。
    void Stop();

   private:
    struct PartitionKey {
        std::string topic;
        int32_t partition{0};

        // 按 topic 和 partition 排序，供 std::map 使用。
        bool operator<(const PartitionKey& other) const {
            return topic < other.topic ||
                   (topic == other.topic && partition < other.partition);
        }
    };

    struct WorkItem {
        PartitionKey partition_key;
        int64_t offset{0};
        std::string key;
        std::string value;
    };

    struct PartitionState {
        std::deque<std::shared_ptr<WorkItem>> pending;
        bool active{false};
    };

    // 消费主循环：拉取消息、处理完成项并控制背压。
    void Loop();
    // 把 Kafka 消息封装成 WorkItem 并按 partition 入队。
    void HandleMessage(RdKafka::Message* message);
    // 提交该 partition 的队首任务，保证同一 partition 串行。
    void SchedulePartition(const PartitionKey& key);
    // 处理已完成任务并提交 offset。
    void DrainCompletions();
    // 暂停或恢复所有已分配 partition。
    void SetPartitionsPaused(bool paused);

    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    std::function<void(const std::string&, const std::string&)> callback_;
    Options options_;
    std::unique_ptr<ThreadPool> workers_;

    std::map<PartitionKey, PartitionState> partitions_;
    size_t inflight_{0};
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
    std::deque<PartitionKey> completions_;
    bool partitions_paused_{false};

    std::atomic<bool> running_{false};
    std::thread thread_;
};

}  // namespace sparkpush
