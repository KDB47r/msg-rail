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
    // 消费者配置副本
    /*
    ┌─────────────────────────┬──────────────────────────────────────────────────────────────────────┐
    │          配置           │                                 作用                                 │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ enable_auto_commit      │ 是否让 librdkafka 自动提交 offset；当前默认 false，手动提交          │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ auto_commit_interval_ms │ 自动提交间隔；只在 enable_auto_commit=true 时有效                    │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ max_poll_interval_ms    │ 两次 poll 的最大间隔；超过会被 broker 认为消费者死了，触发 rebalance │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ session_timeout_ms      │ broker 判断消费者是否存活的时间                                      │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ auto_offset_reset       │ 没有已提交 offset 时从哪开始，earliest 表示从最早消息开始            │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ worker_threads          │ 执行 callback_ 的 worker 线程数                                      │
    ├─────────────────────────┼──────────────────────────────────────────────────────────────────────┤
    │ max_inflight            │ 已入队但未完成的消息上限，达到后暂停拉取，形成背压                   │
    └─────────────────────────┴──────────────────────────────────────────────────────────────────────┘
    */
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
    /*
    - 作为 partitions_ 这个 std::map 的 key；
    - 用 topic + partition 唯一标识一个分区；
    - operator< 让 std::map 能排序。
    */
    struct PartitionKey {
        std::string topic;
        int32_t partition{0};

        // 按 topic 和 partition 排序，供 std::map 使用。
        bool operator<(const PartitionKey& other) const {
            return topic < other.topic ||
                   (topic == other.topic && partition < other.partition);
        }
    };

    //  一条 Kafka 消息在内存里的表示
    struct WorkItem {
        PartitionKey partition_key; //topic / partition
        int64_t offset{0}; // 在 partition 里的 offset
        std::string key; // 
        std::string value; // 
    };


    // partition 状态
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

    /*
    ┌────────────────────┬─────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────┬──────────────────────────┐
    │        成员        │                            类型                             │                          作用                           │          谁访问          │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ consumer_          │ unique_ptr<RdKafka::KafkaConsumer>                          │ 真正的 librdkafka 消费者，负责订阅、poll、commit        │ 消费线程 Loop()          │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ topic_             │ std::string                                                 │ 订阅的 topic 名，也用来给线程池命名                     │ Init() 写入              │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ callback_          │ std::function<void(const std::string&, const std::string&)> │ 业务回调，参数是 Kafka 的 key/value                     │ worker 线程执行          │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ options_           │ Options                                                     │ 消费者配置副本                                          │ Init() 写入，Loop() 读取 │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ workers_           │ unique_ptr<ThreadPool>                                      │ 执行 callback_ 的线程池                                 │ Loop() 提交任务          │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ partitions_        │ std::map<PartitionKey, PartitionState>                      │ 每个 partition 的待处理队列和运行状态                   │ 消费线程                 │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ inflight_          │ size_t                                                      │ 已入队但还没完成的消息总数，用于背压                    │ 消费线程                 │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ completion_mutex_  │ std::mutex                                                  │ 保护 completions_                                       │ 消费线程 + worker 线程   │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ completion_cv_     │ std::condition_variable                                     │ worker 完成后唤醒消费线程                               │ 消费线程 + worker 线程   │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ completions_       │ std::deque<PartitionKey>                                    │ “某个 partition 的队首任务完成了”的通知队列             │ 消费线程 + worker 线程   │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ partitions_paused_ │ bool                                                        │ 当前是否已经暂停了所有 partition，避免重复 pause/resume │ 消费线程                 │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ running_           │ std::atomic<bool>                                           │ 消费循环是否继续运行                                    │ Start/Stop/Loop          │
    ├────────────────────┼─────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────────────┤
    │ thread_            │ std::thread                                                 │ 运行 Loop() 的消费线程                                  │ Start/Stop               │
    └────────────────────┴─────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────┴──────────────────────────┘
  */

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
