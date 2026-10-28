#include "kafka_consumer.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <vector>

#include "logging.h"

namespace sparkpush {

// 析构时调用 Stop，确保消费线程和 worker 池退出。
KafkaConsumer::~KafkaConsumer() { Stop(); }

// 创建 Kafka consumer、订阅 topic，并初始化工作线程池。
bool KafkaConsumer::Init(
    const std::string& brokers, const std::string& group_id,
    const std::string& topic,
    std::function<void(const std::string&, const std::string&)> callback,
    const Options& options) {
    topic_ = topic;
    callback_ = std::move(callback);
    options_ = options;
    options_.worker_threads = std::max<size_t>(1, options_.worker_threads);
    options_.max_inflight =
        std::max(options_.worker_threads, options_.max_inflight);

    std::string errstr;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", group_id, errstr);
    conf->set("enable.partition.eof", "false", errstr);
    conf->set("auto.offset.reset", options_.auto_offset_reset, errstr);
    conf->set("enable.auto.commit",
              options_.enable_auto_commit ? "true" : "false", errstr);
    conf->set("enable.auto.offset.store", "false", errstr);
    conf->set("auto.commit.interval.ms",
              std::to_string(options_.auto_commit_interval_ms), errstr);
    conf->set("max.poll.interval.ms",
              std::to_string(options_.max_poll_interval_ms), errstr);
    conf->set("session.timeout.ms", std::to_string(options_.session_timeout_ms),
              errstr);

    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;
    if (!consumer_) {
        LogError("KafkaConsumer create failed: " + errstr);
        return false;
    }
    std::vector<std::string> topics = {topic_};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        LogError("KafkaConsumer subscribe failed: " + RdKafka::err2str(err));
        return false;
    }
    workers_ = std::make_unique<ThreadPool>(options_.worker_threads,
                                            "kafka-" + topic_);
    return true;
}

// 启动 worker 池和消费线程，重复调用保持幂等。
void KafkaConsumer::Start() {
    if (running_.exchange(true)) return;
    workers_->Start();
    thread_ = std::thread(&KafkaConsumer::Loop, this);
}

// 停止消费，等待在途任务完成，关闭 consumer 并清理分区状态。
void KafkaConsumer::Stop() {
    if (!running_.exchange(false)) return;
    completion_cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    if (workers_) workers_->Stop();
    if (consumer_) {
        consumer_->close();
        consumer_.reset();
    }
    partitions_.clear();
}

// 消费主循环：提交完成项、按背压暂停分区、拉取消息并分发。
void KafkaConsumer::Loop() {
    while (running_ || inflight_ > 0) {
        DrainCompletions();
        if (!running_) {
            std::unique_lock<std::mutex> lock(completion_mutex_);
            completion_cv_.wait_for(lock, std::chrono::milliseconds(10),
                                    [this]() { return !completions_.empty(); });
            continue;
        }
        if (inflight_ >= options_.max_inflight) {
            SetPartitionsPaused(true);
            std::unique_ptr<RdKafka::Message> heartbeat(consumer_->consume(10));
            if (heartbeat && heartbeat->err() != RdKafka::ERR__TIMED_OUT) {
                HandleMessage(heartbeat.get());
            }
            continue;
        }
        SetPartitionsPaused(false);

        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(10));
        if (msg) HandleMessage(msg.get());
    }
    DrainCompletions();
}

// 把 Kafka 消息封装成 WorkItem，按 partition 入队并按需调度。
void KafkaConsumer::HandleMessage(RdKafka::Message* message) {
    switch (message->err()) {
        case RdKafka::ERR_NO_ERROR: {
            auto item = std::make_shared<WorkItem>();
            item->partition_key = {message->topic_name(), message->partition()};
            item->offset = message->offset();
            if (message->key()) item->key = *message->key();
            item->value.assign(static_cast<const char*>(message->payload()),
                               message->len());

            auto& state = partitions_[item->partition_key];
            state.pending.push_back(item);
            ++inflight_;
            if (!state.active) SchedulePartition(item->partition_key);
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
            break;
        default:
            LogError("Kafka consume error: " + message->errstr());
            break;
    }
}

// 提交该 partition 的队首任务，保证同一 partition 严格串行。
void KafkaConsumer::SchedulePartition(const PartitionKey& key) {
    auto it = partitions_.find(key);
    if (it == partitions_.end() || it->second.pending.empty()) return;
    it->second.active = true;
    auto item = it->second.pending.front();
    // worker 线程执行回调，完成后把该 partition 放入完成队列。
    workers_->Submit([this, key, item]() {
        try {
            if (callback_) callback_(item->key, item->value);
        } catch (const std::exception& ex) {
            LogError("Kafka callback exception: " + std::string(ex.what()));
        } catch (...) {
            LogError("Kafka callback unknown exception");
        }
        {
            std::lock_guard<std::mutex> lock(completion_mutex_);
            completions_.push_back(key);
        }
        completion_cv_.notify_one();
    });
}

// 暂停或恢复当前分配到的所有 partition，用于在途消息背压。
void KafkaConsumer::SetPartitionsPaused(bool paused) {
    if (paused == partitions_paused_ || !consumer_) return;
    std::vector<RdKafka::TopicPartition*> assignment;
    const auto assignment_err = consumer_->assignment(assignment);
    if (assignment_err != RdKafka::ERR_NO_ERROR || assignment.empty()) {
        RdKafka::TopicPartition::destroy(assignment);
        return;
    }
    const auto err =
        paused ? consumer_->pause(assignment) : consumer_->resume(assignment);
    RdKafka::TopicPartition::destroy(assignment);
    if (err == RdKafka::ERR_NO_ERROR) {
        partitions_paused_ = paused;
    } else {
        LogError(std::string("Kafka partitions ") +
                 (paused ? "pause" : "resume") +
                 " failed: " + RdKafka::err2str(err));
    }
}

// 处理已完成任务：出队、提交 offset，并调度同 partition 的下一条。
void KafkaConsumer::DrainCompletions() {
    std::deque<PartitionKey> completed;
    {
        std::lock_guard<std::mutex> lock(completion_mutex_);
        completed.swap(completions_);
    }

    for (const auto& key : completed) {
        auto it = partitions_.find(key);
        if (it == partitions_.end() || it->second.pending.empty()) continue;
        auto item = it->second.pending.front();
        it->second.pending.pop_front();
        it->second.active = false;
        --inflight_;

        if (!options_.enable_auto_commit && consumer_) {
            std::vector<RdKafka::TopicPartition*> offsets;
            offsets.push_back(RdKafka::TopicPartition::create(
                key.topic, key.partition, item->offset + 1));
            const auto err = consumer_->commitAsync(offsets);
            RdKafka::TopicPartition::destroy(offsets);
            if (err != RdKafka::ERR_NO_ERROR) {
                LogError("Kafka offset commit failed: " +
                         RdKafka::err2str(err));
            }
        }

        if (!it->second.pending.empty()) {
            SchedulePartition(key);
        } else {
            partitions_.erase(it);
        }
    }
}

}  // namespace sparkpush
