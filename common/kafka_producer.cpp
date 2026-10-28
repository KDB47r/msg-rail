#include "kafka_producer.h"

#include <librdkafka/rdkafkacpp.h>

#include "logging.h"

namespace sparkpush {

// 析构时 flush 最多 3 秒，尽量把缓冲中的消息发送出去。
KafkaProducer::~KafkaProducer() {
    if (producer_) {
        producer_->flush(3000);
    }
}

// 创建 Kafka producer 和 topic，并按低延迟目标设置参数。
bool KafkaProducer::Init(const std::string& brokers, const std::string& topic) {
    std::string errstr;
    topic_name_ = topic;

    // 用 unique_ptr 管理 RdKafka::Conf，任一错误路径提前 return 时自动释放，
    // 避免裸 new/delete 在早退分支泄漏。
    // 自定义删除器：让 unique_ptr 自动释放 RdKafka::Conf。
    auto conf_deleter = [](RdKafka::Conf* c) { delete c; };
    std::unique_ptr<RdKafka::Conf, decltype(conf_deleter)> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL), conf_deleter);
    if (!conf) {
        LogError("KafkaProducer create global conf failed");
        return false;
    }

    // 设置一个 Kafka 配置项，失败时记录错误并返回 false。
    auto set_conf = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            LogError("KafkaProducer set " + key + " failed: " + errstr);
            return false;
        }
        return true;
    };

    // 逐项检查所有配置项，任一失败即记日志并返回 false。
    // Kafka 低延迟配置（牺牲吞吐量换取延迟）：IM 场景下实时性优先于批量效率。
    if (!set_conf("bootstrap.servers", brokers) ||
        !set_conf("acks", "1") ||  // 只等 leader 确认（不等 ISR 全部同步）
        !set_conf("enable.idempotence",
                  "false") ||  // 关闭幂等（减少额外的协议往返）
        !set_conf("compression.type",
                  "none") ||  // 不压缩（消息体小，压缩收益不大）
        !set_conf("linger.ms", "0") ||  // 不等待批量聚合，立即发送
        !set_conf("batch.num.messages", "1") ||  // 每条消息独立发送
        !set_conf("retries", "5") ||  // 发送失败最多重试 5 次
        !set_conf("retry.backoff.ms", "500") ||
        !set_conf("socket.keepalive.enable", "true") ||  // TCP keepalive 保活
        !set_conf("message.timeout.ms", "60000") ||      // 消息超时 60 秒
        !set_conf("queue.buffering.max.messages", "200000")) {
        return false;
    }
    if (!topic_name_.empty()) {
        if (!set_conf("client.id", "spark_push_producer_" + topic_name_)) {
            return false;
        }
    }

    producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
        LogError("KafkaProducer create producer failed: " + errstr);
        return false;
    }

    std::unique_ptr<RdKafka::Conf, decltype(conf_deleter)> topic_conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC), conf_deleter);
    if (!topic_conf) {
        LogError("KafkaProducer create topic conf failed");
        return false;
    }
    topic_.reset(RdKafka::Topic::create(producer_.get(), topic_name_,
                                        topic_conf.get(), errstr));
    if (!topic_) {
        LogError("KafkaProducer create topic failed: " + errstr);
        return false;
    }
    LogInfo("KafkaProducer initialized topic=" + topic_name_);
    return true;
}

// 异步发送一条消息，失败返回 false，poll(0) 驱动发送回调。
bool KafkaProducer::Send(const std::string& key, const std::string& value) {
    if (!producer_ || !topic_) return false;
    RdKafka::ErrorCode err = producer_->produce(
        topic_.get(), RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(value.data()),
        value.size(), key.empty() ? nullptr : &key, nullptr);
    if (err != RdKafka::ERR_NO_ERROR) {
        LogError("Kafka produce failed: " + RdKafka::err2str(err));
        return false;
    }
    producer_->poll(0);
    return true;
}

}  // namespace sparkpush
