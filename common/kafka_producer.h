#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

namespace sparkpush {

class KafkaProducer {
   public:
    // 使用默认构造，producer 和 topic 在 Init 中创建。
    KafkaProducer() = default;
    // 析构时 flush 最多 3 秒，尽量发出缓冲消息。
    ~KafkaProducer();

    // 初始化 producer 并绑定目标 topic。
    bool Init(const std::string& brokers, const std::string& topic);
    // 异步发送一条消息，成功入队返回 true。
    bool Send(const std::string& key, const std::string& value);

   private:
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::string topic_name_;
};

}  // namespace sparkpush
