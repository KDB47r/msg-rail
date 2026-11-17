#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "media_model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 媒体元数据与任务队列 DAO。
// 所有写操作都尽量使用条件 UPDATE，避免 worker/Logic 并发覆盖状态。
class MediaDao {
   public:
    explicit MediaDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool Create(const MediaObject& media, std::string* err);

    bool GetById(const std::string& media_id, MediaObject* media,
                 std::string* err);

    // complete 阶段：对象 HEAD 成功后更新真实大小/ETag 并标记 uploaded。
    bool MarkUploaded(const std::string& media_id, int64_t size_bytes,
                      const std::string& etag, std::string* err);

    // 普通文件没有异步处理步骤：HEAD 校验通过后直接 uploaded -> ready。
    bool MarkReady(const std::string& media_id, std::string* err);

    // 幂等入队：同一 (media_id, task_type) 只会有一条任务。
    // task_type 为 process_image / process_video。
    bool EnqueueTask(const std::string& media_id, const std::string& task_type,
                     int max_attempts, std::string* err);

    bool ListVariants(const std::string& media_id,
                      std::vector<MediaVariant>* variants, std::string* err);

    // 发送消息时把媒体绑定到会话/消息。bound=false 表示条件不满足（已绑定/非 owner）。
    bool BindToMessage(const std::string& media_id, int64_t owner_id,
                       const std::string& session_id,
                       const std::string& message_id, bool* bound,
                       std::string* err);

    // Kafka 投递完全失败时撤销绑定，允许客户端重试。
    bool UnbindMessage(const std::string& media_id, int64_t owner_id,
                       const std::string& message_id, std::string* err);

    // worker 按 task_type 领取任务：事务内 SELECT ... FOR UPDATE SKIP LOCKED。
    bool ClaimNextTask(const std::string& worker_id,
                       const std::string& task_type, MediaTask* task,
                       bool* found, std::string* err);

    bool MarkProcessing(const std::string& media_id, std::string* err);

    bool CompleteImage(const std::string& media_id, int width, int height,
                       const std::string& dominant_color,
                       const std::vector<MediaVariant>& variants,
                       std::string* err);

    // 写入播放对象（转码后可能是 playback.mp4）、显示宽高、时长和封面
    // variant，然后标记 ready。
    bool CompleteVideo(const std::string& media_id, int width, int height,
                       int64_t duration_ms, const std::string& object_key,
                       int64_t size_bytes, const std::string& etag,
                       const std::vector<MediaVariant>& variants,
                       std::string* err);

    // 失败：未超过 max_attempts 时回到 pending 并退避；超过则 failed。
    bool FailTask(const MediaTask& task, const std::string& error,
                  bool* terminal, std::string* err);

    bool RecoverStaleRunningTasks(int stale_seconds, std::string* err);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
