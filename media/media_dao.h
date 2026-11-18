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

    // 批量查询；内部按 100 个 ID 分块，避免超大 IN。
    bool GetByIds(const std::vector<std::string>& media_ids,
                  std::vector<MediaObject>* media, std::string* err);

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

    bool ListVariantsByMediaIds(const std::vector<std::string>& media_ids,
                                std::vector<MediaVariant>* variants,
                                std::string* err);

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

    // ---- 视频 multipart 上传会话 ----
    // 一行对应一个 media_id 的一次 S3 multipart upload。
    bool CreateMultipartSession(const MultipartUploadSession& session,
                                std::string* err);
    bool GetMultipartSession(const std::string& media_id,
                             MultipartUploadSession* out, bool* found,
                             std::string* err);
    bool DeleteMultipartSession(const std::string& media_id,
                                std::string* err);
    // 返回所有过期会话；清理方按 media.status 决定 abort 还是只删会话行。
    bool ListExpiredMultipartSessions(
        int limit, std::vector<MultipartUploadSession>* out,
        std::string* err);

    // 软删除 init 媒体（cleanup/abort 用）。
    bool MarkDeleted(const std::string& media_id, std::string* err);

    // 没有 multipart 会话、长期停留在 init 的单次 PUT 孤儿媒体。
    bool ListStaleInitMedia(int64_t created_before_seconds, int limit,
                            std::vector<MediaObject>* out, std::string* err);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
