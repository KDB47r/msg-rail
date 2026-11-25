#pragma once

#include <cstdint>
#include <string>

#include "mysql_pool.h"

namespace sparkpush {

// session_bot.status: 0=pending, 1=active, 2=rejected, 3=removed
struct SessionBot {
    std::string session_id;
    int64_t bot_user_id{0};
    int status{0};
    int64_t invited_by{0};
    int64_t activated_seq{0};
};

// ai_job.status: queued/running/completed/failed/aborted
struct AiJob {
    std::string job_id;
    std::string session_id;
    int64_t bot_user_id{0};
    std::string mode;  // private / shared
    std::string trigger_message_id;
    int64_t trigger_sender_id{0};
    std::string trigger_sender_name;
    std::string trigger_content;
    int64_t activated_seq{0};
    std::string status;
    std::string msg_id;
    int64_t msg_seq{0};
    std::string error_code;
    std::string error_message;
};

// P6 的 MySQL 访问：bot 绑定关系 + AI 任务幂等记录。
class AiDao {
   public:
    explicit AiDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool GetSessionBot(const std::string& session_id, int64_t bot_user_id,
                       SessionBot* out, std::string* err_msg);

    // 邀请：不存在则插入 pending；已 active 保持 active；rejected/removed
    // 重新回到 pending。
    bool UpsertPendingInvite(const std::string& session_id,
                             int64_t bot_user_id, int64_t invited_by,
                             std::string* err_msg);

    // 同意：status=1，activated_seq 为“小桁已加入”系统消息的序号。
    bool Activate(const std::string& session_id, int64_t bot_user_id,
                  int64_t activated_seq, std::string* err_msg);

    bool Reject(const std::string& session_id, int64_t bot_user_id,
                std::string* err_msg);

    bool Remove(const std::string& session_id, int64_t bot_user_id,
                std::string* err_msg);

    // 返回 true 表示 SQL 成功；created=false 表示唯一键命中（重复触发）。
    bool CreateJob(const AiJob& job, bool* created, std::string* err_msg);

    bool GetJob(const std::string& job_id, AiJob* out, std::string* err_msg);

    bool MarkJobRunning(const std::string& job_id, std::string* err_msg);

    bool CompleteJob(const std::string& job_id, const std::string& msg_id,
                     int64_t msg_seq, std::string* err_msg);

    bool FailJob(const std::string& job_id, const std::string& error_code,
                 const std::string& error_message, std::string* err_msg);

    bool AbortJob(const std::string& job_id, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
