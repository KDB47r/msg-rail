#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "ai_dao.h"
#include "config.h"
#include "conversation_store.h"
#include "kafka_producer.h"
#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {

// P6 AI 编排的 Logic 侧入口。
//
// 热路径只做三件事：判断接收方/mention 是否 bot、查一次 Redis 缓存、
// 投递 ai_trigger。历史读取、LLM 调用、流式输出都在 AI Worker。
class AiService {
   public:
    AiService(const Config& cfg, AiDao* ai_dao, UserDao* user_dao,
              ConversationStore* store, RedisStore* redis_store,
              KafkaProducer* push_producer,
              KafkaProducer* trigger_producer);

    bool IsPrivateBot(int64_t user_id) const;
    bool IsSharedBot(int64_t user_id) const;
    bool IsBot(int64_t user_id) const;

    // 用户消息已成功进入发送链路后调用。AI 触发失败不应让用户消息失败，
    // 但会在 ai_job/前端流里暴露错误。
    bool TriggerPrivate(const std::string& session_id, int64_t from_user,
                        int64_t to_user, const std::string& msg_id,
                        const std::string& content_json);

    bool MaybeTriggerShared(const std::string& session_id, int64_t from_user,
                            int64_t to_user, const std::string& msg_id,
                            const std::string& content_json);

    // 气氛组邀请/同意/拒绝/移除。
    bool Invite(const std::string& session_id, int64_t inviter,
                std::string* err_msg);
    bool Respond(const std::string& session_id, int64_t responder, bool accept,
                 std::string* err_msg);
    bool Remove(const std::string& session_id, int64_t user_id,
                std::string* err_msg);
    bool GetStatus(const std::string& session_id, int64_t requester,
                   nlohmann::json* out, std::string* err_msg);

    // AI Worker 内部接口。
    bool GetContext(const std::string& job_id, nlohmann::json* out,
                    std::string* err_msg);
    bool PushDelta(const std::string& job_id, const std::string& delta,
                   std::string* err_msg);
    bool Finalize(const std::string& job_id, const std::string& content,
                  std::string* err_msg);
    bool Fail(const std::string& job_id, const std::string& code,
              const std::string& message, std::string* err_msg);
    bool Stop(const std::string& job_id, int64_t user_id,
              std::string* err_msg);

   private:
    bool GetSessionBotActive(const std::string& session_id,
                             int64_t bot_user_id, bool* active,
                             SessionBot* row);
    bool SendTrigger(const AiJob& job);
    bool SendChatMessage(const std::string& session_id, int64_t sender_id,
                         const std::string& msg_type,
                         const std::string& content_json, int64_t msg_seq,
                         const std::string& msg_id,
                         const std::vector<int64_t>& targets,
                         bool need_persist, std::string* err_msg);
    bool SendSystemMessage(const std::string& session_id, int64_t bot_user_id,
                           const std::string& content_json,
                           const std::vector<int64_t>& targets,
                           int64_t msg_seq, std::string* err_msg);
    bool ResolveTargets(const AiJob& job, std::vector<int64_t>* targets,
                        std::string* err_msg) const;
    bool CreateAndQueueJob(const AiJob& job, bool* created);
    AiJob BuildJob(const std::string& mode, const std::string& session_id,
                   int64_t bot_user_id, const std::string& trigger_msg_id,
                   int64_t trigger_sender_id,
                   const std::string& trigger_sender_name,
                   const std::string& trigger_content,
                   int64_t activated_seq) const;
    std::string SenderName(int64_t user_id) const;
    static std::string ExtractText(const std::string& content_json);
    static bool HasMention(const std::string& content_json,
                           int64_t bot_user_id, const std::string& text);
    static bool ParseSingleUsers(const std::string& session_id, int64_t* user1,
                                 int64_t* user2);

    Config cfg_;
    AiDao* ai_dao_{nullptr};
    UserDao* user_dao_{nullptr};
    ConversationStore* store_{nullptr};
    RedisStore* redis_store_{nullptr};
    KafkaProducer* push_producer_{nullptr};
    KafkaProducer* trigger_producer_{nullptr};
};

}  // namespace sparkpush
