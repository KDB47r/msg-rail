#include "conversation_store.h"

#include <algorithm>

#include "logging.h"

namespace sparkpush {

ConversationStore::ConversationStore(SessionDao* session_dao,
                                     MessageDao* message_dao,
                                     UserSessionStateDao* state_dao,
                                     RedisStore* redis_store)
    : session_dao_(session_dao),
      message_dao_(message_dao),
      state_dao_(state_dao),
      redis_store_(redis_store) {}

// 获取或创建单聊会话。
bool ConversationStore::GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                                 Session* session,
                                                 std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetOrCreateSingleSession(user1, user2, session,
                                                  err_msg);
}

// 获取或创建房间会话。
bool ConversationStore::GetOrCreateRoomSession(int64_t room_id,
                                               Session* session,
                                               std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetOrCreateRoomSession(room_id, session, err_msg);
}

// 分配消息序号并写消息表，同时更新会话最后序号。
bool ConversationStore::AppendMessage(const Session& session, int64_t sender_id,
                                      const std::string& msg_type,
                                      const std::string& content_json,
                                      int64_t timestamp_ms,
                                      const std::string& client_msg_id,
                                      Message* message, std::string* err_msg) {
    if (!session_dao_ || !message_dao_) {
        if (err_msg) *err_msg = "conversation store not initialized";
        return false;
    }
    int64_t seq = 0;
    if (!session_dao_->AllocateMessageSeq(session.id, &seq, err_msg)) {
        return false;
    }
    Message msg;
    msg.session_id = session.id;
    msg.msg_seq = seq;
    msg.sender_id = sender_id;
    msg.msg_type = msg_type;
    msg.content_json = content_json;
    msg.timestamp_ms = timestamp_ms;
    msg.client_msg_id = client_msg_id;
    msg.msg_id = msg.session_id + "-" + std::to_string(seq);

    if (!message_dao_->InsertMessage(msg, err_msg)) {
        return false;
    }

    // 注：session 最新 seq 就是 IncrSessionMsgSeq 用的 key（session:{sid}:seq）
    // 本身，无需再单独 SET session:last_seq。原来的 SetSessionLastSeq 已合并。

    if (message) {
        *message = msg;
    }
    return true;
}

// read
// 用 Redis 原子递增分配会话序号，首次从 MySQL 恢复下限。
bool ConversationStore::AllocateMessageSeq(const std::string& session_id,
                                           int64_t* next_seq,
                                           std::string* err_msg) {
    if (!redis_store_ || !message_dao_ || !next_seq) {
        if (err_msg) *err_msg = "sequence allocator not initialized";
        return false;
    }

    const auto status = redis_store_->IncrSessionMsgSeq(session_id, next_seq);
    if (status == RedisStore::SessionSeqStatus::kOk) return true;
    if (status == RedisStore::SessionSeqStatus::kError) {
        if (err_msg) *err_msg = "redis sequence allocation failed";
        return false;
    }

    // 新 key 首次迁移或 Redis 数据丢失时，以 MySQL 实际已持久化的最大序号
    // 作为安全下限。初始化 Lua 会再次检查标记，保证并发首次发送不重复分配。
    int64_t persisted_seq = 0;
    if (!message_dao_->GetMaxMessageSeq(session_id, &persisted_seq, err_msg)) {
        return false;
    }
    if (!redis_store_->InitAndIncrSessionMsgSeq(session_id, persisted_seq,
                                                next_seq)) {
        if (err_msg) *err_msg = "redis sequence initialization failed";
        return false;
    }
    return true;
}

// read
// 按序号分页读取会话历史消息。
bool ConversationStore::GetHistory(const std::string& session_id,
                                   int64_t anchor_seq, int limit,
                                   std::vector<Message>* messages,
                                   std::string* err_msg) {
    if (!message_dao_) {
        if (err_msg) *err_msg = "message dao not initialized";
        return false;
    }
    return message_dao_->ListMessages(session_id, anchor_seq, limit, messages,
                                      err_msg);
}

// 更新用户在该会话的已读序号。
bool ConversationStore::MarkRead(int64_t user_id, const std::string& session_id,
                                 int64_t read_seq, std::string* err_msg) {
    if (!state_dao_) {
        if (err_msg) *err_msg = "state dao not initialized";
        return false;
    }
    if (!state_dao_->UpsertReadSeq(user_id, session_id, read_seq, err_msg)) {
        return false;
    }
    if (redis_store_) {
        if (!redis_store_->SetUserReadSeq(user_id, session_id, read_seq)) {
            LogError("SetUserReadSeq failed for user " +
                     std::to_string(user_id));
        }
    }
    return true;
}

// read
// 批量计算用户各会话的未读数和最后序号。
bool ConversationStore::GetUnread(int64_t user_id,
                                  const std::string& session_id,
                                  int64_t* unread, std::string* err_msg) {
    if (!state_dao_ || !session_dao_) {
        if (err_msg) *err_msg = "store not initialized";
        return false;
    }
    if (!unread) {
        if (err_msg) *err_msg = "unread pointer is null";
        return false;
    }

    // 一次 MGET 拿 read_seq + last_seq（两 key 都带 {sid} tag，同 slot）
    // 相比旧实现的两次独立 GET，读路径 RTT 减半。
    int64_t read_seq = 0;
    int64_t last_seq = 0;
    bool need_db_read = true;
    if (redis_store_) {
        int64_t r = -1, l = -1;
        if (redis_store_->GetUnreadSeqs(user_id, session_id, &r, &l)) {
            // seq 初始化标记存在时才信 Redis；read_seq key 不存在表示从未读过。
            read_seq = (r < 0) ? 0 : r;
            last_seq = (l < 0) ? 0 : l;
            need_db_read = false;
        }
    }
    if (need_db_read) {
        // Redis 不可用降级：MySQL 分别查两个源
        if (!state_dao_->GetReadSeq(user_id, session_id, &read_seq, err_msg)) {
            return false;
        }
        Session session;
        if (!session_dao_->GetSessionById(session_id, &session, err_msg)) {
            return false;
        }
        last_seq = session.last_msg_seq;
        // 回填 read_seq 缓存；last_seq 由 IncrSessionMsgSeq 侧统一维护，不回填
        if (redis_store_) {
            redis_store_->SetUserReadSeq(user_id, session_id, read_seq);
        }
    }

    *unread = last_seq > read_seq ? (last_seq - read_seq) : 0;
    return true;
}

// 列出用户的单聊会话及对方信息。
bool ConversationStore::ListUserSingleSessions(int64_t user_id,
                                               std::vector<Session>* sessions,
                                               std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->ListUserSingleSessions(user_id, sessions, err_msg);
}

// 按 session_id 查询会话元数据。
bool ConversationStore::GetSessionById(const std::string& session_id,
                                       Session* session, std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetSessionById(session_id, session, err_msg);
}

}  // namespace sparkpush
