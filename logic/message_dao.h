#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

class MessageDao {
   public:
    explicit MessageDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool InsertMessage(const Message& message, std::string* err_msg);

    // 查询持久层中该会话已使用的最大 msg_seq；会话不存在时返回 0。
    bool GetMaxMessageSeq(const std::string& session_id, int64_t* max_seq,
                          std::string* err_msg);

    bool ListMessages(const std::string& session_id, int64_t anchor_seq,
                      int limit, std::vector<Message>* messages,
                      std::string* err_msg);

    // 读取 after_seq < msg_seq <= before_seq 的最近 limit 条消息，按时间正序。
    // 供气氛组只读取“激活后、触发消息之前”的上下文。
    bool ListMessagesAfter(const std::string& session_id, int64_t after_seq,
                           int64_t before_seq, int limit,
                           std::vector<Message>* messages,
                           std::string* err_msg);

    // 按会话 + 序号读取单条消息；转发时用来定位源消息。
    bool GetMessageBySeq(const std::string& session_id, int64_t msg_seq,
                         Message* message, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
