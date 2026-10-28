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

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
