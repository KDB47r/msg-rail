#pragma once

#include <string>

#include "mysql_pool.h"

namespace sparkpush {

class UserSessionStateDao {
   public:
    explicit UserSessionStateDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool UpsertReadSeq(int64_t user_id, const std::string& session_id,
                       int64_t read_seq, std::string* err_msg);

    bool GetReadSeq(int64_t user_id, const std::string& session_id,
                    int64_t* read_seq, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
