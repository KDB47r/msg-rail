#pragma once

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 简单用户 DAO：目前只操作单库，后续可在这里加入分库分表路由

// UserDao 是专门操作 user 表的数据库访问类（DAO = Data Access Object）
// 把 SQL 封装成 C++ 函数
class UserDao {
   public:
    explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool CreateUser(const std::string& account, const std::string& name,
                    const std::string& password, int64_t* user_id,
                    std::string* err_msg);

    bool GetUserByAccount(const std::string& account, User* user,
                          std::string* err_msg);

    bool GetUserById(int64_t user_id, User* user, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush

//