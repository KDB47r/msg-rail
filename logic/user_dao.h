#pragma once

#include <vector>

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

    // 批量查询用户资料，供会话列表/历史消息聚合头像和昵称。
    bool ListUsersByIds(const std::vector<int64_t>& user_ids,
                        std::vector<User>* users, std::string* err_msg);

    // 昵称精确搜索；调用方负责限制 limit/offset 范围。
    bool SearchUsersByName(const std::string& name, int limit, int offset,
                           std::vector<User>* users, std::string* err_msg);

    // 更新昵称与头像媒体引用（调用方先读取并合并当前值）。
    bool UpdateProfile(int64_t user_id, const std::string& name,
                       const std::string& avatar_media_id,
                       std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush

//