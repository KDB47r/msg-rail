#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

// 好友申请记录。status: 0=pending, 1=accepted, 2=rejected。
struct FriendRequest {
    int64_t id{0};
    int64_t from_user_id{0};
    int64_t to_user_id{0};
    int status{0};
    std::string message;
    int64_t created_at_ms{0};
};

// 好友列表项，除用户资料外带好友建立时间。
struct FriendInfo {
    int64_t user_id{0};
    std::string account;
    std::string name;
    std::string avatar_media_id;
    int64_t created_at_ms{0};
};

// FriendDao 只负责 MySQL 读写；Redis 缓存和业务规则在 FriendService。
class FriendDao {
   public:
    explicit FriendDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool AreFriends(int64_t user_id, int64_t peer_id, bool* out,
                    std::string* err_msg);

    bool ListFriendIds(int64_t user_id, std::vector<int64_t>* ids,
                       std::string* err_msg);

    bool ListFriends(int64_t user_id, std::vector<FriendInfo>* friends,
                     std::string* err_msg);

    // relationship 取值：friend / pending_out / pending_in / none。
    // 仅 pending 状态返回 request_id。
    bool GetRelation(int64_t viewer_id, int64_t peer_id,
                     std::string* relationship, int64_t* request_id,
                     std::string* err_msg);

    // 发送好友申请。同方向 pending 幂等，rejected 重新置为 pending；
    // 反方向 pending 返回 false，err_msg="incoming request pending"，
    // 并通过 request_id 给出对方申请 ID。
    bool SendRequest(int64_t from_user_id, int64_t to_user_id,
                     const std::string& message, int64_t* request_id,
                     std::string* status, std::string* err_msg);

    // 同意/拒绝。校验收件人与 pending 状态，accept 时在同一事务内写双向好友。
    // from_user_id 输出申请发起者，供调用方回填 Redis 缓存。
    bool RespondRequest(int64_t user_id, int64_t request_id, bool accept,
                        int64_t* from_user_id, std::string* err_msg);

    bool ListIncomingRequests(int64_t user_id,
                              std::vector<FriendRequest>* requests,
                              std::string* err_msg);

    bool ListOutgoingRequests(int64_t user_id,
                              std::vector<FriendRequest>* requests,
                              std::string* err_msg);

    bool CountIncomingRequests(int64_t user_id, int64_t* count,
                               std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
