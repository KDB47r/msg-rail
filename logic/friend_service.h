#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "friend_dao.h"
#include "redis_store.h"

namespace sparkpush {

// 好友业务层：MySQL 是权威数据源，Redis 只缓存“是好友”的正向结果。
class FriendService {
   public:
    FriendService(FriendDao* dao, RedisStore* redis) : dao_(dao), redis_(redis) {}

    // 单聊权限热路径：先查 Redis，未命中回源 MySQL 并回填双方。
    bool AreFriends(int64_t user_id, int64_t peer_id, bool* out,
                    std::string* err_msg);

    // 主动把一对好友写入缓存（同意申请后调用）。
    bool WarmFriendCache(int64_t user_id, int64_t peer_id);

    bool GetRelation(int64_t viewer_id, int64_t peer_id,
                     std::string* relationship, int64_t* request_id,
                     std::string* err_msg);

    bool SendRequest(int64_t from_user_id, int64_t to_user_id,
                     const std::string& message, int64_t* request_id,
                     std::string* status, std::string* err_msg);

    bool RespondRequest(int64_t user_id, int64_t request_id, bool accept,
                        int64_t* from_user_id, std::string* err_msg);

    bool ListFriends(int64_t user_id, std::vector<FriendInfo>* friends,
                     std::string* err_msg);

    bool ListIncomingRequests(int64_t user_id,
                              std::vector<FriendRequest>* requests,
                              std::string* err_msg);

    bool ListOutgoingRequests(int64_t user_id,
                              std::vector<FriendRequest>* requests,
                              std::string* err_msg);

    bool CountIncomingRequests(int64_t user_id, int64_t* count,
                               std::string* err_msg);

   private:
    FriendDao* dao_;
    RedisStore* redis_;
};

}  // namespace sparkpush
