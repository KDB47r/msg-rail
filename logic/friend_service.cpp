#include "friend_service.h"

namespace sparkpush {

bool FriendService::AreFriends(int64_t user_id, int64_t peer_id, bool* out,
                               std::string* err_msg) {
    if (!out) {
        if (err_msg) *err_msg = "out pointer is null";
        return false;
    }
    *out = false;
    if (user_id <= 0 || peer_id <= 0 || user_id == peer_id) return true;

    // 正向缓存命中是唯一可以直接返回 true 的快速路径。
    if (redis_ && redis_->IsFriend(user_id, peer_id)) {
        *out = true;
        return true;
    }

    bool is_friend = false;
    if (!dao_->AreFriends(user_id, peer_id, &is_friend, err_msg)) {
        return false;
    }
    if (is_friend) {
        WarmFriendCache(user_id, peer_id);
    }
    *out = is_friend;
    return true;
}

bool FriendService::WarmFriendCache(int64_t user_id, int64_t peer_id) {
    if (!redis_) return false;
    bool ok = redis_->AddFriend(user_id, peer_id);
    ok = redis_->AddFriend(peer_id, user_id) && ok;
    return ok;
}

bool FriendService::GetRelation(int64_t viewer_id, int64_t peer_id,
                                std::string* relationship, int64_t* request_id,
                                std::string* err_msg) {
    return dao_->GetRelation(viewer_id, peer_id, relationship, request_id,
                             err_msg);
}

bool FriendService::SendRequest(int64_t from_user_id, int64_t to_user_id,
                                const std::string& message, int64_t* request_id,
                                std::string* status, std::string* err_msg) {
    return dao_->SendRequest(from_user_id, to_user_id, message, request_id,
                             status, err_msg);
}

bool FriendService::RespondRequest(int64_t user_id, int64_t request_id,
                                   bool accept, int64_t* from_user_id,
                                   std::string* err_msg) {
    if (!dao_->RespondRequest(user_id, request_id, accept, from_user_id,
                              err_msg)) {
        return false;
    }
    if (accept && from_user_id && *from_user_id > 0) {
        WarmFriendCache(user_id, *from_user_id);
    }
    return true;
}

bool FriendService::ListFriends(int64_t user_id,
                                std::vector<FriendInfo>* friends,
                                std::string* err_msg) {
    return dao_->ListFriends(user_id, friends, err_msg);
}

bool FriendService::ListIncomingRequests(
    int64_t user_id, std::vector<FriendRequest>* requests,
    std::string* err_msg) {
    return dao_->ListIncomingRequests(user_id, requests, err_msg);
}

bool FriendService::ListOutgoingRequests(
    int64_t user_id, std::vector<FriendRequest>* requests,
    std::string* err_msg) {
    return dao_->ListOutgoingRequests(user_id, requests, err_msg);
}

bool FriendService::CountIncomingRequests(int64_t user_id, int64_t* count,
                                          std::string* err_msg) {
    return dao_->CountIncomingRequests(user_id, count, err_msg);
}

}  // namespace sparkpush
