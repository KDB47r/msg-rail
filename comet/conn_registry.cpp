#include "conn_registry.h"

#include <functional>

namespace sparkpush {

// 计算用户分片下标。
size_t ConnRegistry::UserShardIndex(int64_t user_id) {
    return std::hash<int64_t>{}(user_id) % kUserShardCount;
}

// 计算房间分片下标。
size_t ConnRegistry::RoomShardIndex(int64_t room_id) {
    return std::hash<int64_t>{}(room_id) % kRoomShardCount;
}

// 登记用户连接。
void ConnRegistry::AddConn(int64_t user_id, const ConnHandle& hdl) {
    auto& shard = user_shards_[UserShardIndex(user_id)];
    std::scoped_lock lock(handle_mutex_, shard.mutex);
    shard.conns[user_id].insert(hdl);
    hdl_to_uid_[hdl] = user_id;
}

// 移除连接，并在最后一条连接断开时清理房间关系。
bool ConnRegistry::RemoveConn(const ConnHandle& hdl, int64_t* out_user_id,
                              std::vector<int64_t>* left_rooms) {
    int64_t user_id = 0;
    {
        std::lock_guard<std::mutex> lock(handle_mutex_);
        auto hit = hdl_to_uid_.find(hdl);
        if (hit == hdl_to_uid_.end()) return false;
        user_id = hit->second;
        hdl_to_uid_.erase(hit);
    }
    if (out_user_id) *out_user_id = user_id;

    bool last_conn = false;
    auto& user_shard = user_shards_[UserShardIndex(user_id)];
    std::unique_lock<std::mutex> user_lock(user_shard.mutex);
    auto user_it = user_shard.conns.find(user_id);
    if (user_it == user_shard.conns.end()) return true;
    user_it->second.erase(hdl);
    if (user_it->second.empty()) {
        user_shard.conns.erase(user_it);
        last_conn = true;
    }
    if (!last_conn) return true;

    // 保持用户分片锁直到房间索引清理完成，避免同一用户重连/重新入房
    // 与最后一条连接断开交错，留下不一致的房间成员关系。
    for (auto& shard : room_shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto it = shard.users.begin(); it != shard.users.end();) {
            if (it->second.erase(user_id) > 0 && left_rooms) {
                left_rooms->push_back(it->first);
            }
            if (it->second.empty()) {
                it = shard.users.erase(it);
            } else {
                ++it;
            }
        }
    }
    return true;
}

// 登记用户加入房间。
bool ConnRegistry::JoinRoom(int64_t room_id, int64_t user_id) {
    auto& user_shard = user_shards_[UserShardIndex(user_id)];
    auto& room_shard = room_shards_[RoomShardIndex(room_id)];
    std::scoped_lock lock(user_shard.mutex, room_shard.mutex);
    return room_shard.users[room_id].insert(user_id).second;
}

// 登记用户离开房间。
bool ConnRegistry::LeaveRoom(int64_t room_id, int64_t user_id) {
    auto& shard = room_shards_[RoomShardIndex(room_id)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.users.find(room_id);
    if (it == shard.users.end() || it->second.erase(user_id) == 0) return false;
    if (it->second.empty()) shard.users.erase(it);
    return true;
}

// 按用户分片批量查询所有连接。
std::vector<ConnHandle> ConnRegistry::ConnsForUsers(
    const std::vector<int64_t>& user_ids) const {
    std::array<std::vector<int64_t>, kUserShardCount> users_by_shard;
    for (int64_t uid : user_ids) {
        users_by_shard[UserShardIndex(uid)].push_back(uid);
    }

    std::vector<ConnHandle> conns;
    for (size_t i = 0; i < kUserShardCount; ++i) {
        if (users_by_shard[i].empty()) continue;
        const auto& shard = user_shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (int64_t uid : users_by_shard[i]) {
            auto it = shard.conns.find(uid);
            if (it != shard.conns.end()) {
                conns.insert(conns.end(), it->second.begin(), it->second.end());
            }
        }
    }
    return conns;
}

// 查询房间内所有连接。
std::vector<ConnHandle> ConnRegistry::ConnsInRoom(int64_t room_id) const {
    std::vector<int64_t> users;
    {
        const auto& shard = room_shards_[RoomShardIndex(room_id)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.users.find(room_id);
        if (it == shard.users.end()) return {};
        users.assign(it->second.begin(), it->second.end());
    }
    return ConnsForUsers(users);
}

// 返回本机全部连接。
std::vector<ConnHandle> ConnRegistry::AllConns() const {
    std::vector<ConnHandle> conns;
    for (const auto& shard : user_shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (const auto& entry : shard.conns) {
            conns.insert(conns.end(), entry.second.begin(), entry.second.end());
        }
    }
    return conns;
}

// 根据连接句柄反查 user_id。
int64_t ConnRegistry::UserIdForConn(const ConnHandle& hdl) const {
    std::lock_guard<std::mutex> lock(handle_mutex_);
    auto it = hdl_to_uid_.find(hdl);
    return it == hdl_to_uid_.end() ? 0 : it->second;
}

}  // namespace sparkpush
