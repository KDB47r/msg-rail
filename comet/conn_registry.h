#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>
#include <websocketpp/common/connection_hdl.hpp>

namespace sparkpush {

using ConnHandle = websocketpp::connection_hdl;

struct ConnHandleLess {
    bool operator()(const ConnHandle& a, const ConnHandle& b) const {
        return std::owner_less<ConnHandle>{}(a, b);
    }
};

class ConnRegistry {
   public:
    void AddConn(int64_t user_id, const ConnHandle& hdl);
    bool RemoveConn(const ConnHandle& hdl, int64_t* out_user_id,
                    std::vector<int64_t>* left_rooms);
    bool JoinRoom(int64_t room_id, int64_t user_id);
    bool LeaveRoom(int64_t room_id, int64_t user_id);
    std::vector<ConnHandle> ConnsForUsers(
        const std::vector<int64_t>& user_ids) const;
    std::vector<ConnHandle> ConnsInRoom(int64_t room_id) const;
    std::vector<ConnHandle> AllConns() const;
    int64_t UserIdForConn(const ConnHandle& hdl) const;

   private:
    static constexpr size_t kUserShardCount = 64;
    static constexpr size_t kRoomShardCount = 64;

    struct UserShard {
        mutable std::mutex mutex;
        std::unordered_map<int64_t, std::set<ConnHandle, ConnHandleLess>> conns;
    };
    struct RoomShard {
        mutable std::mutex mutex;
        std::unordered_map<int64_t, std::set<int64_t>> users;
    };

    static size_t UserShardIndex(int64_t user_id);
    static size_t RoomShardIndex(int64_t room_id);

    std::array<UserShard, kUserShardCount> user_shards_;
    std::array<RoomShard, kRoomShardCount> room_shards_;
    mutable std::mutex handle_mutex_;
    std::map<ConnHandle, int64_t, ConnHandleLess> hdl_to_uid_;
};

}  // namespace sparkpush

//
