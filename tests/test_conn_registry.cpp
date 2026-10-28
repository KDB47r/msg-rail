#include <algorithm>
#include <future>
#include <memory>
#include <vector>

#include "conn_registry.h"
#include "doctest.h"

using namespace sparkpush;

namespace {
ConnHandle MakeHandle(std::vector<std::shared_ptr<void>>* owners) {
    auto owner = std::make_shared<int>(0);
    ConnHandle handle(owner);
    owners->push_back(std::move(owner));
    return handle;
}
}  // namespace

TEST_CASE("ConnRegistry 大房间快照包含所有分片用户连接") {
    ConnRegistry registry;
    std::vector<std::shared_ptr<void>> owners;
    constexpr int kUsers = 512;
    for (int user_id = 1; user_id <= kUsers; ++user_id) {
        auto handle = MakeHandle(&owners);
        registry.AddConn(user_id, handle);
        CHECK(registry.JoinRoom(100, user_id));
    }

    CHECK(registry.ConnsInRoom(100).size() == kUsers);
    CHECK(registry.AllConns().size() == kUsers);
}

TEST_CASE("ConnRegistry 最后连接断开会清理全部房间") {
    ConnRegistry registry;
    std::vector<std::shared_ptr<void>> owners;
    auto first = MakeHandle(&owners);
    auto second = MakeHandle(&owners);
    registry.AddConn(42, first);
    registry.AddConn(42, second);
    registry.JoinRoom(7, 42);
    registry.JoinRoom(8, 42);

    int64_t user_id = 0;
    std::vector<int64_t> left_rooms;
    CHECK(registry.RemoveConn(first, &user_id, &left_rooms));
    CHECK(left_rooms.empty());
    CHECK(registry.RemoveConn(second, &user_id, &left_rooms));
    std::sort(left_rooms.begin(), left_rooms.end());
    CHECK(user_id == 42);
    CHECK(left_rooms == std::vector<int64_t>{7, 8});
    CHECK(registry.ConnsInRoom(7).empty());
}

TEST_CASE("ConnRegistry 不同分片可并发登记与查询") {
    ConnRegistry registry;
    constexpr int kUsers = 256;
    std::vector<std::shared_ptr<void>> owners;
    owners.reserve(kUsers);
    std::vector<ConnHandle> handles;
    handles.reserve(kUsers);
    for (int i = 0; i < kUsers; ++i) handles.push_back(MakeHandle(&owners));

    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 8; ++i) {
        tasks.push_back(std::async(std::launch::async, [&, i]() {
            for (int user_id = i + 1; user_id <= kUsers; user_id += 8) {
                registry.AddConn(user_id, handles[user_id - 1]);
                registry.JoinRoom(900, user_id);
            }
        }));
    }
    for (auto& task : tasks) task.get();

    CHECK(registry.ConnsInRoom(900).size() == kUsers);
    for (int user_id = 1; user_id <= kUsers; ++user_id) {
        CHECK(registry.UserIdForConn(handles[user_id - 1]) == user_id);
    }
}
