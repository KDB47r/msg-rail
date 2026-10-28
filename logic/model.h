#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace sparkpush {

struct User {
    int64_t id{0};
    std::string account;
    std::string name;
    std::string password_hash;
};

enum class SessionType {
    kSingle = 0,
    kGroup = 1,
    kChatroom = 2,
};

struct Session {
    std::string id;  // session_id
    SessionType type{SessionType::kSingle};
    int64_t user1_id{0};
    int64_t user2_id{0};
    int64_t group_id{0};
    int64_t last_msg_seq{0};
};

struct Message {
    std::string msg_id;
    std::string session_id;
    int64_t msg_seq{0};
    int64_t sender_id{0};
    int64_t timestamp_ms{0};
    std::string msg_type;
    std::string content_json;
    std::string client_msg_id;
};

struct UserSessionState {
    int64_t read_seq{0};
};

}  // namespace sparkpush
