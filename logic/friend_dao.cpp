#include "friend_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {

namespace {

bool Begin(MYSQL* conn, std::string* err) {
    if (mysql_autocommit(conn, 0) != 0) {
        if (err) *err = mysql_error(conn);
        return false;
    }
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        if (err) *err = mysql_error(conn);
        mysql_autocommit(conn, 1);
        return false;
    }
    return true;
}

void Finish(MYSQL* conn, bool commit) {
    mysql_query(conn, commit ? "COMMIT" : "ROLLBACK");
    mysql_autocommit(conn, 1);
}

void BindString(MYSQL_BIND* bind, const std::string& value,
                unsigned long* len) {
    *len = static_cast<unsigned long>(value.size());
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = const_cast<char*>(value.data());
    bind->buffer_length = value.empty() ? 1 : *len;
    bind->length = len;
}

void BindInt64(MYSQL_BIND* bind, long long* value) {
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = value;
}

std::string Truncate(const char* buf, unsigned long len, size_t buf_size) {
    return std::string(buf, len < buf_size ? len : buf_size);
}

}  // namespace

bool FriendDao::AreFriends(int64_t user_id, int64_t peer_id, bool* out,
                           std::string* err_msg) {
    if (!out) {
        if (err_msg) *err_msg = "out pointer is null";
        return false;
    }
    *out = false;
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT 1 FROM friend WHERE user_id = ? AND friend_user_id = ? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[2];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    long long peer_buf = peer_id;
    BindInt64(&param[0], &uid_buf);
    BindInt64(&param[1], &peer_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    long long one = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &one;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == 0 || fetch_ret == MYSQL_DATA_TRUNCATED) {
        *out = true;
    } else if (fetch_ret != MYSQL_NO_DATA) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::ListFriendIds(int64_t user_id, std::vector<int64_t>* ids,
                              std::string* err_msg) {
    if (!ids) {
        if (err_msg) *err_msg = "ids pointer is null";
        return false;
    }
    ids->clear();
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql = "SELECT friend_user_id FROM friend WHERE user_id = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    BindInt64(&param[0], &uid_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    long long peer_buf = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &peer_buf;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        ids->push_back(peer_buf);
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::ListFriends(int64_t user_id, std::vector<FriendInfo>* friends,
                            std::string* err_msg) {
    if (!friends) {
        if (err_msg) *err_msg = "friends pointer is null";
        return false;
    }
    friends->clear();
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT u.id, u.account, u.name, u.avatar_media_id, "
        "CAST(UNIX_TIMESTAMP(f.created_at) * 1000 AS SIGNED) "
        "FROM friend f JOIN `user` u ON u.id = f.friend_user_id "
        "WHERE f.user_id = ? ORDER BY f.created_at DESC, u.id ASC";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    BindInt64(&param[0], &uid_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[5];
    memset(result, 0, sizeof(result));
    long long id_buf = 0;
    char account_buf[128] = {0};
    char name_buf[128] = {0};
    char avatar_buf[64] = {0};
    long long created_buf = 0;
    unsigned long account_len = 0;
    unsigned long name_len = 0;
    unsigned long avatar_len = 0;

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = account_buf;
    result[1].buffer_length = sizeof(account_buf);
    result[1].length = &account_len;
    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = name_buf;
    result[2].buffer_length = sizeof(name_buf);
    result[2].length = &name_len;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = avatar_buf;
    result[3].buffer_length = sizeof(avatar_buf);
    result[3].length = &avatar_len;
    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &created_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        FriendInfo info;
        info.user_id = id_buf;
        info.account = Truncate(account_buf, account_len, sizeof(account_buf));
        info.name = Truncate(name_buf, name_len, sizeof(name_buf));
        info.avatar_media_id =
            Truncate(avatar_buf, avatar_len, sizeof(avatar_buf));
        info.created_at_ms = created_buf;
        friends->push_back(std::move(info));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::GetRelation(int64_t viewer_id, int64_t peer_id,
                            std::string* relationship, int64_t* request_id,
                            std::string* err_msg) {
    if (!relationship || !request_id) {
        if (err_msg) *err_msg = "output pointer is null";
        return false;
    }
    *relationship = "none";
    *request_id = 0;

    bool is_friend = false;
    if (!AreFriends(viewer_id, peer_id, &is_friend, err_msg)) return false;
    if (is_friend) {
        *relationship = "friend";
        return true;
    }

    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT id, from_user_id, to_user_id, status FROM friend_request "
        "WHERE (from_user_id = ? AND to_user_id = ?) "
        "   OR (from_user_id = ? AND to_user_id = ?) "
        "ORDER BY updated_at DESC LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[4];
    memset(param, 0, sizeof(param));
    long long viewer_buf = viewer_id;
    long long peer_buf = peer_id;
    BindInt64(&param[0], &viewer_buf);
    BindInt64(&param[1], &peer_buf);
    BindInt64(&param[2], &peer_buf);
    BindInt64(&param[3], &viewer_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    long long id_buf = 0;
    long long from_buf = 0;
    long long to_buf = 0;
    int status_buf = 0;
    MYSQL_BIND result[4];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &from_buf;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &to_buf;
    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &status_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == 0 || fetch_ret == MYSQL_DATA_TRUNCATED) {
        // rejected 视为可重新申请；accepted 但 friend 行缺失的一致性异常
        // 也按 none 处理，用户仍可重新走申请流程。
        if (status_buf == 0) {
            *relationship = (from_buf == viewer_id) ? "pending_out"
                                                    : "pending_in";
            *request_id = id_buf;
        }
    } else if (fetch_ret != MYSQL_NO_DATA) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::SendRequest(int64_t from_user_id, int64_t to_user_id,
                            const std::string& message, int64_t* request_id,
                            std::string* status, std::string* err_msg) {
    if (!request_id || !status) {
        if (err_msg) *err_msg = "output pointer is null";
        return false;
    }
    *request_id = 0;
    *status = "pending";
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    if (!Begin(conn, err_msg)) return false;

    auto rollback = [&](const std::string& msg) {
        Finish(conn, false);
        if (err_msg) *err_msg = msg;
        return false;
    };

    // 1. 同方向已有记录：pending 幂等，rejected 重新激活。
    {
        const char* sql =
            "SELECT id, status FROM friend_request "
            "WHERE from_user_id = ? AND to_user_id = ? FOR UPDATE";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (!stmt) return rollback("mysql_stmt_init failed");
        if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND param[2];
        memset(param, 0, sizeof(param));
        long long from_buf = from_user_id;
        long long to_buf = to_user_id;
        BindInt64(&param[0], &from_buf);
        BindInt64(&param[1], &to_buf);
        if (mysql_stmt_bind_param(stmt, param) != 0 ||
            mysql_stmt_execute(stmt) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        long long id_buf = 0;
        int status_buf = 0;
        MYSQL_BIND result[2];
        memset(result, 0, sizeof(result));
        result[0].buffer_type = MYSQL_TYPE_LONGLONG;
        result[0].buffer = &id_buf;
        result[1].buffer_type = MYSQL_TYPE_LONG;
        result[1].buffer = &status_buf;
        if (mysql_stmt_bind_result(stmt, result) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        int fetch_ret = mysql_stmt_fetch(stmt);
        bool has_row = (fetch_ret == 0 || fetch_ret == MYSQL_DATA_TRUNCATED);
        mysql_stmt_close(stmt);
        if (has_row) {
            *request_id = id_buf;
            if (status_buf == 0) {
                Finish(conn, true);
                return true;
            }
            if (status_buf == 1) {
                *status = "accepted";
                Finish(conn, true);
                return true;
            }
            // rejected -> pending
            const char* update_sql =
                "UPDATE friend_request SET status = 0, message = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?";
            MYSQL_STMT* update_stmt = mysql_stmt_init(conn);
            if (!update_stmt) return rollback("mysql_stmt_init failed");
            if (mysql_stmt_prepare(update_stmt, update_sql,
                                   strlen(update_sql)) != 0) {
                std::string e = mysql_stmt_error(update_stmt);
                mysql_stmt_close(update_stmt);
                return rollback(e);
            }
            MYSQL_BIND update_param[2];
            memset(update_param, 0, sizeof(update_param));
            unsigned long message_len = 0;
            BindString(&update_param[0], message, &message_len);
            long long id_value = id_buf;
            BindInt64(&update_param[1], &id_value);
            if (mysql_stmt_bind_param(update_stmt, update_param) != 0 ||
                mysql_stmt_execute(update_stmt) != 0) {
                std::string e = mysql_stmt_error(update_stmt);
                mysql_stmt_close(update_stmt);
                return rollback(e);
            }
            mysql_stmt_close(update_stmt);
            Finish(conn, true);
            return true;
        }
    }

    // 2. 反方向 pending：不能直接再插一条，返回对方申请 ID。
    {
        const char* sql =
            "SELECT id, status FROM friend_request "
            "WHERE from_user_id = ? AND to_user_id = ? FOR UPDATE";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (!stmt) return rollback("mysql_stmt_init failed");
        if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND param[2];
        memset(param, 0, sizeof(param));
        long long peer_buf = to_user_id;
        long long viewer_buf = from_user_id;
        BindInt64(&param[0], &peer_buf);
        BindInt64(&param[1], &viewer_buf);
        if (mysql_stmt_bind_param(stmt, param) != 0 ||
            mysql_stmt_execute(stmt) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        long long id_buf = 0;
        int status_buf = 0;
        MYSQL_BIND result[2];
        memset(result, 0, sizeof(result));
        result[0].buffer_type = MYSQL_TYPE_LONGLONG;
        result[0].buffer = &id_buf;
        result[1].buffer_type = MYSQL_TYPE_LONG;
        result[1].buffer = &status_buf;
        if (mysql_stmt_bind_result(stmt, result) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        int fetch_ret = mysql_stmt_fetch(stmt);
        bool has_row = (fetch_ret == 0 || fetch_ret == MYSQL_DATA_TRUNCATED);
        mysql_stmt_close(stmt);
        if (has_row && status_buf == 0) {
            *request_id = id_buf;
            Finish(conn, false);
            if (err_msg) *err_msg = "incoming request pending";
            return false;
        }
    }

    // 3. 插入新申请。唯一键兜底并发重复提交。
    {
        const char* sql =
            "INSERT INTO friend_request(from_user_id, to_user_id, status, "
            "message) VALUES(?, ?, 0, ?) "
            "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), status = 0, "
            "message = VALUES(message), updated_at = CURRENT_TIMESTAMP";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (!stmt) return rollback("mysql_stmt_init failed");
        if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND param[3];
        memset(param, 0, sizeof(param));
        long long from_buf = from_user_id;
        long long to_buf = to_user_id;
        BindInt64(&param[0], &from_buf);
        BindInt64(&param[1], &to_buf);
        unsigned long message_len = 0;
        BindString(&param[2], message, &message_len);
        if (mysql_stmt_bind_param(stmt, param) != 0 ||
            mysql_stmt_execute(stmt) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        *request_id = static_cast<int64_t>(mysql_stmt_insert_id(stmt));
        mysql_stmt_close(stmt);
    }

    Finish(conn, true);
    return true;
}

bool FriendDao::RespondRequest(int64_t user_id, int64_t request_id,
                               bool accept, int64_t* from_user_id,
                               std::string* err_msg) {
    if (from_user_id) *from_user_id = 0;
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    if (!Begin(conn, err_msg)) return false;

    auto rollback = [&](const std::string& msg) {
        Finish(conn, false);
        if (err_msg) *err_msg = msg;
        return false;
    };

    // 1. 锁住申请行并校验归属与状态。
    long long from_buf = 0;
    long long to_buf = 0;
    int status_buf = 0;
    {
        const char* sql =
            "SELECT from_user_id, to_user_id, status FROM friend_request "
            "WHERE id = ? FOR UPDATE";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (!stmt) return rollback("mysql_stmt_init failed");
        if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND param[1];
        memset(param, 0, sizeof(param));
        long long id_buf = request_id;
        BindInt64(&param[0], &id_buf);
        if (mysql_stmt_bind_param(stmt, param) != 0 ||
            mysql_stmt_execute(stmt) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND result[3];
        memset(result, 0, sizeof(result));
        result[0].buffer_type = MYSQL_TYPE_LONGLONG;
        result[0].buffer = &from_buf;
        result[1].buffer_type = MYSQL_TYPE_LONGLONG;
        result[1].buffer = &to_buf;
        result[2].buffer_type = MYSQL_TYPE_LONG;
        result[2].buffer = &status_buf;
        if (mysql_stmt_bind_result(stmt, result) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        int fetch_ret = mysql_stmt_fetch(stmt);
        bool has_row = (fetch_ret == 0 || fetch_ret == MYSQL_DATA_TRUNCATED);
        mysql_stmt_close(stmt);
        if (!has_row) return rollback("request not found");
    }
    if (to_buf != user_id) return rollback("not request recipient");
    if (status_buf != 0) return rollback("request already handled");

    // 2. 更新申请状态。
    {
        const char* sql =
            "UPDATE friend_request SET status = ? WHERE id = ? AND status = 0";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (!stmt) return rollback("mysql_stmt_init failed");
        if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        MYSQL_BIND param[2];
        memset(param, 0, sizeof(param));
        int status_value = accept ? 1 : 2;
        long long id_value = request_id;
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &status_value;
        BindInt64(&param[1], &id_value);
        if (mysql_stmt_bind_param(stmt, param) != 0 ||
            mysql_stmt_execute(stmt) != 0) {
            std::string e = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return rollback(e);
        }
        if (mysql_stmt_affected_rows(stmt) == 0) {
            mysql_stmt_close(stmt);
            return rollback("request already handled");
        }
        mysql_stmt_close(stmt);
    }

    // 3. 同意时写双向好友关系。
    if (accept) {
        const char* sql =
            "INSERT IGNORE INTO friend(user_id, friend_user_id) VALUES(?, ?)";
        for (int i = 0; i < 2; ++i) {
            MYSQL_STMT* stmt = mysql_stmt_init(conn);
            if (!stmt) return rollback("mysql_stmt_init failed");
            if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
                std::string e = mysql_stmt_error(stmt);
                mysql_stmt_close(stmt);
                return rollback(e);
            }
            MYSQL_BIND param[2];
            memset(param, 0, sizeof(param));
            long long user_buf = (i == 0) ? user_id : from_buf;
            long long friend_buf = (i == 0) ? from_buf : user_id;
            BindInt64(&param[0], &user_buf);
            BindInt64(&param[1], &friend_buf);
            if (mysql_stmt_bind_param(stmt, param) != 0 ||
                mysql_stmt_execute(stmt) != 0) {
                std::string e = mysql_stmt_error(stmt);
                mysql_stmt_close(stmt);
                return rollback(e);
            }
            mysql_stmt_close(stmt);
        }
    }

    Finish(conn, true);
    if (from_user_id) *from_user_id = from_buf;
    return true;
}

bool FriendDao::ListIncomingRequests(int64_t user_id,
                                     std::vector<FriendRequest>* requests,
                                     std::string* err_msg) {
    if (!requests) {
        if (err_msg) *err_msg = "requests pointer is null";
        return false;
    }
    requests->clear();
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT id, from_user_id, to_user_id, status, message, "
        "CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) "
        "FROM friend_request WHERE to_user_id = ? AND status = 0 "
        "ORDER BY created_at DESC, id DESC";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    BindInt64(&param[0], &uid_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));
    long long id_buf = 0;
    long long from_buf = 0;
    long long to_buf = 0;
    int status_buf = 0;
    char message_buf[512] = {0};
    unsigned long message_len = 0;
    long long created_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &from_buf;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &to_buf;
    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &status_buf;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = message_buf;
    result[4].buffer_length = sizeof(message_buf);
    result[4].length = &message_len;
    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &created_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        FriendRequest request;
        request.id = id_buf;
        request.from_user_id = from_buf;
        request.to_user_id = to_buf;
        request.status = status_buf;
        request.message =
            Truncate(message_buf, message_len, sizeof(message_buf));
        request.created_at_ms = created_buf;
        requests->push_back(std::move(request));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::ListOutgoingRequests(int64_t user_id,
                                     std::vector<FriendRequest>* requests,
                                     std::string* err_msg) {
    if (!requests) {
        if (err_msg) *err_msg = "requests pointer is null";
        return false;
    }
    requests->clear();
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT id, from_user_id, to_user_id, status, message, "
        "CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) "
        "FROM friend_request WHERE from_user_id = ? AND status = 0 "
        "ORDER BY created_at DESC, id DESC";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    BindInt64(&param[0], &uid_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));
    long long id_buf = 0;
    long long from_buf = 0;
    long long to_buf = 0;
    int status_buf = 0;
    char message_buf[512] = {0};
    unsigned long message_len = 0;
    long long created_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &from_buf;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &to_buf;
    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &status_buf;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = message_buf;
    result[4].buffer_length = sizeof(message_buf);
    result[4].length = &message_len;
    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &created_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        FriendRequest request;
        request.id = id_buf;
        request.from_user_id = from_buf;
        request.to_user_id = to_buf;
        request.status = status_buf;
        request.message =
            Truncate(message_buf, message_len, sizeof(message_buf));
        request.created_at_ms = created_buf;
        requests->push_back(std::move(request));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool FriendDao::CountIncomingRequests(int64_t user_id, int64_t* count,
                                      std::string* err_msg) {
    if (!count) {
        if (err_msg) *err_msg = "count pointer is null";
        return false;
    }
    *count = 0;
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT COUNT(*) FROM friend_request WHERE to_user_id = ? AND status = 0";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_buf = user_id;
    BindInt64(&param[0], &uid_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    long long count_buf = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &count_buf;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    *count = count_buf;
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
