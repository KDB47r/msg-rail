#include "danmaku_dao.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <cstring>
#include <sstream>
#include <vector>

#include "logging.h"

namespace sparkpush {

// 参考 sql/schema.sql 中的表结构：
// CREATE TABLE IF NOT EXISTS `video_danmaku` (
//   `id` BIGINT NOT NULL AUTO_INCREMENT,
//   `video_id` VARCHAR(128) NOT NULL,
//   `timeline_ms` BIGINT NOT NULL,
//   `sender_id` BIGINT NOT NULL,
//   `content_json` TEXT NOT NULL,
//   `timestamp_ms` BIGINT NOT NULL,
//   PRIMARY KEY (`id`),
//   KEY `idx_video_timeline` (`video_id`, `timeline_ms`),
//   KEY `idx_sender` (`sender_id`)
// );

bool DanmakuDao::InsertDanmaku(const std::string& video_id, int64_t timeline_ms,
                               int64_t sender_id,
                               const std::string& content_json, int64_t* out_id,
                               std::string* err_msg) {
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
        "INSERT INTO video_danmaku("
        "video_id, timeline_ms, sender_id, content_json, timestamp_ms"
        ") VALUES(?, ?, ?, ?, ?)";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LogError(e);
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LogError("InsertDanmaku prepare failed: " + e);
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[5];
    memset(bind, 0, sizeof(bind));

    unsigned long video_len = static_cast<unsigned long>(video_id.size());
    unsigned long content_len = static_cast<unsigned long>(content_json.size());
    long long timeline_param = timeline_ms;
    long long sender_param = sender_id;
    long long ts_param = static_cast<long long>(time(nullptr)) * 1000LL;

    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(video_id.data());
    bind[0].buffer_length = video_len;
    bind[0].length = &video_len;

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &timeline_param;
    bind[1].is_unsigned = 0;

    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &sender_param;
    bind[2].is_unsigned = 0;

    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = const_cast<char*>(content_json.data());
    bind[3].buffer_length = content_len;
    bind[3].length = &content_len;

    bind[4].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[4].buffer = &ts_param;
    bind[4].is_unsigned = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LogError("InsertDanmaku bind_param failed: " + e);
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LogError("InsertDanmaku execute failed: " + e);
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (out_id) {
        *out_id = static_cast<int64_t>(mysql_insert_id(conn));
    }

    mysql_stmt_close(stmt);
    return true;
}

bool DanmakuDao::ListDanmaku(const std::string& video_id,
                             std::vector<DanmakuItem>* out_list,
                             std::string* err_msg, int64_t from_ms,
                             int64_t to_ms, int limit) {
    if (!out_list) return false;
    out_list->clear();
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

    // video_id 来自用户输入，用 prepared statement 参数绑定；
    // from_ms/to_ms/limit 是数值，拼进 SQL 骨架是安全的
    std::ostringstream oss;
    oss << "SELECT id, video_id, timeline_ms, sender_id, content_json, "
           "timestamp_ms "
        << "FROM video_danmaku WHERE video_id=?";

    // 按时间范围过滤：闭开区间 [from_ms, to_ms)
    if (from_ms > 0) {
        oss << " AND timeline_ms>=" << from_ms;
    }
    if (to_ms > 0 && to_ms > from_ms) {
        oss << " AND timeline_ms<" << to_ms;
    }

    if (limit <= 0) {
        limit = 5000;
    }

    oss << " ORDER BY timeline_ms ASC LIMIT " << limit;

    std::string sql = oss.str();

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        LogError("ListDanmaku stmt_init failed");
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql.c_str(),
                           static_cast<unsigned long>(sql.size())) != 0) {
        std::string e = mysql_stmt_error(stmt);
        if (err_msg) *err_msg = e;
        LogError("ListDanmaku prepare failed: " + e);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long vid_param_len = static_cast<unsigned long>(video_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(video_id.data());
    bind[0].buffer_length = vid_param_len;
    bind[0].length = &vid_param_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        if (err_msg) *err_msg = e;
        LogError("ListDanmaku bind_param failed: " + e);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        if (err_msg) *err_msg = e;
        LogError("ListDanmaku execute failed: " + e);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        if (err_msg) *err_msg = e;
        LogError("ListDanmaku store_result failed: " + e);
        mysql_stmt_close(stmt);
        return false;
    }

    // 结果缓冲区：video_id VARCHAR(128)（utf8mb4 最多 512 字节），
    // content_json TEXT（最多 65535 字节），都留足空间避免截断
    long long id_buf = 0;
    std::vector<char> vid_buf(513, 0);
    unsigned long vid_len = 0;
    long long timeline_buf = 0;
    long long sender_buf = 0;
    std::vector<char> content_buf(65536, 0);
    unsigned long content_len = 0;
    long long ts_buf = 0;

    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = vid_buf.data();
    result[1].buffer_length = static_cast<unsigned long>(vid_buf.size());
    result[1].length = &vid_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &timeline_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &sender_buf;

    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = content_buf.data();
    result[4].buffer_length = static_cast<unsigned long>(content_buf.size());
    result[4].length = &content_len;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &ts_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        if (err_msg) *err_msg = e;
        LogError("ListDanmaku bind_result failed: " + e);
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) {
            break;
        }
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            std::string e = mysql_stmt_error(stmt);
            if (err_msg) *err_msg = e;
            LogError("ListDanmaku fetch failed: " + e);
            mysql_stmt_close(stmt);
            return false;
        }
        DanmakuItem item;
        item.id = id_buf;
        item.video_id.assign(vid_buf.data(),
                             std::min<size_t>(vid_len, vid_buf.size()));
        item.timeline_ms = timeline_buf;
        item.sender_id = sender_buf;
        item.content_json.assign(
            content_buf.data(),
            std::min<size_t>(content_len, content_buf.size()));
        item.timestamp_ms = ts_buf;
        out_list->push_back(std::move(item));
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
