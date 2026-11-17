#include "media_dao.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>

namespace sparkpush {

namespace {

void BindString(MYSQL_BIND* bind, const std::string& value,
                unsigned long* len) {
    *len = static_cast<unsigned long>(value.size());
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = const_cast<char*>(value.data());
    // 空串也给出非 0 的 buffer_length，避免部分 MySQL 版本把它当作 NULL。
    bind->buffer_length = value.empty() ? 1 : *len;
    bind->length = len;
}

void BindInt64(MYSQL_BIND* bind, long long* value) {
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = value;
}

void BindInt(MYSQL_BIND* bind, int* value) {
    bind->buffer_type = MYSQL_TYPE_LONG;
    bind->buffer = value;
}

std::string Truncate(const char* buf, unsigned long len, size_t buf_size) {
    return std::string(buf, std::min<size_t>(len, buf_size));
}

// 只执行、不读结果集。用于 INSERT/UPDATE/DELETE。
bool ExecuteUpdate(MYSQL* conn, const char* sql, MYSQL_BIND* params,
                   my_ulonglong* affected, std::string* err) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err) *err = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0 ||
        (params && mysql_stmt_bind_param(stmt, params) != 0) ||
        mysql_stmt_execute(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (affected) *affected = mysql_stmt_affected_rows(stmt);
    mysql_stmt_close(stmt);
    return true;
}

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

// 同一媒体可能有多个变体（图片 3 档、视频 1 张封面），集中 upsert。
bool UpsertVariants(MYSQL* conn, const std::string& media_id,
                    const std::vector<MediaVariant>& variants,
                    std::string* err) {
    static const char* kSql =
        "INSERT INTO media_variant(media_id, variant, format, width, height, "
        "size_bytes, bitrate_kbps, object_key) VALUES(?,?,?,?,?,?,?,?) "
        "ON DUPLICATE KEY UPDATE format=VALUES(format), width=VALUES(width), "
        "height=VALUES(height), size_bytes=VALUES(size_bytes), "
        "bitrate_kbps=VALUES(bitrate_kbps), object_key=VALUES(object_key)";
    for (const auto& v : variants) {
        unsigned long media_len = 0, variant_len = 0, format_len = 0,
                      key_len = 0;
        MYSQL_BIND bind[8];
        std::memset(bind, 0, sizeof(bind));
        BindString(&bind[0], media_id, &media_len);
        BindString(&bind[1], v.variant, &variant_len);
        BindString(&bind[2], v.format, &format_len);
        int w = v.width, h = v.height, br = v.bitrate_kbps;
        BindInt(&bind[3], &w);
        BindInt(&bind[4], &h);
        long long size = v.size_bytes;
        BindInt64(&bind[5], &size);
        BindInt(&bind[6], &br);
        BindString(&bind[7], v.object_key, &key_len);
        if (!ExecuteUpdate(conn, kSql, bind, nullptr, err)) return false;
    }
    return true;
}

bool MarkTaskDone(MYSQL* conn, const std::string& media_id,
                  const std::string& task_type, std::string* err) {
    static const char* kSql =
        "UPDATE media_task SET status='done', locked_by='', locked_at=NULL "
        "WHERE media_id=? AND task_type=?";
    unsigned long media_len = 0, type_len = 0;
    MYSQL_BIND bind[2];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media_id, &media_len);
    BindString(&bind[1], task_type, &type_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

}  // namespace

bool MediaDao::Create(const MediaObject& media, std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "INSERT INTO media_object(media_id, owner_id, kind, status, "
        "original_name, mime_type, size_bytes, sha256, storage_bucket, "
        "object_key) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    unsigned long media_len = 0, kind_len = 0, status_len = 0, name_len = 0,
                  mime_len = 0, sha_len = 0, bucket_len = 0, key_len = 0;
    MYSQL_BIND bind[10];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media.media_id, &media_len);
    long long owner = media.owner_id;
    BindInt64(&bind[1], &owner);
    BindString(&bind[2], media.kind, &kind_len);
    BindString(&bind[3], media.status, &status_len);
    BindString(&bind[4], media.original_name, &name_len);
    BindString(&bind[5], media.mime_type, &mime_len);
    long long size = media.size_bytes;
    BindInt64(&bind[6], &size);
    BindString(&bind[7], media.sha256, &sha_len);
    BindString(&bind[8], media.storage_bucket, &bucket_len);
    BindString(&bind[9], media.object_key, &key_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::GetById(const std::string& media_id, MediaObject* media,
                       std::string* err) {
    if (!pool_ || !media) {
        if (err) *err = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "SELECT id, media_id, owner_id, kind, status, original_name, "
        "mime_type, size_bytes, sha256, width, height, duration_ms, "
        "blur_hash, dominant_color, storage_bucket, object_key, etag, "
        "session_id, message_id, error_message "
        "FROM media_object WHERE media_id=? LIMIT 1";
    unsigned long media_param_len = 0;
    MYSQL_BIND param[1];
    std::memset(param, 0, sizeof(param));
    BindString(&param[0], media_id, &media_param_len);

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err) *err = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, kSql, std::strlen(kSql)) != 0 ||
        mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    long long id_buf = 0, owner_buf = 0, size_buf = 0, duration_buf = 0;
    int width_buf = 0, height_buf = 0;
    char media_id_buf[64], kind_buf[32], status_buf[32], name_buf[512],
        mime_buf[128], sha_buf[128], blur_buf[256], color_buf[32],
        bucket_buf[128], key_buf[1024], etag_buf[256], session_buf[256],
        message_buf[256], error_buf[1024];
    unsigned long media_id_len = 0, kind_len = 0, status_len = 0,
                  name_len = 0, mime_len = 0, sha_len = 0, blur_len = 0,
                  color_len = 0, bucket_len = 0, key_len = 0, etag_len = 0,
                  session_len = 0, message_len = 0, error_len = 0;

    MYSQL_BIND result[20];
    std::memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = media_id_buf;
    result[1].buffer_length = sizeof(media_id_buf);
    result[1].length = &media_id_len;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &owner_buf;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = kind_buf;
    result[3].buffer_length = sizeof(kind_buf);
    result[3].length = &kind_len;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = status_buf;
    result[4].buffer_length = sizeof(status_buf);
    result[4].length = &status_len;
    result[5].buffer_type = MYSQL_TYPE_STRING;
    result[5].buffer = name_buf;
    result[5].buffer_length = sizeof(name_buf);
    result[5].length = &name_len;
    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = mime_buf;
    result[6].buffer_length = sizeof(mime_buf);
    result[6].length = &mime_len;
    result[7].buffer_type = MYSQL_TYPE_LONGLONG;
    result[7].buffer = &size_buf;
    result[8].buffer_type = MYSQL_TYPE_STRING;
    result[8].buffer = sha_buf;
    result[8].buffer_length = sizeof(sha_buf);
    result[8].length = &sha_len;
    result[9].buffer_type = MYSQL_TYPE_LONG;
    result[9].buffer = &width_buf;
    result[10].buffer_type = MYSQL_TYPE_LONG;
    result[10].buffer = &height_buf;
    result[11].buffer_type = MYSQL_TYPE_LONGLONG;
    result[11].buffer = &duration_buf;
    result[12].buffer_type = MYSQL_TYPE_STRING;
    result[12].buffer = blur_buf;
    result[12].buffer_length = sizeof(blur_buf);
    result[12].length = &blur_len;
    result[13].buffer_type = MYSQL_TYPE_STRING;
    result[13].buffer = color_buf;
    result[13].buffer_length = sizeof(color_buf);
    result[13].length = &color_len;
    result[14].buffer_type = MYSQL_TYPE_STRING;
    result[14].buffer = bucket_buf;
    result[14].buffer_length = sizeof(bucket_buf);
    result[14].length = &bucket_len;
    result[15].buffer_type = MYSQL_TYPE_STRING;
    result[15].buffer = key_buf;
    result[15].buffer_length = sizeof(key_buf);
    result[15].length = &key_len;
    result[16].buffer_type = MYSQL_TYPE_STRING;
    result[16].buffer = etag_buf;
    result[16].buffer_length = sizeof(etag_buf);
    result[16].length = &etag_len;
    result[17].buffer_type = MYSQL_TYPE_STRING;
    result[17].buffer = session_buf;
    result[17].buffer_length = sizeof(session_buf);
    result[17].length = &session_len;
    result[18].buffer_type = MYSQL_TYPE_STRING;
    result[18].buffer = message_buf;
    result[18].buffer_length = sizeof(message_buf);
    result[18].length = &message_len;
    result[19].buffer_type = MYSQL_TYPE_STRING;
    result[19].buffer = error_buf;
    result[19].buffer_length = sizeof(error_buf);
    result[19].length = &error_len;

    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    const int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == MYSQL_NO_DATA) {
        mysql_stmt_close(stmt);
        if (err) *err = "media not found";
        return false;
    }
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    media->id = id_buf;
    media->media_id = Truncate(media_id_buf, media_id_len, sizeof(media_id_buf));
    media->owner_id = owner_buf;
    media->kind = Truncate(kind_buf, kind_len, sizeof(kind_buf));
    media->status = Truncate(status_buf, status_len, sizeof(status_buf));
    media->original_name = Truncate(name_buf, name_len, sizeof(name_buf));
    media->mime_type = Truncate(mime_buf, mime_len, sizeof(mime_buf));
    media->size_bytes = size_buf;
    media->sha256 = Truncate(sha_buf, sha_len, sizeof(sha_buf));
    media->width = width_buf;
    media->height = height_buf;
    media->duration_ms = duration_buf;
    media->blur_hash = Truncate(blur_buf, blur_len, sizeof(blur_buf));
    media->dominant_color = Truncate(color_buf, color_len, sizeof(color_buf));
    media->storage_bucket = Truncate(bucket_buf, bucket_len, sizeof(bucket_buf));
    media->object_key = Truncate(key_buf, key_len, sizeof(key_buf));
    media->etag = Truncate(etag_buf, etag_len, sizeof(etag_buf));
    media->session_id = Truncate(session_buf, session_len, sizeof(session_buf));
    media->message_id = Truncate(message_buf, message_len, sizeof(message_buf));
    media->error_message = Truncate(error_buf, error_len, sizeof(error_buf));
    mysql_stmt_close(stmt);
    return true;
}

bool MediaDao::MarkUploaded(const std::string& media_id, int64_t size_bytes,
                            const std::string& etag, std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_object SET size_bytes=?, etag=?, status='uploaded' "
        "WHERE media_id=? AND status IN ('init','uploaded')";
    unsigned long etag_len = 0, media_len = 0;
    MYSQL_BIND bind[3];
    std::memset(bind, 0, sizeof(bind));
    long long size = size_bytes;
    BindInt64(&bind[0], &size);
    BindString(&bind[1], etag, &etag_len);
    BindString(&bind[2], media_id, &media_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::MarkReady(const std::string& media_id, std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_object SET status='ready', error_message='' "
        "WHERE media_id=? AND status='uploaded'";
    unsigned long media_len = 0;
    MYSQL_BIND bind[1];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media_id, &media_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::EnqueueTask(const std::string& media_id,
                           const std::string& task_type, int max_attempts,
                           std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    if (task_type != "process_image" && task_type != "process_video") {
        if (err) *err = "unsupported media task_type: " + task_type;
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "INSERT INTO media_task(media_id, task_type, status, attempts, "
        "max_attempts, next_run_at) VALUES(?, ?, 'pending', 0, ?, "
        "NOW()) ON DUPLICATE KEY UPDATE media_id=VALUES(media_id)";
    unsigned long media_len = 0, type_len = 0;
    MYSQL_BIND bind[3];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media_id, &media_len);
    BindString(&bind[1], task_type, &type_len);
    int max_attempts_buf = max_attempts;
    BindInt(&bind[2], &max_attempts_buf);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::ListVariants(const std::string& media_id,
                            std::vector<MediaVariant>* variants,
                            std::string* err) {
    if (!pool_ || !variants) {
        if (err) *err = "invalid arguments";
        return false;
    }
    variants->clear();
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "SELECT media_id, variant, format, width, height, size_bytes, "
        "bitrate_kbps, object_key FROM media_variant WHERE media_id=? "
        "ORDER BY id";
    unsigned long media_param_len = 0;
    MYSQL_BIND param[1];
    std::memset(param, 0, sizeof(param));
    BindString(&param[0], media_id, &media_param_len);

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err) *err = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, kSql, std::strlen(kSql)) != 0 ||
        mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    char media_buf[64], variant_buf[64], format_buf[32], key_buf[1024];
    unsigned long media_len = 0, variant_len = 0, format_len = 0, key_len = 0;
    long long size_buf = 0;
    int width_buf = 0, height_buf = 0, bitrate_buf = 0;
    MYSQL_BIND result[8];
    std::memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = media_buf;
    result[0].buffer_length = sizeof(media_buf);
    result[0].length = &media_len;
    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = variant_buf;
    result[1].buffer_length = sizeof(variant_buf);
    result[1].length = &variant_len;
    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = format_buf;
    result[2].buffer_length = sizeof(format_buf);
    result[2].length = &format_len;
    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &width_buf;
    result[4].buffer_type = MYSQL_TYPE_LONG;
    result[4].buffer = &height_buf;
    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &size_buf;
    result[6].buffer_type = MYSQL_TYPE_LONG;
    result[6].buffer = &bitrate_buf;
    result[7].buffer_type = MYSQL_TYPE_STRING;
    result[7].buffer = key_buf;
    result[7].buffer_length = sizeof(key_buf);
    result[7].length = &key_len;

    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        const int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err) *err = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        MediaVariant v;
        v.media_id = Truncate(media_buf, media_len, sizeof(media_buf));
        v.variant = Truncate(variant_buf, variant_len, sizeof(variant_buf));
        v.format = Truncate(format_buf, format_len, sizeof(format_buf));
        v.width = width_buf;
        v.height = height_buf;
        v.size_bytes = size_buf;
        v.bitrate_kbps = bitrate_buf;
        v.object_key = Truncate(key_buf, key_len, sizeof(key_buf));
        variants->push_back(std::move(v));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool MediaDao::BindToMessage(const std::string& media_id, int64_t owner_id,
                             const std::string& session_id,
                             const std::string& message_id, bool* bound,
                             std::string* err) {
    if (bound) *bound = false;
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_object SET session_id=?, message_id=? "
        "WHERE media_id=? AND owner_id=? AND status='ready' "
        "AND session_id='' AND message_id=''";
    unsigned long session_len = 0, message_len = 0, media_len = 0;
    MYSQL_BIND bind[4];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], session_id, &session_len);
    BindString(&bind[1], message_id, &message_len);
    BindString(&bind[2], media_id, &media_len);
    long long owner = owner_id;
    BindInt64(&bind[3], &owner);
    my_ulonglong affected = 0;
    if (!ExecuteUpdate(conn, kSql, bind, &affected, err)) return false;
    if (bound) *bound = (affected > 0);
    return true;
}

bool MediaDao::UnbindMessage(const std::string& media_id, int64_t owner_id,
                             const std::string& message_id,
                             std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_object SET session_id='', message_id='' "
        "WHERE media_id=? AND owner_id=? AND message_id=?";
    unsigned long media_len = 0, message_len = 0;
    MYSQL_BIND bind[3];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media_id, &media_len);
    long long owner = owner_id;
    BindInt64(&bind[1], &owner);
    BindString(&bind[2], message_id, &message_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::ClaimNextTask(const std::string& worker_id,
                             const std::string& task_type, MediaTask* task,
                             bool* found, std::string* err) {
    if (found) *found = false;
    if (!pool_ || !task) {
        if (err) *err = "invalid arguments";
        return false;
    }
    if (task_type != "process_image" && task_type != "process_video") {
        if (err) *err = "unsupported task_type: " + task_type;
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    if (!Begin(conn, err)) return false;

    static const char* kSelect =
        "SELECT id, media_id, task_type, attempts, max_attempts "
        "FROM media_task WHERE task_type=? AND status='pending' "
        "AND next_run_at<=NOW() ORDER BY next_run_at, id LIMIT 1 "
        "FOR UPDATE SKIP LOCKED";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err) *err = "mysql_stmt_init failed";
        Finish(conn, false);
        return false;
    }
    unsigned long type_param_len = 0;
    MYSQL_BIND param[1];
    std::memset(param, 0, sizeof(param));
    BindString(&param[0], task_type, &type_param_len);
    if (mysql_stmt_prepare(stmt, kSelect, std::strlen(kSelect)) != 0 ||
        mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        Finish(conn, false);
        return false;
    }

    long long id_buf = 0;
    int attempts_buf = 0, max_attempts_buf = 0;
    char media_buf[64], type_buf[64];
    unsigned long media_len = 0, type_len = 0;
    MYSQL_BIND result[5];
    std::memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = media_buf;
    result[1].buffer_length = sizeof(media_buf);
    result[1].length = &media_len;
    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = type_buf;
    result[2].buffer_length = sizeof(type_buf);
    result[2].length = &type_len;
    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &attempts_buf;
    result[4].buffer_type = MYSQL_TYPE_LONG;
    result[4].buffer = &max_attempts_buf;
    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        Finish(conn, false);
        return false;
    }
    const int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == MYSQL_NO_DATA) {
        mysql_stmt_close(stmt);
        Finish(conn, true);
        return true;
    }
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err) *err = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        Finish(conn, false);
        return false;
    }
    task->id = id_buf;
    task->media_id = Truncate(media_buf, media_len, sizeof(media_buf));
    task->task_type = Truncate(type_buf, type_len, sizeof(type_buf));
    task->attempts = attempts_buf + 1;
    task->max_attempts = max_attempts_buf;
    mysql_stmt_close(stmt);

    static const char* kUpdate =
        "UPDATE media_task SET status='running', attempts=attempts+1, "
        "locked_by=?, locked_at=NOW() WHERE id=?";
    unsigned long worker_len = 0;
    MYSQL_BIND bind[2];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], worker_id, &worker_len);
    long long task_id = task->id;
    BindInt64(&bind[1], &task_id);
    if (!ExecuteUpdate(conn, kUpdate, bind, nullptr, err)) {
        Finish(conn, false);
        return false;
    }
    Finish(conn, true);
    if (found) *found = true;
    return true;
}

bool MediaDao::MarkProcessing(const std::string& media_id,
                              std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_object SET status='processing', error_message='' "
        "WHERE media_id=? AND status IN ('uploaded','processing')";
    unsigned long media_len = 0;
    MYSQL_BIND bind[1];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], media_id, &media_len);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

bool MediaDao::CompleteImage(const std::string& media_id, int width, int height,
                             const std::string& dominant_color,
                             const std::vector<MediaVariant>& variants,
                             std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    if (!Begin(conn, err)) return false;

    if (!UpsertVariants(conn, media_id, variants, err)) {
        Finish(conn, false);
        return false;
    }

    static const char* kUpdateMedia =
        "UPDATE media_object SET status='ready', width=?, height=?, "
        "dominant_color=?, error_message='' WHERE media_id=? "
        "AND status IN ('uploaded','processing','ready')";
    int w = width, h = height;
    unsigned long color_len = 0, media_len = 0;
    MYSQL_BIND media_bind[4];
    std::memset(media_bind, 0, sizeof(media_bind));
    BindInt(&media_bind[0], &w);
    BindInt(&media_bind[1], &h);
    BindString(&media_bind[2], dominant_color, &color_len);
    BindString(&media_bind[3], media_id, &media_len);
    if (!ExecuteUpdate(conn, kUpdateMedia, media_bind, nullptr, err)) {
        Finish(conn, false);
        return false;
    }

    if (!MarkTaskDone(conn, media_id, "process_image", err)) {
        Finish(conn, false);
        return false;
    }
    Finish(conn, true);
    return true;
}

bool MediaDao::CompleteVideo(const std::string& media_id, int width, int height,
                             int64_t duration_ms,
                             const std::string& object_key,
                             int64_t size_bytes, const std::string& etag,
                             const std::vector<MediaVariant>& variants,
                             std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    if (!Begin(conn, err)) return false;

    if (!UpsertVariants(conn, media_id, variants, err)) {
        Finish(conn, false);
        return false;
    }

    static const char* kUpdateMedia =
        "UPDATE media_object SET status='ready', width=?, height=?, "
        "duration_ms=?, object_key=?, size_bytes=?, etag=?, error_message='' "
        "WHERE media_id=? AND status IN ('uploaded','processing','ready')";
    int w = width, h = height;
    long long duration = duration_ms;
    long long size = size_bytes;
    unsigned long object_key_len = 0, etag_len = 0, media_len = 0;
    MYSQL_BIND media_bind[7];
    std::memset(media_bind, 0, sizeof(media_bind));
    BindInt(&media_bind[0], &w);
    BindInt(&media_bind[1], &h);
    BindInt64(&media_bind[2], &duration);
    BindString(&media_bind[3], object_key, &object_key_len);
    BindInt64(&media_bind[4], &size);
    BindString(&media_bind[5], etag, &etag_len);
    BindString(&media_bind[6], media_id, &media_len);
    if (!ExecuteUpdate(conn, kUpdateMedia, media_bind, nullptr, err)) {
        Finish(conn, false);
        return false;
    }

    if (!MarkTaskDone(conn, media_id, "process_video", err)) {
        Finish(conn, false);
        return false;
    }
    Finish(conn, true);
    return true;
}

bool MediaDao::FailTask(const MediaTask& task, const std::string& error,
                        bool* terminal, std::string* err) {
    if (terminal) *terminal = (task.attempts >= task.max_attempts);
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    const bool stop = (task.attempts >= task.max_attempts);
    const std::string short_error = error.substr(0, 500);
    if (stop) {
        static const char* kSql =
            "UPDATE media_task SET status='failed', last_error=?, "
            "locked_by='', locked_at=NULL WHERE id=?";
        unsigned long error_len = 0;
        MYSQL_BIND bind[2];
        std::memset(bind, 0, sizeof(bind));
        BindString(&bind[0], short_error, &error_len);
        long long id = task.id;
        BindInt64(&bind[1], &id);
        if (!ExecuteUpdate(conn, kSql, bind, nullptr, err)) return false;
        static const char* kMediaSql =
            "UPDATE media_object SET status='failed', error_message=? "
            "WHERE media_id=? AND status IN ('uploaded','processing')";
        unsigned long media_error_len = 0, media_len = 0;
        MYSQL_BIND media_bind[2];
        std::memset(media_bind, 0, sizeof(media_bind));
        BindString(&media_bind[0], short_error, &media_error_len);
        BindString(&media_bind[1], task.media_id, &media_len);
        return ExecuteUpdate(conn, kMediaSql, media_bind, nullptr, err);
    }

    static const char* kSql =
        "UPDATE media_task SET status='pending', last_error=?, "
        "locked_by='', locked_at=NULL, next_run_at=DATE_ADD(NOW(), "
        "INTERVAL ? SECOND) WHERE id=?";
    int delay = std::min(60, std::max(5, task.attempts * 5));
    unsigned long error_len = 0;
    MYSQL_BIND bind[3];
    std::memset(bind, 0, sizeof(bind));
    BindString(&bind[0], short_error, &error_len);
    BindInt(&bind[1], &delay);
    long long id = task.id;
    BindInt64(&bind[2], &id);
    if (!ExecuteUpdate(conn, kSql, bind, nullptr, err)) return false;

    static const char* kMediaSql =
        "UPDATE media_object SET status='uploaded', error_message=? "
        "WHERE media_id=? AND status='processing'";
    unsigned long media_error_len = 0, media_len = 0;
    MYSQL_BIND media_bind[2];
    std::memset(media_bind, 0, sizeof(media_bind));
    BindString(&media_bind[0], short_error, &media_error_len);
    BindString(&media_bind[1], task.media_id, &media_len);
    return ExecuteUpdate(conn, kMediaSql, media_bind, nullptr, err);
}

bool MediaDao::RecoverStaleRunningTasks(int stale_seconds, std::string* err) {
    if (!pool_) {
        if (err) *err = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err) *err = "no mysql connection";
        return false;
    }
    static const char* kSql =
        "UPDATE media_task SET status='pending', locked_by='', locked_at=NULL "
        "WHERE status='running' AND locked_at < DATE_SUB(NOW(), INTERVAL ? SECOND)";
    int seconds = stale_seconds > 0 ? stale_seconds : 300;
    MYSQL_BIND bind[1];
    std::memset(bind, 0, sizeof(bind));
    BindInt(&bind[0], &seconds);
    return ExecuteUpdate(conn, kSql, bind, nullptr, err);
}

}  // namespace sparkpush
