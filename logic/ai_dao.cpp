#include "ai_dao.h"

#include <mysql/mysql.h>

#include <cstdlib>
#include <cstring>
#include <vector>

#include "logging.h"

namespace sparkpush {

namespace {

void BindString(MYSQL_BIND* bind, const std::string& value,
                unsigned long* len) {
    *len = static_cast<unsigned long>(value.size());
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = const_cast<char*>(value.data());
    bind->buffer_length = *len;
    bind->length = len;
}

void BindInt64(MYSQL_BIND* bind, long long* value) {
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = value;
}

bool ExecStatement(MYSQL* conn, const char* sql, MYSQL_BIND* bind,
                   std::string* err_msg) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, static_cast<unsigned long>(strlen(sql))) !=
            0 ||
        mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool ExecUpdate(MYSQL* conn, const char* sql, MYSQL_BIND* bind,
                std::string* err_msg) {
    return ExecStatement(conn, sql, bind, err_msg);
}

}  // namespace

bool AiDao::GetSessionBot(const std::string& session_id,
                          int64_t bot_user_id, SessionBot* out,
                          std::string* err_msg) {
    if (!pool_ || !out) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT session_id, bot_user_id, status, invited_by, activated_seq "
        "FROM session_bot WHERE session_id=? AND bot_user_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[2];
    memset(param, 0, sizeof(param));
    unsigned long sid_len = 0;
    long long bot_buf = bot_user_id;
    BindString(&param[0], session_id, &sid_len);
    BindInt64(&param[1], &bot_buf);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0 || mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_num_rows(stmt) == 0) {
        if (err_msg) *err_msg = "session bot not found";
        mysql_stmt_close(stmt);
        return false;
    }

    char sid_buf[160];
    char status_buf[8];
    long long bot_out = 0;
    long long invited_by = 0;
    long long activated_seq = 0;
    unsigned long sid_out_len = 0;
    unsigned long status_out_len = 0;
    MYSQL_BIND result[5];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = sid_buf;
    result[0].buffer_length = sizeof(sid_buf);
    result[0].length = &sid_out_len;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &bot_out;
    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = status_buf;
    result[2].buffer_length = sizeof(status_buf);
    result[2].length = &status_out_len;
    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &invited_by;
    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &activated_seq;

    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_fetch(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    out->session_id.assign(sid_buf, sid_out_len);
    out->bot_user_id = bot_out;
    out->status = std::atoi(status_buf);
    out->invited_by = invited_by;
    out->activated_seq = activated_seq;
    mysql_stmt_close(stmt);
    return true;
}

bool AiDao::UpsertPendingInvite(const std::string& session_id,
                                int64_t bot_user_id, int64_t invited_by,
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
        "INSERT INTO session_bot "
        "(session_id, bot_user_id, status, invited_by, activated_seq) "
        "VALUES (?, ?, 0, ?, 0) "
        "ON DUPLICATE KEY UPDATE "
        "status=IF(status=1, 1, 0), "
        "invited_by=IF(status=1, invited_by, VALUES(invited_by)), "
        "activated_seq=IF(status=1, activated_seq, 0), "
        "activated_at=IF(status=1, activated_at, NULL)";
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = 0;
    long long bot_buf = bot_user_id;
    long long inviter_buf = invited_by;
    BindString(&bind[0], session_id, &sid_len);
    BindInt64(&bind[1], &bot_buf);
    BindInt64(&bind[2], &inviter_buf);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::Activate(const std::string& session_id, int64_t bot_user_id,
                     int64_t activated_seq, std::string* err_msg) {
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
        "UPDATE session_bot SET status=1, activated_seq=?, activated_at=NOW() "
        "WHERE session_id=? AND bot_user_id=? AND status=0";
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    long long seq_buf = activated_seq;
    unsigned long sid_len = 0;
    long long bot_buf = bot_user_id;
    BindInt64(&bind[0], &seq_buf);
    BindString(&bind[1], session_id, &sid_len);
    BindInt64(&bind[2], &bot_buf);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::Reject(const std::string& session_id, int64_t bot_user_id,
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
        "UPDATE session_bot SET status=2 "
        "WHERE session_id=? AND bot_user_id=? AND status=0";
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = 0;
    long long bot_buf = bot_user_id;
    BindString(&bind[0], session_id, &sid_len);
    BindInt64(&bind[1], &bot_buf);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::Remove(const std::string& session_id, int64_t bot_user_id,
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
        "UPDATE session_bot SET status=3 "
        "WHERE session_id=? AND bot_user_id=? AND status IN (0,1)";
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = 0;
    long long bot_buf = bot_user_id;
    BindString(&bind[0], session_id, &sid_len);
    BindInt64(&bind[1], &bot_buf);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::CreateJob(const AiJob& job, bool* created, std::string* err_msg) {
    if (!pool_ || !created) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT INTO ai_job "
        "(job_id, session_id, bot_user_id, mode, trigger_message_id, "
        " trigger_sender_id, trigger_sender_name, trigger_content, "
        " activated_seq, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued') "
        "ON DUPLICATE KEY UPDATE job_id=job_id";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[9];
    memset(bind, 0, sizeof(bind));
    unsigned long job_len = 0;
    unsigned long sid_len = 0;
    long long bot_buf = job.bot_user_id;
    unsigned long mode_len = 0;
    unsigned long trigger_id_len = 0;
    long long sender_buf = job.trigger_sender_id;
    unsigned long sender_name_len = 0;
    unsigned long content_len = 0;
    long long activated_seq = job.activated_seq;
    BindString(&bind[0], job.job_id, &job_len);
    BindString(&bind[1], job.session_id, &sid_len);
    BindInt64(&bind[2], &bot_buf);
    BindString(&bind[3], job.mode, &mode_len);
    BindString(&bind[4], job.trigger_message_id, &trigger_id_len);
    BindInt64(&bind[5], &sender_buf);
    BindString(&bind[6], job.trigger_sender_name, &sender_name_len);
    BindString(&bind[7], job.trigger_content, &content_len);
    BindInt64(&bind[8], &activated_seq);
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    *created = (mysql_stmt_affected_rows(stmt) > 0);
    mysql_stmt_close(stmt);
    return true;
}

bool AiDao::GetJob(const std::string& job_id, AiJob* out,
                   std::string* err_msg) {
    if (!pool_ || !out) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT job_id, session_id, bot_user_id, mode, trigger_message_id, "
        " trigger_sender_id, trigger_sender_name, trigger_content, "
        " activated_seq, status, msg_id, msg_seq, error_code, error_message "
        "FROM ai_job WHERE job_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    unsigned long job_len = 0;
    BindString(&param[0], job_id, &job_len);
    if (mysql_stmt_bind_param(stmt, param) != 0 ||
        mysql_stmt_execute(stmt) != 0 || mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_num_rows(stmt) == 0) {
        if (err_msg) *err_msg = "ai job not found";
        mysql_stmt_close(stmt);
        return false;
    }

    char job_buf[96];
    char sid_buf[160];
    long long bot_buf = 0;
    char mode_buf[32];
    char trigger_id_buf[192];
    long long sender_buf = 0;
    char sender_name_buf[160];
    char content_buf[8192];
    long long activated_seq = 0;
    char status_buf[32];
    char msg_id_buf[192];
    long long msg_seq = 0;
    char error_code_buf[96];
    char error_message_buf[640];
    unsigned long job_out_len = 0, sid_out_len = 0, mode_out_len = 0;
    unsigned long trigger_out_len = 0, sender_name_out_len = 0;
    unsigned long content_out_len = 0, status_out_len = 0;
    unsigned long msg_id_out_len = 0, error_code_out_len = 0;
    unsigned long error_message_out_len = 0;

    MYSQL_BIND result[14];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = job_buf;
    result[0].buffer_length = sizeof(job_buf);
    result[0].length = &job_out_len;
    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = sid_buf;
    result[1].buffer_length = sizeof(sid_buf);
    result[1].length = &sid_out_len;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &bot_buf;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = mode_buf;
    result[3].buffer_length = sizeof(mode_buf);
    result[3].length = &mode_out_len;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = trigger_id_buf;
    result[4].buffer_length = sizeof(trigger_id_buf);
    result[4].length = &trigger_out_len;
    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &sender_buf;
    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = sender_name_buf;
    result[6].buffer_length = sizeof(sender_name_buf);
    result[6].length = &sender_name_out_len;
    result[7].buffer_type = MYSQL_TYPE_STRING;
    result[7].buffer = content_buf;
    result[7].buffer_length = sizeof(content_buf);
    result[7].length = &content_out_len;
    result[8].buffer_type = MYSQL_TYPE_LONGLONG;
    result[8].buffer = &activated_seq;
    result[9].buffer_type = MYSQL_TYPE_STRING;
    result[9].buffer = status_buf;
    result[9].buffer_length = sizeof(status_buf);
    result[9].length = &status_out_len;
    result[10].buffer_type = MYSQL_TYPE_STRING;
    result[10].buffer = msg_id_buf;
    result[10].buffer_length = sizeof(msg_id_buf);
    result[10].length = &msg_id_out_len;
    result[11].buffer_type = MYSQL_TYPE_LONGLONG;
    result[11].buffer = &msg_seq;
    result[12].buffer_type = MYSQL_TYPE_STRING;
    result[12].buffer = error_code_buf;
    result[12].buffer_length = sizeof(error_code_buf);
    result[12].length = &error_code_out_len;
    result[13].buffer_type = MYSQL_TYPE_STRING;
    result[13].buffer = error_message_buf;
    result[13].buffer_length = sizeof(error_message_buf);
    result[13].length = &error_message_out_len;

    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_fetch(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    out->job_id.assign(job_buf, job_out_len);
    out->session_id.assign(sid_buf, sid_out_len);
    out->bot_user_id = bot_buf;
    out->mode.assign(mode_buf, mode_out_len);
    out->trigger_message_id.assign(trigger_id_buf, trigger_out_len);
    out->trigger_sender_id = sender_buf;
    out->trigger_sender_name.assign(sender_name_buf, sender_name_out_len);
    out->trigger_content.assign(content_buf, content_out_len);
    out->activated_seq = activated_seq;
    out->status.assign(status_buf, status_out_len);
    out->msg_id.assign(msg_id_buf, msg_id_out_len);
    out->msg_seq = msg_seq;
    out->error_code.assign(error_code_buf, error_code_out_len);
    out->error_message.assign(error_message_buf, error_message_out_len);
    mysql_stmt_close(stmt);
    return true;
}

bool AiDao::MarkJobRunning(const std::string& job_id, std::string* err_msg) {
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
        "UPDATE ai_job SET status='running' "
        "WHERE job_id=? AND status='queued'";
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long job_len = 0;
    BindString(&bind[0], job_id, &job_len);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::CompleteJob(const std::string& job_id, const std::string& msg_id,
                        int64_t msg_seq, std::string* err_msg) {
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
        "UPDATE ai_job SET status='completed', msg_id=?, msg_seq=?, "
        "error_code='', error_message='' "
        "WHERE job_id=? AND status IN ('queued','running')";
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    unsigned long msg_id_len = 0;
    long long seq_buf = msg_seq;
    unsigned long job_len = 0;
    BindString(&bind[0], msg_id, &msg_id_len);
    BindInt64(&bind[1], &seq_buf);
    BindString(&bind[2], job_id, &job_len);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::FailJob(const std::string& job_id,
                    const std::string& error_code,
                    const std::string& error_message,
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
        "UPDATE ai_job SET status='failed', error_code=?, error_message=? "
        "WHERE job_id=? AND status IN ('queued','running')";
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    unsigned long code_len = 0;
    unsigned long message_len = 0;
    unsigned long job_len = 0;
    BindString(&bind[0], error_code, &code_len);
    BindString(&bind[1], error_message, &message_len);
    BindString(&bind[2], job_id, &job_len);
    return ExecUpdate(conn, sql, bind, err_msg);
}

bool AiDao::AbortJob(const std::string& job_id, std::string* err_msg) {
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
        "UPDATE ai_job SET status='aborted', error_code='ABORTED', "
        "error_message='stopped by user' "
        "WHERE job_id=? AND status IN ('queued','running')";
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long job_len = 0;
    BindString(&bind[0], job_id, &job_len);
    return ExecUpdate(conn, sql, bind, err_msg);
}

}  // namespace sparkpush
