#include <mysql/mysql.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace {

constexpr const char* kTableName = "persist_stmt_bench";
constexpr const char* kInsertSql =
    "INSERT INTO persist_stmt_bench "
    "(run_id, item_id, session_id, msg_seq, sender_id, msg_type, content_json, "
    "timestamp_ms, client_msg_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

struct Options {
    std::string host = "127.0.0.1";
    unsigned int port = 3306;
    std::string user = "root";
    std::string password;
    std::string database = "spark_push";
    int operations = 2000;
    int warmup_operations = 200;
    int rounds = 7;
};

struct RoundResult {
    double qps = 0.0;
    double p95_us = 0.0;
};

void PrintUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --host HOST          MySQL host (default 127.0.0.1)\n"
        << "  --port PORT          MySQL port (default 3306)\n"
        << "  --user USER          MySQL user (default root)\n"
        << "  --password PASSWORD  MySQL password (or MYSQL_PASSWORD env)\n"
        << "  --database DB        Database (default spark_push)\n"
        << "  --operations N       Timed operations per round (default 2000)\n"
        << "  --warmup N           Warmup operations per mode (default 200)\n"
        << "  --rounds N           Rounds per mode (default 7)\n";
}

bool ParsePositiveInt(const char* text, int* value) {
    if (!text || !value) return false;
    char* end = nullptr;
    long parsed = std::strtol(text, &end, 10);
    if (*text == '\0' || !end || *end != '\0' || parsed <= 0 ||
        parsed > 10000000) {
        return false;
    }
    *value = static_cast<int>(parsed);
    return true;
}

bool ParseArgs(int argc, char** argv, Options* options) {
    if (!options) return false;
    if (const char* password = std::getenv("MYSQL_PASSWORD")) {
        options->password = password;
    }
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) return false;
        std::string value = argv[++i];
        if (arg == "--host") {
            options->host = value;
        } else if (arg == "--port") {
            int port = 0;
            if (!ParsePositiveInt(value.c_str(), &port) || port > 65535)
                return false;
            options->port = static_cast<unsigned int>(port);
        } else if (arg == "--user") {
            options->user = value;
        } else if (arg == "--password") {
            options->password = value;
        } else if (arg == "--database") {
            options->database = value;
        } else if (arg == "--operations") {
            if (!ParsePositiveInt(value.c_str(), &options->operations))
                return false;
        } else if (arg == "--warmup") {
            if (!ParsePositiveInt(value.c_str(), &options->warmup_operations))
                return false;
        } else if (arg == "--rounds") {
            if (!ParsePositiveInt(value.c_str(), &options->rounds))
                return false;
        } else {
            return false;
        }
    }
    return true;
}

bool ExecuteSql(MYSQL* connection, const std::string& sql) {
    if (mysql_query(connection, sql.c_str()) == 0) return true;
    std::cerr << "SQL failed: " << mysql_error(connection) << "\n";
    return false;
}

MYSQL* Connect(const Options& options) {
    MYSQL* connection = mysql_init(nullptr);
    if (!connection) return nullptr;
    mysql_options(connection, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    if (!mysql_real_connect(connection, options.host.c_str(),
                            options.user.c_str(), options.password.c_str(),
                            options.database.c_str(), options.port, nullptr,
                            0)) {
        std::cerr << "MySQL connection failed: " << mysql_error(connection)
                  << "\n";
        mysql_close(connection);
        return nullptr;
    }
    return connection;
}

MYSQL_STMT* PrepareInsert(MYSQL* connection) {
    MYSQL_STMT* statement = mysql_stmt_init(connection);
    if (!statement) return nullptr;
    if (mysql_stmt_prepare(statement, kInsertSql, std::strlen(kInsertSql)) !=
        0) {
        std::cerr << "Prepare failed: " << mysql_stmt_error(statement) << "\n";
        mysql_stmt_close(statement);
        return nullptr;
    }
    return statement;
}

bool BindAndExecute(MYSQL_STMT* statement, long long run_id,
                    long long item_id) {
    std::string session_id = "bench:" + std::to_string(run_id);
    std::string msg_type = "text";
    std::string content_json = "{\"text\":\"prepared statement benchmark\"}";
    std::string client_msg_id =
        "bench-" + std::to_string(run_id) + "-" + std::to_string(item_id);
    long long msg_seq = item_id + 1;
    long long sender_id = 1;
    long long timestamp_ms = 1700000000000LL + item_id;

    unsigned long lengths[] = {
        static_cast<unsigned long>(session_id.size()),
        static_cast<unsigned long>(msg_type.size()),
        static_cast<unsigned long>(content_json.size()),
        static_cast<unsigned long>(client_msg_id.size())};

    MYSQL_BIND bindings[9];
    std::memset(bindings, 0, sizeof(bindings));
    auto bind_int64 = [&](int index, long long* value) {
        bindings[index].buffer_type = MYSQL_TYPE_LONGLONG;
        bindings[index].buffer = value;
    };
    auto bind_string = [&](int index, std::string* value,
                           unsigned long* length) {
        bindings[index].buffer_type = MYSQL_TYPE_STRING;
        bindings[index].buffer = value->data();
        bindings[index].buffer_length = *length;
        bindings[index].length = length;
    };

    bind_int64(0, &run_id);
    bind_int64(1, &item_id);
    bind_string(2, &session_id, &lengths[0]);
    bind_int64(3, &msg_seq);
    bind_int64(4, &sender_id);
    bind_string(5, &msg_type, &lengths[1]);
    bind_string(6, &content_json, &lengths[2]);
    bind_int64(7, &timestamp_ms);
    bind_string(8, &client_msg_id, &lengths[3]);

    if (mysql_stmt_bind_param(statement, bindings) != 0 ||
        mysql_stmt_execute(statement) != 0) {
        std::cerr << "Execute failed: " << mysql_stmt_error(statement) << "\n";
        return false;
    }
    return true;
}

bool RunOperations(MYSQL* connection, bool reuse, long long run_id,
                   int operations, bool collect, RoundResult* result) {
    MYSQL_STMT* reused_statement = reuse ? PrepareInsert(connection) : nullptr;
    if (reuse && !reused_statement) return false;

    std::vector<double> latency_us;
    if (collect) latency_us.reserve(static_cast<size_t>(operations));
    auto round_start = std::chrono::steady_clock::now();

    for (int i = 0; i < operations; ++i) {
        auto operation_start = std::chrono::steady_clock::now();
        MYSQL_STMT* statement = reused_statement;
        if (!reuse) {
            statement = PrepareInsert(connection);
            if (!statement) return false;
        }
        bool ok = BindAndExecute(statement, run_id, i);
        if (!reuse) mysql_stmt_close(statement);
        if (!ok) {
            if (reused_statement) mysql_stmt_close(reused_statement);
            return false;
        }
        if (collect) {
            auto operation_end = std::chrono::steady_clock::now();
            latency_us.push_back(std::chrono::duration<double, std::micro>(
                                     operation_end - operation_start)
                                     .count());
        }
    }

    auto round_end = std::chrono::steady_clock::now();
    if (reused_statement) mysql_stmt_close(reused_statement);
    if (!collect || !result) return true;

    double seconds =
        std::chrono::duration<double>(round_end - round_start).count();
    result->qps = operations / seconds;
    std::sort(latency_us.begin(), latency_us.end());
    size_t p95_index = static_cast<size_t>(
        std::ceil(static_cast<double>(latency_us.size()) * 0.95) - 1.0);
    result->p95_us = latency_us[std::min(p95_index, latency_us.size() - 1)];
    return true;
}

double Median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    size_t middle = values.size() / 2;
    if (values.size() % 2 == 1) return values[middle];
    return (values[middle - 1] + values[middle]) / 2.0;
}

void PrintSummary(const std::string& label,
                  const std::vector<RoundResult>& results) {
    std::vector<double> qps;
    std::vector<double> p95;
    for (const auto& result : results) {
        qps.push_back(result.qps);
        p95.push_back(result.p95_us);
    }
    std::cout << label << ": median_qps=" << Median(qps)
              << " median_p95_us=" << Median(p95) << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseArgs(argc, argv, &options)) {
        PrintUsage(argv[0]);
        return 1;
    }

    MYSQL* connection = Connect(options);
    if (!connection) return 2;

    const std::string create_sql =
        std::string("CREATE TABLE IF NOT EXISTS ") + kTableName +
        " (run_id BIGINT NOT NULL, item_id BIGINT NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, msg_seq BIGINT NOT NULL, "
        "sender_id BIGINT NOT NULL, msg_type VARCHAR(32) NOT NULL, "
        "content_json TEXT NOT NULL, timestamp_ms BIGINT NOT NULL, "
        "client_msg_id VARCHAR(128) NOT NULL, PRIMARY KEY(run_id, item_id)) "
        "ENGINE=InnoDB";
    if (!ExecuteSql(connection, create_sql) ||
        !ExecuteSql(connection, std::string("TRUNCATE TABLE ") + kTableName)) {
        mysql_close(connection);
        return 3;
    }

    long long run_id = 1;
    if (!RunOperations(connection, false, run_id++, options.warmup_operations,
                       false, nullptr) ||
        !RunOperations(connection, true, run_id++, options.warmup_operations,
                       false, nullptr)) {
        ExecuteSql(connection,
                   std::string("DROP TABLE IF EXISTS ") + kTableName);
        mysql_close(connection);
        return 4;
    }

    std::vector<RoundResult> prepare_each_results;
    std::vector<RoundResult> reuse_results;
    std::cout << std::fixed << std::setprecision(1);
    for (int round = 0; round < options.rounds; ++round) {
        RoundResult first;
        RoundResult second;
        bool reuse_first = (round % 2 == 1);
        bool ok = RunOperations(connection, reuse_first, run_id++,
                                options.operations, true, &first) &&
                  RunOperations(connection, !reuse_first, run_id++,
                                options.operations, true, &second);
        if (!ok) {
            ExecuteSql(connection,
                       std::string("DROP TABLE IF EXISTS ") + kTableName);
            mysql_close(connection);
            return 5;
        }
        const RoundResult& prepare_each = reuse_first ? second : first;
        const RoundResult& reuse = reuse_first ? first : second;
        prepare_each_results.push_back(prepare_each);
        reuse_results.push_back(reuse);
        std::cout << "round=" << (round + 1)
                  << " prepare_each_qps=" << prepare_each.qps
                  << " prepare_each_p95_us=" << prepare_each.p95_us
                  << " reuse_qps=" << reuse.qps
                  << " reuse_p95_us=" << reuse.p95_us << "\n";
    }

    PrintSummary("prepare_each", prepare_each_results);
    PrintSummary("reuse", reuse_results);
    std::vector<double> prepare_qps;
    std::vector<double> reuse_qps;
    for (const auto& result : prepare_each_results)
        prepare_qps.push_back(result.qps);
    for (const auto& result : reuse_results) reuse_qps.push_back(result.qps);
    std::cout << "median_speedup=" << Median(reuse_qps) / Median(prepare_qps)
              << "x\n";

    bool cleanup_ok = ExecuteSql(
        connection, std::string("DROP TABLE IF EXISTS ") + kTableName);
    mysql_close(connection);
    return cleanup_ok ? 0 : 6;
}
