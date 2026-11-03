#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "config.h"
#include "logging.h"
#include "service.h"
#include "signal_handler.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/job.conf。\n",
                 prog);
}

ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/job.conf";
    *config_path = default_path;
    bool positional_used = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } else {
            if (positional_used) {
                std::fprintf(stderr, "重复的配置文件参数: %s\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = arg;
            positional_used = true;
        }
    }
    return ArgParseResult::kOk;
}

}  // namespace

namespace sparkpush {

// 初始化并运行 Job 服务，处理退出信号。
int RunJob(const Config& cfg) {
    JobRunner runner(cfg);
    if (!runner.Init()) {
        LogError("JobRunner init failed");
        return 1;
    }
    runner.Start();
    LogInfo("Job runner started. Waiting for Kafka messages...");

    // 注册优雅关闭信号
    InstallSignalHandler();

    // 等待关闭信号
    while (!ShutdownRequested().load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    LogInfo("Job shutting down gracefully...");
    runner.Stop();
    LogInfo("Job shutdown complete");
    return 0;
}

}  // namespace sparkpush

int main(int argc, char** argv) {
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    if (result == ArgParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }
    // 先加载配置以拿到日志级别,再初始化日志到独立文件 logs/job.log。
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::InitLogging("job",
                           /*also_stdout=*/false,
                           sparkpush::kDefaultLogRotateBytes, cfg.log_level);

    int ret = sparkpush::RunJob(cfg);

    sparkpush::ShutdownLogging();
    return ret;
}

/*
你已经把 入口 → Logic → Kafka 打通了。接下来最自然的主线不是继续横向读 Logic，而是：
     
  ▎ 沿 Kafka 往两边追：Job 的两个消费组 → Comet 下行 → 故障路径 → 做一个二次开发。

  建议按下面 5 个阶段走。                                                                                                                                                                                       
  ---
     
  阶段 1：吃透 Job 的 push 消费链

  先看 job/ 和 common/kafka_consumer.*。

  要搞清楚的架构

  同一个 topic push_to_comet，被两个独立消费组消费：

  Kafka: push_to_comet
    ├─ group spark_push_group_push
    │    └─ PushHandler → gRPC → Comet → WebSocket → 浏览器
    └─ group spark_push_group_persist
         └─ PersistHandler → MySQL

  对应代码：

  - job/service.cpp:10-29 JobRunner::Init
  - job/push_handler.cpp:33：cfg.kafka_consumer_group + "_push"
  - job/persist_handler.cpp:129：cfg.kafka_consumer_group + "_persist"

  重点读这几个函数

  ┌───────────────────────────┬─────────────────────────────────┬──────────────────────────────────────────────────────┐
  │           文件            │              函数               │                     要回答的问题                     │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ common/kafka_consumer.h   │ Options                         │ 为什么关闭自动提交？max_inflight 控制什么？          │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ common/kafka_consumer.cpp │ Loop                            │ 谁在 poll？消息怎么进入 worker 池？                  │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ common/kafka_consumer.cpp │ SchedulePartition               │ 为什么同一 partition 必须串行？                      │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ common/kafka_consumer.cpp │ DrainCompletions                │ offset 什么时候提交？提交的是 offset 还是 offset+1？ │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ job/push_handler.cpp      │ HandleMessage                   │ persist_only 为什么直接 return？                     │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ job/push_handler.cpp      │ ProcessPushRequest              │ unary 推送怎么重试？deadline 多少？重试完怎么办？    │
  ├───────────────────────────┼─────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ job/push_handler.cpp      │ SendToStream / StreamWriterLoop │ 流模式怎么入队、怎么重连？                           │
  └───────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────────────────┘

  核心结论你要自己能说出来：

  KafkaConsumer 消费线程 poll
    → 按 partition 放进 pending
    → 同一 partition 一次只调度一条到 worker
    → callback 执行完
    → commitAsync(offset + 1)
    → 再调度同 partition 的下一条

  所以它是 at-least-once：先处理，后提交 offset；如果提交前进程挂了，下次会重复消费。

  动手实验

  1. 启动整套服务，发几条消息。
  2. 用 kafka-consumer-groups.sh --describe 看两个消费组的 lag。
  3. 停掉 Job，继续发消息，观察 lag 增长。
  4. 重启 Job，观察两个消费组各自追平。
  5. 故意让 Comet 不可用，观察 PushToComet 重试和 [DLQ][Push] 日志。
  6. 停掉 MySQL，观察 [DLQ][Persist] 日志。

  自测问题

  1. 为什么 push 和 persist 要用两个消费组？用一个会怎样？
  2. 同一 partition 的消息为什么不能并发处理？
  3. offset 提交失败会怎样？会重复消费吗？
  4. PushHandler 重试几次？重试耗尽后消息去哪了？
  5. persist_only 消息为什么 push 消费组要跳过，persist 消费组却要处理？

  ---

  阶段 2：吃透 persist 消费链

  重点看：

  - job/persist_handler.cpp
  - logic/message_dao.cpp
  - logic/session_dao.cpp
  - logic/user_session_state_dao.cpp
  - sql/schema.sql

  关键函数

  ┌─────────────────────────────────────┬─────────────────────────────────────────────────────────┐
  │                函数                 │                          作用                           │
  ├─────────────────────────────────────┼─────────────────────────────────────────────────────────┤
  │ PersistHandler::HandleMessage :164  │ 解析 PushToCometRequest，只处理 need_persist()          │
  ├─────────────────────────────────────┼─────────────────────────────────────────────────────────┤
  │ PersistHandler::PersistMessage :281 │ 重试入口                                                │
  ├─────────────────────────────────────┼─────────────────────────────────────────────────────────┤
  │ PersistHandler::ExecutePersist :180 │ 开事务、upsert session、insert message、commit/rollback │
  └─────────────────────────────────────┴─────────────────────────────────────────────────────────┘

  要搞懂的点

  1. 为什么 message 表有唯一键 (session_id, msg_seq)？
  2. INSERT ... ON DUPLICATE KEY UPDATE session_id=session_id 怎么实现幂等？
  3. 重复消费同一条消息，会不会插两条？
  4. session.last_msg_seq 为什么用 GREATEST 更新？
  5. 事务里先 upsert session 再 insert message，失败时怎么回滚？
  6. content_json、client_msg_id、timestamp_ms 分别落在哪一列？

  动手实验

  1. 查 message 表，确认一条消息一行。
  2. 用相同 session_id + msg_seq 手工再插一次，观察唯一键和幂等 SQL。
  3. 模拟 MySQL 失败，观察 persist 重试和 offset 是否提交。
  4. 停掉 Job 再重启，确认历史消息没有重复。

  ---

  阶段 3：吃透 Comet 下行

  重点看：

  - comet/comet_grpc_service.cpp
  - comet/comet_server.cpp
  - comet/connection_registry.*

  调用链

  Job
    → CometService::PushToComet / PushStream
    → CometServiceImpl::ProcessPushRequest
    → targets?       → CometServer::PushToUsers
    → room_id?       → CometServer::PushToRoom
    → "broadcast"?   → CometServer::PushToAll
    → ConnectionRegistry 找连接
    → SendText
    → WebSocket → 浏览器

  要搞懂的点

  1. 一个用户多设备、多标签页时，route:user:{uid} 里为什么是 conn_id -> comet_id 的 HASH？
  2. 一个用户在多个 Comet 上在线时，Logic 怎么扇出？
  3. PushToUsers 为什么只推本机连接？跨 Comet 怎么办？
  4. PushToRoom 怎么根据 room_id 找本机连接？
  5. 用户离线时，消息为什么没有丢？它在哪里？
  6. 心跳桶 kHeartbeatBucketCount = 64 解决什么问题？
  7. 连接断开后，UserOffline 怎么删 Redis 路由？

  动手实验

  1. 同一账号开两个浏览器标签页，发一条单聊，观察两边都收到。
  2. 关掉一个标签页，检查 Redis route:user:{uid} 是否少了一个 conn_id。
  3. 关掉所有标签页后再发消息，观察只落库、不推送。
  4. 杀掉 Comet，观察 Job 重试和 DLQ 日志。
  5. 重启 Comet，重新登录，确认能收到新消息。

  自测问题

  1. PushToComet 和 PushStream 分别适合什么场景？
  2. 为什么 PushStream 要跳过 persist_only？
  3. 用户离线时，消息最终存在哪里？下次登录怎么看到？
  4. Comet 崩溃后，Redis 里的旧路由会怎样？会不会把消息推给一个已经不存在的连接？

  ---

  阶段 4：做故障矩阵

  这是最能提升后端能力的一步。不要只读代码，要亲手制造故障。

  ┌────────────┬─────────────────────────────────────────────┬──────────────────────────────┐
  │    故障    │                   观察点                    │           你要记录           │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ Kafka 挂掉 │ 生产者 Send 返回什么？HTTP 还 200 吗？      │ 消息是否丢、是否堆积         │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ Job 挂掉   │ consumer lag 是否增长？                     │ 恢复后是否追平、是否重复     │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ Comet 挂掉 │ Job 重试几次？DLQ 日志？                    │ 推送是否丢、持久化是否仍成功 │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ Redis 挂掉 │ 发消息是否失败？路由查询是否失败？          │ 哪些接口 500，哪些降级       │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ MySQL 挂掉 │ persist 重试？offset 是否提交？             │ 恢复后是否补写、是否重复     │
  ├────────────┼─────────────────────────────────────────────┼──────────────────────────────┤
  │ Logic 挂掉 │ Comet 的 VerifyToken / MessageStream 怎样？ │ 重连行为、消息是否丢         │
  └────────────┴─────────────────────────────────────────────┴──────────────────────────────┘

  建议输出一张表：

  组件 | 故障现象 | 请求是否成功 | 数据是否丢 | 是否重复 | 恢复方式

  这张表比读十遍代码更有价值。

  ---

  阶段 5：补理论，但要“对着代码补”

  不要孤立地看 Kafka/MySQL 书，遇到一个概念就回到代码里找对应位置。

  Kafka

  - partition / replica / ISR
  - consumer group / rebalance
  - offset / auto commit / manual commit
  - at-most-once / at-least-once / exactly-once
  - acks=1 意味着什么？会丢什么？
  - 为什么 key 决定分区？session_id|comet_id 的作用是什么？
  - message.timeout.ms=60000 到了之后消息去哪？

  对应代码：

  - common/kafka_producer.cpp:43-55
  - common/kafka_consumer.cpp
  - logic/grpc_service.cpp:405 的 Kafka key
  - logic/http_handlers_message.cpp:150 的 Kafka key

  MySQL

  - InnoDB 索引
  - 唯一键
  - 事务边界
  - INSERT ... ON DUPLICATE KEY UPDATE
  - 连接池
  - prepared statement

  对应代码：

  - sql/schema.sql
  - job/persist_handler.cpp
  - common/mysql_pool.*
  - logic/message_dao.cpp

  gRPC / WebSocket

  - unary vs bidi streaming
  - ClientReaderWriter 的读写线程
  - request_id 关联请求和响应
  - WebSocket 连接注册、心跳、断线清理

  对应代码：

  - comet/logic_stream_client.*
  - logic/grpc_service.cpp:512-560
  - comet/comet_server.cpp
  - comet/connection_registry.*

  ---

  阶段 6：选一个二次开发题目

  学到这里，应该用一个功能把整条链路再串一遍。推荐三选一：

  题目 A：消息可靠性闭环（推荐）

  做这些事：

  1. 给 KafkaProducer 加 delivery report，确认 broker 是否真的收到；
  2. 把 acks=1 改成更可靠的配置，或解释为什么保持 1；
  3. 给 message 表加去重/状态字段；
  4. 消费失败不要只打 [DLQ] 日志，落一张 dead_letter 表；
  5. 加一个“消息投递状态”查询接口。

  这个题目会同时练到：Kafka、MySQL、事务、重试、幂等、可观测性。

  题目 B：离线消息补拉

  1. 用户重连后，根据 read_seq 拉取未读消息；
  2. 拉取后更新 user_session_state.read_seq；
  3. 处理“重复拉取”和“已读回执”；
  4. 前端展示未读数。

  这个题目会练到：会话、游标、Redis/MySQL 一致性、HTTP 接口。

  题目 C：路由 TTL 与死连接清理

  1. 给 route:user:{uid} 加 TTL 或心跳续期；
  2. Comet 崩溃后，自动清理失效路由；
  3. Job 推送前检查路由有效性；
  4. 加一个“在线状态”查询接口。

  这个题目会练到：Redis 数据结构、TTL、心跳、故障恢复。

  ---

  建议节奏

  ┌────────────┬────────────────────────┬────────────────────────┐
  │    时间    │          内容          │          产出          │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 1-2 天  │ Job push 消费链        │ 时序图 + 5 个自测答案  │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 3-4 天  │ persist 消费链 + MySQL │ 幂等/事务笔记          │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 5-6 天  │ Comet 下行 + 多端实验  │ 下行链路图             │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 7-8 天  │ 故障矩阵               │ 一张故障表             │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 9-10 天 │ Kafka/MySQL 理论补课   │ 概念到代码的映射       │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 11 天起 │ 一个二次开发题目       │ 设计文档 + 代码 + 压测 │
  └────────────┴────────────────────────┴────────────────────────┘

  ---

  最后：不要做的事
  │ 第 3-4 天  │ persist 消费链 + MySQL │ 幂等/事务笔记          │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 5-6 天  │ Comet 下行 + 多端实验  │ 下行链路图             │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 7-8 天  │ 故障矩阵               │ 一张故障表             │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 9-10 天 │ Kafka/MySQL 理论补课   │ 概念到代码的映射       │
  ├────────────┼────────────────────────┼────────────────────────┤
  │ 第 11 天起 │ 一个二次开发题目       │ 设计文档 + 代码 + 压测 │
  └────────────┴────────────────────────┴────────────────────────┘

  ---

  最后：不要做的事

  1. 不要继续从头到尾读 Logic 的 HTTP handler。入口你已经打通了，边际收益很低。
  2. 不要按文件顺序读整个仓库。按消息链路追，一次只追一条。
  3. 不要一上来就上 Kubernetes、服务网格、分布式事务。这个项目当前最值得学的是 Kafka 消费语义、幂等、故障恢复。
  4. 不要只读不跑。Job/Comet/Kafka/Redis/MySQL 的故障实验，比读十遍代码更有用。
  5. 不要同时开三个改造题目。选一个，做完，写设计文档和复盘。
*/