#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <thread>

#include "config.h"
#include "conversation_store.h"
#include "danmaku_dao.h"
#include "group_dao.h"
#include "grpc_service.h"
#include "http_server.h"
#include "kafka_producer.h"
#include "logging.h"
#include "message_dao.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"
#include "session_dao.h"
#include "signal_handler.h"
#include "user_dao.h"
#include "user_session_state_dao.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/logic.conf。\n",
                 prog);
}

ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/logic.conf";
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

// 初始化依赖并启动 Logic 的 gRPC 与 HTTP 服务。
int RunLogic(const Config& cfg) {
    KafkaProducer producer;
    if (!producer.Init(cfg.kafka_brokers, cfg.kafka_push_topic)) {
        LogError("Failed to initialize Kafka push producer");
        return 1;
    }

    // 广播任务 Kafka producer（写入 broadcast_task topic）
    KafkaProducer broadcast_producer;
    if (!broadcast_producer.Init(cfg.kafka_brokers,
                                 cfg.kafka_broadcast_topic)) {
        LogError("Failed to initialize Kafka broadcast producer");
        return 1;
    }

    // 初始化 MySQL 连接池
    MySqlConnectionPool mysql_pool;
    MySqlConfig mc;
    mc.host = cfg.mysql_host;
    mc.port = cfg.mysql_port;
    mc.user = cfg.mysql_user;
    mc.password = cfg.mysql_password;
    mc.db = cfg.mysql_db;
    mc.pool_size = cfg.mysql_pool_size;
    mc.min_pool_size =
        (cfg.mysql_pool_min_size > 0) ? cfg.mysql_pool_min_size : mc.pool_size;
    mc.max_pool_size = (cfg.mysql_pool_max_size > 0)
                           ? cfg.mysql_pool_max_size
                           : std::max(mc.min_pool_size, mc.pool_size);
    mc.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
    if (!mysql_pool.Init(mc)) {
        LogError("Failed to init MySQL pool");
        return 1;
    }
    UserDao user_dao(&mysql_pool);
    DanmakuDao danmaku_dao(&mysql_pool);
    GroupDao group_dao(&mysql_pool);
    GroupMemberDao group_member_dao(&mysql_pool);
    SessionDao session_dao(&mysql_pool);
    MessageDao message_dao(&mysql_pool);
    UserSessionStateDao state_dao(&mysql_pool);

    // 初始化 Redis 连接池
    RedisConnectionPool redis_pool;
    RedisConfig rc;
    rc.host = cfg.redis_host;
    rc.port = cfg.redis_port;
    rc.password = cfg.redis_password;
    rc.db = cfg.redis_db;
    rc.pool_size = cfg.redis_pool_size;
    rc.min_pool_size = (cfg.redis_pool_size > 0) ? cfg.redis_pool_size : 1;
    rc.max_pool_size = (cfg.redis_max_pool_size > 0)
                           ? cfg.redis_max_pool_size
                           : std::max(rc.min_pool_size, cfg.redis_pool_size);
    rc.connect_timeout_ms = cfg.redis_connect_timeout_ms;
    rc.rw_timeout_ms = cfg.redis_rw_timeout_ms;
    rc.idle_timeout_ms = cfg.redis_idle_timeout_ms;
    if (!redis_pool.Init(rc)) {
        LogError("Failed to init Redis pool");
        return 1;
    }
    RedisStore redis_store(&redis_pool);

    ConversationStore conversation_store(&session_dao, &message_dao, &state_dao,
                                         &redis_store);

    // 启动 gRPC 服务器（LogicService）
    /*
    ┌───────────────────────┬───────────────────────────────────────────────────────────────────────────┐
    │         func          │                                   作用                                    │
    ├───────────────────────┼───────────────────────────────────────────────────────────────────────────┤
    │ ServerBuilder builder │ 收集服务器配置                                                             │
    ├───────────────────────┼───────────────────────────────────────────────────────────────────────────┤
    │ AddListeningPort      │ 指定监听地址，例如 0.0.0.0:9100                                            │
    ├───────────────────────┼───────────────────────────────────────────────────────────────────────────┤
    │ RegisterService       │ 把 LogicServiceImpl 注册进去，告诉 gRPC “这些 RPC 方法由这个对象实现”        │
    ├───────────────────────┼───────────────────────────────────────────────────────────────────────────┤
    │ BuildAndStart()       │ 真正创建监听 socket、启动 gRPC 内部线程、开始接收请求，返回 grpc::Server*     │
    └───────────────────────┴───────────────────────────────────────────────────────────────────────────┘
    */
    std::string grpc_addr = cfg.listen_addr + ":" + std::to_string(cfg.listen_port);
    grpc::ServerBuilder builder;

    auto service = std::make_unique<LogicServiceImpl>(
        &conversation_store, &group_member_dao, &user_dao, &producer,
        &broadcast_producer, &redis_store);

    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    
    // 真正启动 grpc 服务
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    LogInfo("Logic gRPC server listening on " + grpc_addr);

    // 启动 HTTP 服务器（登录 / 发送消息）
    /*
        ┌──────────────────────┬────────────────┬──────────────────────────┬──────────────┐
        │         操作         │    传输方式     │           入口           │ 经过 Comet？ │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 注册                 │ HTTP POST      │ Logic 9101 /api/register │ 否           │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 登录                 │ HTTP POST      │ Logic 9101 /api/login    │ 否           │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 拉历史/会话列表/未读   │ HTTP POST      │ Logic 9101               │ 否           │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 建立实时连接          │ WebSocket 握手 │ Comet 9200               │ 是           │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 实时发消息、收推送     │ WS 帧 + gRPC   │ Comet → Logic            │ 是           │
        ├──────────────────────┼────────────────┼──────────────────────────┼──────────────┤
        │ 心跳 ping/pong        │ WS 控制帧      │ Comet                    │ 是           │
        └──────────────────────┴────────────────┴──────────────────────────┴──────────────┘
    */
    HttpApiServer httpServer(static_cast<uint16_t>(cfg.http_port),
                             &conversation_store, &user_dao, &group_dao,
                             &group_member_dao, &redis_store, &producer,
                             &broadcast_producer, &danmaku_dao, cfg);
    httpServer.setThreadNum(cfg.http_threads);
    LogInfo("Logic HTTP server listening on port " +
            std::to_string(cfg.http_port) + " with " +
            std::to_string(cfg.http_threads) + " IO threads");

    // 优雅关闭：收到 SIGTERM/SIGINT 后退出 HTTP 服务器并关闭 gRPC
    InstallSignalHandler([&httpServer, &server]() {
        httpServer.stop();
        if (server) {
            server->Shutdown();
        }
    });

    // gRPC 使用单独线程阻塞 Wait，HTTP 服务器在当前线程运行
    std::thread grpc_thread([&server]() { server->Wait(); });

    // 阻塞在 HTTP 服务器 io_context 上，直到 stop() 被触发
    httpServer.run();

    // 优雅关闭序列
    LogInfo("Logic shutting down gracefully...");
    if (server) {
        server->Shutdown();
    }
    if (grpc_thread.joinable()) {
        grpc_thread.join();
    }
    mysql_pool.Stop();
    LogInfo("Logic shutdown complete");
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
    // 先加载配置以拿到日志级别,再初始化日志到独立文件 logs/logic.log。
    // (LoadConfig 期间的解析告警会先走 stderr,属启动期,可接受)
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::InitLogging("logic",
                           /*also_stdout=*/false,
                           sparkpush::kDefaultLogRotateBytes, cfg.log_level);

    int ret = sparkpush::RunLogic(cfg);

    sparkpush::ShutdownLogging();
    return ret;
}

//

/*
第 1 个：handleLogin（logic/http_handlers_auth.cpp:15）
     
  最短、最典型，用来建立“一个 handler 长什么样”的模板。

  读的时候盯住这条线：

  RequirePost
    → ParseJsonAccountPassword
    → user_dao_->GetUserByAccount(account)     // 查 MySQL
    → 比较 password_hash                       // 前端已做 MD5
    → GenerateSecureToken()
    → redis_store_->SetToken(token, uid, 24h)  // token:{token}
    → WriteJson(resp, 0, "ok", {user_id, token})

  要回答：

  - 密码是明文还是 MD5？谁做的 MD5？
  - token 写到 Redis 的哪个 key？TTL 多长？
  - 管理员账号为什么走特殊分支？
  - 这个函数返回时，浏览器收到了什么？

  第 2 个：handleRegister（http_handlers_auth.cpp:90）

  对比登录看：

  - 先 GetUserByAccount 判断重复
  - 再 CreateUser 写 MySQL
  - 然后直接签发 token（“注册即登录”）
  - 返回和登录一样的 {user_id, token}

  要回答：注册和登录的相同点、不同点分别是什么？

  第 3 个：handleSendMessage（logic/http_handlers_message.cpp:31）重点

  这是 HTTP 路径的核心，也是你后面和 gRPC 的 HandleUpstreamMessage 对照的样板。

  按段读：

  ① 鉴权：from_user 来自 token，不能伪造
  ② 解析 body：to_user_id + content
  ③ 构造 session_id：single:min:max
  ④ AllocateMessageSeq：Redis 分配 msg_seq（必要时 MySQL 兜底）
  ⑤ 拼 msg_id：session_id + "-" + msg_seq
  ⑥ GetUserRoutes(to_user)：查接收方在哪些 Comet 上
  ⑦ 有路由：按 comet 逐个发 Kafka，key = session_id + "|" + comet_id
     无路由：persist_only=true，key = session_id
  ⑧ deliver_ok 决定返回成功还是 500

  要回答：

  - 消息发出去时，MySQL 里已经有了吗？
  - 在线和离线两条分支有什么区别？
  - 为什么在线 key 要带 comet_id？
  - Send 返回 true 代表 Kafka broker 确认了吗？
  - 接口返回成功，对方一定收到了吗？

  第 4 个：其余消息类 handler（同文件）

  handleHistory          :201   查历史（MySQL）
  handleMarkRead         :279   更新已读游标
  handleUnread           :326   MGET 算未读数
  handleSingleSessionList:373   单聊会话列表

  每个只抓三件事：读哪个表 / 哪个 Redis key / 怎么鉴权。第一遍不要逐行抠 SQL。

  读完 handler 后往下走

  handler 只是“壳”，真正干活的在下面三层。按这个顺序：

  1. logic/conversation_store.h / .cpp

  业务聚合层，handler 调得最多的就是它：

  - GetOrCreateSingleSession
  - AllocateMessageSeq
  - AppendMessage
  - GetHistory
  - GetUnread

  重点看 AllocateMessageSeq：Redis 快路径 + MySQL 兜底是怎么切换的。

  2. DAO 层（只看 public 方法和 SQL 大意）

  user_dao.*                  用户查询/创建
  session_dao.*               会话创建/查询、MySQL 最大序号
  message_dao.*               消息插入/历史查询
  user_session_state_dao.*    已读游标

  第一遍可以跳过 prepared statement 的每个 bind 细节。

  3. logic/redis_store.h / .cpp

  先看头文件里的 Key schema 注释，重点：

  token:{token}
  route:user:{uid}
  session:{session_id}:seq
  read_seq / unread
  room:{rid}:comets
  rate:...

  然后看两个 Lua：

  - IncrSessionMsgSeq
  - InitAndIncrSessionMsgSeq

  一个通用阅读技巧

  所有 handler 都是同一个骨架，认出骨架后读起来会快很多：

  1. RequirePost              只允许 POST
  2. AuthenticateRequest      取 token → user_id，失败直接 401
  3. ParseJsonBody            解析 body
  4. 字段校验                  缺字段/非法值 → 400
  5. 调 store / dao / redis / kafka
  6. WriteJson / WriteError   填响应

  你只要在每个 handler 里标出这 6 步，就能快速看出它“特殊”在哪。

  现在不要读的

  这些留到对应阶段：

  - http_handlers_chatroom.cpp / http_handlers_danmaku.cpp → 阶段 7
  - http_handlers_admin.cpp → 阶段 7
  - logic/grpc_service.cpp → 阶段 5（先把 HTTP 和存储吃透）

  对应指南位置：PROJECT_GUIDE.md 阶段 4 精读清单在 :1233，阶段 5 在 :1263。

  建议的节奏

  今天：handleLogin + handleRegister，画出“HTTP → MySQL → Redis → JSON”图
  明天：handleSendMessage，画出“HTTP → seq → 路由 → Kafka”图
  之后：history/unread + ConversationStore + DAO + RedisStore
  最后：回到 grpc_service.cpp，对照 HTTP 与 gRPC 两条入口

  一句话：先读 handleLogin 建立模板，再读 handleSendMessage 抓核心，然后顺着它往下读 ConversationStore → DAO → RedisStore。
*/

/*
- HTTP 的“入口层”你已经读完了：监听 → 解析 → 路由 → handler → 填 resp，这条线你已经打通。
  - 但 HTTP 这条业务链只读完了上半截。handler 里真正干活的调用都指向下一层：AllocateMessageSeq、AppendMessage、GetHistory、GetUnread、GetUserRoutes——这层你还没读。
  - 所以不建议现在直接跳 gRPC。grpc_service.cpp 的 HandleUpstreamMessage 和 handleSendMessage 几乎是同一个骨架，区别只在“谁调用、发送者身份从哪来、要不要回 ack”。存储层没吃透就去看
    gRPC，等于同样的东西读两遍，还看不出差异点。

  接下来 1～2 天：把 HTTP 下半截补完

  1. ConversationStore（最优先）

  - logic/conversation_store.cpp:80 AllocateMessageSeq —— 重点。Redis 快路径在 :88；返回 kNeedsInitialization 时从 MySQL 取 persisted_seq，再 InitAndIncr 在 :101。
  - :42 AppendMessage —— 先拿 seq（:53），再写 MySQL。
  - :110 GetHistory、:142 GetUnread。
  - 要回答：seq 为什么要“Redis 快路径 + MySQL 兜底”两层？消息是先分配 seq 还是先落库？未读数为什么用 MGET 一次拿两个 key？

  2. DAO 层（只看方法签名 + SQL 大意，不抠 bind）

  - logic/session_dao.cpp:230 GetOrCreateSingleSession —— single:min:max 怎么生成、并发重复插入怎么处理。
  - logic/session_dao.cpp:280 AllocateMessageSeq —— MySQL 兜底怎么取已持久化的最大 seq。
  - message_dao：插入消息 + 按 seq 范围查历史。
  - user_session_state_dao：已读游标的读写。

  如果你还没细看 handleHistory / handleMarkRead / handleUnread / handleSingleSessionList，不用单独再读一遍，读 ConversationStore 时对照着看即可。

  3. RedisStore（先 key schema，再两个 Lua）

  - logic/redis_store.h:15-26 的 key 注释。
  - logic/redis_store.cpp:468 IncrSessionMsgSeq。
  - logic/redis_store.cpp:498 InitAndIncrSessionMsgSeq。
  - 重点体会：为什么“判断初始化标记 + INCR”要放进一个 Lua 原子完成，而不是应用层先 GET 再 INCR。

  进 gRPC 前的门禁

  能脱稿答出这 5 个问题，再往下走：

  1. 一条单聊消息从 /api/message/send 进来，seq 在哪生成、消息在哪落库、Kafka key 为什么在线时要带 comet_id？
  - msg_seq 由 handleSendMessage 触发的 AllocateMessageSeq 分配，底层是 Redis 对 session:{sid}:seq 执行 Lua 原子 INCR；首次会从 MySQL 取历史最大序号做初始化。消息本身不在这里落库，而是封装成
   PushToCometRequest（need_persist=true）写入 Kafka push_to_comet，由 Job 的 PersistHandler 异步写入 MySQL 的 message 和 session 表。Kafka key 用 session_id|comet_id，是为了让同一会话到同一 Comet
   的消息进入同一分区、保证该分区内有序，同时不同 Comet 的 fanout 可以并行处理；key 只影响分区，真正的目标 Comet 由 payload 里的 comet_id 决定。

  2. session:{sid}:seq 第一次部署或丢失时，怎么保证新 seq 不撞历史？
  - 当 IncrSessionMsgSeq 发现 session:{sid}:seq_initialized 标记不存在时，不执行 INCR，返回 kNeedsInitialization；AllocateMessageSeq 随后用 SELECT COALESCE(MAX(msg_seq), 0) 查 MySQL
   得到该会话已持久化的最大序号，再调用 InitAndIncrSessionMsgSeq。后者的 Lua 在 Redis 里原子完成“把计数器抬到不低于 MySQL 最大值、写入初始化标记、执行
   INCR”，所以新序号从历史最大值之后开始，且并发首次初始化也只会执行一次。注意：Redis 完全不可用时不会走 MySQL，而是直接发号失败；MySQL 只在 Redis 可连接但计数器未初始化时提供安全下限。

  3. history 和 unread 分别读 MySQL 还是 Redis？为什么？
  -   history 读 MySQL：消息正文持久化在 message 表，GetHistory 最终调用 MessageDao::ListMessages 按 msg_seq 分页查询。unread 正常读 Redis：GetUnread 用一次 MGET 取 read_seq:{sid}:uid:<uid>、session:{sid}:seq       
   和初始化标记，未读数 = last_seq - read_seq；只有 Redis 不可用或序号未初始化时，才降级查 MySQL 的 user_session_state.read_seq 和 session.last_msg_seq。原因是 history 需要消息正文和持久化分页，而 unread
   只是两个计数器的差值，走 Redis 可以把每次查询压到一次 MGET。

  4. HTTP 鉴权和 Comet WS 握手校验 token，用的是同一个 Redis key 吗？
  - 是同一个 Redis key：token:{<token>}，value 是 user_id，登录/注册时用 SETEX 写入，TTL 24 小时。HTTP 路径由 AuthenticateRequest 直接调用 GetUserIdByToken 查这个 key；WebSocket 路径则由 Comet 把 token 通过 gRPC  
   VerifyToken 交给 Logic，Logic 调同一个 GetUserIdByToken 查同一个 key，成功后返回 user_id 并写入路由。Comet 本身不访问 Redis。需要注意，这里说的是“同一个 token 对应同一个 key”，不是全局只有一个
   token；每次登录都会生成新的 token。另外 token 只负责身份认证，管理员权限和会话归属还要额外校验。

  5. DAO 里 MySqlConnGuard + prepared statement 的套路能不能自己复述？
  -  DAO 不自己创建和关闭 MySQL 连接，而是调用 pool_->Acquire() 借出一个 MySqlConnGuard。Guard 内部持有真实的 MYSQL* 和连接池指针，get() 把裸连接交给 DAO；Guard 是 move-only 的 RAII 对象，析构时自动调用
   pool_->Release() 把连接放回空闲队列。DAO 拿到 MYSQL* 后，用 mysql_stmt_init 创建 statement，mysql_stmt_prepare 准备带 ? 的 SQL，mysql_stmt_bind_param 绑定参数，mysql_stmt_execute 执行；如果是 SELECT，再       
   mysql_stmt_store_result、mysql_stmt_bind_result、mysql_stmt_fetch 取结果。最后必须 mysql_stmt_close 关闭 statement，函数返回时 Guard 析构归还连接。连接池本身没有线程，它只是用 mutex + condition_variable       
   保护的空闲连接队列；连接是借给调用 Acquire() 的那个线程使用的。

  然后进阶段 5：gRPC

  按这个顺序：

  1. logic/grpc_service.h —— 先只看 RPC 列表，建立“RPC → 私有 handler”的映射：VerifyToken、SendUpstreamMessage、UserOffline、ReportRoomJoin/Leave、Broadcast、MessageStream。
  2. logic/grpc_service.cpp:47 VerifyToken —— 和 HTTP 的 AuthenticateRequest 对照，确认是同一个 token:{token}。
  3. :78 SendUpstreamMessage —— 薄壳，转给私有 handler。
  4. :241 HandleUpstreamMessage —— 全文精读，和 handleSendMessage 并排看，标出三处差异：调用方变成 Comet、发送者身份从哪来、回包/ack 怎么走。
  5. :443 HandleUserOffline、:462 HandleRoomJoin、:485 HandleRoomLeave —— 先看语义，房间细节可以押后。
  6. :512 MessageStream —— Comet 和 Logic 之间的双向流长连接。
  7. 之后读 common/kafka_producer.cpp，再对照读 comet/logic_stream_client.*。

  chatroom / danmaku / admin 按你说的留到阶段 7，不影响主线。
*/