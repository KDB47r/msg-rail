# MsgRail 锁、条件变量与原子变量清单

> 代码基线：2026-11-05，当前 `master` 工作区。  
> 范围：`common/`、`comet/`、`job/`、`logic/` 下的运行时 C++ 代码。  
> 附录：`tests/`、`load_test/` 的测试/压测代码。  
> 不包含：`third_party/`、`build/`、gRPC/librdkafka/Asio/websocketpp 内部锁。  
> 行号会随代码移动而变化，定位时优先看函数名，行号只作辅助。

## 0. 先看结论

| 关注点 | 结论 |
|---|---|
| 锁数量最多的对象 | `ConnRegistry`：`handle_mutex_` + 64 个用户分片锁 + 64 个房间分片锁 |
| 最细的锁粒度 | `HeartbeatBucket`（64 个心跳桶各一把锁）、每条 gRPC 流各两把锁、每个 Comet 一条发送队列锁 |
| 唯一的读写锁 | `RedisStore::route_cache_mutex_`，使用 `std::shared_mutex` |
| 唯一使用 `std::scoped_lock` 的地方 | `ConnRegistry::AddConn` / `JoinRoom`，同时锁两个分片 |
| 最重要的锁顺序 | `ConnRegistry` 的 `handle -> user -> room` |
| 最大的并发风险 | `LogicStreamClient::Send` 与 `Stop` 对 `streams_` 的生命周期竞争 |
| 没有显式锁、依赖线程封闭或生命周期约束的状态 | KafkaConsumer 的 `partitions_` / `inflight_` / `partitions_paused_`（线程封闭）；Comet 的 `next_heartbeat_bucket_`（线程封闭）；PushHandler 的 `streams_`（启动后只读、Stop 时消费者已停） |
| 没有项目自有锁、依赖库线程安全 | `KafkaProducer` 的 librdkafka producer；gRPC stub/channel |
| 未发现 | `std::recursive_mutex`、`std::once_flag`、`std::call_once`、直接 `pthread_mutex_*` |

理解这份文档时，先区分三类东西：

1. **互斥量/读写锁**：保护一段共享数据，必须和访问路径一起看。
2. **条件变量**：本身不保护数据，只负责“等某个条件成立/被唤醒”，必须与对应 mutex 配对。
3. **原子变量**：只保证单个变量的读写原子性，不等于“这一组状态都安全”。例如 `stream_running_` 是原子变量，但它不能保护 `streams_` 容器或发送队列。

本文表格里的“持有者”指**锁成员所属的类/对象**；实际持锁线程见“并发访问者”列。

---

## 1. 线程与并发访问地图

先建立线程模型，后面的锁才有落点。

| 进程/模块 | 线程或执行单元 | 典型入口 | 会接触的并发原语 |
|---|---|---|---|
| Logic | HTTP IO 线程 | `handleLogin`、`handleSendMessage`、`handleDanmaku*` | Redis 连接池锁、`RedisStore::route_cache_mutex_` |
| Logic | gRPC 服务线程 | `VerifyToken`、`AddRoute`、`RemoveRoute`、`SendUpstreamMessage` | Redis 连接池锁、路由缓存读写锁 |
| Logic | 任意业务线程 | `KafkaProducer::Send` | 无项目锁，依赖 librdkafka 内部线程安全 |
| Comet | Asio/websocketpp IO 线程 | `OnValidate`、`OnOpen`、`OnMessage`、`OnClose`、`OnPong` | `ConnRegistry` 分片锁、心跳桶锁、`ConnContext` 原子量、`LogicStreamClient` 队列锁 |
| Comet | 心跳线程 | `ScanNextHeartbeatBucket` | 64 个心跳桶锁、`ConnContext` 原子量、`heartbeat_stopping_` |
| Comet | `LogicStreamClient` writer 线程 | `WriterLoop` | 每条流的 `send_queue_mutex`、`pending_callbacks_mutex` |
| Comet | `LogicStreamClient` reader 线程 | `ReaderLoop` | 每条流的 `pending_callbacks_mutex`；回调里还会 `SendText` |
| Comet | `grpc_pool_` 工作线程 | `NotifyUserOffline`、`NotifyRoomJoin/Leave` 等 | `ThreadPool` 锁 |
| Comet | gRPC 服务线程 | `PushToUsers`、`PushToRoom`、`PushToAll` | `ConnRegistry` 分片锁；发送时依赖 websocketpp/asio 的写串行化 |
| Job | Kafka 消费线程（push/broadcast 各一个） | `KafkaConsumer::Loop` | `completion_mutex_`、分区状态（线程封闭） |
| Job | Kafka worker 线程 | `PushHandler::HandleMessage` / `HandleBroadcastTask` | `stub_mu_`、每个 Comet 的 `queue_mutex` |
| Job | 每个 Comet 的 writer 线程 | `PushHandler::StreamWriterLoop` | 对应 Comet 的 `queue_mutex` |
| 通用 | `TimerWheel` 工作线程 | `worker_loop` | 时间轮 `mutex_`、`TimerState` 原子量 |
| 通用 | main/信号处理 | `ShutdownRequested()`、连接池 `Stop`、日志 `ShutdownLogging` | 关闭原子标志、连接池锁、日志初始化锁 |

---

## 2. 互斥量与条件变量全量清单

### 2.1 `common/` 基础设施

| 锁 / 条件变量 | 持有者 | 保护的共享数据 / 解决的冲突 | 加锁、等待、通知调用处 | 并发访问者 |
|---|---|---|---|---|
| `ThreadPool::mutex_` + `ThreadPool::cv_` | 每个 `ThreadPool` 实例，例如 Kafka worker 池、Comet 的 `grpc_pool_` | `tasks_` 任务队列、`stopping_` 停止标志；解决多生产者提交任务与多 worker 取任务的竞争，以及 Stop 与 worker 的退出竞争 | `Start()` 加锁检查 `workers_`（25）；`Submit()` 加锁入队（63）；`WorkerLoop()` 用 `unique_lock` + `cv_.wait(predicate)` 等待任务（81-82）；`Stop()` 先加锁置 `stopping_`（41），`notify_all`（45），join 后再加锁交换清空 `tasks_`（54） | 任意 `Submit` 调用线程、worker 线程、Stop/析构线程 |
| `MySqlConnectionPool::mutex_` + `cv_` | 每个 `MySqlConnectionPool` 实例 | `idle_` 空闲连接队列、`total_conns_` 总连接数、`stopping_`、`inited_`、`cfg_`；解决多业务线程借还连接、扩容、停止之间的竞争 | 析构加锁关闭空闲连接（48）；`Init()` 加锁预建连接（103）；`Acquire()` 加锁取空闲连接/扩容/`cv_.wait`/`wait_for`（141、161、165）；`mysql_ping` 前主动 `unlock()`（176），失败后重新加锁减计数（182），再 `lock()`（187）；`Release()` 加锁归还并 `notify_one`（201-213）；`Stop()` 加锁置停止并 `notify_all`（219-224）；`PopIdleConnectionUnlocked()`/`CleanupIdleUnlocked()` 只允许持锁调用 | 业务线程（HTTP/gRPC/DAO）、`Stop`/析构线程 |
| `RedisConnectionPool::mutex_` + `cv_` | 每个 `RedisConnectionPool` 实例 | `idle_`、`total_conns_`、`stopping_`、`cfg_`；解决多业务线程借还 Redis 连接、坏连接销毁、停止之间的竞争 | 析构加锁释放空闲连接（44）；`Init()` 加锁预建连接（101）；`Acquire()` 加锁取连接/扩容/等待（130、145、149）；`PopIdleUnlocked()`/`CleanupIdleUnlocked()` 持锁调用；`Release()` 加锁，坏连接或 `ctx->err` 直接销毁并 `notify_one`（194-204） | 业务线程、`Release`（由 `RedisConnGuard` 析构触发）、`Stop`/析构线程 |
| `KafkaConsumer::completion_mutex_` + `completion_cv_` | 每个 `KafkaConsumer` 实例；Job 中有 push 和 broadcast 两个消费者 | `completions_` 完成通知队列；解决 worker 线程完成任务后通知消费线程“某 partition 可继续”的跨线程交接 | worker 回调末尾加锁 `completions_.push_back(key)` 并 `notify_one`（148-151）；消费线程 `DrainCompletions()` 加锁 `swap` 出队列（180-182）；`Loop()` 在停止阶段加锁 `wait_for`（86-88）；`Stop()` `notify_all`（71） | 消费线程与线程池 worker 线程 |
| `TimerWheel::mutex_` + `TimerWheel::cv_` | 全局 `TimerWheel` 单例 | `near_`、`levels_` 时间轮桶、`current_tick_`、`current_absolute_tick_`、`next_wakeup_tick_`、`has_next_wakeup_`、`stopping_`；解决任意线程注册定时器与 worker 推进时间轮的竞争 | `add_timer()` 加锁插入节点、重建最近唤醒时间，必要时 `notify_one`（79-85）；`worker_loop()` 用 `unique_lock` 等待/定时等待（91-100），推进后 `unlock()` 执行 `mark_expired()`，再 `lock()`（109-111）；析构加锁置 `stopping_` 并 `notify_one`（25-28）；所有 `*_locked` 函数要求持锁 | 任意 `add_timer`/`LocalTimer::schedule` 调用线程、TimerWheel worker、析构线程 |
| `g_init_mutex`（`common/logging.cpp`） | 进程级全局 | 日志初始化/关闭过程，以及 `g_logger` 的写入；解决 `InitLogging`/`ShutdownLogging` 之间的竞争 | `InitLogging()` 加锁创建 sink/logger（67）；`ShutdownLogging()` 加锁 flush/reset（99）；`LogLine()` **不加锁**直接复制 `g_logger`（114） | 任意日志调用线程、初始化/关闭线程 |

#### 补充说明

- **连接池的等待条件**：MySQL/Redis 的 `cv_.wait(lock)` 没有 predicate，外层 `while (!stopping_)` 循环负责处理虚假唤醒；超时路径用 `wait_for`，返回空 Guard。
- **MySQL 的锁外检查**：`Acquire()` 在 `ValidateConnection()`（`mysql_ping`）前主动释放锁，避免网络 I/O 长时间占用池锁；这是有意的锁范围优化。
- **Redis 热路径**：`RedisConnectionPool::Acquire()` 不做 PING 验证，连接失效在真正执行命令时发现，Guard 标记 broken 后由 `Release()` 销毁。
- **Kafka 的线程封闭**：`partitions_`、`inflight_`、`partitions_paused_` 只由消费线程访问，所以没有锁；worker 只通过 `completions_` 与消费线程交接。详见第 5 节。
- **日志的已知问题**：`g_logger` 是 `std::shared_ptr`，`LogLine()` 的复制没有受 `g_init_mutex` 保护；如果 `ShutdownLogging()` 与日志调用并发，存在数据竞争/悬空风险。详见第 6 节。

### 2.2 `comet/` 连接与流

| 锁 / 条件变量 | 持有者 | 保护的共享数据 / 解决的冲突 | 加锁、等待、通知调用处 | 并发访问者 |
|---|---|---|---|---|
| `ConnRegistry::handle_mutex_` | 每个 `ConnRegistry`，即每个 Comet 实例 | `hdl_to_uid_`：连接句柄到 user_id 的反查表；解决连接句柄反查与连接增删的竞争 | `AddConn()` 与用户分片锁一起用 `std::scoped_lock` 加锁（20）；`RemoveConn()` 先单独加锁查找/删除 `hdl_to_uid_`（30-35）；`UserIdForConn()` 加锁查询（136） | 多个 Asio IO 线程；Comet 停止线程；可能的查询方 |
| `ConnRegistry::UserShard::mutex`（64 个） | `ConnRegistry` 的每个 `UserShard` | `conns`：`user_id -> set<ConnHandle>`；解决同一用户多连接增删、按用户查连接、全量遍历的竞争 | `AddConn()`（20）；`RemoveConn()` 用 `unique_lock` 持有用户分片（40）；`ConnsForUsers()` 逐个用户分片加锁拷贝（98）；`AllConns()` 逐个分片加锁拷贝（126）；`JoinRoom()` 虽然只改房间表，也用 `scoped_lock` 锁用户分片（72），与 `RemoveConn()` 的最后连接清理互斥 | IO 线程（连接建立/关闭）、gRPC 推送线程（按用户查连接）、Comet `Stop()` |
| `ConnRegistry::RoomShard::mutex`（64 个） | `ConnRegistry` 的每个 `RoomShard` | `users`：`room_id -> set<user_id>`；解决加入/离开房间、按房间查成员、最后连接断开清理的竞争 | `JoinRoom()` 用 `scoped_lock`（72）；`LeaveRoom()` 加锁删除（79）；`RemoveConn()` 在持有用户分片时逐个房间分片加锁清理（53）；`ConnsInRoom()` 先加锁拷贝用户列表（114），释放后再调用 `ConnsForUsers()` | IO 线程（聊天室加入/离开）、gRPC 推送线程（按房间查连接）、`RemoveConn()` |
| `CometServer::heartbeat_buckets_[i].mutex`（64 个） | `CometServer` 的每个 `HeartbeatBucket` | `entries`：`(ConnHandle, weak_ptr<ConnContext>)` 心跳条目；解决 IO 线程加入连接与心跳线程扫描/删除的竞争 | `AddToHeartbeatBucket()` 加锁 `push_back`（268）；`ScanNextHeartbeatBucket()` 加锁遍历、清理失效/关闭连接、生成 ping/close 动作（289-303）；`next_heartbeat_bucket_` 只由心跳线程推进（279） | OnOpen 的 IO 线程、心跳线程 |
| `CometServer::heartbeat_wait_mutex_` + `heartbeat_wait_cv_` | 每个 `CometServer` | 不保护业务容器，只用于心跳线程的等待/唤醒配对；等待谓词是原子量 `heartbeat_stopping_` | 心跳线程 `unique_lock` + `wait_for` 10ms（328-334）；`StopHeartbeat()` 置 `heartbeat_stopping_=true` 后 `notify_all`（348-349） | 心跳线程、`StopHeartbeat()` 调用线程 |
| `LogicStreamClient::StreamState::send_queue_mutex` + `send_queue_cv` | `LogicStreamClient` 中每条 `StreamState` | 单条流的 `send_queue`；解决多个 `Send()` 调用者与单条流 writer 线程的出队竞争 | `Send()` 加锁入队并 `notify_one`（169-172）；`WriterLoop()` 用 `unique_lock` + `cv.wait(predicate)` 等待队列非空或停止（102-105）；`Stop()` `notify_all`（71） | 多个 IO 线程（`Send`）、该流 writer 线程、`Stop()` |
| `LogicStreamClient::StreamState::pending_callbacks_mutex` | `LogicStreamClient` 中每条 `StreamState` | `pending_callbacks_`：`request_id -> 回调`；解决 writer 线程登记回调与 reader 线程取出回调的竞争 | `WriterLoop()` 在 `Write` 前加锁插入（115-117）；`ReaderLoop()` 读响应后加锁查找并移出（139-144）；回调在锁外执行（147-149） | 该流 writer 线程、该流 reader 线程 |

### 2.3 `job/` 推送

| 锁 / 条件变量 | 持有者 | 保护的共享数据 / 解决的冲突 | 加锁、等待、通知调用处 | 并发访问者 |
|---|---|---|---|---|
| `PushHandler::stub_mu_` | 每个 `PushHandler` | `comet_stubs_`：`comet_id -> gRPC Stub` 懒加载缓存；解决多个 Kafka worker 同时首次创建同一 Comet stub 的竞争 | `GetStub()` 全程加锁：查缓存、`CreateChannel`、`NewStub`、插入（272-282）；返回裸指针后立即释放锁 | 多个 Kafka worker（push/broadcast 回调） |
| `PushHandler::StreamState::queue_mutex` + `queue_cv` | `PushHandler` 中每个 Comet 的 `StreamState` | 该 Comet 的 `send_queue`；解决多个 Kafka worker 投递与单个 writer 线程取消息的竞争 | `SendToStream()` 加锁入队并 `notify_one`（217-220）；`StreamWriterLoop()` 用 `unique_lock` + `cv.wait(predicate)`（155-165）；`Stop()` 对所有队列 `notify_all`（239） | Kafka worker 线程、该 Comet writer 线程、`Stop()` |

### 2.4 `logic/` 路由缓存

| 锁 / 条件变量 | 持有者 | 保护的共享数据 / 解决的冲突 | 加锁、等待、通知调用处 | 并发访问者 |
|---|---|---|---|---|
| `RedisStore::route_cache_mutex_`（`std::shared_mutex`） | 每个 `RedisStore`，即每个 Logic 实例 | `route_cache_`：`user_id -> RouteCacheEntry` 本地路由缓存；解决读多写少的缓存查询与失效/回填竞争，读之间不互斥 | `GetUserRoutes()` 先用 `shared_lock` 查缓存（160-165）；未命中释放读锁，查 Redis 后用 `unique_lock` 回填（174-179）；`InvalidateRouteCache()` 用 `unique_lock` 删除（220）；`AddRoute`/`RemoveRoute` 成功后调用失效（124、145） | 多个 Logic HTTP IO 线程、gRPC 服务线程 |

#### 补充说明

- **ConnRegistry 的锁顺序**：`handle -> user -> room`。`AddConn` 同时锁 `handle + user`；`RemoveConn` 先释放 `handle`，再持有 `user`，最后锁每个 `room`；`JoinRoom` 同时锁 `user + room`；`ConnsInRoom` 先拷贝房间用户并在释放房间锁后才查用户分片，因此不会形成 `room -> user` 的反向边。详见第 4 节。
- **ConnRegistry 的分片目的**：按 `user_id`/`room_id` 哈希到 64 个分片，降低单个大 map 的锁竞争；代价是 `AllConns()` 需要逐分片加锁，`RemoveConn()` 需要扫描所有房间分片。
- **心跳桶的目的**：64 个桶轮转扫描，避免每 500ms 一次扫描全部连接；每个桶独立加锁，OnOpen 与扫描线程只在对应桶上竞争。
- **`ConnsInRoom()` 是快照语义**：它在房间锁内拷贝 user_id 列表，释放锁后才查各用户分片；返回后房间成员关系可能已经变化，调用方需要容忍句柄失效或发送失败。
- **流客户端的锁分离**：队列锁和回调表锁是两把独立的锁，writer 先释放队列锁再拿回调锁；reader 只在回调表锁内做 map 操作，回调本身在锁外执行，避免回调重入死锁。
- **PushHandler 的 `stub_mu_` 不覆盖 RPC**：`GetStub()` 返回裸指针后就释放锁，`ProcessPushRequest()` 的 gRPC 调用在锁外执行；stub 的 lifetime 由 `comet_stubs_` 持有到 `PushHandler` 析构。
- **RedisStore 的本地缓存**：`route_cache_mutex_` 只保护本进程的 map；多 Logic 实例之间不会即时失效，默认受 1 秒 TTL 影响。这不是锁能解决的问题，是缓存一致性策略。

### 2.5 条件变量与等待谓词一览

| 条件变量 | 等待方 | 谓词 / 退出条件 | 唤醒方 |
|---|---|---|---|
| `ThreadPool::cv_` | worker 线程 | `stopping_ \|\| !tasks_.empty()` | `Submit()` 入队后 `notify_one`；`Stop()` `notify_all` |
| `MySqlConnectionPool::cv_` | `Acquire()` 调用者 | 无 predicate；外层 `while (!stopping_)` 循环处理虚假唤醒，`wait_for` 超时返回空 Guard | `Release()` `notify_one`；`Stop()`/析构 `notify_all` |
| `RedisConnectionPool::cv_` | `Acquire()` 调用者 | 无 predicate；外层 `while (!stopping_)` 循环处理虚假唤醒 | `Release()` `notify_one`；析构 `notify_all` |
| `KafkaConsumer::completion_cv_` | 消费线程（停止/排空阶段） | `!completions_.empty()` | worker 完成任务后 `notify_one`；`Stop()` `notify_all` |
| `TimerWheel::cv_` | 时间轮 worker | 无最近唤醒时间时 `stopping_ \|\| has_next_wakeup_`；有唤醒时间时 `stopping_ \|\| next_wait_time_locked() < wait_time` | `add_timer()` 发现更早唤醒时 `notify_one`；析构 `notify_one` |
| `CometServer::heartbeat_wait_cv_` | 心跳线程 | `heartbeat_stopping_ == true` | `StopHeartbeat()` `notify_all` |
| `LogicStreamClient::StreamState::send_queue_cv` | 该流 writer 线程 | `!send_queue.empty() \|\| !stream_running_` | `Send()` `notify_one`；`Stop()` `notify_all` |
| `PushHandler::StreamState::queue_cv` | 该 Comet writer 线程 | `!send_queue.empty() \|\| !stream_running_` | `SendToStream()` `notify_one`；`Stop()` `notify_all` |

---

## 3. 原子变量全量清单

### 3.1 运行时原子变量

| 原子变量 | 持有者 | 写入口 | 读入口 | 内存序 | 用途 / 备注 |
|---|---|---|---|---|---|
| `g_level`（`common/logging.cpp:23`） | 进程级日志模块 | `InitLogging()` 的 `store`（84） | `InitLogging()` 的 `load`（87）、`ShouldLog()` 的 `load`（109） | `relaxed` | 控制日志级别；只传递一个整数级别，不发布其他数据，relaxed 足够 |
| `ShutdownRequested()` 内的 `std::atomic<bool>`（`common/signal_handler.h:10-13`） | 进程级静态变量 | 信号处理函数 `store(true, relaxed)`（25） | 各服务主循环 `load(relaxed)`，例如 `job/main.cpp:70` | `relaxed` | 优雅关闭标志；信号处理函数与主循环之间只需要最终可见的标志位 |
| `HttpServerLite::Impl::stopped`（`common/net/http_server_lite.cpp:97`） | 每个 `HttpServerLite` 的 `Impl` | `stop()` 的 `store(true, relaxed)`（345） | `DoAccept()` 的 `load(relaxed)`（307） | `relaxed` | 控制 accept 回调是否继续重新挂起；真正的停止还依赖 `acceptor.close()` 和 `io_ctx.stop()` |
| `KafkaConsumer::running_`（`common/kafka_consumer.h:158`） | 每个 `KafkaConsumer` | `Start()` / `Stop()` 的 `exchange`（63、70） | `Loop()` 的条件判断（83、85） | 默认 `seq_cst` | 控制消费循环是否继续；线程 join 负责生命周期同步，原子量只做运行标志 |
| `TimerWheel::next_id_`（`common/timer_wheel.h:120`） | 全局 `TimerWheel` | `add_timer()` 的 `fetch_add(1, relaxed)`（64） | 返回值写入 `TimerToken`/`TimerNode` | `relaxed` | 生成唯一 TimerId；只需要唯一性，不需要顺序同步 |
| `TimerState::ready`（`common/timer_wheel.h:14`） | 每个定时器的共享状态（`shared_ptr<TimerState>`，由 TimerWheel 节点和 LocalTimer 共同持有） | 零延迟定时器 `store(true, release)`（66）；到期时 `store(true, release)`（282） | `LocalTimer::execute_if_ready()` 的 `load(acquire)`（38） | `release` / `acquire` | 向定时器持有者发布“已到期”；与 `cancelled` 配合决定是否执行回调 |
| `TimerState::cancelled`（`common/timer_wheel.h:15`） | 每个定时器的共享状态 | `LocalTimer::cancel()` 的 `store(true, release)`（23） | TimerWheel 到期检查 `load(acquire)`（281）；`LocalTimer::execute_if_ready()` 的 `load(acquire)`（33） | `release` / `acquire` | 跨线程取消标志；只能保证标志可见，不能单独保证 `LocalTimer` 对象的整体线程安全 |
| `ConnContext::last_pong_ms`（`comet/comet_server.h:34`） | 每条 WebSocket 连接的 `ConnContext` | `OnValidate()` 初始化 `store(relaxed)`（189）；`UpdatePong()` 刷新 `store(relaxed)`（247） | `ScanNextHeartbeatBucket()` 读取 `load(relaxed)`（299） | `relaxed` | 记录最后活跃时间，用于心跳超时判断；时间戳只用于近似判断，不需要同步其他数据 |
| `ConnContext::closed`（`comet/comet_server.h:35`） | 每条连接的 `ConnContext` | `OnClose()` 的 `store(true, release)`（207） | `OnMessage()`（225）、`UpdatePong()`（246）、心跳扫描（294）的 `load(acquire)` | `release` / `acquire` | 关闭标志；使 IO 线程/心跳线程在连接关闭后不再处理消息或刷新 pong |
| `CometServer::heartbeat_stopping_`（`comet/comet_server.h:107`） | 每个 `CometServer` | `ScheduleHeartbeat()` 的 `store(false, release)`（322）；`StopHeartbeat()` 的 `store(true, release)`（348） | 心跳线程循环中的 `load(acquire)`（329、333、335） | `release` / `acquire` | 心跳线程停止标志，配合 `heartbeat_wait_cv_` 的谓词使用 |
| `CometServer::stopped_`（`comet/comet_server.h:108`） | 每个 `CometServer` | `Stop()` 的 `compare_exchange_strong(expected, true)`（115，默认 seq_cst） | `ScanNextHeartbeatBucket()`（305）、posted lambda（307）的 `load(acquire)` | 写默认 `seq_cst`，读 `acquire` | 保证 `Stop()` 幂等，并阻止停止后的心跳扫描再投递 ping/close |
| `CometServer::conn_id_counter_`（`comet/comet_server.h:119`） | 每个 `CometServer` | `OnValidate()` 的 `fetch_add(1, relaxed)`（158） | 生成的 sequence 用于拼接 `conn_id`（159） | `relaxed` | 为每条新连接生成唯一序号，`conn_id = comet_id:sequence` |
| `LogicStreamClient::stream_running_`（`comet/logic_stream_client.h:85`） | 每个 `LogicStreamClient` | `Start()` 置 true（42）；`Stop()` 置 false（64） | `Running()`（48）、`WriterLoop()`（99、104、107）、`ReaderLoop()`（135、152） | 默认 `seq_cst` | 控制读写循环；注意它不能保护 `streams_`、`send_queue`，这些有各自的锁/生命周期假设 |
| `LogicStreamClient::stopped_`（`comet/logic_stream_client.h:86`） | 每个 `LogicStreamClient` | `Stop()` 的 `compare_exchange_strong(expected, true)`（60） | 同一个 `Stop()` 入口（60） | 默认 `seq_cst` | 保证 `Stop()` 幂等，可被显式调用和析构重复触发 |
| `LogicStreamClient::request_id_counter_`（`comet/logic_stream_client.h:88`） | 每个 `LogicStreamClient` | `NextRequestId()` 的 `fetch_add(1, relaxed)`（18） | `Send()` 中用于选流和设置 `request_id`（161-165） | `relaxed` | 生成流内唯一 request_id；选流公式是 `request_id % stream_count_` |
| `PushHandler::stream_running_`（`job/push_handler.h:52`） | 每个 `PushHandler` | `InitStreams()` 置 true（77）；`Stop()` 置 false（237） | `HandleMessage()`（304）、`StreamWriterLoop()`（152、157、160、177、180） | 默认 `seq_cst` | 流模式生命周期标志；`Stop()` 先停消费者，再置 false 并唤醒 writer |

#### 原子变量的内存序解释

- **`relaxed` 的使用场景**：日志级别、关闭标志、时间戳、唯一 ID。它们只需要“这个变量本身读写不撕裂/不丢更新”，不需要用它来发布另一块数据。
- **`release/acquire` 的使用场景**：`TimerState::ready/cancelled`、`ConnContext::closed`、`CometServer::heartbeat_stopping_`。这些标志需要让另一个线程在看到标志时，也能看到此前写入的相关状态。
- **默认 `seq_cst` 的使用场景**：`KafkaConsumer::running_`、`CometServer::stopped_`、`LogicStreamClient::stream_running_/stopped_`、`PushHandler::stream_running_`。这些是生命周期开关，数量少、竞争不极端，使用默认顺序更容易推理。
- **原子量不是锁**：`stream_running_` 为 true 不代表 `streams_` 可以安全遍历；`stopped_` 为 false 也不代表 `send_queue` 的入队/出队安全。容器和队列仍要靠对应 mutex 或严格的生命周期约束。

### 3.2 测试与压测代码中的原子变量

| 文件 | 原子变量 | 用途 |
|---|---|---|
| `load_test/load_tester.cpp:127-128` | `Counter::sent`、`Counter::failed` | 压测客户端统计发送/失败数；多个客户端线程并发累加 |
| `load_test/async_ws_bench.cpp:133-144` | `Stats` 的 `conn_attempted`、`conn_established`、`conn_failed`、`msg_sent`、`ack_recv`、`error_recv`、`push_recv`、`unmatched_ack`、`bytes_sent`、`bytes_recv`、`send_eagain`、`send_errors` | 异步 WebSocket 压测统计；多连接线程并发累加 |
| `load_test/async_ws_bench.cpp:157` | `Stats::latencies_mutex`（`std::mutex`） | 保护 `latencies_us` 延迟样本容器；`AddLatency` 加锁 push |
| `load_test/async_ws_bench.cpp:626-627` | `AsyncClient::running_`、`AsyncClient::send_enabled_` | 控制单个压测客户端是否继续运行/发送 |
| `load_test/http_bench.cpp:46-51` | `g_requests_sent`、`g_responses_received`、`g_errors`、`g_bytes_sent`、`g_bytes_received`、`g_active_conns` | HTTP 压测全局统计；多线程并发累加 |
| `tests/test_thread_pool.cpp:26、44、62` | `counter`、`sum`、`executed` | 验证线程池任务执行次数、结果和完成标志 |
| `tests/test_config.cpp:19` | `static std::atomic<int> counter` | 原子递增生成唯一临时配置文件名，避免测试间同名覆盖 |
| `tests/test_timer_wheel.cpp:15、37` | `executed` | 验证定时器回调是否在到期后执行 |

测试/压测代码的原子量只服务于统计或测试同步，不参与服务运行时的业务不变量；`third_party/doctest.h` 内部锁不在统计范围内。

---

## 4. 锁顺序与死锁分析

### 4.1 ConnRegistry 是唯一有多把业务锁嵌套的地方

`ConnRegistry` 的加锁关系可以画成：

```text
AddConn:      handle ─┐
                      ├─ std::scoped_lock
              user ───┘

RemoveConn:   handle（查/删后立即释放）
                 └─> user ──> room[0..63]（逐个加锁、释放）

JoinRoom:     user ─┐
                    ├─ std::scoped_lock
              room ─┘

LeaveRoom:    room

ConnsInRoom:  room（拷贝 user 列表后释放）
                 └─> ConnsForUsers() ──> user[0..63]（逐个加锁、释放）
```

有效锁顺序是：

```text
handle -> user -> room
```

说明：`std::scoped_lock` 内部使用死锁避免算法，实际加锁顺序不保证与书写顺序一致；这里画的是逻辑锁层次。其他显式顺序路径（尤其 `RemoveConn` 的 `user -> room`）遵循这个层次，`std::scoped_lock` 负责在多锁场景下避免互相等待。

为什么没有死锁：

1. `AddConn` 只同时拿 `handle + user`，不会在持有 `handle` 时去拿 `room`。
2. `RemoveConn` 先释放 `handle`，再拿 `user`，最后在持有 `user` 时逐个拿 `room`；不会反向去拿 `handle`，也不会在持有 `room` 时回头拿 `user`。
3. `JoinRoom` 用 `std::scoped_lock` 同时锁 `user + room`，与 `RemoveConn` 的 `user -> room` 方向一致。
4. `ConnsInRoom` 在持有 `room` 时只拷贝用户列表，释放 `room` 后才调用 `ConnsForUsers()` 去拿 `user`，故意避免了 `room -> user` 的反向边。
5. `AllConns()`/`ConnsForUsers()` 每次只拿一个分片锁，不嵌套。

需要注意的不是死锁，而是**临界区长度**：`RemoveConn` 在最后一条连接断开时，持有用户分片锁扫描全部 64 个房间分片。若同一用户分片上的连接频繁建立/断开，会被这段清理阻塞。数据量更大时，可以考虑维护 `user -> rooms` 的反向索引，或把房间清理拆成“锁内收集 + 锁外批量处理”，但要重新处理与重连/重新入房的交错一致性。

### 4.2 其他模块的嵌套情况

| 模块 | 是否嵌套多把锁 | 说明 |
|---|---|---|
| `ThreadPool` | 否 | 只持有 `mutex_`，任务在锁外执行 |
| MySQL/Redis 连接池 | 否 | 只持有池 `mutex_`；MySQL 的 `mysql_ping` 在锁外 |
| `KafkaConsumer` | 否 | 只持有 `completion_mutex_`；分区状态线程封闭 |
| `TimerWheel` | 否 | 时间轮锁内只操作链表，`mark_expired()` 在锁外 |
| 日志 | 否 | `g_init_mutex` 只保护初始化/关闭；`LogLine` 不加锁（因此有风险，见第 6 节） |
| `CometServer` 心跳 | 否 | 桶锁只保护 entries；收集动作后释放锁再 `post`，不在桶锁内做网络操作 |
| `LogicStreamClient` | 否（同一时刻只持一把） | 队列锁释放后才拿回调表锁；reader 回调在锁外执行 |
| `PushHandler` | 否 | `stub_mu_` 只保护 stub map，RPC 在锁外；队列锁只保护单 Comet 队列 |
| `RedisStore` | 否 | 读锁/写锁只在缓存访问期间持有，Redis 查询在锁外 |

一个重要的设计点：**不要在持锁时执行用户回调或网络 I/O**。项目里大部分地方遵守了这一点：

- `ThreadPool` 在锁外执行任务。
- `TimerWheel::mark_expired()` 在锁外执行。
- `LogicStreamClient` 在锁外执行响应回调。
- `RedisStore` 在锁外查 Redis。
- `ConnRegistry` 返回 `ConnHandle` 副本，发送 WebSocket 消息在锁外进行。

例外/近似例外：

- `MySQLConnectionPool::Acquire()` 在锁内调用 `CreateConnectionUnlocked()` 建连（扩容路径），建连可能较慢；`mysql_ping` 则特意放到了锁外。
- `PushHandler::GetStub()` 在 `stub_mu_` 内创建 channel/stub，首次访问某个 Comet 时可能阻塞其他 worker 的首次 stub 创建。
- `ConnRegistry::RemoveConn()` 在用户分片锁内扫描所有房间分片。

---

## 5. 没有显式锁的共享状态：线程封闭与生命周期假设

这部分很重要：读代码时不能只搜 `mutex`，还要知道哪些状态“靠约束而不是靠锁”。

| 状态 | 为什么没有锁 | 依赖的约束 |
|---|---|---|
| `KafkaConsumer::partitions_`、`inflight_`、`partitions_paused_` | 只有消费线程 `Loop()`/`HandleMessage()`/`DrainCompletions()` 访问 | 严格的线程封闭；worker 只把完成通知放入受锁保护的 `completions_` |
| `KafkaConsumer::consumer_`、`workers_`、`callback_`、`options_` | `Init()` 在 `Start()` 前完成，`Stop()` join 消费线程和 worker 后才销毁 | 生命周期顺序：Init -> Start -> Stop；运行期只读 |
| `CometServer::next_heartbeat_bucket_` | 只有心跳线程 `ScanNextHeartbeatBucket()` 访问 | 心跳线程单线程推进；心跳线程在 `StopHeartbeat()` 中 join |
| `ConnContext::user_id`、`conn_id`、`handle`、`heartbeat_bucket` | `OnValidate()` 写入，连接发布后基本只读 | `OnValidate` 完成前连接尚未进入 registry/心跳桶；`OnOpen` 之后这些字段不再修改 |
| `ConnContext::last_pong_ms`、`closed` | 跨线程读写，但已经用原子量 | 见第 3 节；只保护单个标志/时间戳，不保护整个对象 |
| `CometServer::use_stream_`、`stream_count_`、`channel_`、`stream_client_` | 配置/启动阶段写入，运行期读取 | 启动前初始化；`Stop()` 先停止 IO，再 `stream_client_->Stop()` |
| `PushHandler::comet_addrs_` | `ParseCometTargets()` 在 `Init()` 中写完，运行期只读 | Init 在 Start 前；运行期不修改 |
| `PushHandler::streams_` | `InitStreams()` 在 Start 前写完；运行期只读 map，`Stop()` 在消费者停止后清空 | 正常顺序：Init -> Start -> Stop；`SendToStream` 只从 Kafka 回调进入，Stop 先停消费者 |
| `PushHandler::use_stream_`、`cfg_` | 构造/Init 后只读 | 不在运行期修改 |
| `LogicStreamClient::streams_` | `Start()` 在发送前建立；`Stop()` 在 join 后清空 | **这是有风险的假设**：`Send()` 必须保证不与 `Stop()` 并发；当前代码没有强制这个约束，见第 6 节 |
| `LogicStreamClient::StreamState::stream`、`ctx`、`writer_thread`、`reader_thread` | Start 建立，Stop 在 WritesDone/TryCancel 后 join | Stop 与 Send/Write 的并发需要上层保证；gRPC 的 `WritesDone`/`TryCancel` 用于唤醒阻塞 Read/Write |
| `LocalTimer::state_`、`callback_` | 设计为单线程使用；跨线程只通过 `TimerState` 原子量通信 | `schedule`/`cancel`/`execute_if_ready` 应由同一个事件循环线程调用；不是线程安全对象 |
| `KafkaProducer::producer_`、`topic_` | 项目层没有 mutex | 依赖 librdkafka Producer 的线程安全；`producer_`/`topic_` 在 `Init` 后只读使用，析构前必须确保所有发送线程已停止 |
| `HttpServerLite::Impl::callback`、`threads` | 配置阶段写入 | `setHttpCallback`/`setThreadNum` 必须在 `run()` 前调用；运行期只读 |
| `RedisStore::pool_`、`route_cache_ttl_ms_` | 构造/初始化后只读 | 运行期不修改；缓存 map 本身由 `route_cache_mutex_` 保护 |
| gRPC `Stub`/`Channel`、librdkafka 内部对象 | 项目层没有锁 | 依赖 gRPC/librdkafka 的线程安全契约；对象生命周期由上层保证 |

**读代码时的判断规则**：

- 看到 `std::atomic`，先问“它保护的是单个变量，还是一组不变量？” 如果是一组，通常还缺锁或生命周期约束。
- 看到 `unordered_map`/`queue` 被多个线程访问，先找对应 mutex；找不到就找“是否只由一个线程访问”或“是否在启动后只读”。
- 看到 `std::thread` 成员，先看谁 join、join 前是否还会被其他线程访问。

---

## 6. 已知并发风险与改进建议

按“可能导致崩溃/数据竞争”优先，不是按代码风格排序。

### 6.1 `LogicStreamClient::Send()` 与 `Stop()` 的 `streams_` 生命周期竞争（高）

相关代码：

- `Send()` 从 `streams_[stream_idx]` 取 `StreamState`（`comet/logic_stream_client.cpp:161-166`）。
- `Stop()` 在 join 完读写线程后执行 `streams_.clear()`（90）。
- `Send()` 没有检查 `stopped_`/`stream_running_`，也没有和 `Stop()` 共用生命周期锁。

如果 `Send()` 与 `Stop()` 并发，可能出现：

1. `Send()` 正在读 `streams_`，`Stop()` 清空 vector，导致越界/悬空访问。
2. `Stop()` 已置 `stream_running_=false`，`Send()` 仍把请求放入队列，但 writer 已退出，请求永远没有响应。
3. `CometServer::HandleTextMessage()` 里的 `stream_client_->Running()` 检查（527）与 `Send()` 之间存在 TOCTOU：检查通过后，Stop 仍可能发生。
4. 如果 `Start()` 中途失败，`streams_` 被 `clear()`（37），但 `Send()` 没有检查 `streams_.empty()` 或 `stream_idx < streams_.size()`，直接调用会越界。正常路径由 `Running()` 挡住，但组件的 API 本身没有防御。

建议至少选一种：

- 用一把生命周期锁保护 `streams_` 的读取/清空，`Send()` 在锁内取到 `shared_ptr<StreamState>` 后释放锁再入队；
- 或者明确约定 `Stop()` 只能在所有 `Send()` 调用者停止后调用（例如先停 IO/拒绝新消息，再 join，再 Stop）；
- 在 `Send()` 内检查 `stopped_`，Stop 后直接返回/走 unary 兜底。

### 6.2 流运行期断开后 `Running()` 仍可能为 true（中高）

`WriterLoop()` 在 `Write` 失败时 `break`（120-123），`ReaderLoop()` 在 `Read` 返回 false 时退出（135-150），但两者都没有把 `stream_running_` 置为 false。结果是：

- `Running()` 仍然返回 true；
- 上层继续走流模式，`Send()` 继续入队；
- 没有自动回退到 unary，队列可能持续增长；
- 如果所有流都断了，系统看起来“运行中”，实际没有发送能力。

建议：

- 为每条流维护健康状态，读写线程退出时标记 unhealthy；
- 所有流都不可用时把 `stream_running_` 置 false，并通知上层；
- `Send()` 在流不健康时回退到 unary，或拒绝并返回明确错误；
- 增加队列长度/发送失败监控。

### 6.3 日志关闭与并发日志调用的数据竞争（中）

`LogLine()` 直接执行 `auto lg = g_logger;`（`common/logging.cpp:114`），而 `ShutdownLogging()` 在 `g_init_mutex` 内执行 `g_logger.reset()`（99-103）。二者并发时：

- `std::shared_ptr` 的读写不是线程安全的；
- 可能出现数据竞争，甚至在 logger 销毁后仍尝试 `lg->log()`。

虽然 `rotating_file_sink_mt` / `stdout_color_sink_mt` 本身是线程安全的 sink，但**指针生命周期**没有保护。

建议：

- 用 `std::atomic_load(&g_logger)` / `std::atomic_store(&g_logger, ...)` 管理 `shared_ptr`（C++20 有 `std::atomic<std::shared_ptr>` 的替代方案）；
- 或者让 `LogLine()` 在 `g_init_mutex` 下取一份局部 `shared_ptr` 再释放锁；
- 并确保 `ShutdownLogging()` 发生在所有工作线程 join 之后。

### 6.4 信号处理函数里执行 `std::function` 回调（中）

`common/signal_handler.h`：

- 信号处理函数里执行 `ShutdownRequested().store(...)`（相对安全，前提是 `atomic<bool>` 无锁）；
- 然后执行 `s_callback()`（26-29）。`std::function` 调用可能分配内存、加锁、做复杂清理，不属于 async-signal-safe 操作；
- `s_callback` 是静态 `std::function`，`InstallSignalHandler()` 的赋值没有同步保护。

建议：

- 信号处理函数只设置 `volatile sig_atomic_t` 或 lock-free 的 `std::atomic` 标志；
- 主循环检测到标志后执行真正的清理；
- `InstallSignalHandler()` 在创建任何业务线程之前调用一次。

### 6.5 `ConnRegistry::RemoveConn()` 临界区偏长（中低）

`RemoveConn()` 在最后一条连接断开时持有用户分片锁，遍历 64 个房间分片（52-64）。这保证“最后连接断开”和“重新入房”不会交错，但会阻塞同一用户分片上的其他连接操作。

可选优化：

- 维护 `user_id -> set<room_id>` 反向索引，减少全房间扫描；
- 或者在用户分片锁内只标记/收集需要清理的房间，释放用户锁后再按房间分片处理，但要重新设计与 `JoinRoom` 的交错语义。

### 6.6 MySQL 连接池在锁内建连（低到中）

`Acquire()` 的扩容路径在持有 `mutex_` 时调用 `CreateConnectionUnlocked()`（153-157），建连涉及网络，可能拉长锁持有时间。`ValidateConnection()`/`mysql_ping` 则已经在锁外（176-179），这是正确的方向。

如果连接建立时间成为瓶颈，可以考虑：

- 锁外创建连接，创建完成后重新加锁检查 `total_conns_` 是否仍低于上限；
- 若超过上限，关闭新建连接并复用空闲连接；
- 预热连接池，减少运行期扩容。

### 6.7 `LocalTimer` 不是线程安全对象（低到中）

`TimerState` 的 `ready`/`cancelled` 是原子量，但 `LocalTimer::state_` 和 `callback_` 是普通成员：

- `schedule()`/`cancel()`/`execute_if_ready()` 若在不同线程并发调用，会对 `state_`/`callback_` 产生数据竞争；
- `cancel()` 与到期回调之间存在“先检查后执行”的窗口，取消是 best-effort，不能保证回调绝不执行。

建议：

- 明确 `LocalTimer` 只能在所属事件循环线程使用；
- 若需要跨线程取消，给 `LocalTimer` 自身加锁，或用 `std::atomic<bool>` + 一次性执行保护（但仍要保护 `callback_` 的生命周期）。

### 6.8 心跳桶关闭条目延迟清理（低）

`OnClose()` 只设置 `closed=true` 并清理 `ConnRegistry`，不会立即从心跳桶删除条目。心跳扫描在 `ScanNextHeartbeatBucket()` 中看到 `closed` 或 weak_ptr 失效后才 erase（293-296）。这是可接受的延迟清理，但连接频繁建立/断开时，桶里的条目会短暂增多；不是锁正确性问题。

### 6.9 本地路由缓存的多实例一致性（低，非锁问题）

`RedisStore::route_cache_mutex_` 只保护当前 Logic 进程的缓存。`AddRoute`/`RemoveRoute` 的 `InvalidateRouteCache()` 也只影响本进程；其他 Logic 实例默认要等 1 秒 TTL 才能看到新路由。多 Logic 部署时需要接受这个短暂不一致，或用 Redis Pub/Sub 做跨实例失效。

### 6.10 幂等 `Stop()` 不等于并发安全（低到中）

下面这些实现都用原子 CAS/exchange 做“只执行一次”的防重入：

- `LogicStreamClient::Stop()`（`stopped_` CAS）；
- `CometServer::Stop()`（`stopped_` CAS）；
- `KafkaConsumer::Start()/Stop()`（`running_` exchange）；
- `ThreadPool::Stop()`（`stopping_` 检查）。

它们能保证**重复调用不会重复清理**，但不能保证“第二个并发调用返回时，第一个调用的清理已经完成”。例如 A 调用 `LogicStreamClient::Stop()` 正在 join/clear，B 也调用 `Stop()` 会立即返回；如果 B 随后销毁对象，A 仍在访问对象，就会出问题。正确用法仍然是：

- 由一个明确的生命周期所有者调用 Stop；
- 其他线程只负责请求停止，不负责销毁；
- 析构前确保 Stop 的第一次调用已经完成（或让析构函数自己承担唯一一次 Stop）。

同样，`ThreadPool` 的 `Start()`/`Stop()` 不应与彼此并发调用；它们适合“启动一次、停止一次”的所有者线程模型。

---

## 7. 设计上的可借鉴点

如果要在面试/评审里讲这个项目的并发设计，可以抓住下面几点：

1. **按对象划分锁，而不是一把全局锁**：线程池、连接池、时间轮、每个流、每个 Comet 各自持有自己的锁，避免无关路径互相阻塞。
2. **分片降低热点**：`ConnRegistry` 64 个用户分片 + 64 个房间分片，心跳 64 个桶；把大 map/大扫描拆成小临界区。
3. **每条流/每个 Comet 独立队列**：`LogicStreamClient` 和 `PushHandler` 都用 per-stream queue + per-stream writer，慢 Comet 不会直接堵住其他 Comet 的发送路径。
4. **读写锁用于读多写少的缓存**：`RedisStore` 的路由缓存用 `shared_mutex`，读操作可并行；未命中时释放读锁再查 Redis，避免锁升级死锁。
5. **条件变量带谓词**：线程池、Kafka、时间轮、流队列的 `wait` 都检查“停止或队列非空”等谓词，处理虚假唤醒。
6. **线程封闭减少锁**：Kafka 消费线程独占 `partitions_`/`inflight_`，Comet 心跳线程独占 `next_heartbeat_bucket_`，用结构约束替代锁。
7. **锁外做 I/O/回调**：MySQL ping、Redis 查询、gRPC 响应回调、WebSocket 发送大多在锁外执行；这是避免长临界区和回调重入死锁的关键。
8. **原子量只做标志/计数**：`fetch_add` 生成 ID、`store/load` 控制生命周期；真正的一组共享数据仍然交给 mutex。

一句话总结：

> 这个项目的并发策略是“**对象自持锁 + 分片降竞争 + 每流隔离 + 线程封闭 + 原子量只做标志/计数**”；最需要补强的是 `LogicStreamClient` 的生命周期约束和运行期断流后的状态恢复。

---

## 8. 附录：如何复查这份清单

可以用下面的命令重新扫描显式并发原语（在仓库根目录执行）：

```bash
rg -n 'std::(mutex|shared_mutex|recursive_mutex|atomic|atomic_bool|condition_variable|lock_guard|unique_lock|shared_lock|scoped_lock)|\.lock\(\)|\.unlock\(\)|\.notify_(one|all)\(|\.wait(_for|_until)?\(|compare_exchange|fetch_add|memory_order_' \
  common comet job logic
```

测试与压测代码单独扫描：

```bash
rg -n 'std::(mutex|atomic|atomic_bool|condition_variable|lock_guard|unique_lock)|fetch_add|memory_order_' \
  tests load_test
```

维护时建议优先更新以下内容：

- 新增/删除锁成员；
- 新增跨线程共享容器；
- 修改 `Stop()`/析构顺序；
- 修改 gRPC 流、Kafka 消费者、心跳线程的生命周期；
- 把某个 `std::atomic` 从 relaxed 改成 acquire/release，或反过来。

> 本文是“代码事实清单”，不是“所有并发问题都已修复”的证明。第 6 节列出的风险仍然存在，尤其是 `LogicStreamClient` 的 `Send/Stop` 竞争和日志关闭竞争。
