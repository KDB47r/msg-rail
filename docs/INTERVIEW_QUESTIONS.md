# MsgRail 项目面试问题清单

> 面向 C++ 后端面试，基于当前 MsgRail / spark_push 仓库整理。  
> 建议用法：先不看答案，按题号口头回答；能画出链路并说出失败边界，才算真的掌握。

面试官通常会先判断三件事：

1. 你是否真的跑通过全链路；
2. 你是否知道每个设计的代价；
3. 你是否能主动说出没做好的地方。

然后按“项目开场 → Kafka/可靠性 → C++/网络/并发 → Redis/MySQL → 分布式/生产化”的顺序追问。

---

## 第一轮：架构与链路

1. **用 3 分钟画一下单聊全链路**，标出协议、端口、进程边界和存储。
   - 追问：哪一步是同步的？哪一步是异步的？

2. **为什么拆成 Comet、Logic、Job？**
   - 如果合成一个进程，会损失什么？
   - 如果只拆成两个进程，你会怎么拆？

3. **这三个组件谁有状态？**
   - Comet 有状态，怎么扩缩容？
   - Logic 基本无状态，为什么还不能随便多开？
   - Job 为什么没有监听端口？

4. **为什么消息要经过 Kafka，而不是 Logic 直接 gRPC 调 Comet？**
   - 延迟增加了多少？
   - 哪些场景下这个延迟不可接受？

5. **多台机器上的 Comet 怎么通信？**
   - Comet 之间直接连吗？
   - Logic 知道 Comet 的 IP 吗？
   - Job 怎么找到目标 Comet？

6. **单聊、聊天室、弹幕三条链路最大的区别是什么？**
   - 聊天室为什么要两阶段扇出？
   - 为什么要有 `room:{id}:comets`？
   - 弹幕和聊天室在语义上有什么不同？

---

## 第二轮：Kafka 与可靠投递（重点）

7. **在线 key 用 `session_id|comet_id`，离线 key 用 `session_id`，分别保证什么？不保证什么？**
   - 同一会话到两个 Comet，有全局顺序吗？
   - 在线切离线后，key 变了，顺序怎么保证？

8. **一条单聊消息，用户同时在 2 个 Comet 上，会产生几条 Kafka 记录？**
   - persist 组会写几次？
   - 为什么不会重复插入？
   - 如果 Comet 有 10 个，这种设计有什么问题？

9. **为什么 push 和 persist 用两个消费组？**
   - 如果只用一个消费组会怎样？
   - 两个组的 offset 是共享的吗？
   - 一个组 lag 了会不会影响另一个组？

10. **`KafkaProducer::Send` 返回 true 代表什么？**
    - 消息到 broker 了吗？
    - `acks=1`、`enable.idempotence=false`、没有 delivery report 回调，会有什么后果？
    - 如果 broker 挂了，Logic 多久能知道？

11. **为什么手动提交 offset？`offset + 1` 是什么意思？**
    - 哪些场景会重复消费？
    - 哪些场景会丢消息？
    - `commitAsync` 和 `commitSync` 有什么区别？

12. **消费回调失败后，offset 还会提交吗？**
    - 你的 `PersistHandler` 重试耗尽后怎么处理？
    - 这算 at-least-once 吗？
    - 如果要业务级重试，怎么改？

13. **同一 partition 串行、不同 partition 并行，代码怎么实现的？**
    - 为什么同一 partition 不能并发？
    - 如果允许并发，offset 提交会有什么问题？
    - `pending` 队列和 `active` 标志分别起什么作用？

14. **`max_inflight` 的背压怎么工作？**
    - 为什么 pause 之后还要 `consume(10)`？
    - 如果 worker 全部卡住，Kafka 会认为消费者死了吗？
    - `max.poll.interval.ms` 在这里为什么不会超时？

15. **只有 4 个 partition，Job 扩到 8 个有用吗？**
    - 一个消费组里，partition 和 consumer 的对应关系是什么？
    - 如果增加 partition，会带来什么新问题？
    - rebalance 时在途消息怎么办？你有 rebalance callback 吗？

16. **Kafka 挂了、Job 挂了、Comet 挂了、MySQL 挂了，分别会发生什么？**
    - 哪些消息会丢？
    - 哪些消息会重？
    - 哪些消息只是延迟？

17. **为什么消息体用 protobuf 不用 JSON？**
    - key 为什么不用 `msg_id`？
    - Kafka 的 key 和 value 分别承担什么职责？

---

## 第三轮：C++、网络与并发

18. **WebSocket 连接的完整生命周期是什么？**
    - 握手时怎么鉴权？
    - 心跳谁发？
    - 半开连接怎么发现？
    - 慢客户端会不会拖垮 Comet？

19. **`ConnRegistry` 为什么分片？**
    - 按 user 和 room 两个维度怎么组织？
    - 两个索引如何保持一致？
    - 锁粒度是什么？会不会有死锁？

20. **Comet 到 Logic 的 4 条 `MessageStream` 怎么选流？**
    - `request_id` 怎么生成、怎么匹配响应？
    - 回调在哪个线程执行？
    - 回调里再调用 `SendText` 安全吗？

21. **`WriterLoop` 和 `ReaderLoop` 的数据怎么流动？**
    - `Write` 返回 true 是否代表 Logic 处理成功？
    - 如果 Logic 重启，流断了，Comet 怎么办？
    - 你的代码有自动重连吗？pending callback 会不会永远不返回？

22. **unary、client-streaming、bidi streaming 为什么这样选？**
    - Job 推 Comet 默认为什么用 unary？
    - stream 模式下 offset 什么时候提交？
    - 如果先提交 offset、消息还在本地队列，进程挂了会怎样？

23. **服务优雅退出时，各组件按什么顺序关闭？**
    - Kafka producer 要不要 flush？
    - consumer 要不要排空在途任务？
    - WebSocket、gRPC、MySQL、Redis 的连接怎么关？
    - `Stop()` 和 `Loop()` 的并发路径有没有竞态？

24. **C++ 细节追问：**
    - 路由缓存为什么用 `shared_mutex`？
    - `condition_variable` 为什么必须带 predicate？
    - `std::atomic` 用了哪些 memory order？
    - `shared_ptr<WorkItem>` 是否必要？能不能用 `unique_ptr`？
    - `TopicPartition`、`ClientContext`、`ClientWriter` 谁负责释放？
    - 线程池 `Stop()` 时队列里的任务怎么办？

25. **Comet 的 gRPC 线程里同步做房间扇出，会有什么问题？**
    - 一个 10 万人的房间，`PushToRoom` 会阻塞多久？
    - WebSocket 发送队列有上限吗？
    - 如果客户端接收很慢，内存会涨吗？

26. **你自研的 HTTP 服务器怎么处理 keep-alive、Content-Length、chunked、超时和慢连接？**
    - llhttp 负责什么，Asio 负责什么？
    - HTTP 线程数和 IO 线程数怎么定？

---

## 第四轮：Redis 与 MySQL

27. **Redis 里存了哪些 key？**
    - `route:user:{uid}` 为什么用 HASH `conn_id -> comet_id`，不用 SET？
    - 一个用户多设备、多标签页怎么处理？
    - token 的 TTL 多长？续期吗？

28. **会话 `msg_seq` 为什么用 Lua？**
    - Redis 重启或 key 丢失后怎么恢复？
    - 如果 Redis 和 MySQL 的序号不一致，怎么保证安全？
    - 高并发下 Redis `INCR` 是瓶颈吗？

29. **本地路由缓存 1 秒，多 Logic 实例有什么一致性问题？**
    - 一个 Logic 删除路由，另一个 Logic 的缓存会立即失效吗？
    - 这段时间内消息会推到哪里？
    - 怎么解决？

30. **`RoomJoinAtomic` 的 Lua 怎么保证原子性？**
    - 为什么要 `comets`、`comet_count`、`online_count` 三个结构？
    - Comet 崩溃后计数不准怎么办？
    - 重复 join/leave 会不会把计数弄坏？

31. **MySQL 表结构、索引、唯一键怎么设计的？**
    - `message` 表为什么唯一键是 `(session_id, msg_seq)`？
    - `ON DUPLICATE KEY UPDATE` 有什么副作用？
    - `session.last_msg_seq = GREATEST(...)` 解决什么问题？

32. **为什么 session 和 message 在同一个事务里？**
    - 事务隔离级别是什么？
    - 两个消费者同时写同一个 session 会怎样？
    - 持久化失败后现在只打 DLQ 日志，消息是不是永久丢了？怎么补偿？

33. **历史消息为什么用 `anchor_seq` 分页，不用 offset？**
    - 深分页怎么优化？
    - `message` 表大了怎么分库分表？按 session 还是 user？
    - 未读数怎么算？`user_session_state` 和 `session.last_msg_seq` 怎么配合？

---

## 第五轮：分布式与生产化

34. **多 Comet、多 Logic、多 Job 分别怎么部署？**
    - `comet_targets` 静态配置有什么问题？
    - 增加一个 Comet 要改哪些地方？
    - 服务发现、健康检查、优雅下线怎么做？

35. **Comet 进程被 kill -9，Redis 路由谁来清？**
    - 现在有 TTL 或租约吗？
    - 如果没有，消息会一直往死 Comet 推吗？
    - 你会怎么设计心跳和租约？

36. **端到端不丢、不重、不乱序，你现在分别做到了什么程度？**
    - 生产端做到了吗？
    - 消费端做到了吗？
    - 推送失败和落库失败分别怎么补偿？
    - 客户端需要做什么去重？

37. **监控看哪些指标？**
    - Kafka lag、推送失败率、P99 延迟、在线连接数、Redis 命中率、MySQL 慢查询。
    - 一条消息从浏览器到 MySQL，怎么做全链路追踪？

38. **压测数据是多少？**
    - 多少连接？多少 QPS？P99 多少？
    - 瓶颈在 Redis seq、Kafka partition、MySQL 写入，还是 Comet 连接？
    - 一个直播间 100 万人，架构怎么改？

39. **这个项目哪部分是你独立设计的？**
    - 最大难点是什么？
    - 讲一个你调过的最难的 bug。
    - 如果重做一次，你会改哪三件事？

---

## 面试官最可能抓住不放的 10 个问题

这些是从当前项目代码里直接长出来的，面试官很容易继续追：

1. **你说 at-least-once，但 `PersistHandler` 重试耗尽只打日志就 return，`KafkaConsumer` 照样提交 offset。这算 at-least-once 吗？**
   - 代码线索：`job/persist_handler.cpp`、`common/kafka_consumer.cpp`

2. **`KafkaProducer::Send` 返回 true 时，消息真的到 broker 了吗？`acks=1` 且没有 delivery callback，你怎么知道发成功了？**
   - 代码线索：`common/kafka_producer.cpp`

3. **`use_push_stream=true` 时，offset 先提交、消息还在本地队列，Job 挂了这条消息去哪了？**
   - 代码线索：`job/push_handler.cpp`

4. **一条聊天室消息发给 3 个 Comet，persist 组写 3 次，为什么？怎么优化？**
   - 代码线索：`logic/grpc_service.cpp`、`job/persist_handler.cpp`

5. **同一 session 在线/离线 key 不同，落到不同 partition，持久化顺序怎么保证？**
   - 代码线索：`logic/grpc_service.cpp`、`job/persist_handler.cpp`

6. **多 Logic 时，本地路由缓存只失效自己，另一台继续往已经断开的 Comet 推，怎么办？**
   - 代码线索：`logic/redis_store.h`、`logic/redis_store.cpp`

7. **`PushToComet` 永远返回 code 0，Job 怎么知道 Comet 内部推送失败？**
   - 代码线索：`comet/comet_grpc_service.cpp`

8. **Comet 到 Logic 的 `MessageStream` 断了以后，`stream_running_` 还是 true，`Send` 还在入队，callback 永远不回，怎么处理？**
   - 代码线索：`comet/logic_stream_client.cpp`

9. **HTTP `/api/chatroom/leave` 是 no-op，`unsubscribe` 只删 MySQL 成员，不删实时订阅。用户调用 leave 后还能收到房间消息吗？**
   - 代码线索：`logic/http_handlers_chatroom.cpp`

10. **WS 弹幕只持久化 `message`，不写 `video_danmaku`，`/api/danmaku/list` 查不到；这是有意的吗？**
    - 代码线索：`logic/http_handlers_danmaku.cpp`、`job/persist_handler.cpp`

---

## 面试官会怎么判断你

### 减分回答

- “Kafka 保证消息不丢。”
- “用了 Kafka 所以是分布式。”
- “offset 就是消息 ID。”
- “acks=1 没问题。”
- “没有压测，但应该挺快。”
- “代码都是我写的”，但画不出链路、说不出失败场景。

### 加分回答

- 主动说清楚 key 只保证 partition 内有序，不保证跨 Comet 全局有序。
- 主动区分“入队成功”和“broker 确认”。
- 主动承认业务失败仍会提交 offset，DLQ 目前只是日志。
- 主动说多 Logic、Comet 崩溃、服务发现这些还没做。
- 能给出改进方案，而不是只说“项目就是这样”。
- 有压测数据、故障演练、监控指标。

---

## 时间有限时优先准备这 8 个

1. 单聊全链路。
2. 为什么拆 Comet/Logic/Job，为什么经过 Kafka。
3. 多 Comet 跨机怎么路由。
4. Kafka key 设计、顺序边界、离线/在线差异。
5. push/persist 两个消费组，以及 N 个 Comet 重复持久化。
6. 手动提交 offset、at-least-once 的真实边界。
7. 生产端 `Send` 返回 true 的含义和 `acks=1` 的代价。
8. 项目哪里还没做好，以及你会怎么改。
