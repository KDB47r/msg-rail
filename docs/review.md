# MsgRail 面试复盘

> 评审对象：`docs/answer.md`  
> 评审角色：C++ 后端面试官  
> 评审方式：链路完整性 → 设计因果 → 可靠性边界 → 失败场景  
> 维护约定：每轮评审新增一节，不覆盖历史；代码事实变化时同步更新“代码事实核对”。

## 当前状态

| 项目 | 内容 |
|---|---|
| 最近更新 | 2026-11-05 |
| 当前轮次 | 第三轮（Kafka 与可靠投递） |
| 评审范围 | `docs/answer.md` 第二轮 2.1–2.4 + 2.5–2.11 学习答案 |
| 综合评分 | 6.8 / 10（第一轮 5.5 → 第二轮 6.0 → 第三轮 6.8） |
| 结论 | 2.2/2.4 基本正确；2.1 仍混淆 `msg_id`/`msg_seq` 和跨 key 顺序；2.3 未解释 lag；2.5–2.11 已给出标准答案 |

### 第一轮分项评分（历史）

| 维度 | 分数 | 评价 |
|---|---:|---|
| 链路表达 | 7 | 顺序基本正确，能看出确实跑通过 |
| 事实准确 | 5 | 多处组件职责、调用方向和同步/异步关系说错 |
| 架构因果 | 4 | 知道“拆了”，但解释“为什么拆、代价是什么”不足 |
| 边界与失败场景 | 4 | 对异步 ACK、断流、扩容限制、重复消费认识不足 |

### 做得好的地方

- 能按“登录 → WebSocket → 发消息 → Kafka → Job → 推送”讲完整条主链路。
- 知道 token 存 Redis、WebSocket 连接 Comet、Comet 向 Logic 鉴权。
- 知道系统拆成 Comet、Logic、Job 三个节点，Comet 可以多实例。
- 知道聊天室和弹幕是房间维度广播，单聊是点对点。
- 1.4 没有编造延迟数据，承认“还没具体测试过”，这是诚实的。

### 第一轮问题总览

| 题号 | 主题 | 评级 | 主要问题 |
|---|---|---|---|
| 1.1 | 单聊全链路 | 需重写 | 鉴权时机、session_id 来源、Job→Comet 同步/异步、gRPC 流配置说错 |
| 1.2 | 为什么拆三个节点 | 需重写 | “Logic 等 Job”“Kafka 因为 Job 慢”“partition 分开 push/persist”均为错误因果 |
| 1.3 | 三个节点怎么扩容 | 需重写 | “Logic 多节点有分布式锁争夺”是最大硬伤 |
| 1.4 | Kafka 延迟与代价 | 需补强 | 没数据；ACK 语义说得还不够弱；收益没有讲透 |
| 1.5 | 多 Comet 怎么通信 | 需重写 | “Logic 知道 Comet IP”错误；Comet 本地扇出职责被忽略 |
| 1.6 | 单聊/聊天室/弹幕区别 | 需补强 | 只讲了“点对点 vs 房间”，缺少成员关系、实时订阅、持久化差异 |

---

## 1.1 单聊全链路

**评级：需重写。** 链路顺序是对的，但事实错误密度最高。

### 保留的部分

- 登录/注册走 Logic HTTP，token 存 Redis，24 小时。
- 浏览器通过 WebSocket 连 Comet。
- Comet 握手时向 Logic 调 `VerifyToken`，是 unary 同步。
- 消息走 Comet → Logic → Kafka → Job → Comet。
- Job 到 Comet 默认 unary，可选 client-streaming。

### 必须纠正

1. **注册和登录不是同一件事。**  
   注册是 `INSERT user`；登录是 `SELECT user` 并校验密码哈希。两者都走 Logic HTTP，但业务动作不同。token 也不只用于 WebSocket，HTTP API 鉴权同样使用。

2. **“浏览器返回 JSON 包”方向说反了。**  
   是浏览器把 JSON 发给 Comet，Comet 再转给 Logic。

3. **Comet 不是每条消息都重新鉴权。**  
   WebSocket 握手阶段 `OnValidate` 调一次 `VerifyToken`，之后把 `user_id`、`conn_id` 存在 `ConnContext` 里；后续消息直接使用 `ctx.user_id`，不会再次查 token。Comet 只校验消息格式和字段。

4. **“gRPC 默认采用双向流”不准确。**  
   gRPC 没有“默认双向流”这种说法。是项目配置 `use_grpc_stream=true`，所以默认走多条 `MessageStream` 双向流；关闭开关或流初始化失败时才走 unary。

5. **“线程池异步向 W 线程 send”也不准确。**  
   流模式下，`stream_client_->Send` 直接按 `request_id % stream_count` 选流并放入该流的 `send_queue`，不经过线程池。线程池 `grpc_pool_` 是用在 unary 兜底路径。

6. **`session_id` 不是 Redis 维护的。**  
   Logic 直接拼：
   - 单聊：`single:<min_uid>:<max_uid>`
   - 聊天室：`room:<room_id>`
   - 弹幕：`danmaku:<video_id>`

   Redis 维护的是 `session:{sid}:seq`，用 `INCR` 分配 `msg_seq`，不是 `session_id` 本身。

7. **Job → Comet 的默认 unary 是同步阻塞，不是异步。**  
   Kafka worker 线程调用 `ProcessPushRequest`，里面同步调用 `stub->PushToComet`，3 秒 deadline，失败重试 3 次（100ms → 400ms → 1600ms）。整体链路是异步解耦的，但这次 RPC的。

8. **“unary 兜底”目前并不完整。**  
   `MessageStream` 在运行时断开后，`stream_running_` 可能仍然是 `true`，`Running()` 仍返回 `true`，后续消息继续进 `send_queue`，不会自动切到 unary，pending callback 可能永远不返回。  
   可以主动说：“当前 unary 兜底只覆盖流初始化失败；运行期断流没有自动重连和切换，这是待改进点。”

9. **“为了减少重复 header 帧”不是 gRPC 的主要理由。**  
   更准确的说法是：gRPC 提供强类型契约、HTTP/2 多路复用和流式能力；长连接流可以减少重复建连和部分 header 开销，但不要把它说成唯一目的。

### 面试官会追问

- Comet 每条消息都鉴权吗？token 过期后，已经建立的 WebSocket 会断吗？
- `session_id` 谁生成的？Redis 里的 seq key 是什么？
- Job → Comet 的 unary 会阻塞 Kafka worker 多久？重试几次？
- 流断了以后，`pending_callbacks_` 里的回调怎么处理？
- `VerifyToken` 在 Asio IO 线程上同步调用，会不会阻塞其他连接？

### 建议表述

> 注册和登录都走 Logic HTTP，但注册是写 MySQL，登录是查 MySQL 并校验密码哈希；成功后 Logic 生成 token，用 `SETEX` 写 Redis 24 小时。浏览器拿 token 去 Comet 建 WebSocket；Comet 在握手阶段同步 unary 调 Logic 的 `VerifyToken`，Logic 校验 token 并把 `route:user:{uid}` 的 `conn_id -> comet_id` 写进 Redis，Comet 把 `user_id`、`conn_id` 存进连接上下文。之后这条连接的消息不再重复校验 token。  
>
> 用户发消息时，浏览器把 JSON 发给 Comet，Comet 组装 `UpstreamMessageRequest`，带 `from_user_id`、`comet_id`、`conn_id`、`scene`、`content_json`，默认走项目配置的 `MessageStream` 到 Logic。Logic 构造 `single:min:max` 的 `session_id`，用 Redis `INCR` 分配 `msg_seq`，查 `route:user:{to_uid}` 得到目标 `comet_id`；每个目标 Comet 一条 `PushToCometRequest`，protobuf 序列化后按 `session_id|comet_id` 写入 Kafka。  
>
> Job 的 push 和 persist 两个消费组各自消费：push 组默认同步 unary 调目标 Comet 的 `PushToComet`，Comet 查本机 `ConnRegistry` 后通过 WebSocket 发给接收者；persist 组写 MySQL。Job 到 Comet 的 unary 是阻塞调用，client-streaming 是可选的高吞吐模式。

**代码依据：** `comet/comet_server.cpp`、`logic/grpc_service.cpp`、`job/push_handler.cpp`、`comet/logic_stream_client.cpp`。

---

## 1.2 为什么拆三个节点

**评级：需重写。** 三个节点的职责大致知道，但核心因果错了。

### 保留的部分

- 三个节点职责不同。
- 单节点会让连接和业务互相影响。
- 两个节点时合并 Comet 和 Logic，是一个可以讨论的方案。

### 必须纠正

1. **拆分原因不是简单的“IO 密集 vs CPU 密集”。**  
   更核心的是状态、失败域和扩缩容维度不同：
   - Comet 有状态：WebSocket、连接注册表、本机房间成员；
   - Logic 基本无状态：业务、路由、写 Kafka，按 QPS 扩；
   - Job 是异步 worker：按 Kafka 消费能力扩；
   - 三者发布、故障、扩容节奏不同。

2. **“Logic 等 Job 处理完才返回”是错的。**  
   Logic 生产 Kafka 后立即返回，不等待 Job。Kafka 的作用不是“因为 Job 慢所以加队列”，而是解耦、削峰、缓冲、重放、失败隔离和独立扩展。

3. **push 和 persist 不是靠 partition 分开的。**  
   同一个 topic `push_to_comet`，两个独立消费组：
   - `spark_push_group_push`
   - `spark_push_group_persist`

   每个消费组各自收到完整一份消息；partition 是在每个消费组内部做并行和顺序，不是“一个 partition 给 push，一个 partition 给 persist”。

4. **“Logic 处理完还要推送回去”不符合当前代码。**  
   当前版本 Logic 不直接推 Comet；Logic 只负责路由、生成序号、写 Kafka。真正推 Comet 的是 Job。

### 面试官会追问

- Logic 会不会等 Job？Kafka 挂了，Logic 还能返回成功吗？
- push 和 persist 为什么用两个消费组？用一个会怎样？
- 两个节点时，为什么合并 Comet + Logic，而不是 Logic + Job？
- 合并后 Comet 变成有状态节点，怎么扩容？

### 建议表述

> 拆分的核心不是简单的 IO/CPU，而是状态和失败域不同：Comet 持有长连接和本机房间状态，扩容要按连接数；Logic 是无状态业务和路由，按 QPS 扩；Job 是异步 worker，按 Kafka 消费能力扩。  
>
> Kafka 的作用是解耦和缓冲，不是 Logic 等 Job。Logic 生产消息后立即返回，不等待 Job；push 和 persist 用两个独立消费组消费同一个 topic，各自维护 offset，互不拖累。push 组负责推 Comet，persist 组负责写 MySQL。  
>
> 如果只保留两个节点，我会合并 Comet 和 Logic，因为这样少一次网络跳转、鉴权和路由更直接；代价是合并后的节点变成有状态节点，发布会掉连接，Logic 扩容也会受连接数影响。

**代码依据：** `job/push_handler.cpp`、`job/persist_handler.cpp`、`logic/grpc_service.cpp`。

---

## 1.3 三个节点怎么扩容

**评级：需重写。** “Logic 多节点有分布式锁争夺”是本轮最大的硬伤之一。

### 必须纠正

1. **Logic 多实例的正常链路没有应用层分布式锁。**  
   Redis 使用的是：
   - `INCR` 分配 seq；
   - Lua 脚本做房间上下线原子操作；
   - 路由 HASH 直接读写。

   这些是原子操作，不是分布式锁。Logic 可以多实例，真正的限制是：
   - Comet 只配置了一个 `logic_grpc_target`，需要 L4 VIP / LB / 服务发现；
   - `MessageStream` 目前没有自动重连；
   - 每个 Logic 进程有本地路由缓存，TTL 1 秒，跨实例失效不及时；
   - Redis 的 seq 是共享热点，可能成为瓶颈。

2. **“Comet 扩容缓解网络压力”不准确。**  
   Comet 扩容主要分摊连接数、fd、WebSocket IO/CPU、单机带宽和本机房间扇出压力，不是减少总网络流量。扩容还需要 WebSocket 负载均衡、唯一 `comet_id`、用户重连后重新写路由、Job 的 `comet_targets` 增加新节点。

3. **Job 多实例受 partition 数限制。**  
   当前脚本创建 4 个 partition，一个消费组最多 4 个 consumer 真正干活；Job 扩到 8 个，多出来的实例分不到 partition。每个 Job 还必须配置完整的 `comet_targets`。

### 面试官会追问

- Logic 多节点到底需不需要分布式锁？锁在哪一行代码？
- Comet 扩容后，新连接怎么被路由？老连接怎么办？
- Job 扩到 8 个为什么没用？增加 partition 会带来什么新问题？
- 多 Logic 时，本地路由缓存 1 秒不一致怎么办？

### 建议表述

> Comet 可以多实例，但需要 WebSocket 负载均衡、唯一 `comet_id`、路由注册和 Job 地址表更新。Logic 也可以多实例，当前正常链路没有应用层分布式锁，Redis 用 `INCR` 和 Lua 做原子操作；真正的限制是 Comet 只配了一个 `logic_grpc_target`、`MessageStream` 没有自动重连、本地路由缓存 1 秒跨实例不一致。  
>
> Job 适合多实例，同一个消费组按 partition 分配，但当前只有 4 个 partition，所以最多 4 个 Job 有效并行；每个 Job 都需要完整的 `comet_targets`，否则消费到目标 Comet 的消息会找不到地址。

**代码依据：** `logic/redis_store.*`、`comet/logic_stream_client.cpp`、`conf/job.conf`、`scripts/start_all.sh`。

---

## 1.4 Kafka 延迟与代价

**评级：需补强。** 诚实是优点，但面试里只回答“没测过”不够。

### 必须纠正

1. **因果仍然错了。**  
   不是“Job 慢才加 Kafka”，而是为了解耦、缓冲、重放、失败隔离和独立扩展。即使 Job 很快，Kafka 仍然有这些价值。

2. **ACK 语义比你说的还要弱。**  
   `KafkaProducer::Send` 返回 `true` 只代表消息进了 librdkafka 的本地发送队列，不代表 broker 已经收到。当前配置：
   - `acks=1`
   - `enable.idempotence=false`
   - 没有 delivery report 回调
   - `produce` 后只 `poll(0)`

   所以：

   ```text
   HTTP/WS 返回成功
     ≠ broker 收到
     ≠ 消息落盘
     ≠ 接收方收到
   ```

3. **“消息传递精准”这个说法太模糊。**  
   面试官会追问：你说的是不丢、不重、有序，还是接收方确认？这是三个不同的可靠性目标。

4. **没有延迟数据是弱项。**  
   不需要编数据，但至少应该能说本地单机大概几毫秒到几十毫秒，并说明受 Kafka、Job worker、gRPC 重试、Comet 扇出影响。要测的话，至少在这些点打时间戳：
   - 浏览器发送时间
   - Comet `received_time_ms`
   - Logic 生产 Kafka 时间
   - Job 消费时间
   - Comet 推送时间

   然后算 P50/P99。

### 面试官会追问

- ACK 到底在什么时候返回？broker 确认了吗？
- 如果业务要求“接收方确认收到”，这条链路怎么改？
- 延迟预算多少？P99 能接受多少？
- Kafka 挂了，Logic 还能返回成功吗？

### 建议表述

> Kafka 的收益是解耦、削峰、重放、失败隔离和独立扩展，不是“Job 慢所以 Logic 等它”。代价是 ACK 变成异步语义：HTTP/WS 返回成功只代表 Logic 把消息交给了 Kafka producer，甚至不代表 broker 已经确认；接收方是否收到更无法保证。  
>
> 延迟我目前没有完整压测数据，本地单机预估是几毫秒到几十毫秒，受 Kafka、Job 消费和 Comet 扇出影响。如果业务要求强实时或送达确认，应该走同步链路或额外的 ack/回执，而不是只依赖这条异步推送链路。

**代码依据：** `common/kafka_producer.cpp`、`common/kafka_consumer.cpp`。

---

## 1.5 多台 Comet 怎么通信

**评级：需重写。** “Logic 知道 Comet IP”是关键事实错误。

### 必须纠正

1. **Logic 不知道 Comet 的 IP。**  
   Logic 只知道 `comet_id`；Redis 路由里存的是 `conn_id -> comet_id`；Kafka 消息里带的是 `comet_id`；只有 Job 的 `comet_targets` 知道 `comet_id -> IP:port`。Comet 自己只配置 `logic_grpc_target`，不知道其他 Comet 的地址。

   跨机链路是：

   ```text
   Comet A -> Logic -> Kafka -> Job -> Comet B
   ```

2. **“所有业务无脑抛给 Logic”不准确。**  
   Comet 还负责 WebSocket 连接管理、心跳、本机 `ConnRegistry`、本机房间成员、聊天室本地 `PushToRoom`、断线清理和 `UserOffline` 上报。Logic 负责业务和路由，但不负责本机连接扇出。

3. **Job 拿到的是 `comet_id`，不是 IP。**  
   Job 查：

   ```text
   comet_targets[comet_id] = ip:port
   ```

   再发起 gRPC。`comet_id` 是逻辑地址，`IP:port` 是物理地址；Logic 和 Kafka 只传逻辑地址，Job 持有地址簿。

### 面试官会追问

- Logic 的路由表里到底存的是什么？
- Job 怎么知道 `comet-2` 在 10.0.0.22:9205？
- Comet 之间完全不通信，那聊天室跨机广播怎么完成？
- Comet 崩溃后 Redis 里的路由谁清？

### 建议表述

> Comet 之间不直接通信。Comet 只主动连 Logic，等待 Job 把下行消息推过来。Logic 只知道 `comet_id`，不知道 Comet 的 IP；Redis 路由保存 `conn_id -> comet_id`，Kafka 消息里也带 `comet_id`；只有 Job 的 `comet_targets` 保存 `comet_id -> IP:port` 的映射。  
>
> 所以跨机消息是 Comet A → Logic → Kafka → Job → 目标 Comet B。Comet 本身不转发给其他 Comet，聊天室也是 Logic 按 `room:{id}:comets` 拆成多条 Kafka 消息，每个 Comet 只做本机扇出。

**代码依据：** `logic/redis_store.*`、`conf/job.conf`、`job/push_handler.cpp`、`comet/comet_server.cpp`。

---

## 1.6 单聊、聊天室、弹幕的区别

**评级：需补强。** 方向对，但只停留在“点对点 vs 房间”。

### 必须纠正

1. **单聊是用户路由，聊天室是房间路由。**  
   - 单聊：`route:user:{uid} -> comet_id`，`targets = [to_user]`
   - 聊天室：`room:{room_id}:comets -> {comet_id...}`，`targets = []`，`room_id = ...`

   Comet 收到后分别走 `PushToUsers` 和 `PushToRoom`。

2. **聊天室不是“所有房间成员”，而是“所有在线订阅者”。**  
   MySQL `group_member` 是成员关系；Comet 本机 `ConnRegistry` 是实时订阅。用户必须既在房间成员里，又建立了 WebSocket 并 `chatroom_join`，才能收到实时消息；离线成员不会收到推送，只能拉历史。

3. **聊天室是两阶段扇出。**  
   ```text
   Logic -> Kafka：按 comet_id 拆多条
   Job -> Comet：每条推给一个 Comet
   Comet -> 本机连接：PushToRoom
   ```

   不是 Logic 直接遍历所有用户。

4. **弹幕不是“比 room 多两个字段”这么简单。**  
   弹幕还有：
   - `session_id = danmaku:<video_id>`
   - `video_id`
   - `timeline_ms`
   - `MSG_TYPE_DANMAKU`
   - 时间轴语义
   - HTTP 和 WebSocket 两条发送路径
   - 持久化行为不一致

   尤其是最后一点：
   - HTTP `/api/danmaku/send`：同步写 `video_danmaku` 和 `message`
   - WebSocket `type=danmaku`：Job 只持久化 `message`，不写 `video_danmaku`
   - `/api/danmaku/list` 查的是 `video_danmaku`
   - 所以 WebSocket 发的弹幕，用列表接口查不到

   这是一个非常适合主动指出的“我知道项目还有问题”的点。

### 面试官会追问

- 聊天室成员关系和实时订阅有什么区别？
- 用户调用 HTTP leave 后，还能收到房间消息吗？
- HTTP 弹幕和 WebSocket 弹幕落库有什么区别？
- 弹幕的时间轴字段是干什么的？为什么聊天室没有？

### 建议表述

> 单聊是用户路由：Logic 查 `route:user:{uid}`，按目标 Comet 拆 Kafka 消息，Comet 用 `PushToUsers` 推给本机连接。  
>
> 聊天室是房间路由：Logic 查 `room:{room_id}:comets`，每个 Comet 一条 Kafka 消息，Comet 再用本机 `ConnRegistry` 做 `PushToRoom`。MySQL 的 `group_member` 是成员关系，Comet 本地房间注册表是在线订阅，离线成员不会收到实时推送。  
>
> 弹幕共享房间扇出，但 `session_id=danmaku:<video_id>`，消息带 `video_id` 和 `timeline_ms`，有时轴语义。当前 HTTP 弹幕会同步写 `video_danmaku` 和 `message`，WebSocket 弹幕只持久化 `message`，所以 `/api/danmaku/list` 看不到 WebSocket 弹幕，这是当前实现的一个不一致点。

**代码依据：** `logic/grpc_service.cpp`、`comet/comet_grpc_service.cpp`、`logic/http_handlers_danmaku.cpp`、`job/persist_handler.cpp`。

---

## 三个最致命的问题

| # | 错误说法 | 正确说法 | 风险 |
|---|---|---|---|
| 1 | “Job 慢，Logic 等它，所以加 Kafka” | Logic 不等 Job；Kafka 是为了解耦、缓冲、重放和独立扩展 | 面试官会怀疑你没理解异步链路 |
| 2 | “Logic 知道 Comet 的 IP 吧” | Logic 只知道 `comet_id`；Job 的 `comet_targets` 才知道 IP:port | 直接暴露组件职责没记清 |
| 3 | “Logic 多开会有分布式锁争夺” | 正常链路没有应用层分布式锁；限制是单 `logic_grpc_target`、流无重连、本地路由缓存 1 秒 | 面试官会追问锁在哪，答不上来会很减分 |

---

## 代码事实核对

> 这一节作为后续复盘的“事实底稿”。如果代码改动，先更新这里，再改上面的点评。

| 事实 | 结论 | 代码依据 |
|---|---|---|
| token TTL | 24 小时，`SETEX token:{token}` | `logic/http_handlers_auth.cpp` |
| Comet 鉴权 | 握手 `OnValidate` 同步 unary `VerifyToken`；后续消息不重复鉴权 | `comet/comet_server.cpp` |
| `session_id` | Logic 拼接 `single:min:max` / `room:id` / `danmaku:video_id` | `logic/grpc_service.cpp` |
| Redis 序号 | `session:{sid}:seq` 用 `INCR` 分配 `msg_seq` | `logic/redis_store.*` |
| `msg_id` | `session_id + "-" + msg_seq`，是派生值；不要和 `msg_seq` 混用 | `logic/grpc_service.cpp`、`logic/http_handlers_message.cpp` |
| 用户路由 | `route:user:{uid}` HASH，`conn_id -> comet_id` | `logic/redis_store.*` |
| Kafka key | 在线用 `session_id` 与 `comet_id` 拼接，离线只用 `session_id` | `logic/grpc_service.cpp` |
| 消费组 | push `..._push`，persist `..._persist` | `job/push_handler.cpp`、`job/persist_handler.cpp` |
| Producer `Send` | 返回 `true` 仅代表本地入队；无 delivery report | `common/kafka_producer.cpp` |
| Job → Comet | 默认同步 unary，3 秒 deadline，3 次重试 | `job/push_handler.cpp` |
| Comet 地址 | Logic 只知道 `comet_id`；Job `comet_targets` 持有 IP:port | `conf/job.conf`、`logic/grpc_service.cpp` |
| Logic 多实例 | 正常链路无应用层分布式锁；Redis 用 `INCR`/Lua | `logic/redis_store.*` |
| 上游断流 | `MessageStream` 无自动重连，`stream_running_` 可能仍为 `true` | `comet/logic_stream_client.cpp` |
| `PushToComet` 返回值 | 总是返回 gRPC OK + code 0，Job 无法感知内部推送失败 | `comet/comet_grpc_service.cpp` |
| 弹幕持久化 | HTTP 写 `video_danmaku` + `message`；WebSocket 只写 `message` | `logic/http_handlers_danmaku.cpp`、`job/persist_handler.cpp` |

---

## 面试回答的三层结构

目前回答 80% 停留在第一层，缺少第二层和第三层：

```text
第一层：链路 —— 谁调用谁，传什么，存哪里
第二层：原因 —— 为什么这样拆、为什么用 Kafka、为什么两个消费组
第三层：代价 —— 延迟、可靠性、扩展性、故障场景
```

示例：

- **为什么 Kafka？**  
  不只是“解耦”，还要说“代价是 ACK 变异步，可能丢/重，需要幂等和补偿”。
- **为什么多 Comet？**  
  不只是“提升并发”，还要说“Comet 有状态，扩容需要负载均衡、路由更新和重连”。
- **为什么两个消费组？**  
  不只是“分开”，还要说“同一个 group 内一条消息只会给一个 consumer，两个 group 才能各读一遍”。

---

## 第一轮行动项

### P0：必须重写

- [x] 重写 1.1，控制在 90 秒内，只保留确定事实。
- [x] 重写 1.2，明确“Logic 不等 Job”和“两个消费组，不是 partition 分开”。
- [x] 重写 1.3，删掉“分布式锁争夺”，改成真实扩容限制。
- [x] 重写 1.5，明确“Logic 只知道 `comet_id`，Job 才有地址表”。

### P1：补强

- [ ] 1.4 加一个延迟测量方案，并明确 ACK 的真实语义。
- [ ] 1.6 补成员关系、实时订阅、两阶段扇出、弹幕持久化差异。
- [ ] 每个回答至少补一句“这个设计的代价是什么”。
- [x] 准备以下追问的答案：
  - Comet 每条消息都鉴权吗？
  - `session_id` 谁生成？Redis 存什么？
  - Job → Comet 的 unary 是同步还是异步？
  - Logic 知道 Comet 的 IP 吗？
  - Logic 多节点到底需不需要分布式锁？

### P2：加分项

- [ ] 主动指出 `MessageStream` 断流不重连、`stream_running_` 卡住的问题。
- [ ] 主动指出 `PushToComet` 永远返回 code 0，Job 无法感知内部推送失败。
- [ ] 主动指出 WebSocket 弹幕不写 `video_danmaku`，列表接口查不到。
- [ ] 主动指出 producer `Send` 返回 true 只代表本地入队，不代表 broker ACK。

---

## 第二轮评审（2026-11-04）

> 本轮性质：针对第一轮点评的修正回应，不是完整口述版。  
> 结论：方向正确，关键错误修正了一半；仍有 3 个事实错误和 2 个未闭合的架构问题。

### 本轮评分

| 维度 | 第一轮 | 第二轮 | 说明 |
|---|---:|---:|---|
| 链路表达 | 7 | 7.5 | 修正后更清楚，能看出回看了代码 |
| 事实准确 | 5 | 6 | 鉴权、同步 unary、无锁已改对；`session_id`/`msg_id` 又错 |
| 架构因果 | 4 | 5.5 | 状态/失败域说对了；Kafka 因果仍未讲透 |
| 边界与失败场景 | 4 | 4.5 | 心跳、扩容有进步；1.4/1.6 未补 |
| 综合 | 5.5 | 6.0 | 进步明显，但还不是完整面试回答 |

### 已修正

| 项 | 第一轮问题 | 第二轮状态 |
|---|---|---|
| Comet 鉴权时机 | 误以为每条消息都鉴权 | 已改为“握手时鉴权一次，后续用心跳保活” |
| gRPC 双向流 | 误说成 gRPC 默认 | 已改为“项目配置默认启用” |
| Job → Comet unary | 误说成异步 | 已确认是同步阻塞、带 deadline 和重试 |
| 密码处理 | 表述含糊 | 已说明前端算 MD5、服务端字符串比较 |
| 三节点拆分原因 | 只讲 IO/CPU | 已补状态、失败域、扩缩容维度 |
| Logic 多实例锁 | 误以为有分布式锁争夺 | 已改为 Redis `INCR` 原子自增，无应用层锁 |
| Logic 与 Comet IP | 误以为 Logic 知道 IP | 已改为 Logic 只传 `comet_id`，Job 查 `comet_targets` |
| 追问 1/3/4/5 | — | 基本正确 |

### 仍未闭合

| 优先级 | 位置 | 现状 | 必须改成 |
|---|---|---|---|
| P0 | 1.2 | 没有明确收回“Logic 等 Job”；Kafka 因果仍未重述 | “Logic 生产后立即返回，不等 Job；Kafka 用于解耦/缓冲/重放/独立扩展” |
| P0 | 1.2 | “分区 -> 消费者组”仍然含糊 | “push/persist 是两个独立 consumer group，各自消费同一个 topic 的全量 partition；partition 不是按业务类型切分” |
| P0 | 1.1 / 追问 2 | 新错误：`session_id` 包含 `comet_id`；Redis 存 `msg_id` | `session_id` 只由 user/room/video 字段生成；Redis 存 `msg_seq`，`msg_id` 是派生值 |
| P1 | 1.1 | 仍说“IO 线程轮询 send、等待 io 返回” | IO 线程只入 `send_queue`；每条流一个 WriterLoop 负责 `Write`，ReaderLoop 负责回调和响应 |
| P0 | 1.4 | 没有修改 | 补 ACK 语义（本地入队 ≠ broker ACK）和延迟测量方案 |
| P1 | 1.6 | 没有修改 | 补成员关系 vs 实时订阅、两阶段扇出、弹幕持久化差异 |
| P1 | 1.3 | 只补了 Redis 热 key | 补 `logic_grpc_target`、`MessageStream` 无重连、本地路由缓存 1 秒 |
| P2 | 1.1 | “重复注册没有防备”不准确 | 代码有重复账号预检和 `uk_account` 唯一键，返回 409；缺的是限流、验证码、风控 |
| P2 | 1.1 | 心跳被当成鉴权/过期处理 | 心跳只判断连接存活（ping/pong，60 秒超时），不校验 token 是否过期 |
| P2 | 1.1 | “浏览器返回给服务端 JSON”方向仍说反 | 浏览器发送给 Comet；Comet 返回 ACK/错误给浏览器 |

### 1.1 第二轮点评

**改对的部分：**

- 握手鉴权一次、后续不重复鉴权，正确。
- Comet 有心跳桶，正确；但心跳只保活，不代表重新鉴权。
- 项目配置默认双向流，不是 gRPC 默认，正确。
- Job → Comet unary 同步阻塞，正确。
- 前端做 MD5、服务端字符串比较，符合当前代码；但这只是 demo 级做法，不是生产级密码存储。

**仍然要改的：**

1. **`session_id` 不包含 `comet_id`。**  
   单聊只由两个 `user_id` 生成；`comet_id` 只用于路由和 Kafka key。正确关系是：

   ```text
   单聊：session_id = "single:" + min(uidA, uidB) + ":" + max(uidA, uidB)
   聊天室：session_id = "room:" + room_id
   弹幕：session_id = "danmaku:" + video_id

   Redis：session:{session_id}:seq -> msg_seq
   msg_id = session_id + "-" + msg_seq
   ```

   `msg_seq` 是会话内递增序号；`msg_id` 是 `session_id + msg_seq` 派生出的唯一 ID。不要把两者混为一谈。

2. **流发送模型仍然说错。**  
   不是“IO 线程轮询 send、发送后等待 IO 返回”。实际是：

   ```text
   IO 线程 -> LogicStreamClient::Send
           -> 按 request_id % stream_count 选流
           -> 放入该流的 send_queue
   WriterLoop -> 从 send_queue 取出并 Write
   ReaderLoop -> Read 响应，按 request_id 找 callback 并执行
   ```

   IO 线程只负责入队，不等待网络写完成；回调在 ReaderLoop 线程执行。回调里如果做重活，会阻塞这条流的读。

3. **心跳不等于 token 续期。**  
   心跳桶是 64 个 bucket、500ms tick、60 秒超时，用 ping/pong 判断连接是否还活着。它不会检查 token 是否过期。token 过期后，已经建立的 WebSocket 仍然可用，直到连接断开或重新握手。

4. **“重复注册没有防备”不准确。**  
   `handleRegister` 会先 `GetUserByAccount` 查重，数据库还有 `uk_account` 唯一键，重复注册返回 409。当前缺的是限流、验证码、风控和密码安全，不是完全没有重复注册保护。

5. **“浏览器返回给服务端 JSON”方向仍然说反。**  
   浏览器是发送方，Comet 是接收方；Comet 处理后再给浏览器返回 ACK 或错误。不要用“浏览器返回给服务端”这种表述。

6. **断流兜底仍然没有回答。**  
   运行期 `MessageStream` 断开后，`stream_running_` 仍可能为 `true`，`Send` 继续入队，不会自动切 unary，pending callback 可能永远不返回。这是可以主动说的加分点。

**建议口述：**

> 单聊的 `session_id` 是 `single:min_uid:max_uid`，聊天室是 `room:room_id`，弹幕是 `danmaku:video_id`；`comet_id` 不进入 `session_id`，只用于选目标 Comet 和拼 Kafka key。Redis 的 `session:{sid}:seq` 用 `INCR` 分配 `msg_seq`，`msg_id` 再用 `session_id + "-" + msg_seq` 派生。  
>
> Comet 到 Logic 的流模式下，IO 线程只把请求放进对应流的 `send_queue`；每条流一个 WriterLoop 负责写、一个 ReaderLoop 负责读和回调。IO 线程不等待网络写完成，回调在 ReaderLoop 线程执行。

### 1.2 第二轮点评

**改对的部分：**

- 已经能从“快慢”上升到状态、失败域、扩缩容维度，方向正确。
- 知道 Comet、Logic、Job 的职责不同。

**仍然缺的三句话：**

1. **Logic 不等待 Job。**  
   Logic 生产 Kafka 后立即返回；Job 什么时候消费、消费多久，Logic 不关心。

2. **Kafka 不是“因为 Job 慢”才加的。**  
   即使 Job 很快，Kafka 仍然提供解耦、削峰、重放、失败隔离和独立扩展。

3. **push/persist 是两个消费组，不是两个 partition。**  
   两个 group 各自消费同一个 topic 的全量 partition，各自维护 offset；一个 group lag 不会阻塞另一个 group 提交 offset。它们仍共享 broker、网络和 DB 资源，所以不是绝对“互不影响”。

**建议口述：**

> Logic 生产 Kafka 后立即返回，不等待 Job。Kafka 的作用是解耦、削峰、重放和失败隔离，不是“Job 慢所以加队列”。push 和 persist 是两个独立消费组，各自消费同一个 topic 的全量 partition；partition 只负责组内并行和顺序，不按业务类型切分。

### 1.3 第二轮点评

**改对的部分：**

- 已明确当前正常链路没有应用层分布式锁。
- 已理解 Redis `INCR` 是原子操作，多 Logic 共享 Redis 可以解决 seq 分配。
- 主动提到 Redis 热 key/过热问题，这是加分点。

**仍然缺的部分：**

- 多 Logic 的真实限制不是锁，而是：
  - Comet 只配置了一个 `logic_grpc_target`；
  - `MessageStream` 没有自动重连；
  - 每个 Logic 有 1 秒本地路由缓存，跨实例失效不及时。
- Comet 扩容不是“缓解网络压力”这么简单，需要 WebSocket 负载均衡、唯一 `comet_id`、路由注册、重连，以及 Job 的 `comet_targets` 更新。

#### 关于“增加 partition 能不能解决 Job 并行度”

能提高上限，但不是无脑加：

1. 一个消费组的最大有效并行度 = partition 数。当前 4 个 partition，最多 4 个 consumer 能分到 partition；Job 扩到 8 个，多出来的实例会空闲。单进程内 `job_worker_threads` 再多，也不能让同一个 partition 并发处理。
2. 增加 partition 会改变 key 到 partition 的映射。同一 session 的历史消息和新消息可能落在不同 partition，跨 partition 不保证顺序；如果业务依赖严格顺序，不能直接在线扩容，应该停写排空、迁移到新 topic，或设计兼容的迁移方案。
3. 单个热 session 仍然只落一个 partition，加 partition 不能拆热 key；要拆热 key 就会牺牲该 session 的顺序。
4. partition 越多，rebalance、元数据、broker 压力和端到端延迟都会增加，不是免费的。
5. partition 数不解决业务幂等、DLQ、offset 提交语义和 MySQL 写入一致性；这些要另外做。

### 1.4 第二轮点评

这一题没有进入第二轮修改，仍然是 P0。

必须补上的两点：

- `KafkaProducer::Send` 返回 `true` 只代表本地入队，不代表 broker 收到；当前 `acks=1`、无 delivery report，ACK 语义比“生产到 Kafka”还弱。
- 延迟不能只说“没测过”；至少给出本地预估、测量点和 P50/P99 的测量方案。

### 1.5 第二轮点评

**已改对：**

> Logic 不需要知道 Comet 的 IP，只组装 `comet_id`；Job 用 `comet_targets` 找到目标 Comet。

再补一句就完整：

> Comet 之间不直接通信，但 Comet 不是“所有业务都抛给 Logic”；它自己负责连接管理、心跳、本机 `ConnRegistry` 和本机房间扇出。

### 1.6 第二轮点评

这一题没有修改，仍然是 P1。

至少补三点：

1. 聊天室成员关系在 MySQL，实时订阅在 Comet；离线成员收不到实时推送。
2. 聊天室是两阶段扇出：Logic 按 `room:{id}:comets` 拆 Kafka 消息，Comet 再做本机 `PushToRoom`。
3. 弹幕不只是多 `video_id` 和 `timestamp`：还有 `danmaku:<video_id>`、`timeline_ms` 和时间轴语义；HTTP 弹幕写 `video_danmaku` + `message`，WebSocket 弹幕只写 `message`，所以 `/api/danmaku/list` 查不到 WebSocket 弹幕。

### 追问回答点评

| 问题 | 你的回答 | 判定 | 点评 |
|---|---|---|---|
| Comet 每条消息都鉴权吗？ | 握手鉴权，后续心跳桶保活 | 基本正确 | 心跳只判断连接存活，不校验 token 过期 |
| `session_id` 谁生成？Redis 存什么？ | Logic 根据 `user_id`、`comet_id` 拼；Redis 存 `msg_id` | **错误** | 单聊只用两个 `user_id`；Redis 存 `msg_seq`，不是 `msg_id` |
| Job → Comet 的 unary 同步还是异步？ | 同步 | 正确 | 补一句：3 秒 deadline，3 次重试 |
| Logic 知道 Comet 的 IP 吗？ | 不知道 | 正确 | 补一句：Job 的 `comet_targets` 才知道 IP:port |
| Logic 多节点需要分布式锁吗？ | 不需要 | 正确 | 正常链路无锁；但仍有单 target、断流、本地缓存三个限制 |

### 第三轮 P0

- [ ] 1.2 明确说三句话：Logic 不等 Job；Kafka 为解耦/缓冲/重放；push/persist 是两个消费组。
- [ ] 1.1 修正 `session_id`/`msg_id`：`comet_id` 不进入 `session_id`，Redis 存 `msg_seq`。
- [ ] 1.1 修正流发送模型：IO 线程入队，WriterLoop 写，ReaderLoop 回调。
- [ ] 1.4 补 ACK 语义和延迟测量方案。
- [ ] 1.6 补成员关系、实时订阅、两阶段扇出、弹幕持久化差异。
- [ ] 1.3 补多 Logic 限制，以及“增加 partition 的代价和顺序风险”。

### 可以直接背的四句话

1. **Logic 生产 Kafka 后立即返回，不等待 Job；Kafka 是为了解耦、削峰、重放和失败隔离。**
2. **`session_id` 只由 user/room/video 字段生成；Redis 存 `msg_seq`；`msg_id = session_id + "-" + msg_seq`；`comet_id` 只用于路由和 Kafka key。**
3. **push 和 persist 是两个独立消费组，各自消费同一个 topic 的全量 partition；partition 不是按业务类型切分的。**
4. **Logic 只知道 `comet_id`；只有 Job 的 `comet_targets` 知道 `comet_id -> IP:port`。**

---

## 第三轮评审模板

> 新一轮评审直接复制这一节到文件末尾，填写后保留旧内容。

```markdown
## 第三轮评审（YYYY-MM-DD）

- 评审对象：
- 本轮重点：
- 上轮遗留问题是否修正：
- 新发现问题：
- 本轮评分：
- 下一步：
```

### 第三轮提交前自检

- [ ] 每个回答是否包含“链路、原因、代价”三层？
- [ ] 是否还有“吧”“无脑”“大概”等不确定表述？
- [ ] 关键结论是否能对应到具体代码文件或函数？
- [ ] 是否主动说了一个当前实现的缺陷？
- [ ] 是否能回答“如果这个组件挂了，消息会丢、会重，还是只是延迟”？

---

## 第三轮评审（2026-11-05）

> 评审对象：`docs/answer.md` 第三轮 2.1–2.4；2.5–2.11 是学习答案，不是你的原始回答。  
> 本轮结论：2.2、2.4 已经基本正确；2.1 仍有“`msg_id` 当序号”和“key 变了还能保证顺序”的硬伤；2.3 方向对，但没有解释 lag，也没有说清 push 为什么也应独立消费。

### 本轮评分

| 维度 | 第二轮 | 第三轮 | 说明 |
|---|---:|---:|---|
| 链路表达 | 7.5 | 7.5 | 2.2 的 fanout 描述清楚 |
| 事实准确 | 6 | 6.5 | 2.4 正确；2.1 仍混淆 `msg_id` 和 `msg_seq` |
| 架构因果 | 5.5 | 6.5 | 2.3 知道两个消费组独立，但没讲透“为什么必须两个组” |
| 边界与失败场景 | 4.5 | 6 | 2.2 提到了重复持久化和多余消费；2.1 没讲跨 key 顺序边界 |
| 综合 | 6.0 | 6.8 | 有明显进步，但 2.1/2.3 还不能直接上考场 |

### 2.1 在线/离线 Kafka key 与顺序

**判定：部分正确，6/10。关键概念要重写。**

**答对的部分：**

- 在线 key 用 `session_id|comet_id`，同一会话到同一 Comet 的消息会落到同一 partition，partition 内有序。
- 离线不用 `comet_id`，因为用户不在线时没有目标 Comet；此时只需要持久化。
- 你意识到在线切离线后 key 会变化，这是本题的核心。

**必须纠正：**

1. **顺序不是靠 `msg_id`，而是靠 partition 内的 offset 和生产顺序。**  
   `msg_seq` 是 Logic 用 Redis `INCR` 分配的逻辑序号；`msg_id = session_id + "-" + msg_seq` 是派生值。Kafka 不会读 `msg_id` 来排序。

2. **在线切离线后，Kafka 层面没有全局顺序。**  
   在线消息的 key 是 `session_id|comet_id`，离线消息的 key 是 `session_id`，两者可能落到不同 partition。Kafka 只保证同一个 partition 内有序，不保证跨 partition 顺序。`msg_seq` 只能用于事后排序、去重和分页，不能让 Kafka 跨 partition 重排。

3. **同一会话发到两个 Comet，也没有全局顺序。**  
   两条记录 key 不同，可能在不同 partition；两个 Comet 各自收到一条，彼此没有先后保证。如果同一个用户同时连在两个 Comet 上，还可能收到重复消息，客户端要按 `msg_id` 去重。

4. **“负载均衡”要说得更准确。**  
   key 是稳定哈希，不是轮询；它把不同 session/comet 分散到不同 partition，但热 session、热 Comet 仍会形成热 partition。这个 key 设计的首要目标是“同一会话到同一 Comet 保序 + 按 Comet 并行 fanout”，不是均匀负载。

5. **Redis 路由查询失败时，不一定真的离线。**  
   `GetUserRoutes` 失败或返回空时，`comet_to_users` 为空，代码会走 `persist_only` 分支：只落库、不推送。所以“没有路由”既可能是用户离线，也可能是 Redis 异常；这两种情况在代码里没有区分。

**建议口述：**

> 在线 key 是 `session_id|comet_id`，保证同一会话到同一个 Comet 的消息落在同一 partition，在这个 Comet 内有序；离线 key 是 `session_id`，保证同一会话的离线持久化消息落在同一 partition。但它们不保证全局顺序：在线切离线、同一会话 fanout 到多个 Comet，都会换 key、换 partition，Kafka 只保证 partition 内有序。`msg_seq` 是逻辑序号，`msg_id` 是派生值，可以用于客户端排序和去重，但不能让 Kafka 跨 partition 重排。如果业务要求严格全局顺序，就要让同一会话只用一个 key、接受串行消费，或者由消费端按 `msg_seq` 重排。

### 2.2 两个 Comet、两条 Kafka 记录与重复持久化

**判定：基本正确，8/10。**

**答对的部分：**

- 用户同时在 2 个 Comet 上，`GetUserRoutes` 会返回 2 个去重后的 `comet_id`，Logic 会产生 2 条 Kafka 记录。
- persist 组会消费两次，靠 MySQL 唯一键防重复插入。
- 你知道 Comet 数量增加会产生更多 Kafka 记录和不必要的消费。

**要补的细节：**

1. 两条记录携带**同一个** `msg_seq` 和 `msg_id`，因为序号只分配一次；每条记录的 `targets` 都包含该用户，`need_persist=true`。
2. “persist 写两次”准确说是**两次事务尝试**：两次 `session` upsert + 两次 `message` insert。第一次插入成功；第二次命中 `uk_session_seq (session_id,msg_seq)`，`ON DUPLICATE KEY UPDATE session_id=session_id` 变成 no-op。`session.last_msg_seq` 用 `GREATEST`，也不会回退。最终只有一行消息。
3. 如果 10 个 Comet：10 条 Kafka 记录、10 次 push RPC、10 次 persist 事务。push 的 10 次是合理的 fanout；persist 的 10 次是冗余写。优化方向是“只持久化一次”：例如只让其中一条记录带 `need_persist=true`，或单独发一条 `session_id` 维度的持久化记录，或者把持久化从 fanout 记录中拆出去。
4. 同一个用户多端在线时，本来就会收到多份推送；客户端要按 `msg_id` 去重，不能假设服务端只送一次。

**建议口述：**

> 一个用户同时在两个 Comet 上，Logic 会产生两条 Kafka 记录，两条记录携带同一个 `msg_seq`，每条都带 `need_persist=true`。persist 组会把两条都消费到，因此会有两次事务尝试；但 `message` 表有 `(session_id,msg_seq)` 唯一键，第二次 insert 是幂等的 no-op，`session.last_msg_seq` 又用 `GREATEST`，所以不会重复插入、不会回退。Comet 越多，fanout 记录和 persist 冗余写越多；push 的多条是必要的，persist 可以优化成只写一次。

### 2.3 两个消费组与 lag

**判定：方向对，6.5/10。**

**答对的部分：**

- push 和 persist 是两个独立消费组，各自维护 offset。
- 两个组消费速度不同，互不共享 offset。
- 你诚实地说没理解 lag，这比编一个定义好。

**要补的因果：**

1. **分开的主要原因不只是 persist 慢。**  
   push 也可能很慢：Job 要同步 gRPC 调 Comet，单次 3 秒 deadline，失败重试 3 次；Comet 网络抖动或客户端扇出慢都会拖住 push。persist 慢在 MySQL 连接、事务和磁盘。两者是不同资源、不同失败域，分开后可以独立扩缩容、独立重试、互不阻塞。

2. **如果只用一个消费组，一条消息只会被组内一个 consumer 消费。**  
   这样要么只推送、要么只持久化；如果要两者都做，就得在同一个 callback 里串行执行 push 和 persist。结果是 MySQL 慢会拖慢 push，push 失败也会影响持久化，而且无法分别增加 push worker 和 persist worker。

3. **lag 的定义：**  
   `lag = log-end offset - committed offset`，按 partition 计算，表示这个消费组还有多少条消息没消费完。可以看总量，也可以看单 partition。  
   - push 组 lag 大：实时推送延迟。  
   - persist 组 lag 大：历史落库延迟。  
   - 一个组 lag 不会直接改变另一个组的 offset，但它们共享 broker、网络、CPU、MySQL 等资源，所以不是绝对互不影响。

4. 代码里两个组分别是 `spark_push_group_push` 和 `spark_push_group_persist`；广播消费组用的是基础组 `spark_push_group`，监听的是另一个 topic。

**建议口述：**

> push 和 persist 消费同一个 topic，但属于两个独立 consumer group，各自消费全量 partition、各自提交 offset。分组的目的不是单纯因为“persist 写磁盘慢”，而是 push 和 persist 的资源、失败域、扩缩容维度不同：push 慢在 gRPC 和 Comet 网络，persist 慢在 MySQL；分开后一边 lag 或重试不会阻塞另一边。lag 就是 `log-end offset - committed offset`，按 partition 统计，表示这个组还有多少消息没处理。push 组 lag 代表实时延迟，persist 组 lag 代表历史延迟。它们 offset 不共享，但共享底层资源。

### 2.4 `KafkaProducer::Send` 返回 true 的含义

**判定：结论正确，8/10；但只答了第一层。**

**答对的部分：**

- `Send` 返回 true 只代表消息成功进入 librdkafka 的本地发送队列，不代表 broker 已经收到。
- 注意术语是 **librdkafka**，不是 libkafka。

**要补的后果：**

当前配置：

- `acks=1`：只等 leader 写入，不等 ISR 全部同步。
- `enable.idempotence=false`：重试可能产生重复，也可能在重试时改变顺序。
- 没有 delivery report 回调：应用层拿不到最终投递结果。
- `retries=5`、`retry.backoff.ms=500`。
- `message.timeout.ms=60000`：60 秒还没发出去就失败丢弃。
- `queue.buffering.max.messages=200000`：本地队列满时 `produce` 返回 `ERR__QUEUE_FULL`，`Send` 才返回 false。
- `linger.ms=0`、`batch.num.messages=1`：低延迟优先。

因此：

```text
Send 返回 true
  ≠ broker 收到
  ≠ leader 落盘
  ≠ ISR 副本同步
  ≠ 消费方收到
```

broker 挂掉时：

- 本地队列未满，`Send` 仍可能返回 true，HTTP/WS 已经回成功；消息在队列里重试，60 秒后超时丢弃，应用层没有回调感知。
- 本地队列满，`produce` 返回队列满，`Send` 返回 false，Logic 才会回 500。
- `acks=1` 下，leader 确认后如果 leader 在副本同步前崩溃，消息可能丢失。
- 关闭幂等后，producer 重试可能重复写入；如果有多条在途请求，重试还可能打乱顺序。

**建议口述：**

> `Send` 返回 true 只说明 `produce()` 把消息放进了 librdkafka 的本地队列，`poll(0)` 也不会等 broker 确认，所以它不代表 broker 收到。当前 `acks=1`、关闭幂等、没有 delivery report：broker 挂掉时 Logic 可能仍然返回成功，消息在本地重试，60 秒后超时丢弃；队列满了才会立即返回失败。`acks=1` 还可能丢，关闭幂等还可能重和乱序。要可靠，需要 delivery report、更合适的 acks、幂等 producer，或者业务侧对账补偿。

### 2.5–2.11 标准答案

#### 2.5 为什么手动提交 offset？`offset + 1` 是什么？

**为什么手动：**

- `KafkaConsumer::Init` 设置 `enable.auto.commit=false`、`enable.auto.offset.store=false`。
- 自动提交是按时间提交，可能业务还没处理完 offset 就前进了；进程崩溃会丢消息。
- 当前实现是业务 callback 完成后，`DrainCompletions` 才提交该 partition 的 offset。

**`offset + 1`：**

Kafka 的 committed offset 表示“下一条要消费的位置”。处理完 offset = 100 的消息，就提交 101；重启后从 101 开始，不会重复消费 100。

**重复消费的场景：**

- 业务处理成功，但进程在提交 offset 前崩溃。
- `commitAsync` 提交失败，或提交请求还没到 broker 就崩溃。
- rebalance 时 partition 被撤销，在途任务已经处理但 offset 没提交，新 owner 重新消费。
- producer 关闭幂等后重试，产生重复 Kafka 记录。
- 新消费组没有历史 offset，`auto.offset.reset=earliest` 会从最早消息重放。

**丢消息的场景：**

- callback 抛异常，`KafkaConsumer` 捕获后仍把完成项入队，offset 照样提交。
- `PersistHandler` 重试耗尽只打 `[DLQ][Persist]` 日志，callback 正常返回，offset 照样提交。
- `PushHandler` 在 unary 模式重试耗尽只打 `[DLQ][Push]` 日志，offset 照样提交。
- `use_push_stream=true` 时，`HandleMessage` 只是把请求放进流的 `send_queue` 就返回，offset 在真正写 gRPC 前就提交了；此时进程崩溃，队列里的消息丢失。
- 生产端 `Send` 返回 true 但 broker 一直不可用，60 秒超时后消息被丢弃。

**`commitAsync` 和 `commitSync`：**

- `commitAsync`：非阻塞，适合高吞吐；本代码没有设置 `offset_commit_cb`，提交请求发起后，真正的 broker 结果不会回调到业务，失败只记录 `commitAsync` 的立即错误码。它不会阻塞消费线程，也不会在应用层自动重试。
- `commitSync`：阻塞等待 broker 响应，能明确知道成功/失败，可以重试；更慢，适合 shutdown、rebalance 等需要确定提交结果的场景。

**一句话：**

> 手动提交是为了“先处理、后提交”；`offset+1` 是下一条要消费的位置；正常路径是 at-least-once，但异常、重试耗尽和 stream 模式入队都会让业务层变成 at-most-once。

#### 2.6 消费回调失败后，offset 还会提交吗？

**会。**

`KafkaConsumer::SchedulePartition` 的 worker lambda：

```cpp
try {
    callback_(item->key, item->value);
} catch (const std::exception& ex) {
    LogError(...);
} catch (...) {
    LogError(...);
}
completions_.push_back(key);
```

异常被吞掉，完成项照常入队，`DrainCompletions` 照常 `commitAsync(offset + 1)`。所以 callback 抛异常，消息不会被 Kafka 重投。

`PersistHandler` 的情况：

- `PersistMessage` 最多执行 4 次（`attempt=0..3`，即 3 次重试）。
- 退避 100ms → 400ms → 1600ms。
- 4 次都失败后，只打印 `[DLQ][Persist] All retries exhausted`，然后返回。
- `HandleMessage` 返回，KafkaConsumer 认为处理完成，提交 offset。

**这算 at-least-once 吗？**

- Kafka 到消费 callback：是 at-least-once；处理完才提交，崩溃会重放。
- 业务持久化：不是严格 at-least-once。重试耗尽后消息被“确认”但没写 MySQL，属于业务级丢失。
- 当前 `[DLQ]` 只是日志，不是真正的死信 topic/表，也没有补偿任务。

**怎么改：**

1. callback 返回成功/失败，失败时不要提交 offset。
2. 对可重试错误，暂停 partition，退避重试；注意会阻塞该 partition。
3. 用 retry topic：失败消息带 `attempt` 和 `next_retry_at` 转入重试 topic，主 topic 可以继续。
4. 超过最大重试次数进真正的 DLQ topic/表，带告警和人工/定时重放工具。
5. 消费端保持幂等；当前 `message` 的 `(session_id,msg_seq)` 唯一键已经提供了 DB 幂等基础。
6. 推送失败和持久化失败分开处理，不要让 push 的重试拖住 persist。

**一句话：**

> 当前代码 callback 抛异常或重试耗尽都会提交 offset；Kafka 层面是 at-least-once，但业务持久化不是严格 at-least-once，`[DLQ]` 目前只是日志，要真正闭环需要 retry topic/DLQ 表和补偿。

#### 2.7 同一 partition 串行、不同 partition 并行怎么实现？

`KafkaConsumer` 的核心结构：

```cpp
std::map<PartitionKey, PartitionState> partitions_;
```

每个 partition 状态：

```cpp
struct PartitionState {
    std::deque<std::shared_ptr<WorkItem>> pending;
    bool active{false};
};
```

流程：

1. `HandleMessage` 把消息放进对应 partition 的 `pending`，`inflight_++`。
2. 如果该 partition 的 `active == false`，调用 `SchedulePartition`。
3. `SchedulePartition` 把 `active=true`，把队首任务提交给 worker 线程池。
4. worker 执行 callback，完成后把 partition key 放入 `completions_`。
5. 消费线程 `DrainCompletions` 弹出完成项，`pending.pop_front()`、`active=false`、`inflight_--`，提交 `offset+1`，再调度同 partition 的下一条。

因此：

- 同一 partition 同时只有一个 callback 在跑，顺序严格。
- 不同 partition 有独立的 `PartitionState`，可以同时提交给多个 worker，并行处理。
- `pending` 负责保序和预取，`active` 负责“一个 partition 只允许一个在途任务”。
- `inflight_` 是全局已消费但未完成的数量，用于背压。

**为什么同一 partition 不能并发：**

Kafka 的 offset 是一个分区级水位。如果 offset 100 和 101 并发处理，101 先完成并提交 102，100 后失败，那么 100 在重启后会被跳过。即使做“只提交连续 offset”的复杂逻辑，业务副作用也可能已经乱序。所以最简单可靠的方案是分区内串行。

**一句话：**

> 每个 partition 一个 pending 队列和一个 active 标志，同一时刻只把一个任务交给 worker；完成后提交 offset+1 再调度下一条。不同 partition 可以并行，因为它们的 offset 和顺序互不影响。

#### 2.8 `max_inflight` 背压怎么工作？为什么 pause 后还要 `consume(10)`？

当前配置：`job_max_inflight=1024`，`job_worker_threads=4`。

`KafkaConsumer::Loop`：

```cpp
if (inflight_ >= options_.max_inflight) {
    SetPartitionsPaused(true);
    std::unique_ptr<RdKafka::Message> heartbeat(consumer_->consume(10));
    if (heartbeat && heartbeat->err() != RdKafka::ERR__TIMED_OUT) {
        HandleMessage(heartbeat.get());
    }
    continue;
}
SetPartitionsPaused(false);
```

含义：

- `inflight_` 包括已入 `pending` 和正在 worker 执行的消息。
- 达到上限时，暂停所有已分配 partition，停止继续拉取，防止内存无限增长。
- 但暂停后仍然调用 `consume(10)`。`consume()` 不只是取消息，它还是 librdkafka 的 poll 入口，负责心跳、组管理和 rebalance 事件。
- 如果只 sleep 不 poll，超过 `max.poll.interval.ms=300000`（5 分钟）会被 broker 认为消费者失败，触发 rebalance。
- `session.timeout.ms=45000` 是心跳超时；现代 Kafka 的心跳由后台线程发送，但应用仍必须定期 poll，否则 max.poll.interval 会生效。

**worker 全部卡住会怎样：**

- 只要消费线程还在循环调用 `consume(10)`，Kafka 不会仅因为 worker 卡住就判定消费者死亡；它看到的是 poll 还在继续。
- 但 `inflight_` 会停在 1024，partition 保持 paused，lag 持续增长，新消息不再被拉取。
- 如果消费线程本身也卡住/进程挂起，超过 5 分钟才触发 rebalance；如果进程被 kill，session timeout 45 秒后触发。
- `max_inflight` 是软上限：代码在消费前判断，暂停后仍可能返回已经预取的消息，但总体有界。

**一句话：**

> `max_inflight` 达到上限就 pause 所有 partition，形成背压；但必须继续 `consume(10)`，因为 poll 还负责心跳和组管理，不 poll 会触发 rebalance。worker 慢只会让 lag 增长，不会仅因为处理慢就被判定死亡；消费线程停止 poll 才会。

#### 2.9 只有 4 个 partition，Job 扩到 8 个有用吗？

**没有线性收益。**

- 一个 consumer group 内，一个 partition 同一时刻只能分配给一个 consumer。
- 4 个 partition，最多 4 个 consumer 真正拿到分区；第 5–8 个 consumer 空闲。
- Job 每个进程有两个 consumer：push 组和 persist 组，各自独立分配。8 个 Job 进程在每组里最多 4 个活跃，可能一个进程拿到 push 分区但没有 persist 分区。
- 每个进程还有 `job_worker_threads=4`。如果 4 个进程各拿 1 个 partition，每个进程同时只有 1 个 partition 的 callback 在跑，4 个 worker 里只有 1 个忙；如果 1 个进程拿到 4 个 partition，它才能用满 4 个 worker。

**增加 partition 的收益和代价：**

收益：

- 提高消费组最大并行度。
- 可以让更多 Job 实例真正干活。

代价/新问题：

1. key 到 partition 的映射会变化。同一 session 的旧消息在旧 partition，新消息可能去新 partition；跨 partition 不保证顺序。
2. 在线/离线 key 本来就会换 partition；增加 partition 会扩大这个顺序窗口。
3. 热 session/热 key 仍然只落一个 partition，加 partition 不能拆热 key；要拆就牺牲该 session 的顺序。
4. 更多 partition 意味着更多元数据、文件句柄、rebalance 开销和端到端延迟。
5. 如果要求严格顺序，不能直接在线扩容；要停写、排空旧 topic，再迁移到新 topic，或者设计版本化的 partitioner。

**rebalance 时在途消息怎么办：**

- 当前 `KafkaConsumer::Init` 只调用了 `subscribe(topics)`，没有设置 `rebalance_cb`，也没有处理 revoke/assign 事件。
- partition 被撤销时，`pending` 里的任务和在途 callback 没有统一的“停止调度、等待完成、commitSync、清理状态”逻辑。
- 可能出现的后果：在途任务继续执行，`commitAsync` 对已撤销 partition 失败；新 owner 从上次已提交 offset 重新消费，造成重复；`pending` 里未处理的消息可能滞留或丢失调度。
- 正确做法：注册 rebalance callback；revoke 时 pause、等待/取消在途任务、`commitSync` 已处理 offset、清空该 partition 状态；assign 时重置状态。或者使用 cooperative-sticky assignor 并实现同样的清理逻辑。

**一句话：**

> 4 个 partition 的消费组最多 4 个有效 consumer，8 个 Job 会有 4 个闲着；加 partition 能提高并行上限，但会改变 key 映射、引入顺序问题，还要处理 rebalance 在途任务。当前代码没有 rebalance callback，rebalance 时可能重复消费或提交失败。

#### 2.10 Kafka、Job、Comet、MySQL 挂了分别会怎样？

| 故障 | 实时推送 | 持久化 | 消息命运 |
|---|---|---|---|
| Kafka 不可用 | Logic 可能仍返回成功，Job 收不到 | 无法消费 | 本地队列重试，60 秒后可能丢；队列满才立即失败 |
| Job 不可用 | 延迟 | 延迟 | Kafka 保留，重启后重放；可能重复；retention 到期会丢 |
| Comet 不可用 | 该 Comet 上的连接收不到 | persist 组仍可写 | 实时丢、历史可能在；路由残留会导致反复失败 |
| MySQL 不可用 | push 组不受直接影响 | 重试耗尽后丢 | 用户可能实时收到，但历史缺失 |
| Redis 不可用（补充） | 路由/序号/token 失效 | persist_only 可能仍写 | 在线用户可能被当成离线，只落库不推送 |

展开：

**Kafka 挂了：**

- `KafkaProducer::Send` 只保证本地入队；broker 不可用时 `Send` 仍可能返回 true，Logic 已经回成功。
- 消息在 librdkafka 队列里重试，`message.timeout.ms=60000` 后超时丢弃；没有 delivery report，应用层不知道。
- 如果本地队列达到 `queue.buffering.max.messages=200000`，`produce` 返回 `ERR__QUEUE_FULL`，`Send` 返回 false，Logic 才会报错。
- Job 没有消息可消费，实时推送和历史落库都停。

**Job 挂了：**

- Logic 继续生产，消息留在 Kafka；HTTP/WS 仍可能返回成功。
- 实时推送延迟；历史落库延迟。
- Job 重启后从各自 group 的 committed offset 继续消费；处理成功但未提交的消息会重放，可能重复。
- 如果 Job 停机超过 topic retention（脚本没有显式配置，默认通常 7 天），消息会被删除，永久丢失。

**Comet 挂了：**

- 该 Comet 上的 WebSocket 全部断开；客户端需要重连、重新 `VerifyToken`、重新写路由和房间。
- 如果 Job 正好在推送，gRPC 失败会重试 3 次（4 次调用，每次 3 秒 deadline），最后只打 `[DLQ][Push]`，offset 仍提交；实时推送丢失。
- persist 组是独立消费组，仍会把消息写进 MySQL；所以用户重连后通常能从历史里补到。
- 致命的是 Redis 路由没有 TTL：Comet 崩溃前没来得及 `UserOffline`/`ReportRoomLeave`，`route:user` 和 `room:{rid}:comets` 会残留死 Comet。Job 会继续往死地址推，浪费重试和时间；`online_count` 也会偏高。
- `PushToComet` 内部无论 `ProcessPushRequest` 是否真的把消息发给了连接，都会返回 gRPC OK + code 0；只有连接层失败才会被 Job 感知。Comet 进程活着但本机发送失败时，Job 认为成功。

**MySQL 挂了：**

- persist 组每次 `ExecutePersist` 失败后重试 3 次（4 次执行，100ms/400ms/1600ms），仍失败就打印 `[DLQ][Persist]` 并返回。
- KafkaConsumer 随后提交 offset，消息不会因为 MySQL 恢复而自动重写；历史出现缺口。
- push 组是独立消费组，实时推送通常不受影响；用户能收到消息，但历史查不到。
- 历史、未读、mark_read、会话列表、聊天室列表等依赖 MySQL 的接口会失败；Redis 未读路径可能还能工作一段时间。

**一句话：**

> Kafka 挂了可能“假成功”后丢；Job 挂了主要是延迟和重放重复；Comet 挂了实时丢但历史大多还在，还会留下 Redis 死路由；MySQL 挂了 push 还能走，但 persist 重试耗尽后历史永久缺口，当前没有真正的 DLQ/补偿。

#### 2.11 为什么用 protobuf？Kafka key 为什么不用 `msg_id`？

**为什么消息体用 protobuf：**

- 有 `.proto` 契约，字段编号明确，跨语言代码生成，类型安全。
- 二进制编码比 JSON 更紧凑，序列化/反序列化更快，适合 gRPC 和 Kafka 高频内部通信。
- 字段编号支持向后/向前兼容，新增字段不容易破坏旧消费者。
- gRPC 原生使用 protobuf；浏览器端仍然用 JSON，因为浏览器原生支持、调试方便。
- 当前项目是混合模式：HTTP/WebSocket 边界用 JSON；gRPC/Kafka 信封用 protobuf；`ChatMessage.content_json` 里又嵌了一段业务 JSON，兼顾灵活性和协议效率。

**Kafka key 和 value 的职责：**

- key：决定分区（默认 hash）、影响同一 key 的顺序、可影响 compaction；本项目中 key 只承担分区/顺序职责。
- value：完整的 `PushToCometRequest` protobuf，包含 `ChatMessage`、`targets`、`room_id`、`scene`、`need_persist` 等。
- 当前 `PushHandler` 和 `PersistHandler` 都忽略 key，只解析 value；key 不参与业务去重。

**为什么 key 不用 `msg_id`：**

- `msg_id` 每条消息都唯一；如果用它做 key，同一 session 的消息会被 hash 到不同 partition，Kafka 就无法保证该 session 的顺序。
- `msg_id` 是 `session_id + "-" + msg_seq` 的派生值，适合客户端去重/展示，不适合做“顺序单元”。
- 在线 key 用 `session_id|comet_id`：同一 session 到同一 Comet 共用一个顺序单元；离线 key 用 `session_id`：同一 session 的离线持久化共用一个顺序单元。
- 代价是：同一 session 到不同 Comet 不保全局序；在线/离线切换也不保全局序。这是为了 fanout 并行度和按 Comet 隔离做的取舍。

**一句话：**

> protobuf 负责内部的高效、强类型、可演进；JSON 只用在浏览器边界。Kafka key 是分区和顺序键，不是消息 ID；`msg_id` 每条唯一，拿它当 key 会把同一 session 打散到不同 partition，失去会话内顺序。当前 key 用 `session_id|comet_id`/`session_id`，换来的是按 Comet 并行 fanout，代价是没有跨 Comet、跨在线/离线的全局顺序。

### 本轮后行动项（第四轮前）

#### P0：必须能脱口而出

- [ ] 2.1 改掉“`msg_id` 保证顺序”的说法：顺序是 partition offset；逻辑序号是 `msg_seq`；`msg_id` 是派生值。
- [ ] 2.1 明确说：在线切离线、同一会话多 Comet，都**没有全局顺序**。
- [ ] 2.2 说清“两条记录、两次 persist 尝试、一行消息”，以及 10 Comet 的冗余写。
- [ ] 2.3 用自己的话解释 lag，并说清 push 和 persist 为什么必须是两个消费组。
- [ ] 2.4 说清 `Send true`、`acks=1`、无 delivery report、60 秒超时、队列满的区别。
- [ ] 2.5 说清 `offset+1`、重复/丢失场景、`commitAsync`/`commitSync`。
- [ ] 2.6 说清“重试耗尽仍提交 offset，所以不是严格 at-least-once”。
- [ ] 2.8 说清 pause 后为什么还要 `consume(10)`，以及 `max.poll.interval.ms`。
- [ ] 2.9 说清 4 partition 对 8 Job 的限制，以及扩容 partition 的顺序代价。
- [ ] 2.10 能按 Kafka/Job/Comet/MySQL 分别说“丢、重、延迟”。
- [ ] 2.11 说清 key 是分区键，不是消息 ID；`msg_id` 不能做会话顺序键。

#### P1：加分项

- [ ] 主动指出 `PersistHandler` 的 `[DLQ]` 只是日志，不是真正死信队列。
- [ ] 主动指出 `PushToComet` 永远返回 code 0，Job 无法感知 Comet 内部推送失败。
- [ ] 主动指出 stream 模式先提交 offset、后真正写 gRPC，进程崩溃会丢本地队列。
- [ ] 主动指出 `KafkaConsumer` 没有 `rebalance_cb`，rebalance 时在途/pending 消息可能重复或提交失败。
- [ ] 主动给出“persist 只写一次”的优化方案。
- [ ] 主动给出“Comet 崩溃后 Redis 死路由无 TTL”的清理方案（租约/心跳/TTL）。

### 可以背下来的十个结论

1. Kafka 只保证 partition 内有序，不保证跨 partition 有序；key 变了，顺序边界就变了。
2. `msg_seq` 是逻辑序号，`msg_id = session_id + "-" + msg_seq` 是派生值；不要拿 `msg_id` 当顺序来源。
3. 一个用户在两个 Comet 上：两条 Kafka 记录、两次 persist 尝试、MySQL 唯一键保证只有一行。
4. push 和 persist 是两个独立消费组，各自消费全量 partition、各自提交 offset；一边 lag 不直接改另一边 offset，但共享底层资源。
5. `Send` 返回 true 只代表进入 librdkafka 本地队列，不代表 broker 收到；`acks=1` 还可能丢，关闭幂等还可能重/乱序。
6. 手动提交是在 callback 完成后提交 `offset+1`；offset+1 是下一条要消费的位置。
7. callback 抛异常、Persist/Push 重试耗尽、stream 模式入队，都会让 offset 继续前进，业务层可能丢。
8. 同一 partition 必须串行，因为 offset 是分区级水位；并发完成会让后面的 offset 覆盖前面的失败。
9. `max_inflight` 满了要 pause，但仍要 `consume(10)` 维持 poll；worker 卡住不会仅因处理慢触发 rebalance，消费线程停止 poll 才会。
10. 4 个 partition 最多 4 个有效 consumer；加 partition 能扩并行，但会改 key 映射、引入顺序问题，还要处理 rebalance。
11. `Protobuf` 用于内部高效通信，JSON 用于浏览器边界；Kafka key 是分区/顺序键，value 才是完整业务消息。

## 维护约定

1. `docs/answer.md` 放原始回答；`docs/review.md` 放点评、纠正和行动项。
2. 每轮评审新增一节，不覆盖历史，方便对比进步。
3. 代码事实变化时，先更新“代码事实核对”，再修改相关点评。
4. 如果某个问题已修正，在行动项前标记 `[x]`，并在新一轮评审里写“已修正”。
5. 评分只用于纵向对比，不用于自我否定；重点看“下一轮能不能把追问接住”。
