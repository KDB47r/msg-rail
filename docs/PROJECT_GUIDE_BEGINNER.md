# SparkPush 小白入门笔记（先看这份）

> 如果你看 `PROJECT_GUIDE.md` 觉得头大，先看这份。
>
> 这份只讲三件事：
> 1. 这个系统像什么（用生活里的东西打比方）
> 2. 一条单聊消息怎么走（只讲这一条）
> 3. 第一周该看哪几个文件、做什么实验
>
> 大文档是“字典”，遇到问题再查，**不要从头读到尾**。

---

## 0. 先用一句话理解整个项目

SparkPush 就是一个**聊天消息的快递系统**：

| 生活里的东西 | 项目里的进程 | 它干什么 |
|---|---|---|
| 小区驿站 / 前台 | **Comet** | 记住谁在线，帮人收件、送件 |
| 调度中心 | **Logic** | 查地址、贴单号、决定包裹走哪条线路 |
| 传送带 / 中转仓 | **Kafka** | 包裹先排队，防止一下子太多送不过来 |
| 配送员 | **Job** | 从传送带取包裹，送到驿站；同时抄一份到档案室 |
| 白板 | **Redis** | 记临时信息：谁在线、在哪个驿站、消息编到几号 |
| 档案室 | **MySQL** | 永久保存聊天记录 |
| 展示厅 | **web_demo** | 提供网页，让你能看到效果 |

一句话：

> **Comet 记“人在哪”，Logic 算“这条消息是第几条、该发给谁”，Kafka 排队，Job 送货并存档，Redis 记临时状态，MySQL 记永久记录。**

---

## 1. 四个进程分别干什么

### Comet（接入层）

- 它手里握着很多条**一直不挂的电话线**（WebSocket 长连接）。
- 它只知道：**这台机器上，哪些用户正连着**。
- 它不查数据库，也不负责给消息编号。
- 生活比喻：小区驿站。住户来了就登记，走了就划掉；有人寄包裹，它转给调度中心。

### Logic（业务层）

- 它负责登录、发消息、历史记录、房间、弹幕这些**业务规则**。
- 它要知道“某个用户现在在哪个 Comet”，才能把消息送过去。
- 它不握着客户端的长连接。
- 生活比喻：调度中心。查地址、贴单号、决定走哪条线路。

### Job（异步层）

- 它从 Kafka 传送带上取任务。
- 一个 Job 组负责**送货**：把消息推给目标 Comet。
- 另一个 Job 组负责**存档**：把消息写进 MySQL。
- 生活比喻：配送部。一个小组送货，一个小组抄档案。

### web_demo（展示层）

- 就是一个静态网页服务器，提供登录页、聊天页、弹幕页。
- 它不处理业务，只是让你方便地看到效果。

**为什么要拆成四个进程？**

一个人既接电话、又查地址、又送货，电话一多就忙不过来。拆开以后，哪个部门忙，就单独给哪个部门加人。

---

## 2. 先认识 6 个编号 / 名词

这是整个项目最基础的“人名地名”，记住它们，代码就能看懂一半。

| 名词 | 人话解释 | 例子 |
|---|---|---|
| `user_id` | 用户编号，数据库里的主键 | alice = 1，bob = 2 |
| `token` | 通行证。登录成功后发给你，之后每次请求都要带着 | 一串 48 位字符 |
| `conn_id` | **这一次连接**的编号。同一个人开两个网页，就有两个 conn_id | `comet-1:7` |
| `session_id` | **聊天窗口**的编号。单聊、房间、弹幕各有一种 | `single:1:2`、`room:1001` |
| `msg_seq` | 这个聊天窗口里的**第几条**消息 | 15 |
| `msg_id` | 消息的唯一编号，规则是 `session_id + "-" + msg_seq` | `single:1:2-15` |

还有一个词在 Redis 里经常出现：

| 名词 | 人话解释 |
|---|---|
| `route` | 白板上写的“这个用户现在在哪个 Comet”。Redis 里的 key 叫 `route:user:{用户id}` |

### 用 alice 和 bob 举个例子

- alice 的 `user_id = 1`，bob 的 `user_id = 2`。
- alice 登录，拿到 `token`。
- alice 打开网页，连上 Comet，Comet 给她分配 `conn_id = comet-1:7`。
- Logic 在 Redis 白板上写：

```text
route:user:{1}
  comet-1:7  ->  comet-1
```

意思是：**alice 现在在 comet-1 这台机器上，连接编号是 comet-1:7。**

- alice 给 bob 发消息，Logic 算出：
  - `session_id = single:1:2`（两个人的聊天窗口）
  - `msg_seq = 15`（这个窗口里的第 15 条）
  - `msg_id = single:1:2-15`

> 小知识：单聊的 `session_id` 会把两个 user_id **从小到大排序**再拼接。这样 alice 发给 bob、bob 发给 alice，用的是同一个聊天窗口。

---

## 3. 一条单聊消息的完整旅程（重点）

只讲这一条。房间、弹幕、广播，都是这条路的变化。

先把图看一眼，不用全懂：

```text
alice 浏览器
   │ ① 登录，拿到 token
   ▼
Logic ──② 把 token 记到 Redis 白板
   ▲
   │ ③ alice 连 Comet，Comet 拿 token 来问 Logic
   │ ④ Logic 在白板写：alice 在 comet-1 的 conn-7
   │
Comet ◄──⑤ alice 发消息
   │ ⑥ Comet 转给 Logic
   ▼
Logic ──⑦ 算 session_id、seq、msg_id；查 bob 在哪
   │ ⑧ 把“推送任务”放上 Kafka 传送带
   ▼
Kafka ──⑨a──> Job（送货） ──⑩a──> comet-1 ──⑪a──> bob 浏览器
   └──⑨b──> Job（存档） ──⑩b──> MySQL
```

下面一步一步说。

### 第 1 步：alice 登录

alice 在网页上输入账号密码，Logic 的登录接口会：

1. 去 MySQL 查这个用户；
2. 比较密码；
3. 生成一个随机 `token`；
4. 把 `token -> user_id` 写进 Redis，有效期 24 小时；
5. 把 token 返回给网页。

代码位置：`logic/http_handlers_auth.cpp` 的 `handleLogin`。

> 网页登录时密码填 `123456` 就行，网页会自己转成 MD5；如果你用 `curl` 调接口，要直接填 MD5 后的值。

### 第 2 步：alice 连上 Comet

网页用 `ws://localhost:9200/ws?token=...` 连 Comet。

Comet 会拿着 token 去问 Logic：“这个通行证是真的吗？”

Logic 去 Redis 查 `token:{token}`，查到 user_id，然后在白板上写一条路由：

```text
route:user:{1}  里增加一项：comet-1:7 -> comet-1
```

意思是：**alice 现在在 comet-1 上。**

代码位置：
- Comet 这边：`comet/comet_server.cpp` 的 `OnValidate`
- Logic 这边：`logic/grpc_service.cpp` 的 `VerifyToken`

### 第 3 步：alice 发消息

alice 在网页输入“你好 bob”，网页通过 WebSocket 发了一包 JSON：

```json
{"type":"single_chat","to_user_id":2,"content":{"text":"你好 bob"}}
```

Comet 收到后，不自己做业务，而是转给 Logic。

代码位置：`comet/comet_server.cpp` 的 `HandleTextMessage`。

### 第 4 步：Logic 算编号、查地址

Logic 做四件事：

1. 算出聊天窗口：`session_id = single:1:2`
2. 给消息编号：去 Redis 把 `session:{single:1:2}:seq` 加 1，得到 `msg_seq = 15`
3. 拼出消息编号：`msg_id = single:1:2-15`
4. 查 bob 在哪：去 Redis 找 `route:user:{2}`，发现 bob 也在 `comet-1`

代码位置：`logic/grpc_service.cpp` 的 `HandleUpstreamMessage`。

### 第 5 步：把“推送任务”放上 Kafka

Logic 不直接把消息推给 Comet，而是写一条任务到 Kafka：

- topic 叫 `push_to_comet`
- key 是 `session_id|comet_id`，例如 `single:1:2|comet-1`
- 内容是一条 `PushToCometRequest`，里面写着：
  - 目标 Comet：`comet-1`
  - 目标用户：bob（user_id=2）
  - 消息正文
  - 是否需要存档：是

代码位置：`logic/grpc_service.cpp` 的 `HandleUpstreamMessage` 后半段。

### 第 6 步：两个 Job 各拿一份

Kafka 有一个特点：**同一个 topic，可以被多个“消费组”各读一份，互不抢。**

这个项目有两个组：

- `spark_push_group_push`：负责**送货**（推送给 Comet）
- `spark_push_group_persist`：负责**存档**（写 MySQL）

所以同一条消息：

- 送货组把它推给 `comet-1`；
- 存档组把它写进 MySQL 的 `message` 表。

代码位置：
- 送货：`job/push_handler.cpp`
- 存档：`job/persist_handler.cpp`

### 第 7 步：Comet 找到 bob 的连接，发出去

`comet-1` 收到推送任务后，在自己本机的连接表里找 bob，找到就通过 WebSocket 把消息发给他。

代码位置：`comet/comet_grpc_service.cpp` 的 `ProcessPushRequest`，以及 `comet/comet_server.cpp` 的 `PushToUsers`。

### 第 8 步：Comet 给 alice 回一个 ACK

alice 会收到：

```json
{"type":"ack","client_msg_id":"...","msg_id":"single:1:2-15"}
```

**注意：ACK 的意思是“包裹已经收下，并放上传送带了”，不是“bob 已经看到了”。**

真正送到 bob，是后面 Job 和 Comet 的事。

### 对方不在线会怎样？

如果 Logic 在 Redis 里查不到 bob 的路由（bob 没连 WebSocket），那么：

- 不推送；
- 但仍然把任务写进 Kafka，并标记 `persist_only = true`；
- Job 的送货组看到 `persist_only` 就跳过；
- 存档组照常写 MySQL。

所以：**离线消息不会丢，会存在 MySQL 里；但当前版本不会自动推给上线后的 bob，需要客户端自己拉历史记录。**

---

## 4. 四个最常见的“为什么”

### 为什么要有 Kafka，Logic 直接推给 Comet 不行吗？

行，但会出问题：

- Comet 卡了，Logic 也得跟着等；
- Comet 重启，这条消息可能就丢了；
- 消息一多，Logic 忙不过来。

有了 Kafka：

- Logic 把任务放上传送带就能返回，速度快；
- Job 可以慢慢送、失败重试；
- 送货和存档可以各干各的。

生活比喻：寄快递不是快递员站在你家门口等你把包裹交给他，而是你先放到驿站/快递柜，后面慢慢配送。

### 为什么用户在哪要用 Redis，不用 MySQL？

因为**每条消息都要查一次“对方在哪”**，这个操作非常频繁。

- Redis 在内存里，快；
- MySQL 在磁盘上，适合永久保存，不适合高频查路由。

生活比喻：白板用来记“谁今天在哪个驿站”，档案室用来存“去年谁给谁寄过什么”。你不会为了看今天谁在，去翻档案室。

### 为什么消息编号要放在 Redis？

因为同一个聊天窗口的消息必须**一条一条按顺序编号**。

Redis 的 `INCR` 是原子操作，多个人同时发消息也不会编出两个 15。

如果 Redis 重启丢了编号，Logic 会去 MySQL 查这个会话目前最大的 `msg_seq`，从那个数继续往后编。这个“兜底”逻辑在 `logic/conversation_store.cpp` 的 `AllocateMessageSeq`。

### 为什么单聊的 session_id 要把 user_id 排序？

为了让 alice 发给 bob、bob 发给 alice，用的是**同一个聊天窗口**。

如果不排序：

- alice 发给 bob 可能是 `single:1:2`
- bob 发给 alice 可能是 `single:2:1`

那就变成两个窗口了，历史记录和未读数都会乱。

---

## 5. 第一周只看这 6 个地方

不要一上来读整个文件。**只看下面这些函数，其他先跳过。**

| 顺序 | 文件 | 只看什么 | 看完要能回答 |
|---|---|---|---|
| 1 | `proto/spark_push.proto` | `ChatMessage`、`UpstreamMessageRequest`、`PushToCometRequest`、`LogicService` | 消息里有哪些字段？ |
| 2 | `logic/http_handlers_auth.cpp` | `handleLogin` | 登录后 token 存哪？ |
| 3 | `logic/http_handlers_message.cpp` | `handleSendMessage` 的前 200 行 | HTTP 发消息和 WS 发消息有什么不同？ |
| 4 | `comet/comet_server.cpp` | `OnValidate`、`OnOpen`、`OnMessage`、`HandleTextMessage` 里 `single_chat` 的分支、`PushToUsers` | Comet 怎么知道用户在哪个连接？ |
| 5 | `job/push_handler.cpp` | `HandleMessage`、`ProcessPushRequest` | Job 怎么把消息推给 Comet？ |
| 6 | `job/persist_handler.cpp` | `ExecutePersist` | 消息写进了哪张表？ |

### 这周先不要看

下面这些不是不重要，而是**第一周看会打击信心**：

- `common/kafka_consumer.cpp` 的分区/提交细节
- `comet/logic_stream_client.cpp` 的双向流
- `common/timer_wheel.cpp` 的时间轮算法
- `comet/conn_registry.cpp` 的分片锁
- `common/net/http_server_lite.cpp` 的 HTTP 解析
- `logic/redis_store.cpp` 里的 Lua 脚本

先把一条消息走通，再回头看这些“性能优化”和“边界处理”。

---

## 6. 读代码的笨办法（很有效）

### 三遍法

1. **第一遍只看名字**：这个函数叫什么？它大概是干什么的？
2. **第二遍只看数据**：输入从哪来？输出到哪去？中间多了什么字段？
3. **第三遍才看细节**：错误处理、锁、缓存、日志、重试。

第一遍千万不要逐行抠。

### 只读“主路径”

看到这种代码先跳过：

```cpp
if (!redis_store_) { ... }
if (err) { ... }
std::lock_guard<std::mutex> lock(...);
LOG_ERROR << ...;
```

先找“正常情况下的那几行”，把主流程串起来。等主流程懂了，再回来看这些分支。

### 用工具验证，不要靠脑补

- 不确定 token 在哪：`redis-cli` 查一下
- 不确定消息有没有落库：`mysql` 查一下
- 不确定消息有没有进 Kafka：看 Job 日志
- 不确定连接还在不在：看 Comet 日志 / `route:user:{id}`

### 一次只回答一个问题

不要想着“今天把整个项目看懂”。今天只回答：

> “登录之后，token 到底存在哪里？”

明天只回答：

> “alice 连上 WebSocket 后，Redis 里多了什么？”

这样一周下来，你会发现自己真的记住了。

---

## 7. 七天计划（每天 1 小时左右）

### 第 1 天：把它跑起来，登录一次

```bash
cd /home/wdh/Linux/Spark_Push
./scripts/start_all.sh
./scripts/status.sh
```

然后打开浏览器：`http://localhost:9001/index.html`

用 alice / `123456` 登录。

再用命令行登录一次（注意密码要填 MD5）：

```bash
curl -s -X POST http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}'
```

你会看到类似：

```json
{"code":0,"message":"ok","data":{"user_id":1,"token":"一长串字符","name":"alice"}}
```

**完成标志**：你能把返回里的 `token` 指出来。

---

### 第 2 天：看登录代码

打开 `logic/http_handlers_auth.cpp`，只看 `handleLogin`。

边看边回答：

1. 密码是怎么比较的？
2. token 是怎么生成的？
3. token 存到了 Redis 的哪个 key？过期时间多久？

然后用 `redis-cli` 验证：

```bash
redis-cli
> KEYS token:*
> GET token:你的token
```

**完成标志**：你能画出“浏览器 → Logic → Redis”三个框，并标出 token 的流向。

---

### 第 3 天：看 WebSocket 鉴权和路由

打开 `comet/comet_server.cpp`，只看 `OnValidate` 和 `OnOpen`。

再用浏览器登录 alice，保持页面打开，然后：

```bash
redis-cli
> HGETALL route:user:{1}
```

你会看到类似：

```text
comet-1:7
comet-1
```

意思是：alice 在 `comet-1`，连接编号是 `comet-1:7`。

**完成标志**：你能解释 `route:user:{1}` 里的 field 和 value 分别是什么。

---

### 第 4 天：看发消息和编号

打开 `logic/grpc_service.cpp`，只看 `HandleUpstreamMessage` 的前半段：

- 怎么算 `session_id`
- 怎么分配 `msg_seq`
- 怎么拼 `msg_id`

然后用 `redis-cli` 看序号变化：

```bash
redis-cli
> GET session:{single:1:2}:seq
```

在网页上给 bob 发一条消息，再查一次，数字应该加 1。

**完成标志**：你能说出 `msg_id = single:1:2-15` 里的每一部分是怎么来的。

---

### 第 5 天：看 Job 送货和存档

打开：

- `job/push_handler.cpp` 的 `HandleMessage`、`ProcessPushRequest`
- `job/persist_handler.cpp` 的 `ExecutePersist`

然后做个小破坏：

```bash
./scripts/stop_all.sh   # 先全停
# 只启动 MySQL、Redis、Kafka、Logic、Comet，不启动 Job
```

再发几条消息，观察 Kafka 里消息堆积；然后启动 Job，观察它把消息消费掉。

**完成标志**：你能解释为什么 Job 停了，消息没有丢。

---

### 第 6 天：看 Comet 怎么推给 bob

打开：

- `comet/comet_grpc_service.cpp` 的 `ProcessPushRequest`
- `comet/comet_server.cpp` 的 `PushToUsers`

然后开两个浏览器窗口：

- 窗口 A：alice 登录
- 窗口 B：bob 登录

alice 给 bob 发消息，观察 bob 窗口收到。

**完成标志**：你能说出“为什么 Logic 不直接发 WebSocket，而要经过 Kafka 和 Job”。

---

### 第 7 天：自己讲一遍 + 改一个小地方

合上文档，拿一张纸，画一遍：

```text
alice 登录 → 连 WS → 发消息 → Logic 编号 → Kafka → Job → Comet → bob
```

然后做一个小改动（任选一个）：

- 在 `handleLogin` 里加一行日志，打印“用户 xxx 登录成功”；
- 把 Comet 的 ACK JSON 里加一个字段；
- 把 `/health` 的返回加一个字段。

改完重新编译、运行、验证。

**完成标志**：你能不看文档，用自己的话讲 3 分钟。

---

## 8. 三个今天就能做的小实验

### 实验 1：登录，拿 token

```bash
curl -s -X POST http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}'
```

### 实验 2：用 HTTP 发一条消息

把上一步返回的 token 复制到下面：

```bash
curl -s -X POST http://127.0.0.1:9101/api/message/send \
  -H 'Authorization: Bearer 你的token' \
  -H 'Content-Type: application/json' \
  -d '{"to_user_id":2,"content":"你好 bob"}'
```

然后查 MySQL：

```bash
mysql -uroot -p123456 spark_push \
  -e "SELECT id,session_id,msg_seq,sender_id,content_json FROM message ORDER BY id DESC LIMIT 3;"
```

> 如果 bob 没在线，这条消息会走 `persist_only`：只存档，不推送。这正好可以验证“离线消息会保存”。

### 实验 3：两个网页互相发消息

1. 打开两个浏览器窗口（或一个正常窗口 + 一个无痕窗口）。
2. 都打开 `http://localhost:9001/index.html`。
3. 一个登录 alice，一个登录 bob。
4. alice 给 bob 发消息。

然后观察：

```bash
redis-cli
> HGETALL route:user:{1}
> HGETALL route:user:{2}
> GET session:{single:1:2}:seq
```

**完成标志**：你能看到两个用户的路由，以及 seq 的增长。

---

## 9. 术语小抄

| 术语 | 一句话解释 |
|---|---|
| HTTP | 寄一封信，寄完就结束 |
| WebSocket | 一直不挂的电话，双方随时能说话 |
| gRPC | 服务之间打电话的方式 |
| Protobuf | 打电话时说的“结构化暗号”，比 JSON 更省、更快 |
| Kafka | 传送带，消息先排队，后面慢慢处理 |
| topic | 传送带的名字，比如 `push_to_comet` |
| partition | 一条传送带分成几格，同一格里的包裹有顺序 |
| consumer group | 一组取货员；不同组可以各取一份，互不抢 |
| Redis | 白板，记临时状态，读写很快 |
| MySQL | 档案室，永久保存 |
| 连接池 | 一队已经接通的电话线，用完放回去，下次接着用 |
| 线程池 | 一队工人，有活就派一个 |
| ACK | 回执：“包裹已收下” |
| DLQ | 死信：“这封包裹实在送不到”，目前只打日志 |

---

## 10. 自测 5 问

看完这份文档，试着回答：

1. 登录成功后，token 存在哪里？
2. alice 连上 WebSocket 后，Redis 里哪个 key 记录了她所在的 Comet？
3. `msg_id` 是怎么生成的？
4. Comet 给 alice 回 ACK，代表 bob 已经收到了吗？
5. bob 不在线时，消息还会推送吗？会被保存吗？

### 答案

1. Redis 的 `token:{token}`，值是 user_id，24 小时过期。
2. `route:user:{1}`，是一个 Hash，field 是 conn_id，value 是 comet_id。
3. `session_id + "-" + msg_seq`，比如 `single:1:2-15`。
4. 不代表。ACK 只代表 Logic 已经把推送任务写进 Kafka。
5. 不推送；但会写进 MySQL 的 `message` 表（`persist_only`），等客户端自己拉历史。

---

## 11. 看懂这些之后，再去看什么

当你能够独立讲清楚上面那条单聊链路，再按这个顺序扩展：

1. **聊天室**：多了“房间成员”和“按 Comet 扇出”
2. **弹幕**：多了视频 ID 和时间轴
3. **广播**：分成“全站”和“房间”两种
4. **可靠性**：Kafka 手动提交、重试、DLQ
5. **性能**：连接池、线程池、时间轮、双向流

那时候再打开 `PROJECT_GUIDE.md`，直接看：

- 第 5 章：核心链路
- 第 6 章：关键设计点
- 第 7 章：分阶段阅读顺序（其中 **7.9** 是逐文件精读清单，**7.10** 是阶段验收表）
- 第 8 章：二次开发题目
- 第 13 章：common 模块每个函数的一句话说明、结构体字段速查
- 第 14 章：Comet / Logic / Job 结构体与成员字段速查

---

## 最后一句

你不需要一次懂完。

**先跟着一条消息走一遍**，把这条线走通，剩下的房间、弹幕、广播，都是同一条路的变化。

如果有哪一步卡住了，就停在那一步，用日志、`redis-cli`、MySQL 去验证，不要急着往后翻。
