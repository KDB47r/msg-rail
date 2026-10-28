# SparkPush 源码解读与学习路线

> **如果觉得这份文档太难，请先读同目录下的 `PROJECT_GUIDE_BEGINNER.md`（小白版）。**
>
> 这份文档是“查阅手册”，不需要从头读到尾；小白版只讲一条单聊链路和第一周该做什么。

> 面向读者：正在学习 C++ / Linux 后端开发，已经能编译并跑通本项目，希望从“会跑”进阶到“看懂、能改、能讲清楚”的开发者。
>
> 文档基于当前仓库源码逐文件阅读整理，关键位置均标注 `文件:行号`，方便对照阅读。
>
> 阅读建议：第一次先看第 1～4 章建立全局地图；然后严格按第 7 章的阶段顺序读代码；第 6 章的设计解析可以在读对应源码时回头查；第 8 章用于选二次开发题目。

---

## 目录

1. [项目定位与技术栈](#1-项目定位与技术栈)
2. [目录结构与构建产物](#2-目录结构与构建产物)
3. [整体架构](#3-整体架构)
4. [数据契约：Proto / Kafka / Redis / MySQL / JSON](#4-数据契约)
5. [核心链路逐条拆解](#5-核心链路逐条拆解)
6. [关键设计点深度解析](#6-关键设计点深度解析)
7. [源码阅读顺序（重点）](#7-源码阅读顺序重点)
8. [二次开发路线](#8-二次开发路线)
9. [调试与验证工具箱](#9-调试与验证工具箱)
10. [代码中值得注意的问题与改进点](#10-代码中值得注意的问题与改进点)
11. [与 docs 目录 PDF 的对应关系](#11-与-docs-目录-pdf-的对应关系)
12. [附录：文件职责速查表](#12-附录文件职责速查表)
13. [common 模块：函数与结构体速查](#13-common-模块函数与结构体速查)
14. [业务模块结构体与成员字段速查](#14-业务模块结构体与成员字段速查)

---

## 1. 项目定位与技术栈

### 1.1 一句话理解

SparkPush 是一个**类 IM 的分布式消息推送系统**：浏览器或测试客户端通过 WebSocket 接入 Comet，Comet 把上行消息交给 Logic；Logic 负责鉴权、路由、会话序号、消息组装，然后把“推送任务”写入 Kafka；Job 异步消费 Kafka，一路把消息推回目标 Comet，一路把消息落盘 MySQL。系统支持：

- **单聊**（single）
- **聊天室**（chatroom，房间广播）
- **弹幕**（danmaku，按视频时间轴）
- **全站/房间广播**（broadcast）
- **HTTP 业务接口**：注册、登录、历史消息、未读、已读、房间管理、弹幕列表、管理后台

它的核心不是“聊天 UI”，而是**长连接管理 + 消息路由 + 异步解耦 + 可靠投递**这套后端骨架。

### 1.2 技术栈

| 层次 | 技术 | 在项目中的位置 | 学习价值 |
|---|---|---|---|
| 语言/构建 | C++17, CMake 3.14+ | 全项目 | 现代 C++、多模块构建 |
| 网络 I/O | standalone Asio 1.30.2 | `common/net`、`comet` | 异步 I/O、io_context、回调生命周期 |
| WebSocket | websocketpp 0.8.2 | `comet` | 长连接握手、帧、ping/pong |
| HTTP 解析 | llhttp 9.2.1 | `common/net/http_server_lite.cpp` | 增量 HTTP 解析、keep-alive |
| RPC | gRPC + Protobuf | `proto`、`logic`、`comet`、`job` | unary / 双向流 / 客户端流 |
| 消息队列 | Kafka 4.x（KRaft）+ librdkafka | `common/kafka_*`、`logic`、`job` | 解耦、削峰、分区顺序、消费组 |
| 缓存 | Redis + hiredis | `logic/redis_store.*` | 路由表、序号、Lua 原子操作 |
| 数据库 | MySQL 8 + mysqlclient | `logic/*_dao.*`、`job/persist_handler.*` | DAO、预处理语句、连接池、事务 |
| 日志 | spdlog | `common/logging.*` | 分级日志、轮转 |
| 序列化 | Protobuf（内部）+ nlohmann/json（边界） | 全局 | 内外协议分层 |
| 测试 | doctest（header-only） | `tests` | 可执行规格 |
| 前端 | 原生 HTML/JS | `web_demo/static` | 仅用于演示 |

> 版本被根 `CMakeLists.txt` 严格锁定（llhttp 9.2.1、Asio 1.30.2、websocketpp 0.8.2），这是课程环境预装依赖的设计，避免“每个人环境不同”。

---

## 2. 目录结构与构建产物

### 2.1 顶层目录

```text
Spark_Push/
├── CMakeLists.txt          # 顶层构建：锁定依赖版本、聚合子目录
├── cmake/                  # 自定义 CMake 查找模块
├── proto/                  # 内部 RPC/消息契约（spark_push.proto）
├── common/                 # 基础设施：配置/日志/线程池/时间轮/连接池/Kafka/HTTP
│   └── net/                # 自研极简 HTTP/1.1 服务器（asio + llhttp）
├── logic/                  # 业务核心：HTTP API、LogicService(gRPC)、DAO、Redis
├── comet/                  # 接入层：WebSocket 网关、连接表、心跳、CometService(gRPC)
├── job/                    # 异步层：Kafka 消费、推送 Comet、落盘 MySQL
├── conf/                   # logic.conf / comet.conf / job.conf
├── sql/schema.sql          # 建库建表 + 测试账号
├── scripts/                # start_all.sh / stop_all.sh / status.sh
├── tests/                  # doctest 单元测试
├── load_test/              # 压测客户端 + 使用说明
├── web_demo/               # 静态页面 + 静态文件服务器
├── docs/                   # 课程 PDF（架构、WebSocket、Kafka、gRPC、房间/弹幕）
├── build/                  # 构建产物（已被 .gitignore 忽略）
└── logs/                   # 运行日志
```

### 2.2 构建产物

| 产物 | 源入口 | 作用 |
|---|---|---|
| `build/logic/logic_server` | `logic/main.cpp` | 业务进程：HTTP 9101 + gRPC 9100 |
| `build/comet/comet_server` | `comet/main.cpp` | 接入进程：WS 9200 + gRPC 9205 |
| `build/job/job_server` | `job/main.cpp` | 异步进程：Kafka 消费 + 推送 + 落盘 |
| `build/web_demo/web_demo_server` | `web_demo/web_server.cpp` | 静态文件服务器 9001 |
| `build/tests/unit_tests` | `tests/doctest_main.cpp` | 单元测试 |
| `build/load_test/*` | `load_test/*.cpp` | 功能客户端与压测工具 |

### 2.3 规模感知（源码行数，不含第三方头文件）

| 模块 | 约行数 | 说明 |
|---|---:|---|
| `logic/` | ~5200 | 最大头，HTTP handler 已按业务域拆分 |
| `comet/` | ~1400 | WebSocket 网关 + 连接管理 |
| `job/` | ~1100 | 推送与持久化两个消费者 |
| `common/` | ~1900 | 基础设施 |
| `tests/` | ~600 | 6 个测试文件 |
| `load_test/` | ~2600 | 压测工具 |
| `proto/` | ~250 | 协议定义 |

---

## 3. 整体架构

### 3.1 进程、端口与职责

| 进程 | 端口 | 对外协议 | 核心职责 |
|---|---:|---|---|
| `logic_server` | 9100 | gRPC | 鉴权、上行消息处理、路由注册、广播 |
| `logic_server` | 9101 | HTTP | 登录/注册/发消息/历史/未读/房间/弹幕/后台 |
| `comet_server` | 9200 | WebSocket | 维持海量客户端长连接、上下行转发 |
| `comet_server` | 9205 | gRPC | 接收 Job 的下行推送（`CometService`） |
| `job_server` | — | Kafka 消费 | 推送消息到 Comet、异步落盘 MySQL |
| `web_demo_server` | 9001 | HTTP | 提供静态演示页面 |
| Kafka | 9092 | — | 消息解耦与缓冲 |
| Redis | 6379 | — | token、路由、序号、房间在线状态 |
| MySQL | 3306 | — | 用户、会话、消息、群组、弹幕 |

### 3.2 架构图

```text
                        ┌──────────────────────────────────────────────┐
                        │           浏览器 / ws_client_cli / 压测       │
                        └───────┬───────────────────────┬──────────────┘
                    HTTP 9101   │                       │  WebSocket 9200
                 (登录/历史/API) │                       │  (/ws?token=...)
                                v                       v
┌───────────────────────────────────────┐   ┌──────────────────────────────────────┐
│              Logic 业务层              │   │              Comet 接入层             │
│  logic_server                         │   │  comet_server                        │
│  ├─ HTTP API (9101)                   │   │  ├─ WebSocket 网关 (9200)             │
│  ├─ LogicService gRPC (9100)          │   │  ├─ 本地连接表 ConnRegistry            │
│  ├─ ConversationStore / DAO           │   │  ├─ 心跳扫描 (64 桶)                   │
│  ├─ RedisStore (token/route/seq/room) │   │  ├─ CometService gRPC (9205)          │
│  └─ KafkaProducer                     │   │  └─ LogicStreamClient / unary 调用    │
└───────┬───────────────────────▲───────┘   └───────▲──────────────────┬───────────┘
        │                       │                   │                  │
        │ Kafka produce         │ gRPC              │ gRPC PushToComet │ gRPC 上行
        v                       │ (上行/鉴权)        │ / PushStream     │ (发消息/下线/房间)
┌───────────────────────────────────────────────┐   │                  │
│ Kafka                                         │   │                  │
│  ├─ push_to_comet  (4 partitions)             │   │                  │
│  └─ broadcast_task (4 partitions)             │   │                  │
└───────┬───────────────────────┬───────────────┘   │                  │
        │ consume(_push)        │ consume(_persist│   │                  │
        v                       v                 │   │                  │
┌───────────────────────┐  ┌──────────────────────┴───┴──┐               │
│ PushHandler           │  │ PersistHandler              │               │
│ → gRPC 到目标 Comet    │  │ → MySQL 事务落盘             │               │
└───────────┬───────────┘  └─────────────────────────────┘               │
            └────────────────────────────────────────────────────────────┘
```

### 3.3 三条主要数据路径

**① 控制面（HTTP）**

```text
浏览器 → Logic HTTP 9101 → MySQL / Redis
```

负责登录、注册、会话列表、历史消息、已读、未读、房间管理、弹幕列表、管理后台。

**② 上行数据面（客户端发消息）**

```text
客户端 --WS--> Comet --gRPC--> Logic --Kafka--> Job
```

Comet 只做协议转换和转发，不做业务；Logic 决定 session、seq、路由、是否持久化。

**③ 下行数据面（消息推送）**

```text
Job --gRPC--> Comet --WS--> 客户端
```

Job 根据 `PushToCometRequest.comet_id` 找到目标 Comet，Comet 再按用户或房间查本地连接表并发送。

**④ 旁路：异步持久化**

`push_to_comet` 这个 topic 被两个消费组同时消费：

- `spark_push_group_push`：负责实时推送
- `spark_push_group_persist`：负责写 MySQL

两者互不影响：推送慢了不会阻塞落盘，落盘失败也不会阻塞推送（但各自都会有重试/死信日志）。

### 3.4 为什么这样拆分

| 拆分 | 解决的问题 | 代价 |
|---|---|---|
| Comet 独立 | 长连接是有状态、占 fd/内存的；业务逻辑可以独立扩容 | 需要路由表告诉 Logic “用户在哪个 Comet” |
| Logic 独立 | 业务规则集中，HTTP 和 gRPC 复用同一套存储/路由 | Logic 成为关键路径 |
| Kafka 解耦 | 峰值削峰、上下游故障隔离、多消费组复用同一条消息 | 增加端到端延迟、需要处理重复/顺序 |
| Job 独立 | 推送和落盘可独立扩缩容、独立重试 | 运维复杂度上升 |
| Redis 路由 | 跨 Comet 的用户定位 | 路由一致性、故障清理 |

一句话：**Comet 管连接，Logic 管业务，Kafka 管解耦，Job 管异步执行，Redis 管路由，MySQL 管持久化。**

### 3.5 进程内线程模型（读并发代码前先建立这张图）

| 进程 | 线程 | 做什么 |
|---|---|---|
| Logic | 主线程 | 运行 HTTP `io_context`（`http_threads` 个线程共同 run） |
| Logic | gRPC 线程池 | 处理 `LogicService` 的 unary / stream 请求 |
| Logic | librdkafka 内部线程 | 实际网络发送、重试 |
| Comet | asio IO 线程（`comet_io_threads`） | 所有 WebSocket 回调、连接读写 |
| Comet | `grpc_pool_` 线程池 | 执行 `UserOffline`/`ReportRoomJoin`/`ReportRoomLeave`/`SendUpstreamMessage`（非流模式）等异步 gRPC 调用 |
| Comet | 心跳线程 | 每 500ms 扫描一个桶（64 桶轮转） |
| Comet | gRPC server 线程池 | 处理 Job 的 `PushToComet` / `PushStream` |
| Comet | 每条 Logic 双向流的 reader/writer 线程 | `LogicStreamClient` 的收发 |
| Job | Kafka 消费线程 × 3 | push、broadcast、persist 各一个 |
| Job | 工作线程池 × N | 每个 consumer 内部按 partition 串行调度任务 |
| Job | librdkafka 内部线程 | 网络与心跳 |

> 重要：WebSocket 回调运行在 asio IO 线程里，**不能在里面做长时间阻塞操作**。Comet 把 gRPC 调用丢给 `grpc_pool_` 正是为了避免阻塞 IO 线程。但 `OnValidate` 里的 `VerifyToken` 是同步阻塞调用，是一个可以改进的点（见第 10 章）。

---

## 4. 数据契约

这一章是“看代码前的地图”。先记住这些 key、topic、表名，读代码时就不会迷路。

### 4.1 Protobuf：内部强类型契约

文件：`proto/spark_push.proto`

**枚举**

- `Scene`：`SCENE_SINGLE / SCENE_CHATROOM / SCENE_DANMAKU`
- `MsgType`：`TEXT / SYSTEM / DANMAKU / BROADCAST`
- `BroadcastScope`：`ALL / GROUP`

**核心消息**

| 消息 | 用途 |
|---|---|
| `ChatMessage` | 统一消息模型：msg_id、session_id、msg_seq、sender_id、timestamp、type、content_json、client_msg_id |
| `UpstreamMessageRequest` | Comet → Logic 的上行请求 |
| `PushToCometRequest` | Logic/Job → Comet 的下行推送任务，同时携带持久化所需字段 |
| `StreamMessage` / `StreamResponse` | Comet ↔ Logic 双向流包装，`oneof payload` + `request_id` |
| `BroadcastTaskRequest` | Logic → Kafka 的全站广播任务 |

**服务**

| 服务 | 调用方向 | 方法 |
|---|---|---|
| `LogicService` | Comet → Logic | `VerifyToken`、`SendUpstreamMessage`、`UserOffline`、`ReportRoomJoin/Leave`、`Broadcast`、`MessageStream`(bidi) |
| `CometService` | Job → Comet | `PushToComet`(unary)、`PushStream`(client stream) |
| `JobService` | 预留 | 当前主链路走 Kafka，未实际使用 |

> 设计要点：**JSON 只存在于系统边界**（浏览器 WebSocket、HTTP、MySQL 的 `content_json` 列），内部跨进程一律 Protobuf。两者的转换集中在 `proto/proto_enums.h` 和 `comet/websocket_utils.cpp`。

### 4.2 Kafka

| Topic | 生产者 | 消费组 | Key 设计 | 目的 |
|---|---|---|---|---|
| `push_to_comet` | Logic | `spark_push_group_push` / `spark_push_group_persist` | 在线：`session_id\|comet_id`；离线：`session_id`；房间广播：`broadcast\|room_id\|comet_id` | 同一会话到同一 Comet 落同一分区，保证顺序 |
| `broadcast_task` | Logic | `spark_push_group` | `task_id` | 全站广播任务 |

- 分区数：`scripts/start_all.sh` 创建时指定 **4 个分区**（`start_all.sh:117`）。
- Producer 配置（`common/kafka_producer.cpp:39-51`）：`acks=1`、关闭幂等、不压缩、`linger.ms=0`、单条批量。作者明确选择**低延迟优先于吞吐**。
- 消费语义：手动提交 offset，回调执行完成后才提交，属于 **at-least-once**（可能重复，不会丢）。

### 4.3 Redis Key 设计

定义集中在 `logic/redis_store.h:15-25`。

| Key | 类型 | 含义 | TTL |
|---|---|---|---|
| `token:{<token>}` | String | token → user_id | 24h（SETEX） |
| `route:user:{<uid>}` | Hash | conn_id → comet_id，多设备/多标签页 | 无 |
| `session:{<sid>}:seq` | String | 会话最新消息序号 | 无 |
| `session:{<sid>}:seq_initialized` | String | 序号是否已从 MySQL 恢复过的标记 | 无 |
| `read_seq:{<sid>}:uid:<uid>` | String | 用户在该会话的已读序号 | 无 |
| `room:{<rid>}:comets` | Set | 该房间用户分布在哪些 Comet | 无 |
| `room:{<rid>}:comet_count:<cid>` | String | 该 Comet 上该房间的用户数 | 无 |
| `room:{<rid>}:online_count` | String | 房间在线人数 | 无 |
| `rate:msg:{<uid>}` | ZSet | 滑动窗口限流（代码保留但热路径已注释） | 窗口 + 1s |

**hash tag**：`{...}` 让同一维度的 key 落在 Redis Cluster 同一 slot，从而允许 `MGET` / `EVAL`。这是面试常问点，代码里 `GetUnreadSeqs`（`redis_store.cpp:281`）和房间 Lua 都用到了。

### 4.4 MySQL 表

文件：`sql/schema.sql`

| 表 | 作用 | 关键索引 |
|---|---|---|
| `user` | 账号、昵称、密码哈希 | `uk_account` |
| `session` | 会话元数据（单聊/房间），`last_msg_seq` | `uk_session_id` |
| `message` | 消息正文，`content_json` 存 JSON 文本 | `uk_session_seq(session_id,msg_seq)` |
| `user_session_state` | 每个用户在每个会话的已读游标 | `uk_user_session` |
| `im_group` | 群/聊天室/弹幕房，`group_type` 区分 | `idx_type_id` |
| `group_member` | 群成员关系 | `uk_group_user` |
| `video_danmaku` | 弹幕（按 video_id + timeline_ms 查询） | `idx_video_timeline` |
| `broadcast` | 广播任务记录（当前代码未强依赖） | `uk_task_id` |

测试账号：alice/bob/carol/dave，密码均为 `123456`，但库里存的是 **MD5**：`e10adc3949ba59abbe56e057f20f883e`。登录时前端先做 MD5，后端直接字符串比较。

### 4.5 WebSocket JSON 协议

**上行（客户端 → Comet）**

| type | 必填字段 | 说明 |
|---|---|---|
| `single_chat` | `to_user_id`, `content` | 单聊 |
| `chatroom` / `chatroom_msg` | `group_id` 或 `room_id` | 聊天室消息 |
| `danmaku` | `group_id`, `video_id`, `timeline_ms` | 弹幕 |
| `chatroom_join` | `group_id` | 加入房间（只更新本机连接表 + Redis 路由） |
| `chatroom_leave` | `group_id` | 离开房间 |

示例：

```json
{"type":"single_chat","to_user_id":2,"content":{"text":"hello"},"client_msg_id":"c-1"}
```

**下行（Comet → 客户端）**

- ACK：`{"type":"ack","client_msg_id":"c-1","msg_id":"single:1:2-15"}`
- 错误：`{"type":"error","message":"..."}`
- 业务消息：直接把 Logic 组装好的 `content_json` 推给客户端（其中包含 `type`、`msg_id`、`from_user_id`、`content` 等）

> 注意：HTTP 发消息路径构造的 `content` 是字符串（`http_handlers_message.cpp:112`），WebSocket 路径则原样保留客户端 JSON（`content` 通常是对象）。前端 `index.html` 对两种结构都做了兼容。这是历史演进留下的不一致，二次开发时可以统一。

### 4.6 HTTP API 一览

所有业务接口都只接受 **POST**，返回统一结构：

```json
{"code":0,"message":"ok","data":{...}}
```

| 路径 | 鉴权 | 作用 |
|---|---|---|
| `/api/login` | 无 | 登录，返回 token |
| `/api/register` | 无 | 注册并直接返回 token |
| `/api/message/send` | Bearer token | HTTP 直接发单聊消息（绕过 WebSocket） |
| `/api/session/history` | Bearer + 会话归属 | 拉历史消息 |
| `/api/session/mark_read` | Bearer + 会话归属 | 标记已读 |
| `/api/session/unread` | Bearer + 会话归属 | 查未读数 |
| `/api/session/list_single` | Bearer | 单聊会话列表 |
| `/api/chatroom/list` | Bearer | 我加入的聊天室 |
| `/api/chatroom/join` | Bearer | 加入聊天室（写 `group_member`） |
| `/api/chatroom/leave` | Bearer | 空操作（保留语义） |
| `/api/chatroom/unsubscribe` | Bearer | 取消订阅（删成员） |
| `/api/chatroom/online_count` | Bearer | 在线人数（Redis 优先，MySQL 兜底） |
| `/api/danmaku/send` | Bearer | HTTP 发弹幕（同步写库 + Kafka 推送） |
| `/api/danmaku/list` | Bearer | 按视频/时间范围拉弹幕 |
| `/api/admin/chatroom/create` | Bearer + admin | 创建聊天室 |
| `/api/admin/chatroom/list` | Bearer + admin | 列出聊天室 |
| `/api/admin/broadcast` | Bearer + admin | 全站/房间广播 |
| `/health` | 无 | 健康检查 |
| `/metrics` | 无 | 极简 Prometheus 文本 |

鉴权实现：`logic/http_server.cpp:306` 的 `AuthenticateRequest`，优先 `Authorization: Bearer <token>`，降级读 JSON body 的 `token` 字段。
越权防护：`logic/http_server.cpp:355` 的 `CheckSessionAccess`，单聊校验双方 user_id、房间查成员表、弹幕会话公开。

---

## 5. 核心链路逐条拆解

> 建议：每读一条链路，自己在纸上画一次时序图。下面每条都给出可以直接对照的源码位置。

### 5.1 启动顺序

`scripts/start_all.sh` 的顺序：

```text
检查编译产物
  → 检查 MySQL（只检查端口，不启动）
  → 启动 Redis
  → 启动 Kafka（KRaft，首次自动 format）
  → 创建 topic（4 分区）
  → 启动 logic_server，等 9100/9101
  → 启动 comet_server，等 9200/9205
  → 启动 job_server
  → 启动 web_demo_server 9001
```

三个服务都从**项目根目录**启动，因为日志目录 `logs/` 和配置文件路径都是相对路径。手动启动时务必 `cd` 到项目根目录。

`logic/main.cpp:70` 的 `RunLogic` 做了这些事：

1. 初始化 Kafka producer（push 和 broadcast 两个）
2. 初始化 MySQL 连接池 + 各 DAO
3. 初始化 Redis 连接池 + `RedisStore`
4. 组装 `ConversationStore`
5. 启动 gRPC `LogicService`（9100）
6. 启动 HTTP 服务器（9101）
7. 注册信号处理，gRPC 在独立线程 `Wait()`，主线程跑 HTTP

### 5.2 登录 + WebSocket 鉴权 + 路由注册

这是理解整个系统的第一把钥匙。

```text
① 客户端 POST /api/login
   Logic handleLogin (http_handlers_auth.cpp:14)
   → 查 user 表（或 admin 内存校验）
   → 比较 MD5 密码
   → GenerateSecureToken() 生成 48 位 hex
   → Redis SETEX token:{token} 86400 user_id
   → 返回 {user_id, token}

② 客户端连接 ws://comet:9200/ws?token=xxx
   Comet OnValidate (comet_server.cpp:116)
   → 从 resource 提取 token
   → 分配 conn_id = comet_id:自增序号
   → 同步调用 Logic.VerifyToken

③ Logic VerifyToken (grpc_service.cpp:45)
   → Redis GET token:{token} 得到 uid
   → AddRoute(uid, comet_id, conn_id)
     即 Redis HSET route:user:{uid} conn_id comet_id
   → 返回 user_id

④ Comet OnOpen (comet_server.cpp:156)
   → registry_.AddConn(uid, hdl)   # 本机内存连接表
   → AddToHeartbeatBucket(ctx)     # 心跳分桶
```

之后该用户在 Redis 里就有了一条“用户在哪个 Comet 的哪条连接上”的记录。Logic 发消息时靠它定位目标 Comet。

**为什么要 conn_id 粒度？**
同一用户可能多设备、多标签页在线。`route:user:{uid}` 是 Hash（conn_id → comet_id），一个用户可对应多条连接；`GetUserRoutes` 用 `HVALS` 取出去重后的 comet 列表。

**本地缓存**
`RedisStore::GetUserRoutes`（`redis_store.cpp:146`）有 1 秒 TTL 的进程内缓存，读多写少，降低 Redis 压力；`AddRoute/RemoveRoute` 会主动失效缓存。

### 5.3 单聊上行完整链路

以客户端发 `{"type":"single_chat","to_user_id":2,...}` 为例。

```text
客户端
  │ WS 文本帧
  v
Comet OnMessage (comet_server.cpp:180)
  → HandleTextMessage (comet_server.cpp:370)
  → ParseUpstreamMessage (websocket_utils.cpp:55) 解析 type/to_user_id/group_id/...
  → 构造 UpstreamMessageRequest
  → 二选一：
     A. use_grpc_stream=true  → stream_client_->Send(...)
     B. use_grpc_stream=false → grpc_pool_.Submit(unary stub->SendUpstreamMessage)
  │
  v
Logic SendUpstreamMessage (grpc_service.cpp:66)
  → HandleUpstreamMessage (grpc_service.cpp:224)
    ① 计算 session_id = "single:<min>:<max>"
    ② GetUserRoutes(to_user) → 目标 comet 列表
    ③ AllocateMessageSeq(session_id) → Redis Lua INCR
    ④ msg_id = session_id + "-" + seq
    ⑤ 解析 content_json，注入 msg_id/create_time/from_user_id
    ⑥ 对每个 comet 构造 PushToCometRequest
       - targets = [to_user]
       - scene / to_user_id / room_id
       - need_persist = true
       → Kafka send(key = session_id + "|" + comet_id)
    ⑦ 若目标不在线（无路由）：
       - persist_only = true
       - Kafka send(key = session_id)
  → 返回 UpstreamMessageReply（含 ChatMessage）
  │
  v
Comet 收到 gRPC 响应
  → 给客户端回 ACK：{"type":"ack","client_msg_id":..., "msg_id":...}
```

关键代码位置：

- session_id 规则：`grpc_service.cpp:257-263`
- 路由查询：`grpc_service.cpp:266-271`
- 序号分配：`grpc_service.cpp:314-325`
- 消息组装与 enrichment：`grpc_service.cpp:327-363`
- Kafka 扇出：`grpc_service.cpp:369-395`
- 离线仅持久化：`grpc_service.cpp:399-417`
- Comet ACK：`comet_server.cpp:459-497`

**注意 ACK 的时机**：ACK 表示 Logic 已经把推送任务**写进 Kafka**，不代表 Job 已经推到对端、更不代表已落库。真正的端到端完成需要 Job 消费成功。这是一个“异步确认”模型，学习时要区分清楚。

### 5.4 Kafka 消费：推送与持久化

Job 里有两个 Processor，各自持有独立的 `KafkaConsumer`：

**PushHandler**（`job/push_handler.cpp`）

- push 消费组：`spark_push_group_push`（`push_handler.cpp:32`）
- broadcast 消费组：`spark_push_group`（`push_handler.cpp:47`）
- `HandleMessage`（`:263`）：
  - `persist_only=true` 直接跳过推送
  - 流模式 → `SendToStream` 入队
  - unary 模式 → `ProcessPushRequest`
- `ProcessPushRequest`（`:294`）：
  - 懒加载 `CometService::Stub`
  - 每次 RPC 3 秒 deadline
  - 最多重试 3 次，退避 100ms → 400ms → 1600ms
  - 全部失败打 `[DLQ][Push]` 日志（死信只落日志，没有补偿队列）

**PersistHandler**（`job/persist_handler.cpp`）

- persist 消费组：`spark_push_group_persist`（`persist_handler.cpp:128`）
- `HandleMessage`（`:160`）：只处理 `need_persist=true`
- `ExecutePersist`（`:175`）在一个事务里：
  1. 单聊：upsert `session`（`last_msg_seq=GREATEST(...)`）
  2. 聊天室：upsert `session`（type=2）
  3. `INSERT message ... ON DUPLICATE KEY UPDATE`（幂等）
  4. commit
- `PersistMessage`（`:275`）：同样 3 次重试 + `[DLQ][Persist]`

**KafkaConsumer 的顺序与背压**（`common/kafka_consumer.cpp`）

- 每个 partition 维护一个 `pending` 队列，**同一 partition 严格串行**，不同 partition 并行（`:126-145`）
- 工作线程数 `job_worker_threads`，最大在途 `job_max_inflight`
- 达到上限时 pause 所有 partition，只 consume 心跳（`:86-93`）
- 任务完成后由消费线程统一 `commitAsync(offset+1)`（`:167-200`）

这保证了：**同一 `session_id|comet_id` 的消息有序**，同时不同会话可以并行。

### 5.5 下行：Job → Comet → 客户端

```text
Job PushHandler
  → CometService::Stub::PushToComet(unary)
  │   或 PushStream(client stream)
  v
Comet CometServiceImpl::PushToComet (comet_grpc_service.cpp:50)
  → ProcessPushRequest (comet_grpc_service.cpp:11)
     ① targets 非空 → PushToUsers(msg, user_ids)
     ② room_id > 0  → PushToRoom(msg, room_id)
     ③ 兜底：按 session_id 前缀解析 room:/danmaku:，或 broadcast → PushToAll
  → CometServer::PushToUsers (comet_server.cpp:506)
     → registry_.ConnsForUsers(user_ids)  # 按用户分片查询
     → ws_.send(hdl, payload)
```

消息体直接使用 `msg.content_json()`；如果为空则退化为 `{"msg_id":"..."}`（`comet_server.cpp:500`）。

### 5.6 聊天室链路

聊天室比单聊多两层状态：**本地连接表**和**Redis 房间路由**。

**加入房间**

```text
客户端 HTTP POST /api/chatroom/join
  → 校验房间存在且 group_type=1
  → INSERT IGNORE group_member（持久化成员关系）

客户端 WS 发送 {"type":"chatroom_join","group_id":1001}
  → Comet AddUserToRoom
     → registry_.JoinRoom(room_id, user_id)   # 本机内存
     → NotifyRoomJoin(room_id, user_id)       # 异步 RPC 到 Logic
  → Logic ReportRoomJoin (grpc_service.cpp:443)
     → Redis INCR room:{rid}:online_count
     → EVAL RoomJoinAtomic:
         INCRBY room:{rid}:comet_count:{cid} +1
         从 0→1 时 SADD room:{rid}:comets {cid}
  → Comet 回 {"type":"ack","op":"chatroom_join"}
```

**发房间消息**

```text
Comet → Logic SendUpstreamMessage(scene=CHATROOM, group_id=...)
  → session_id = "room:<rid>"
  → GetRoomComets(rid) 得到所有相关 Comet
  → 对每个 Comet：
      PushToCometRequest { comet_id, message, room_id=rid, targets=[] }
      Kafka key = session_id|comet_id
  → Job → Comet PushToComet
  → Comet ProcessPushRequest 看到 room_id > 0
  → PushToRoom → registry_.ConnsInRoom(rid) → 本地所有房间成员连接
```

**关键理解：房间扇出是“两层”的。**

- Logic 层按 **Comet 维度**扇出（同一个 Comet 只发一条，而不是给每个用户发一条）
- Comet 层再按 **本地成员**扇出

这样跨 Comet 的流量从 `用户数` 降为 `Comet 数`，是可扩展性的关键。

### 5.7 弹幕链路

弹幕有两条入口，行为不完全一样：

**入口 A：HTTP `/api/danmaku/send`**（`http_handlers_danmaku.cpp:20`）

```text
鉴权 → 解析 video_id/timeline_ms/text
  → DanmakuDao::InsertDanmaku 写 video_danmaku 表
  → AppendMessage：Redis 分配 seq + 写 message 表
  → GetRoomComets(1001)（固定房间）或退化为成员路由
  → Kafka 推送（targets 可能为空，room_id 未设置！）
```

注意：HTTP 路径里 `PushToCometRequest` **没有设置 `room_id`**（`http_handlers_danmaku.cpp:167-173`）。当 `room_comets` 非空时，`targets` 也是空的，Comet 只能走 `session_id` 前缀兜底：`danmaku:<video_id>` 会被 `ProcessPushRequest` 用 `std::stoll` 解析成 room_id——**如果 video_id 是数字，就会被当成房间号推给错误房间；不是数字则直接丢弃**。这是一个值得在二次开发中修正的真实缺口。

**入口 B：WebSocket `{"type":"danmaku",...}`**

```text
Comet → Logic SendUpstreamMessage(scene=DANMAKU)
  → session_id = "danmaku:<video_id>"
  → GetRoomComets(group_id)  # 注意这里 group_id 是房间
  → 每个 Comet 一条 PushToCometRequest{room_id=group_id}
  → Job → Comet PushToRoom(group_id)
  → video_danmaku 表不会被写（PersistHandler 只写 message/session）
```

所以：**WS 发的弹幕能实时看到，但 `/api/danmaku/list` 拉不到**（它读 `video_danmaku`）；HTTP 发的弹幕能落 `video_danmaku`，但实时推送路径不完整。这是一个非常适合作为二次开发第一题的“真实缺口”。

### 5.8 全站广播 / 房间广播

**全站广播**

```text
POST /api/admin/broadcast {scope:"all", text:"..."}
  → 构造 content_json
  → BroadcastTaskRequest → Kafka broadcast_task
  → Job PushHandler::HandleBroadcastTask (push_handler.cpp:333)
  → 遍历配置中的所有 comet_targets
  → 每个 Comet 一条 unary PushToComet（无 targets、无 room_id）
  → Comet ProcessPushRequest 兜底：session_id=="broadcast" → PushToAll
```

**房间广播**

```text
scope="chatroom"（HTTP）或 gRPC Broadcast(scope=GROUP)
  → GetRoomComets(room_id)
  → 每个 Comet 一条 PushToCometRequest{room_id=...}
  → 走 push_to_comet（不是 broadcast_task）
  → Comet PushToRoom
```

为什么房间广播不走 `broadcast_task`？因为它需要精确的 Comet 列表，而全站广播只需要遍历配置。这也是代码注释里强调的“真房间扇出”。

### 5.9 心跳与断线清理

**心跳**（`comet_server.cpp:218-304`）

- 64 个桶（`kHeartbeatBucketCount`），连接按 `conn_id % 64` 分桶
- 心跳线程每 500ms 扫描**一个**桶，所以一条连接约每 32 秒被检查一次
- 检查时：`now - last_pong > 60s` 则关闭，否则发 ping
- 客户端 WebSocket 库自动回 pong，`OnPong` 更新 `last_pong_ms`

**断线**

```text
OnClose (comet_server.cpp:165)
  → registry_.RemoveConn
      - 删除 hdl → uid
      - 删除用户连接集合中的该连接
      - 若是该用户最后一条连接：清理所有房间成员关系并返回 left_rooms
  → NotifyUserOffline(uid, conn_id)
      → Logic UserOffline → Redis HDEL route:user:{uid} conn_id
  → 对每个 left_room：NotifyRoomLeave
      → Logic ReportRoomLeave → Redis 在线人数 -1 + Lua 条件 SREM
```

### 5.10 优雅关闭

三个进程都注册了 `SIGTERM/SIGINT`（`common/signal_handler.h`）：

- Logic：停 HTTP → 停 gRPC → 停 MySQL 池
- Comet：停心跳 → 停监听 → 关闭所有连接 → 停 io → 停流客户端
- Job：等信号 → 停两个 handler → 停 consumer → 停线程池 → 关 Kafka/MySQL

`ThreadPool::Stop` 会先把队列里剩余任务执行完再退出（`thread_pool.cpp:36-54`），`KafkaConsumer::Stop` 会等在途任务排空（`kafka_consumer.cpp:65-75`）。这是“优雅”的关键。

---

## 6. 关键设计点深度解析

这一章建议在读到对应代码时再回来看。每个点都给出“是什么、为什么、在哪、你可以怎么验证”。

### 6.1 Kafka 分区键 = 顺序保证

**做法**：在线推送的 key 是 `session_id + "|" + comet_id`。

**为什么**：Kafka 只保证**同一分区内**有序。把同一会话、同一目标 Comet 的消息放到同一 key，就能保证同一个会话的推送顺序；不同会话可以分散到不同分区并行。

**注意**：`persist_only` 消息用 `session_id` 做 key，和在线消息的 `session_id|comet_id` 不在同一分区。对“离线消息”来说，只有持久化语义，不要求和在线消息严格交错，因此可以接受。

**验证**：用 `kafka-console-consumer --property print.key=true` 观察 key；或改 key 为随机值，再看客户端消息顺序是否乱。

### 6.2 at-least-once 与手动提交

`KafkaConsumer` 关闭自动提交（`enable.auto.commit=false`），并在**回调执行完成后**提交 `offset+1`（`kafka_consumer.cpp:182-192`）。

- 进程崩溃 → 未提交的 offset 会被重新消费 → 消息可能重复
- 因此 `message` 表用 `UNIQUE(session_id,msg_seq)` + `ON DUPLICATE KEY UPDATE` 做幂等
- 推送侧没有去重，客户端可能收到重复消息；需要“恰好一次”时要自己做 msg_id 去重

**思考题**：如果 Kafka 提交成功但进程在提交后、业务完成前崩溃，会发生什么？如果业务完成但提交前崩溃，又会发生什么？分别对应 at-least-once 的哪一侧？

### 6.3 会话序号分配：Redis INCR + MySQL 兜底

这是本项目最值得学习的几个设计之一。

问题：消息需要 `session_id` 内严格递增的 `msg_seq`，且 Redis 可能重启/丢数据。

方案（`conversation_store.cpp:76-103` + `redis_store.cpp:449-504`）：

1. 正常路径：Lua 脚本检查 `session:{sid}:seq_initialized` 是否存在
   - 存在 → 直接 `INCR session:{sid}:seq`，一次 RTT
   - 不存在 → 返回 -1，表示需要初始化
2. 初始化路径：
   - 从 MySQL 查 `MAX(msg_seq)` 作为安全下限
   - Lua 原子地：若未初始化，把计数器提升到 `max(current, persisted_seq)`，写入初始化标记，再 `INCR`
   - 并发首次发送时只有一个调用真正初始化，其余直接 INCR

优点：

- 热路径一次 Redis RTT
- Redis 丢失后不会从 1 重新开始，避免与历史消息冲突
- 初始化是原子的，避免并发重复分配

**思考题**：Redis 主从切换丢失最近写入的 seq 怎么办？高并发下单个 key 的 INCR 上限在哪里？如果让你改成分段号段（每次取 1000 个号），需要改哪些函数？

### 6.4 Redis Lua 与 hash tag

项目里有三段 Lua：

| 脚本 | 位置 | 作用 |
|---|---|---|
| 房间上下线 | `redis_store.cpp:16-25` | `INCRBY` + 条件 `SADD/SREM`，消除并发竞态 |
| 序号分配/初始化 | `redis_store.cpp:32-43` | 条件 INCR + 下限提升 |
| 滑动窗口限流 | `redis_store.cpp:47-56` | ZSet 先判后写 |

**hash tag**：`room:{<rid>}:...`、`session:{<sid>}:...`、`read_seq:{<sid>}:...` 都带 `{...}`，保证同一逻辑实体的 key 在同一 slot，Cluster 下才能用 `EVAL`/`MGET`。

**学习点**：Lua 在 Redis 中是单线程原子执行的，适合“读-判断-写”组合。但脚本不要写太长，否则会阻塞其他命令。

### 6.5 连接池：RAII + 自动扩容 + 空闲回收

`common/mysql_pool.*` 和 `common/redis_pool.*` 结构几乎对称：

- `Acquire(timeout_ms)` 从 idle 队列取连接；没有则按需创建（不超过 max）
- 返回 `MySqlConnGuard` / `RedisConnGuard`，析构自动归还（move-only，防止重复归还）
- 空闲超过 `idle_timeout_ms` 且总数大于 min 时回收
- Redis 热路径跳过 PING；MySQL 可选 `ping_on_borrow`
- 坏连接标记 `MarkBroken()` 后销毁，不污染池

**学习点**：这是 C++ RAII 的经典应用。读的时候重点看 Guard 的移动构造/移动赋值/析构，以及 `Release` 里对 `stopping_` 和坏连接的处理。

### 6.6 线程池

`common/thread_pool.*`：固定大小、`mutex + condition_variable + queue`。

- `Start()` 懒启动
- `Submit()` 在 stopping 时拒绝并打日志（不静默丢任务）
- worker 捕获异常，避免一个任务炸掉整个线程
- `Stop()` 先置 stopping，再 join；worker 会把队列里剩余任务执行完

用途：Comet 的 gRPC 调用池、Kafka 消费的工作池。

### 6.7 时间轮与 LocalTimer

`common/timer_wheel.*` 是一个**分层时间轮**：

- 近端 256 个槽（8 位），4 层、每层 64 个槽（6 位）
- tick 默认 10ms
- `add_timer(delay)` 返回 `shared_ptr<TimerState>`，到期置 `ready`
- `LocalTimer` 封装成“单次定时器”，由调用线程轮询 `execute_if_ready()` 执行回调

项目里主要给 Comet 心跳线程用（`comet_server.cpp:274-294`）。`tests/test_timer_wheel.cpp` 是最好的入门材料——先看测试，再看实现。

**对初学者**：这个文件算法性最强，第一遍不必逐行抠，知道“它能在 O(1) 左右插入定时器、按 tick 推进”即可。想深入可以画一张 8+6+6+6+6 位的分层图。

### 6.8 Comet 连接表的分片设计

`comet/conn_registry.*`：

- 用户表按 `user_id` 哈希分 64 个 shard
- 房间表按 `room_id` 哈希分 64 个 shard
- 另有全局 `hdl → uid` 映射（`handle_mutex_` 保护）
- `ConnsForUsers` 先把 user 按 shard 分组，再逐 shard 加锁，减少锁竞争
- `RemoveConn` 在用户最后一条连接断开时，持用户分片锁清理所有房间，避免重连竞态

**学习点**：分片锁的思想——不是一把大锁保护所有连接，而是按 key 分散到多个互斥锁。读的时候注意锁顺序（`AddConn` 同时锁 `handle_mutex_` 和 shard mutex，用 `std::scoped_lock` 避免死锁）。

### 6.9 gRPC 双向流与客户端流

**Comet → Logic：双向流**（`comet/logic_stream_client.*`）

- 启动 N 条 `MessageStream`（`grpc_stream_count`）
- 每条流一个 writer 线程 + 一个 reader 线程
- 每个请求分配 `request_id`，`req_id % N` 轮询选流
- writer 把请求写入流；reader 按 `request_id` 找到回调并执行
- `Stop()` 用 `WritesDone + TryCancel` 唤醒阻塞的 `Read()`

**Job → Comet：客户端流**（`job/push_handler.cpp:65-204`）

- 每个 Comet 一条 `PushStream`
- 每个 Comet 一个发送队列 + writer 线程
- 写失败 → 指数退避重连（500ms → 8s），重连后重试当前消息
- 流不存在时回退到 unary

**收益与风险**：

- 收益：减少连接建立/销毁开销，吞吐更高（代码注释称 16K → 33K QPS，需自己复测）
- 风险：流断开需要重连；`PushStream` 没有应用层 ACK，消息可能丢；队列无界可能 OOM

### 6.10 背压

- Kafka 消费侧：`job_max_inflight` 达到上限就 pause partition
- 连接池：`Acquire(timeout)` 阻塞等待
- 线程池：任务队列无界（这是一个隐患，高负载下可能堆积）
- PushHandler 流模式：发送队列无界（另一个隐患）

**学习点**：背压的本质是“上游生产速度 > 下游消费速度时，让上游停下来或拒绝”。项目在 Kafka 层做了，但在线程池/流队列层没做，可以作为改进题。

---

## 7. 源码阅读顺序（重点）

> 原则：**先骨架后血肉，先数据流后并发细节，先能跑通再抠算法。**
>
> 每一阶段都给出：目标 → 按顺序读的文件 → 必须回答的问题 → 动手实验。完成实验再进入下一阶段。
>
> 如果你觉得下面的阶段目标还不够具体，直接跳到 **7.9 逐文件精读清单（扩展版）**：那里按“打开哪个文件、先看哪个函数、第一遍可以跳过什么”逐条列出。读完后用 **7.10 阶段验收表** 自测。

### 阶段 0：把它跑起来并“看见”它（0.5～1 天）

**目标**：建立感性认识，知道每个端口对应什么。

**动作**：

```bash
cd /home/wdh/Linux/Spark_Push
./scripts/start_all.sh
./scripts/status.sh
```

打开 `http://localhost:9001/index.html`，用 alice / 123456 登录，给 bob 发消息；再开一个窗口用 bob 登录，观察接收。

**观察**：

```bash
tail -f logs/logic.log logs/comet.log logs/job.log
redis-cli
> KEYS *
> HGETALL route:user:{1}
> GET session:{single:1:2}:seq
```

**必须回答**：

1. 一条消息从 alice 到 bob，经过了哪些进程？
2. `route:user:{1}` 里的 field 和 value 分别是什么？
3. 为什么 Kafka 里有两个消费组？

### 阶段 1：基础设施 common（2～3 天）

**目标**：掌握项目所有“轮子”，后面读业务代码不再被基础设施打断。

**顺序**：

1. `common/config.h` / `config.cpp`
   - 重点：`LoadConfig` 的容错、默认值推导（`http_port = listen_port+1`、`comet_grpc_port = listen_port+100`）
   - 实验：改一个非法整数，看日志回退
2. `common/logging.h` / `logging.cpp`
   - 重点：级别映射、`ShouldLog` 热路径短路、轮转
   - 实验：把 `conf/logic.conf` 的 `log_level` 改成 `DEBUG`，重启看日志量
3. `common/thread_pool.*`
   - 重点：生产者-消费者、Stop 语义、异常捕获
   - 实验：读 `tests/test_thread_pool.cpp`，跑 `./build/tests/unit_tests`
4. `common/mysql_pool.*`、`common/redis_pool.*`
   - 重点：RAII Guard、自动扩容、空闲回收、坏连接处理
   - 实验：把 MySQL 停掉，看 Logic 启动失败日志；再启动恢复
5. `common/kafka_producer.*`、`common/kafka_consumer.*`
   - 重点：手动提交、partition 串行、inflight 背压
   - 这是全项目最值得精读的并发文件之一
6. `common/net/http_server_lite.*`
   - 重点：asio accept/read/write 链、llhttp 回调、keep-alive
   - 不必背 API，理解“一个连接一个 Session，shared_ptr 保活”即可
7. `common/timer_wheel.*` + `local_timer.*`
   - 先看测试，再看实现；第一遍允许“知其然”

**必须回答**：

- 连接池的 Guard 为什么必须 move-only？
- KafkaConsumer 如何保证同一 partition 串行？
- `ThreadPool::Stop()` 返回时，队列里的任务一定执行完了吗？
- HTTP 服务器如何避免回调执行时对象被销毁？

### 阶段 2：协议与数据模型（0.5～1 天）

**顺序**：

1. `proto/spark_push.proto`（逐条读注释）
2. `sql/schema.sql`（对照表结构）
3. `logic/model.h`
4. `proto/proto_enums.h`
5. `comet/websocket_utils.*` + `tests/test_websocket.cpp`

**必须回答**：

- 为什么内部用 Protobuf，边界用 JSON？
- `PushToCometRequest` 里哪些字段是“推送”用的，哪些是“持久化”用的？
- `persist_only` 和 `need_persist` 的区别是什么？
- `client_msg_id` 和 `msg_id` 分别由谁生成、用在哪里？

### 阶段 3：Logic 的 HTTP 与存储（3～4 天）

**目标**：先走“同步、好调试”的 HTTP 路径，建立对 DAO/Redis/MySQL 的直觉。

**顺序**：

1. `logic/main.cpp`（看组装关系，不求细节）
2. `logic/http_server.h` / `http_server.cpp`
   - 路由表 `:215`
   - `AuthenticateRequest` `:306`
   - `CheckSessionAccess` `:355`
3. `logic/http_handlers_auth.cpp`（登录/注册）
4. `logic/http_handlers_message.cpp`（发消息/历史/已读/未读/会话列表）
5. `logic/conversation_store.*`（业务聚合层）
6. `logic/user_dao.*`、`session_dao.*`、`message_dao.*`、`user_session_state_dao.*`
7. `logic/redis_store.h` / `redis_store.cpp`（重点：key 设计与 Lua）

**动手实验**：

```bash
# 登录（密码是 MD5 后的 123456）
TOKEN=$(curl -s -X POST http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["token"])')

# 发消息
curl -s -X POST http://127.0.0.1:9101/api/message/send \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"to_user_id":2,"content":"hello from http"}'

# 查历史
curl -s -X POST http://127.0.0.1:9101/api/session/history \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"session_id":"single:1:2","limit":20}'
```

然后用 `redis-cli` 和 `mysql` 验证每一步的数据变化。

**必须回答**：

- HTTP 发消息路径为什么能做到很少的 Redis 访问（seq 分配 + 路由查询），而不用先查 MySQL？
- 未读数是怎么用一个 `MGET` 算出来的？
- 为什么 `session_id` 要 `min:max` 排序？
- 如果 Redis 里 `seq_initialized` 不存在，会发生什么？

### 阶段 4：Comet 接入层（3～4 天）

**目标**：理解长连接的生命周期。

**顺序**：

1. `comet/main.cpp` → `comet/app.cpp`（启动顺序）
2. `comet/comet_server.h`（先看数据成员，建立结构感）
3. `comet/conn_registry.*`
4. `comet/comet_server.cpp`
   - `OnValidate` `:116`
   - `OnOpen` / `OnClose` `:156` / `:165`
   - `OnMessage` → `HandleTextMessage` `:370`
   - 心跳 `:218-304`
   - `PushToUsers/PushToRoom/PushToAll` `:506-521`
5. `comet/comet_grpc_service.*`
6. `comet/logic_stream_client.*`（第二遍再抠并发）

**动手实验**：

- 用 `load_test/ws_client_cli` 或浏览器 DevTools 的 Network → WS 观察帧
- 登录后 `redis-cli HGETALL route:user:{1}`，关闭页面，再看是否被 HDEL
- 把 `log_level` 调成 `INFO`，观察握手、关闭、心跳日志
- 手动 `kill -9 comet_server`，观察 Logic 里的路由是否变成脏数据（这就是第 10 章的改进点）

**必须回答**：

- 一条 WebSocket 连接在 Comet 里有哪些状态？存在哪几个容器里？
- 为什么 `OnValidate` 里要生成 `conn_id`？它和 `user_id` 是什么关系？
- 心跳为什么分 64 个桶，而不是一个线程遍历所有连接？
- `OnClose` 时哪些清理是本机的，哪些要通知 Logic？

### 阶段 5：Logic gRPC 与 Kafka 扇出（3～4 天）

**目标**：把“上行 → Kafka”的主链路彻底打通。

**顺序**：

1. `logic/grpc_service.h`
2. `logic/grpc_service.cpp`
   - `VerifyToken` `:45`
   - `HandleUpstreamMessage` `:224`（**全文精读**）
   - `HandleRoomJoin/Leave` `:443` / `:465`
   - `MessageStream` `:491`
3. 回到 `logic/redis_store.cpp` 的 `IncrSessionMsgSeq`、`InitAndIncrSessionMsgSeq`
4. `common/kafka_producer.cpp`

**动手实验**：

- 用 `kafka-console-consumer` 消费 `push_to_comet`，观察 `PushToCometRequest` 的 key 和 payload
- 停掉 Job，连续发消息，观察 topic 堆积；启动 Job 后观察消费
- 把 `use_grpc_stream` 在 `true/false` 之间切换，用压测工具对比

**必须回答**：

- 为什么 `Kafka key` 要带 `comet_id`？
- 目标用户离线时，消息是怎么保证不丢的？
- `MessageStream` 如何把响应匹配回请求？
- `HandleUpstreamMessage` 在什么情况下返回 500？返回 500 时消息可能已经写进 Kafka 了吗？

### 阶段 6：Job 异步层（2～3 天）

**目标**：理解“消费-处理-提交”的可靠性模型。

**顺序**：

1. `job/main.cpp` → `job/service.cpp`
2. `job/push_handler.h` / `push_handler.cpp`
3. `job/persist_handler.h` / `persist_handler.cpp`
4. 回看 `common/kafka_consumer.cpp`

**动手实验**：

- 把 MySQL 停掉，发消息，观察 PersistHandler 重试和 `[DLQ][Persist]`
- 把 Comet 停掉，观察 PushHandler 重试和 `[DLQ][Push]`
- 查 `message` 表，确认 `session_id + msg_seq` 唯一约束在重复消费时生效

**必须回答**：

- 推送失败重试 3 次后消息去哪了？系统会丢消息吗？
- 为什么持久化要放在事务里？`session` 和 `message` 不一致会怎样？
- 两个消费组消费同一个 topic，offset 是共享的吗？
- 如果 Job 重启，未提交的 offset 会怎样？

### 阶段 7：房间、弹幕、广播（2～3 天）

**顺序**：

1. `logic/http_handlers_chatroom.cpp`
2. `logic/http_handlers_danmaku.cpp`
3. `logic/http_handlers_admin.cpp`
4. `logic/group_dao.*`、`danmaku_dao.*`
5. 回到 `comet/comet_grpc_service.cpp` 的 `ProcessPushRequest`
6. `job/push_handler.cpp` 的 `HandleBroadcastTask`

**动手实验**：

- 两个浏览器加入同一房间，观察 `room:{rid}:comets` 和 `online_count`
- 一个 Comet 上多个用户时，确认 Logic 只发一条房间消息（看 Kafka key）
- 对比 HTTP 弹幕和 WS 弹幕在 `video_danmaku` / `message` 两张表里的差异

**必须回答**：

- 房间消息为什么不是“每个用户一条 Kafka 消息”？
- `room:{rid}:comets` 什么时候 SADD、什么时候 SREM？
- 全站广播和房间广播的路径有什么不同？
- 关掉一个 Comet，`room:comets` 会自动清理吗？（答案：不会，除非连接正常关闭并触发 leave）

### 阶段 8：测试与压测（2～3 天）

**顺序**：

1. `tests/*`（先跑，再读）
2. `load_test/README.md`（非常详细，建议通读）
3. `load_test/ws_client_cli.cpp`
4. `load_test/async_ws_bench.cpp`（只看整体结构，不必逐行）

**动作**：

```bash
./build/tests/unit_tests
./build/load_test/ws_client_cli --account alice --password 123456 --to-user 2
# 按 README 做一轮小规模压测，记录 P50/P95/P99
```

### 7.9 逐文件精读清单（扩展版）

> 这一节把每个阶段拆到“打开哪个文件、先看哪个函数、第一遍可以跳过什么”。
> 建议第一遍只看“第一遍”列，能用自己的话讲清楚这一层负责什么；第二遍再抠“第二遍”列。

#### 阶段 0：跑通并观察（0.5 天）

1. `README.md`、`scripts/start_all.sh`、`scripts/stop_all.sh`、`scripts/status.sh`
   - 第一遍：知道怎么启动、停止、看状态、日志在 `logs/` 下。
   - 第二遍：看脚本里每个进程用了哪个配置文件、启动顺序是什么。
2. `conf/logic.conf`、`conf/comet.conf`、`conf/job.conf`
   - 第一遍：记住地址和端口：
     - Logic gRPC `9100`
     - Logic HTTP `9101`
     - Comet WebSocket `9200`
     - Comet gRPC `9205`
     - Kafka `9092`、Redis `6379`、MySQL `3306`
   - 第二遍：看 `comet_targets`、`use_grpc_stream`、`use_push_stream` 这几个开关。
3. `web_demo/static/app.js`
   - 第一遍：只看三段：登录拿 token、`new WebSocket("ws://.../ws?token=...")`、发送消息 JSON。
   - 不用读页面样式和 DOM 细节。
4. 用工具观察一次：
   - `redis-cli HGETALL route:user:{1}`：看路由 Hash。
   - `redis-cli GET token:{...}`：看登录态。
   - `kafka-console-consumer`：看 `push_to_comet` 里的消息。
   - `mysql`：看 `message` 表是否落盘。

**验收**：不看文档，画出“浏览器 → Comet → Logic → Kafka → Job → Comet → 浏览器”的图，并标出每个进程用的端口。

#### 阶段 1：common（2～3 天）

按下面的顺序打开文件。每个文件都先看 `.h` 里的结构体和成员，再看 `.cpp` 里的实现。

> 查字典：common 每个函数的一句话说明见 **第 13 章**；每个结构体成员的类型和用途见 **第 13 章** 与 **第 14 章**。

1. `common/config.h` / `config.cpp`
   - 第一遍：`Config` 的字段分类（网络、Kafka、Redis、MySQL、Comet、Job、日志）。
   - 第一遍：`LoadConfig` 的三段：填默认值 → 逐行解析 `key=value` → 推导 `http_port` / `comet_grpc_port`。
   - 第二遍：`ParseIntOr`、`ParseInt64Or`、`ParseLogLevelOr` 的“非法值回退默认值”策略。
   - 可以跳过：每个 `else if` 的重复分支，知道模式即可。

2. `common/logging.h` / `logging.cpp`
   - 第一遍：`LogLevel` 枚举、`LogInfo/LogError` 宏/函数、`ShouldLog` 的热路径短路。
   - 第二遍：`InitLogging` 的目录创建、文件轮转、`flush_on(warn)`；`LogStream` 的 RAII 写法。
   - 验收问题：为什么 `LogDebug` 在 INFO 级别下几乎不产生字符串拼接开销？

3. `common/thread_pool.h` / `thread_pool.cpp`
   - 第一遍：成员 `workers_`、`tasks_`、`mutex_`、`cv_`、`stopping_`。
   - 第一遍：`Start` → `Submit` → `WorkerLoop` → `Stop` 的生命周期。
   - 第二遍：`Stop()` 为什么要先 `notify_all` 再 `join`；队列里的任务会不会执行完。
   - 验收问题：`Submit` 在 `Stop` 过程中被调用会发生什么？

4. `common/mysql_pool.h` / `mysql_pool.cpp`
   - 第一遍：`MySqlConfig` 字段；`MySqlConnGuard` 的 RAII（构造拿连接、析构归还）。
   - 第一遍：`MySqlConnectionPool::Init` / `Acquire` / `Release` / `Stop`。
   - 第二遍：自动扩容 `total_conns_ < max_pool_size`；空闲回收 `CleanupIdleUnlocked`；`mysql_ping` 健康检查。
   - 验收问题：为什么 `MySqlConnGuard` 必须 move-only？

5. `common/redis_pool.h` / `redis_pool.cpp`
   - 第一遍：`RedisConfig` 字段；`RedisConnGuard` 的 RAII 和 `MarkBroken`。
   - 第一遍：`Acquire` 的等待/超时/自动扩容；`Release` 对坏连接的处理。
   - 第二遍：为什么热路径跳过 `PING`；`ctx->err` 什么时候会被置位。
   - 验收问题：连接池里的连接坏了，什么时候会发现？怎么避免坏连接反复被借出？

6. `common/kafka_producer.h` / `kafka_producer.cpp`
   - 第一遍：`Init` 的配置项（`acks=1`、`linger.ms=0`、`retries=5`、队列上限）。
   - 第一遍：`Send` 如何指定 key、如何 `poll(0)`。
   - 验收问题：为什么这里牺牲吞吐换低延迟？消息发送失败时 `Send` 返回什么？

7. `common/kafka_consumer.h` / `kafka_consumer.cpp`（**重点**）
   - 第一遍：`Options`、`PartitionKey`、`WorkItem`、`PartitionState` 四个结构体。
   - 第一遍：`Init` 的 Kafka 配置；`Start`/`Stop`；`Loop` 的消费循环。
   - 第二遍：`HandleMessage` 如何把消息放进 partition 队列；`SchedulePartition` 如何保证同一 partition 串行；`DrainCompletions` 如何提交 offset。
   - 第二遍：`max_inflight` 到上限时 `SetPartitionsPaused(true)` 的背压逻辑。
   - 验收问题：两个 partition 能并行吗？同一 partition 内顺序怎么保证？offset 是在回调前还是回调后提交？

8. `common/net/http_server_lite.h` / `http_server_lite.cpp`
   - 第一遍：`HttpRequest`、`HttpResponse` 的成员和 API；`HttpCallback` 的签名。
   - 第一遍：`HttpServerLite::run` 如何启动 accept 和 IO 线程。
   - 第二遍：`Session::DoRead` → `llhttp_execute` → `BuildResponse` → `WriteThen` 的 keep-alive 循环。
   - 第二遍：`ParseCtx` 的字段；`shared_from_this()` 如何保证回调期间对象不被销毁。
   - 可以跳过：llhttp 的每个回调细节，第一遍知道“增量解析 + 回调填 Request”即可。

9. `common/timer_wheel.h` / `timer_wheel.cpp` + `local_timer.*`
   - 第一遍：`TimerState`、`TimerToken`、`TimerNode`、`Bucket` 的字段；`LocalTimer` 的 `schedule/cancel/execute_if_ready`。
   - 第一遍：`TimerWheel::add_timer` 和 `worker_loop` 的整体流程。
   - 第二遍：分层时间轮的 `near_`、`levels_`、`current_tick_`、`shift_locked`；第一遍允许“知其然”。
   - 验收问题：`LocalTimer` 为什么不是自己开线程，而是复用全局 `TimerWheel`？

10. `common/signal_handler.h`
    - 第一遍：`ShutdownRequested()` 全局原子标志；`InstallSignalHandler` 注册 `SIGTERM/SIGINT`。
    - 验收问题：为什么信号处理函数里只置标志，不做复杂清理？

#### 阶段 2：协议与数据模型（0.5～1 天）

1. `proto/spark_push.proto`
   - 第一遍：按注释把消息分成四组：客户端上行、Logic 下行、Comet 推送、Job 广播。
   - 第二遍：注意 `oneof payload`、`persist_only`、`need_persist`、`targets` 这些字段的用途。
2. `sql/schema.sql`
   - 第一遍：`user`、`session`、`message`、`user_session_state`、`im_group`、`group_member`、`video_danmaku`、`broadcast`。
   - 第二遍：唯一索引和联合索引，特别是 `message(session_id, msg_seq)`。
3. `logic/model.h`
   - 第一遍：`User`、`SessionType`、`Session`、`Message`、`UserSessionState` 的字段。
   - 第二遍：这些结构体和 SQL 表、Protobuf 消息之间的对应关系。
4. `proto/proto_enums.h`
   - 第一遍：字符串和枚举的互转；为什么协议边界用字符串、内部用枚举。
5. `comet/websocket_utils.h` / `websocket_utils.cpp` + `tests/test_websocket.cpp`
   - 第一遍：`UpstreamMessageMeta` 的字段；`ParseUpstreamMessage` 的兼容规则。
   - 第二遍：单聊、房间、弹幕三种 JSON 的差异。

**验收**：能说出 `msg_id`、`client_msg_id`、`session_id`、`msg_seq` 分别由谁生成、存在哪里。

#### 阶段 3：Logic HTTP 与存储（3～4 天）

1. `logic/main.cpp`
   - 第一遍：依赖组装顺序：Kafka producer → MySQL pool → DAO → Redis pool → RedisStore → ConversationStore → gRPC/HTTP server。
   - 第二遍：优雅关闭顺序：HTTP stop → gRPC shutdown → MySQL pool stop。
2. `logic/http_server.h` / `http_server.cpp`
   - 第一遍：`onRequest` 的路由表；`AuthenticateRequest`；`CheckSessionAccess`；CORS。
   - 第二遍：错误响应格式；为什么每个 handler 都要先鉴权/校验会话归属。
3. `logic/http_handlers_auth.cpp`
   - 第一遍：`handleRegister`、`handleLogin`；密码 MD5；token 写入 Redis 的 TTL。
   - 第二遍：登录失败/重复注册的返回码。
4. `logic/http_handlers_message.cpp`
   - 第一遍：`handleSendMessage` 的完整流程：鉴权 → 会话 → 分配 seq → 发 Kafka → 返回 msg_id。
   - 第二遍：`handleHistory`、`handleMarkRead`、`handleUnread`、`handleSingleSessionList`。
5. `logic/conversation_store.h` / `conversation_store.cpp`
   - 第一遍：`GetOrCreateSingleSession`、`AllocateMessageSeq`、`AppendMessage`、`GetHistory`、`GetUnread`。
   - 第二遍：`AllocateMessageSeq` 的 Redis 快路径 + MySQL 兜底。
6. DAO 层，按依赖顺序：
   - `user_dao.*`：用户查询/创建。
   - `session_dao.*`：会话创建/查询、MySQL 侧最大序号。
   - `message_dao.*`：消息插入/历史查询。
   - `user_session_state_dao.*`：已读游标。
   - 第一遍只看每个 DAO 的 public 方法签名和 SQL 大意，不逐行读 prepared statement。
7. `logic/redis_store.h` / `redis_store.cpp`
   - 第一遍：Key schema 注释；token、route、seq、read_seq、room、rate 六类 key。
   - 第二遍：`IncrSessionMsgSeq` / `InitAndIncrSessionMsgSeq` 的 Lua；`GetUserRoutes` 的缓存和去重。
   - 验收问题：`session_id` 为什么排序？`seq_initialized` 不存在时会发生什么？

#### 阶段 4：Comet 接入层（3～4 天）

1. `comet/main.cpp` → `comet/app.cpp`
   - 第一遍：WebSocket server、gRPC server、信号处理、`server.Run()` 的启动顺序。
2. `comet/comet_server.h`
   - 第一遍：先只看结构体：`ConnContext`、`WsConnectionBase`、`HeartbeatEntry`、`HeartbeatBucket`。
   - 第一遍：再看 `CometServer` 的成员：`registry_`、`logic_stub_`、`grpc_pool_`、`stream_client_`、心跳桶。
3. `comet/conn_registry.h` / `conn_registry.cpp`
   - 第一遍：`UserShard`、`RoomShard`、`hdl_to_uid_` 三张表。
   - 第二遍：`AddConn`、`RemoveConn`、`ConnsForUsers`、`ConnsInRoom` 的加锁范围。
4. `comet/comet_server.cpp`，按回调顺序读：
   - `OnValidate`：token 提取 → gRPC `VerifyToken` → 写 `ConnContext`。
   - `OnOpen`：加入 `ConnRegistry` 和心跳桶。
   - `OnMessage` → `HandleTextMessage`：解析 JSON → 上行到 Logic。
   - `OnClose`：标记关闭 → 清本机注册表 → 通知 Logic 下线/离开房间。
   - `PushToUsers` / `PushToRoom` / `PushToAll`：Job 下行入口。
   - 心跳：`AddToHeartbeatBucket` → `ScanNextHeartbeatBucket` → `ping/close`。
5. `comet/comet_grpc_service.h` / `.cpp`
   - 第一遍：`PushToComet` 和 `PushStream` 两个入口；如何调用 `CometServer` 的推送方法。
6. `comet/logic_stream_client.h` / `.cpp`（第二遍再抠）
   - 第一遍：`PendingRequest`、`StreamState`；`Send` 如何分配 `request_id`。
   - 第二遍：writer/reader 线程、`pending_callbacks_` 匹配响应、`Stop` 如何唤醒 `Read`。
   - 验收问题：为什么 `conn_id` 要在握手时生成？心跳为什么分 64 个桶？

#### 阶段 5：Logic gRPC 与 Kafka 扇出（3～4 天）

1. `logic/grpc_service.h`
   - 第一遍：七个 RPC 方法；哪些是 unary，哪个是双向流。
2. `logic/grpc_service.cpp` 的 `VerifyToken`
   - 第一遍：Redis 查 token → 返回 user_id → `AddRoute` 写 `route:user:{uid}`。
3. `logic/grpc_service.cpp` 的 `HandleUpstreamMessage`（**全文精读**）
   - 第一遍：单聊/房间/弹幕分支；会话创建；`AllocateMessageSeq`；构造 Kafka 消息。
   - 第二遍：`targets` 怎么算出来；`persist_only`、`need_persist` 怎么填；Kafka key 怎么选。
4. `HandleUserOffline`、`HandleRoomJoin`、`HandleRoomLeave`
   - 第一遍：路由清理、房间在线人数、`room:{rid}:comets` 的增删。
5. `MessageStream`
   - 第一遍：双向流的读写循环；`request_id` 如何把响应匹配回请求。
   - 第二遍：流断开/取消时的清理。
6. 回看 `logic/redis_store.cpp` 的 `IncrSessionMsgSeq`、`InitAndIncrSessionMsgSeq`
   - 第一遍：Lua 的输入/输出；初始化标记。
7. `common/kafka_producer.cpp`
   - 第一遍：`Send` 的 key/value 如何对应分区和顺序。
   - 验收问题：目标用户离线时消息去哪了？Kafka key 为什么带 `comet_id`？

#### 阶段 6：Job 异步层（2～3 天）

1. `job/main.cpp` → `job/service.cpp`
   - 第一遍：创建 `PushHandler`、`PersistHandler`，启动两个 Kafka consumer，等待关闭信号。
2. `job/push_handler.h` / `push_handler.cpp`
   - 第一遍：`HandleMessage` → `ProcessPushRequest` → `GetStub` → `PushToComet` 的 unary 路径。
   - 第二遍：重试/指数退避、`[DLQ][Push]` 日志、`comet_addrs_` 解析。
3. `job/persist_handler.h` / `persist_handler.cpp`
   - 第一遍：`HandleMessage` → `PersistMessage` → `ExecutePersist` 的事务流程。
   - 第二遍：`session` 与 `message` 的一致性、`ON DUPLICATE KEY UPDATE` 的幂等。
4. 回看 `common/kafka_consumer.cpp`
   - 第一遍：验证“回调成功后才提交 offset”与 at-least-once 的关系。
5. 可选：`job/push_handler.cpp` 的 `InitStreams`、`StreamWriterLoop`、`ReconnectStream`
   - 第一遍：每个 Comet 一条客户端流；发送队列 + writer 线程。
   - 第二遍：断流重连和队列积压。
   - 验收问题：推送失败重试 3 次后消息去哪了？持久化失败会重试吗？

#### 阶段 7：房间、弹幕、广播（2～3 天）

1. `logic/http_handlers_chatroom.cpp`
   - 第一遍：房间列表、加入、退出、在线人数；`room:{rid}:comets` 的读写。
2. `logic/http_handlers_danmaku.cpp`
   - 第一遍：HTTP 弹幕发送/列表；`video_danmaku` 表。
3. `logic/http_handlers_admin.cpp`
   - 第一遍：管理员建房间、列房间、全站广播。
4. `logic/group_dao.*`、`danmaku_dao.*`
   - 第一遍：`ImGroup`、`DanmakuItem` 字段；DAO 方法。
5. `comet/comet_grpc_service.cpp` 的 `ProcessPushRequest`
   - 第一遍：房间消息只推给该房间在本 Comet 上的连接，而不是每个用户一条 Kafka 消息。
6. `job/push_handler.cpp` 的 `HandleBroadcastTask`
   - 第一遍：全站广播 vs 房间广播的路径差异。
   - 验收问题：`room:{rid}:comets` 什么时候 SADD/SREM？Comet 挂掉会自动清理吗？

#### 阶段 8：测试与压测（2～3 天）

1. `tests/*`
   - 先跑 `./build/tests/unit_tests`，再按测试名找对应源码。
   - 优先看：线程池、时间轮、Redis/MySQL 池、WebSocket 解析、序号分配。
2. `load_test/README.md`
   - 第一遍：压测工具的参数、指标含义、如何起多个 Comet。
3. `load_test/ws_client_cli.cpp`
   - 第一遍：登录 → 建连 → 发消息 → 收消息的客户端流程。
4. `load_test/async_ws_bench.cpp`
   - 第一遍：只看整体结构（epoll + 非阻塞连接 + 统计），不逐行抠状态机。
   - 验收问题：P50/P95/P99 分别代表什么？瓶颈在 Comet、Logic 还是 Job？

### 7.10 阶段验收表

| 阶段 | 必须能讲清 | 必须动手验证 | 卡住时回看 |
|---|---|---|---|
| 0 | 四个进程、端口、一条消息的路径 | 登录并互发一条消息 | 第 3 章架构图 |
| 1 | RAII、线程池 Stop、连接池扩容、Kafka 分区串行 | 跑单元测试；停一次 MySQL/Redis 看日志 | 13、14 章速查表 |
| 2 | `msg_id` / `session_id` / `msg_seq` 的生成者 | 用 Redis/MySQL 对照一条消息 | 4.1～4.4 |
| 3 | HTTP 发消息的完整调用链 | 用 curl 登录、发消息、查历史 | 5.2、5.3 |
| 4 | WebSocket 生命周期、路由注册、心跳 | 连/断 WS，观察 `route:user:{uid}` | 5.2、5.9 |
| 5 | 上行消息如何进 Kafka、key 怎么选 | 用 console-consumer 看 topic | 5.3、6.1 |
| 6 | at-least-once、重试、持久化事务 | 停 Job/MySQL 观察堆积和 DLQ | 5.4、6.2 |
| 7 | 房间路由与广播的差异 | 两个浏览器进同一房间 | 5.6～5.8 |
| 8 | 压测指标和瓶颈定位 | 跑一轮小规模压测 | `load_test/README.md` |

### 7.11 建议先跳过的部分

- `common/timer_wheel.cpp` 的分层算法细节（第一遍）
- `load_test/async_ws_bench.cpp` 的 epoll 状态机（除非你要做压测工具）
- `web_demo/static/*.html` 的 UI 逻辑（只需要知道它调了哪些 API）
- `third_party/json.hpp`、`third_party/doctest.h`（第三方库）

### 一张图总结阅读顺序

```text
阶段0 跑通
  ↓
阶段1 common（配置/日志/线程池/连接池/Kafka/HTTP/时间轮）
  ↓
阶段2 proto + SQL + JSON 解析
  ↓
阶段3 Logic HTTP + DAO + Redis        ← 同步路径，最好调试
  ↓
阶段4 Comet WebSocket + 连接表 + 心跳
  ↓
阶段5 Logic gRPC + Kafka 上行扇出
  ↓
阶段6 Job 消费 + 推送 + 落盘
  ↓
阶段7 房间/弹幕/广播（组合场景）
  ↓
阶段8 测试 + 压测 + 性能分析
```

---

## 8. 二次开发路线

> 原则：**每一题都要有明确的验收标准，能自己复现、能说清楚改前改后的差异。**
>
> 不要一上来就“重构架构”。先做能在一个文件内完成、能立刻验证的小改动。

### Level 1：熟悉代码（1～2 周）

| # | 任务 | 涉及文件 | 验收标准 |
|---|---|---|---|
| 1 | 新增一个 `GET /api/ping` 或 `POST /api/echo` 接口 | `logic/http_server.cpp` 路由表 + 新 handler | `curl` 返回正确 JSON，CORS 正常 |
| 2 | 给 `/metrics` 增加真实计数器：上行消息数、Kafka 发送失败数、在线连接数 | `logic/grpc_service.cpp`、`comet/comet_server.cpp`、`http_server.cpp` | `/metrics` 输出随操作变化 |
| 3 | 把 `log_level` 做成运行时可通过信号/接口调整 | `common/logging.*` | 不重启进程即可切换级别 |
| 4 | 为 `ConversationStore::AllocateMessageSeq` 补一个并发测试 | `tests/`（需要抽接口或接测试 Redis） | 并发 100 次无重复 seq |
| 5 | 修复 `MessageDao::ListMessages` 的 4096 字节截断 | `logic/message_dao.cpp:235` | 插入 >4KB 的 content_json 后能完整读回 |
| 6 | 统一 HTTP 与 WS 的 `content` 结构 | `http_handlers_message.cpp:107`、`comet_server.cpp:455` | 前端两种入口渲染一致 |

**第 5 题的背景**：`ListMessages` 用固定 `char content_buf[4096]` 接收 `TEXT` 列，超长内容会被 MySQL 截断，代码把 `MYSQL_DATA_TRUNCATED` 当成功处理。改法：先 `mysql_stmt_fetch_column` 拿到真实长度，再动态分配二次 fetch；或者把 `content_json` 改成 `MEDIUMTEXT` 并调大缓冲。

### Level 2：补功能（2～4 周）

| # | 任务 | 思路 | 涉及文件 | 验收标准 |
|---|---|---|---|---|
| 7 | **离线消息补拉** | WS `OnOpen` 后，Logic 查该用户所有会话的 `read_seq` 与 `last_msg_seq`，把未读消息通过 Comet 推给客户端；或客户端登录后调 `/api/session/unread` + `/history` | `comet_server.cpp:156`、`logic/grpc_service.cpp`、`conversation_store.*` | 用户离线期间的消息，重新上线后能收到/拉到 |
| 8 | **消息撤回** | 新增 `POST /api/message/recall`，校验发送者与时间窗口；写一条 `system` 消息到 Kafka；客户端按 `msg_id` 删除/替换 | 新 handler、`proto`、`job/persist_handler.cpp` | 双方客户端都能看到撤回效果，历史记录也一致 |
| 9 | **已读回执** | `mark_read` 后向对端发一条控制消息（走 push topic，`targets=[peer]`） | `http_handlers_message.cpp`、`grpc_service.cpp` | 对端实时看到“已读” |
| 10 | **弹幕闭环** | WS 弹幕也写 `video_danmaku`：给 `PushToCometRequest` 加 `video_id/timeline_ms`，或让 PersistHandler 按 scene 分支写两张表 | `proto`、`grpc_service.cpp`、`persist_handler.cpp` | WS 发的弹幕能被 `/api/danmaku/list` 拉到 |
| 11 | **修复 HTTP 弹幕推送** | HTTP 路径构造 `PushToCometRequest` 时设置 `room_id`，不要依赖 session 前缀 | `http_handlers_danmaku.cpp:167` | 房间内用户能实时收到 HTTP 弹幕 |
| 12 | **房间在线列表** | Comet 暴露本机房间成员；Logic 聚合各 Comet 或维护 Redis Set | `conn_registry.*`、`comet_grpc_service.*`、`redis_store.*` | 能查询房间在线用户列表 |
| 13 | **多 Comet 部署** | 再起一个 comet（改 `comet_id`/端口），`job.conf` 的 `comet_targets` 加一项；验证路由、房间扇出、广播 | `conf/comet.conf`、`conf/job.conf` | 两个 Comet 上的用户能互相发消息 |
| 14 | **路由 TTL / 死节点清理** | 给 `route:user:{uid}` 增加心跳时间戳或 TTL；Logic 发现 Comet 长时间无心跳则清理 | `redis_store.*`、`comet_server.cpp` | `kill -9` Comet 后，路由能在 N 秒内自愈 |

**第 7 题**是本项目最自然的扩展：现在的“离线消息”只是落库，客户端要靠历史接口自己拉。把“上线自动同步”做出来，你会完整走一遍 Comet/Logic/Redis/MySQL 的协作。

### Level 3：架构与性能（1～2 个月）

| # | 任务 | 思路 | 难点 | 验收 |
|---|---|---|---|---|
| 15 | **序号生成改造** | 号段模式（一次取 1000 个）/ Snowflake / 分片计数器 | 保证会话内严格递增；崩溃后不能回退 | 压测 QPS 提升，且无重复/倒退 |
| 16 | **持久化批量提交** | PersistHandler 聚合 N 条或 T 毫秒，一次事务批量 INSERT | 失败重试的粒度、延迟与吞吐权衡 | 用 `mysql_stmt_bench` 对比 |
| 17 | **异步化 VerifyToken** | Comet 用 async gRPC 或把校验放进 `grpc_pool_`，不阻塞 IO 线程 | 握手回调的线程安全、超时 | 高并发握手时 IO 线程不卡顿 |
| 18 | **推送去重** | 客户端/Comet 按 `msg_id` 做短期去重（LRU/布隆） | 内存上限、过期策略 | at-least-once 下客户端不重复显示 |
| 19 | **有界队列与背压** | 线程池任务队列、PushStream 队列加上限；满了拒绝/降级 | 拒绝策略、监控 | 高负载下内存不无限增长 |
| 20 | **可观测性** | 接入 Prometheus + OpenTelemetry，端到端 trace_id | 跨进程上下文传播 | 能看到一条消息的完整耗时分布 |
| 21 | **安全加固** | bcrypt/argon2 替代 MD5；WS 用 wss；token 刷新；限流重新启用 | 兼容旧客户端 | 通过基本安全测试 |

### 推荐的前三个题目（按顺序）

1. **修复 `MessageDao` 截断**（小而完整，练 DAO + 预处理语句）
2. **弹幕闭环**（跨 proto/Logic/Job/MySQL，练数据流）
3. **离线消息补拉**（跨 Comet/Logic/Redis/MySQL，练业务设计）

### 8 周学习计划（可直接照做）

| 周 | 目标 | 产出 |
|---|---|---|
| 1 | 跑通 + 读第 1～4 章 + 阶段 0/1 | 画出架构图，能解释每个端口 |
| 2 | 阶段 2/3：proto、SQL、HTTP、DAO | 用 curl 完成登录/发消息/历史，并用 Redis/MySQL 验证 |
| 3 | 阶段 3 深入：Redis key、Lua、seq | 写一篇“一条消息的 seq 是如何产生的”笔记 |
| 4 | 阶段 4：Comet、WS、心跳、连接表 | 用 DevTools 观察 WS 帧；解释断线清理 |
| 5 | 阶段 5：gRPC、Kafka 扇出 | 用 Kafka 命令行看到 key/payload；画出上行时序图 |
| 6 | 阶段 6：Job、重试、落盘 | 停依赖做故障注入，记录重试与 DLQ 日志 |
| 7 | 阶段 7/8：房间/弹幕/广播 + 压测 | 跑一轮压测，记录 P95/P99 |
| 8 | Level 1 二次开发 + 复盘 | 提交一个小 PR（或 patch），写清改前/改后 |

---

## 9. 调试与验证工具箱

### 9.1 常用命令

```bash
cd /home/wdh/Linux/Spark_Push

# 启停与状态
./scripts/start_all.sh
./scripts/status.sh
./scripts/stop_all.sh

# 日志
tail -f logs/logic.log logs/comet.log logs/job.log

# 健康检查
curl -s http://127.0.0.1:9101/health
curl -s http://127.0.0.1:9101/metrics

# 单元测试
./build/tests/unit_tests
```

### 9.2 登录与 HTTP 调用

```bash
# 密码必须先做 MD5（123456 的 MD5）
TOKEN=$(curl -s -X POST http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["token"])')

# 单聊
curl -s -X POST http://127.0.0.1:9101/api/message/send \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"to_user_id":2,"content":"hello"}'

# 历史
curl -s -X POST http://127.0.0.1:9101/api/session/history \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"session_id":"single:1:2","anchor_seq":0,"limit":20}'

# 未读
curl -s -X POST http://127.0.0.1:9101/api/session/unread \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"session_id":"single:1:2"}'
```

> 注意：所有业务接口只接受 POST；`/health`、`/metrics` 是 GET。

### 9.3 Redis 观察

```bash
redis-cli
> KEYS *
> HGETALL route:user:{1}
> GET session:{single:1:2}:seq
> GET session:{single:1:2}:seq_initialized
> GET read_seq:{single:1:2}:uid:2
> SMEMBERS room:{1001}:comets
> GET room:{1001}:online_count
> MONITOR          # 实时看命令（学习时非常有用，注意性能）
```

### 9.4 Kafka 观察

```bash
# 需要 Java 17 + KAFKA_HOME
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic push_to_comet \
  --property print.key=true \
  --property key.separator=' | '

# 查看消费组 lag
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

### 9.5 MySQL 观察

```bash
mysql -uroot -p123456 spark_push
```

```sql
SELECT id, session_id, msg_seq, sender_id, msg_type, timestamp_ms
FROM message ORDER BY id DESC LIMIT 10;

SELECT * FROM session ORDER BY id DESC LIMIT 5;
SELECT * FROM user_session_state LIMIT 10;
SELECT * FROM video_danmaku ORDER BY id DESC LIMIT 10;
```

### 9.6 WebSocket 客户端

```bash
# 单聊模式
./build/load_test/ws_client_cli \
  --account alice --password 123456 --to-user 2

# 聊天室模式
./build/load_test/ws_client_cli \
  --account alice --password 123456 --room-id 1001
```

### 9.7 故障注入清单

| 操作 | 观察点 | 学到什么 |
|---|---|---|
| 停 Job，发消息 | Kafka lag 增长 | 解耦与削峰 |
| 停 Comet，发消息 | PushHandler 重试/DLQ | 重试与死信 |
| 停 MySQL，发消息 | Logic 启动失败 / Persist 重试 | 依赖边界 |
| 停 Redis | Logic 启动失败 | Redis 在关键路径 |
| `kill -9` Comet | Redis 脏路由 | 需要路由自愈 |
| 改 Kafka 分区数 | 顺序与并行 | 分区键设计 |

---

## 10. 代码中值得注意的问题与改进点

以下不是“挑刺”，而是**带着问题读代码**的抓手。每条都适合作为二次开发题目。

| # | 问题 | 位置 | 影响 | 建议 |
|---|---|---|---|---|
| 1 | `ListMessages` 用 4096 字节固定缓冲接收 TEXT | `logic/message_dao.cpp:235` | 长消息被截断且被当作成功 | 二次 fetch / 动态缓冲 |
| 2 | HTTP 弹幕推送未设置 `room_id` | `logic/http_handlers_danmaku.cpp:167` | `targets` 为空时按 `video_id` 兜底，数字 ID 会推错房间、非数字则丢弃 | 显式设置 `room_id=1001` |
| 3 | WS 弹幕不写 `video_danmaku` | `logic/grpc_service.cpp` + `job/persist_handler.cpp` | `/api/danmaku/list` 拉不到 | 扩展 proto 或 Persist 分支 |
| 4 | `OnValidate` 同步调用 `VerifyToken` | `comet/comet_server.cpp:143` | 阻塞 asio IO 线程，握手高并发时卡顿 | async gRPC / 线程池 |
| 5 | Comet 崩溃后 Redis 路由不清理 | `logic/redis_store.cpp:103` | 消息推给死节点，重试后进 DLQ | 路由 TTL / 心跳续约 |
| 6 | PushHandler 流模式队列无界 | `job/push_handler.cpp:61` | 下游慢时内存增长 | 有界队列 + 拒绝策略 |
| 7 | 线程池任务队列无界 | `common/thread_pool.h:38` | 高负载堆积 | 有界队列 + 背压 |
| 8 | `HandleBroadcastTask` 的 unary 调用无 deadline/重试 | `job/push_handler.cpp:372` | 广播可能长时间阻塞 | 加 deadline/重试 |
| 9 | 密码 MD5 无盐 | `logic/http_handlers_auth.cpp:69` | 不安全 | bcrypt/argon2 |
| 10 | 限流代码被注释 | `logic/grpc_service.cpp:237-240`、`http_handlers_message.cpp:38` | 无发送限流 | 重新启用并压测开销 |
| 11 | 弹幕 HTTP 路径固定房间 1001 | `logic/http_handlers_danmaku.cpp:69` | 多视频无法隔离 | video_id ↔ room_id 映射 |
| 12 | `DanmakuDao::InsertDanmaku` 用 `time(nullptr)*1000` | `logic/danmaku_dao.cpp:72` | 毫秒精度丢失 | 用 `system_clock` 毫秒 |
| 13 | `HandleUpstreamMessage` 未判空 `producer_` | `logic/grpc_service.cpp:389` | 理论空指针风险（main 已初始化） | 加守卫 |
| 14 | HTTP 与 WS 的 content 结构不一致 | 见 4.5 节 | 前端需兼容两套格式 | 统一协议 |
| 15 | `room:{rid}:online_count` 可能重复加减 | 多连接重复 join | 在线人数不准 | 按 conn 去重或按连接计数 |

> 读代码时建议维护一张自己的“疑问清单”，每解决一个就划掉。上面这些可以作为起点。

---

## 11. 与 docs 目录 PDF 的对应关系

`docs/` 里是课程配套讲义，建议与本指南配合阅读：

| PDF | 对应本文章节 | 阅读时机 |
|---|---|---|
| `1.0-环境安装.pdf` | 阶段 0 | 环境有问题时 |
| `1.1-项目整体架构和技术选型及部署.pdf` | 第 1～3 章 | 第一遍建立全局观 |
| `2.0-WebSocket原理与海量长连接管理.pdf` | 5.2、5.9、6.8 | 读 Comet 前 |
| `2.1-Comet 接入层-海量连接如何管理.pdf` | 5.9、6.8 | 读 `conn_registry.*` 时 |
| `2.2-消息全链路追踪.pdf` | 5.3、5.4 | 读完上下行链路后 |
| `3.0-Kafka原理与可靠消息处理.pdf` | 4.2、5.4、6.1、6.2 | 读 Kafka 代码前 |
| `3.1-Logic 消息中枢-路由、序号与 Kafka 入口.pdf` | 5.3、6.3、6.4 | 读 `grpc_service.cpp` 时 |
| `4.1-gRPC 深入- Unary vs 双向流.pdf` | 6.9 | 读 `logic_stream_client.*` 时 |
| `5.1-Kafka 消费与 Job-有序并行、可靠处理与分层扇出.pdf` | 5.4、6.10 | 读 Job 前 |
| `6.0-房间设计 — 三层成员、分层扇出与广播.pdf` | 5.6、5.8 | 读房间/广播时 |
| `6.1-弹幕设计 — 实时房间与视频时间轴.pdf` | 5.7 | 读弹幕时 |
| `6.2-房间弹幕测试.pdf` | 阶段 8 | 做压测时 |

---

## 12. 附录：文件职责速查表

### 12.1 common

| 文件 | 职责 | 阅读优先级 |
|---|---|---|
| `config.h/.cpp` | key=value 配置加载、默认值推导 | ★★★ |
| `logging.h/.cpp` | spdlog 封装、级别、轮转 | ★★★ |
| `thread_pool.h/.cpp` | 固定线程池 | ★★★ |
| `mysql_pool.h/.cpp` | MySQL 连接池 + RAII Guard | ★★★ |
| `redis_pool.h/.cpp` | Redis 连接池 + RAII Guard | ★★★ |
| `kafka_producer.h/.cpp` | Kafka 生产者封装 | ★★☆ |
| `kafka_consumer.h/.cpp` | 分区串行消费、手动提交、背压 | ★★★★ |
| `timer_wheel.h/.cpp` | 分层时间轮 | ★★☆ |
| `local_timer.h/.cpp` | 单次定时器封装 | ★★☆ |
| `signal_handler.h` | SIGTERM/SIGINT | ★★☆ |
| `net/http_server_lite.h/.cpp` | asio + llhttp 的极简 HTTP 服务器 | ★★★ |

### 12.2 logic

| 文件 | 职责 | 阅读优先级 |
|---|---|---|
| `main.cpp` | 组装所有依赖、启动 gRPC/HTTP | ★★★ |
| `grpc_service.h/.cpp` | LogicService 实现，核心业务 | ★★★★★ |
| `http_server.h/.cpp` | 路由、鉴权、CORS、越权校验 | ★★★★ |
| `http_handlers_auth.cpp` | 登录/注册 | ★★★ |
| `http_handlers_message.cpp` | 发消息/历史/已读/未读/会话列表 | ★★★★ |
| `http_handlers_chatroom.cpp` | 房间列表/加入/退出/在线人数 | ★★★ |
| `http_handlers_danmaku.cpp` | 弹幕发送/列表 | ★★★ |
| `http_handlers_admin.cpp` | 后台建房间/广播 | ★★★ |
| `conversation_store.h/.cpp` | 会话/消息/未读聚合 | ★★★★ |
| `redis_store.h/.cpp` | Redis key、Lua、路由缓存 | ★★★★★ |
| `user_dao.*` | 用户表 | ★★☆ |
| `session_dao.*` | 会话表、MySQL 序号分配（旧路径） | ★★★ |
| `message_dao.*` | 消息表 | ★★★ |
| `group_dao.*` | 群/成员表 | ★★☆ |
| `danmaku_dao.*` | 弹幕表 | ★★☆ |
| `user_session_state_dao.*` | 已读游标 | ★★☆ |
| `model.h` | 领域模型 | ★★★ |

### 12.3 comet

| 文件 | 职责 | 阅读优先级 |
|---|---|---|
| `main.cpp` / `app.cpp` | 启动 WebSocket + gRPC | ★★★ |
| `comet_server.h/.cpp` | 核心：握手、消息、推送、心跳 | ★★★★★ |
| `conn_registry.h/.cpp` | 分片连接表 | ★★★★★ |
| `comet_grpc_service.h/.cpp` | 接收 Job 推送 | ★★★★ |
| `logic_stream_client.h/.cpp` | 与 Logic 的双向流 | ★★★★ |
| `websocket_utils.h/.cpp` | 上行 JSON 解析 | ★★★ |

### 12.4 job

| 文件 | 职责 | 阅读优先级 |
|---|---|---|
| `main.cpp` / `service.cpp` | 启动与组合 | ★★★ |
| `push_handler.h/.cpp` | Kafka → gRPC 推送、重试、流模式 | ★★★★★ |
| `persist_handler.h/.cpp` | Kafka → MySQL 事务落盘 | ★★★★ |

### 12.5 其他

| 路径 | 职责 |
|---|---|
| `proto/spark_push.proto` | 所有内部协议 |
| `proto/proto_enums.h` | 字符串 ↔ 枚举 |
| `sql/schema.sql` | 建表与测试数据 |
| `conf/*.conf` | 三个进程的配置 |
| `scripts/*.sh` | 一键启停 |
| `tests/*` | 单元测试 |
| `load_test/*` | 压测工具 |
| `web_demo/*` | 演示页面与静态服务器 |

---

## 13. common 模块：函数与结构体速查

> 这一章是“查字典”用的，不需要从头背到尾。读 common 代码时，遇到函数先在这里看一句话；对应结构体字段见本章各节和第 14 章。
>
> 第一次读 common，仍建议按 **7.9 阶段 1** 的顺序：先 config/logging，再 thread_pool，再连接池，最后 Kafka/HTTP/时间轮。

### 13.1 config

**函数**

| 函数 | 一句话作用 |
|---|---|
| `LoadConfig(path)` | 读取 `key=value` 配置文件，填默认值、逐项解析、推导端口。 |
| `Trim(s)` | 去掉字符串首尾的空格、Tab、`\r`、`\n`。 |
| `ParseIntOr(key, value, default)` | 解析 `int`，失败时告警并回退默认值。 |
| `ParseInt64Or(key, value, default)` | 解析 `int64_t`，失败时告警并回退默认值。 |
| `ParseLogLevelOr(value, default)` | 解析日志级别：支持 `TRACE/DEBUG/INFO/WARN/ERROR/FATAL` 和数字 0～5，非法值回退。 |

**`Config` 字段**

网络与 Logic：

| 成员 | 类型 | 用途 |
|---|---|---|
| `listen_addr` | `std::string` | 服务监听地址，通常是 `0.0.0.0`。 |
| `listen_port` | `int` | 监听端口：Logic 进程解释为 gRPC 端口，Comet 进程解释为 WebSocket 端口。 |
| `http_port` | `int` | Logic HTTP 端口；未显式配置时由 `listen_port + 1` 推导。 |
| `logic_grpc_target` | `std::string` | Comet 连接 Logic 的 gRPC 地址，如 `127.0.0.1:9100`。 |

Kafka：

| 成员 | 类型 | 用途 |
|---|---|---|
| `kafka_brokers` | `std::string` | Kafka broker 列表，如 `127.0.0.1:9092`。 |
| `kafka_push_topic` | `std::string` | 上行推送任务 topic，Logic 写、Job 的 PushHandler 读。 |
| `kafka_broadcast_topic` | `std::string` | 广播任务 topic，Logic 写、Job 的 Broadcast 消费。 |
| `kafka_consumer_group` | `std::string` | Kafka 消费组基础名称，具体 consumer 会在此基础上加后缀。 |

Redis：

| 成员 | 类型 | 用途 |
|---|---|---|
| `redis_host` | `std::string` | Redis 地址。 |
| `redis_port` | `int` | Redis 端口，默认 6379。 |
| `redis_password` | `std::string` | Redis 密码，空表示不 AUTH。 |
| `redis_db` | `int` | Redis 数据库编号，0 表示默认库。 |
| `redis_pool_size` | `int` | 连接池目标大小（也用于推导最小/最大连接数）。 |
| `redis_max_pool_size` | `int` | 连接池最大连接数；0 表示按 `pool_size` 推导。 |
| `redis_connect_timeout_ms` | `int` | 建连超时，毫秒。 |
| `redis_rw_timeout_ms` | `int` | 读写超时，毫秒。 |
| `redis_idle_timeout_ms` | `int` | 空闲连接回收阈值，毫秒。 |

MySQL：

| 成员 | 类型 | 用途 |
|---|---|---|
| `mysql_host` | `std::string` | MySQL 地址。 |
| `mysql_port` | `int` | MySQL 端口，默认 3306。 |
| `mysql_user` | `std::string` | MySQL 用户名。 |
| `mysql_password` | `std::string` | MySQL 密码。 |
| `mysql_db` | `std::string` | 数据库名，默认 `spark_push`。 |
| `mysql_pool_size` | `int` | 连接池目标大小。 |
| `mysql_pool_min_size` | `int` | 最小连接数；0 表示按 `pool_size` 推导。 |
| `mysql_pool_max_size` | `int` | 最大连接数（自动扩容上限）；0 表示按 `pool_size` 推导。 |
| `mysql_idle_timeout_ms` | `int` | 空闲连接回收阈值，毫秒。 |

Comet：

| 成员 | 类型 | 用途 |
|---|---|---|
| `comet_id` | `std::string` | 当前 Comet 的唯一编号，如 `comet-1`；用于 `conn_id` 和路由值。 |
| `comet_targets` | `std::string` | Job 使用的 Comet 地址表，格式 `comet-1=127.0.0.1:9205,comet-2=...`。 |
| `comet_io_threads` | `int` | Comet 的 asio IO 线程数，至少 1。 |
| `comet_grpc_pool_size` | `int` | Comet 内部异步 gRPC 调用线程池大小。 |
| `comet_grpc_port` | `int` | Comet 给 Job 调用的 gRPC 端口；未显式配置时由 `listen_port + 100` 推导。 |
| `use_grpc_stream` | `bool` | Comet → Logic 是否使用双向流；`false` 时走 unary RPC。 |
| `grpc_stream_count` | `int` | 双向流条数，多流可提高吞吐。 |
| `use_push_stream` | `bool` | Job → Comet 是否使用客户端流；`false` 时走 unary。 |

Job、HTTP、管理员、日志：

| 成员 | 类型 | 用途 |
|---|---|---|
| `job_worker_threads` | `int` | 每个 Job Kafka consumer 的并行工作线程数。 |
| `job_max_inflight` | `int` | 每个 consumer 的最大在途消息数，达到后暂停拉取形成背压。 |
| `http_threads` | `int` | Logic HTTP 服务器的 IO 线程数。 |
| `admin_account` | `std::string` | 管理员账号。 |
| `admin_password_hash` | `std::string` | 管理员密码的 MD5 值。 |
| `admin_user_id` | `int64_t` | 管理员用户 ID，默认 `900000000000`。 |
| `log_level` | `int` | 日志级别：0 TRACE、1 DEBUG、2 INFO、3 WARN、4 ERROR、5 FATAL。 |
| `cors_allow_origin` | `std::string` | HTTP CORS 允许的来源，默认 `*`。 |

### 13.2 logging

**函数**

| 函数 | 一句话作用 |
|---|---|
| `InitLogging(program, also_stdout, max_file_bytes, log_level)` | 创建 `logs/<program>.log` 的 logger，设置轮转、输出目标和级别。 |
| `ShutdownLogging()` | flush 并销毁全局 logger。 |
| `ShouldLog(level)` | 判断当前级别是否放行，供日志宏做热路径短路。 |
| `LogLine(level, msg)` | 按级别输出一行；logger 未初始化时回退到 `stderr`。 |
| `LogTrace/LogDebug/LogInfo/LogWarn/LogError/LogFatal` | 各日志级别的便捷入口，内部先 `ShouldLog` 再 `LogLine`。 |
| `RedactToken(token, keep)` | 记录 token/密钥时只保留前 `keep` 位，其余用 `...` 代替。 |
| `LogStream::operator<<` | 把一段内容追加到流式日志缓冲区。 |
| `LogStream::~LogStream` | 析构时把缓冲区一次性写入 `LogLine`。 |
| `ToSpdLevel(level)`（匿名命名空间） | 项目日志级别转 spdlog 级别。 |

**`LogStream` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `level_` | `int` | 本条日志的级别。 |
| `oss_` | `std::ostringstream` | 暂存 `<<` 拼接出来的内容，析构时统一输出。 |

### 13.3 thread_pool

**函数**

| 函数 | 一句话作用 |
|---|---|
| `ThreadPool(thread_num, name)` | 创建线程池；`thread_num=0` 时回退到硬件并发数。 |
| `~ThreadPool()` | 调用 `Stop()`，保证线程回收。 |
| `Start()` | 创建并启动 `thread_num_` 个工作线程。 |
| `Stop()` | 停止接收新任务，等待队列中的任务执行完，再 join 所有线程。 |
| `Submit(task)` | 把任务放入队列并唤醒一个工作线程；停止过程中会丢弃并告警。 |
| `WorkerLoop(worker_id)` | 工作线程主循环：等待任务、取出、执行、捕获异常。 |
| `size()` | 返回线程数。 |
| `ResolveThreadNum(n)`（匿名命名空间） | `0` 回退到 `hardware_concurrency()`，并保证至少 1。 |

**成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `thread_num_` | `size_t` | 工作线程数量。 |
| `name_` | `std::string` | 线程池名字，用于日志。 |
| `workers_` | `std::vector<std::thread>` | 工作线程对象。 |
| `tasks_` | `std::queue<std::function<void()>>` | 待执行任务队列。 |
| `mutex_` | `std::mutex` | 保护 `tasks_` 和 `stopping_`。 |
| `cv_` | `std::condition_variable` | 任务到达/停止时唤醒工作线程。 |
| `stopping_` | `bool` | 是否进入停止流程。 |

### 13.4 mysql_pool

**函数**

| 函数 | 一句话作用 |
|---|---|
| `MySqlConnGuard(pool, conn)` | 保存连接池和裸连接，析构时自动归还。 |
| `MySqlConnGuard(MySqlConnGuard&&)` | 移动构造：转移连接所有权，避免重复归还。 |
| `operator=(MySqlConnGuard&&)` | 移动赋值：先归还自己持有的连接，再接管对方的。 |
| `~MySqlConnGuard()` | 析构时调用 `pool_->Release(conn_)` 归还连接。 |
| `get()` | 返回裸 `MYSQL*`，供 DAO 执行 SQL。 |
| `MySqlConnectionPool::Init(cfg)` | 保存配置、推导 min/max、创建 `min_pool_size` 个连接。 |
| `Acquire(timeout_ms)` | 借一个连接；无空闲时自动扩容或等待；超时返回空 Guard。 |
| `Stop()` | 进入停止状态，唤醒等待者，拒绝新的获取请求。 |
| `CreateConnectionUnlocked()` | 创建 MySQL 连接，设置连接/读写超时、字符集、自动重连。 |
| `PopIdleConnectionUnlocked()` | 先清理超时空闲连接，再从 `idle_` 取一个。 |
| `CleanupIdleUnlocked()` | 回收超过 `idle_timeout_ms` 的空闲连接，但不低于最小连接数。 |
| `ValidateConnection(&conn)` | 按 `ping_on_borrow` 决定是否 `mysql_ping`；坏连接关闭并置空。 |
| `Release(conn)` | 归还连接；池已停止时直接关闭。 |

**`MySqlConfig` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `host` / `port` | `std::string` / `int` | MySQL 地址和端口。 |
| `user` / `password` / `db` | `std::string` | 登录用户名、密码、数据库名。 |
| `pool_size` | `int` | 目标连接数。 |
| `min_pool_size` | `int` | 最小连接数；`<=0` 时按 `pool_size` 推导。 |
| `max_pool_size` | `int` | 自动扩容上限；`<=0` 时按 `pool_size` 推导。 |
| `idle_timeout_ms` | `int` | 空闲连接回收阈值。 |
| `enable_auto_grow` | `bool` | 空闲连接不足时是否允许动态扩容。 |
| `ping_on_borrow` | `bool` | 借出连接时是否 `mysql_ping` 做健康检查。 |
| `connect_timeout_ms` | `int` | 建连超时。 |
| `rw_timeout_ms` | `int` | 读写超时。 |
| `charset` | `std::string` | 连接字符集，默认 `utf8mb4`。 |

**`ConnEntry`（池内部）**

| 成员 | 类型 | 用途 |
|---|---|---|
| `conn` | `MYSQL*` | 空闲连接指针。 |
| `last_used` | `steady_clock::time_point` | 上次归还时间，用于空闲回收。 |

**`MySqlConnectionPool` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `mutex_` | `std::mutex` | 保护 `idle_`、`total_conns_`、`stopping_`。 |
| `cv_` | `std::condition_variable` | 连接归还或停止时唤醒等待者。 |
| `idle_` | `std::deque<ConnEntry>` | 空闲连接队列。 |
| `total_conns_` | `int` | 当前总连接数（空闲 + 借出）。 |
| `stopping_` | `bool` | 池是否正在停止。 |
| `inited_` | `bool` | 是否已经成功初始化。 |
| `cfg_` | `MySqlConfig` | 初始化时保存的配置。 |

### 13.5 redis_pool

**函数**

| 函数 | 一句话作用 |
|---|---|
| `RedisConnGuard(pool, ctx)` | 保存连接池和 `redisContext*`，析构时自动归还。 |
| `RedisConnGuard(RedisConnGuard&&)` | 移动构造：转移连接所有权。 |
| `operator=(RedisConnGuard&&)` | 移动赋值：先归还自己的连接，再接管对方的。 |
| `~RedisConnGuard()` | 析构时归还连接；坏连接交给池销毁。 |
| `get()` | 返回裸 `redisContext*`。 |
| `MarkBroken()` | 标记连接已损坏，析构时销毁而不是归还。 |
| `RedisConnectionPool::Init(cfg)` | 保存配置、推导 min/max、创建 `min_pool_size` 个连接。 |
| `Acquire(timeout_ms)` | 借连接；无空闲时自动扩容或等待；超时返回空 Guard。 |
| `CreateContextUnlocked()` | 建连、AUTH、SELECT、设置读写超时。 |
| `PopIdleUnlocked()` | 先清理超时空闲连接，再取一个。 |
| `CleanupIdleUnlocked()` | 回收超过 `idle_timeout_ms` 的空闲连接。 |
| `Release(ctx, broken)` | 归还连接；`broken`、`ctx->err` 或池停止时直接销毁。 |

**`RedisConfig` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `host` / `port` | `std::string` / `int` | Redis 地址和端口。 |
| `password` | `std::string` | Redis 密码，空表示不 AUTH。 |
| `db` | `int` | 数据库编号。 |
| `pool_size` | `int` | 目标连接数。 |
| `min_pool_size` | `int` | 最小连接数；`<=0` 时按 `pool_size` 推导。 |
| `max_pool_size` | `int` | 自动扩容上限；`<=0` 时按 `pool_size` 推导。 |
| `connect_timeout_ms` | `int` | 建连超时。 |
| `rw_timeout_ms` | `int` | 读写超时。 |
| `idle_timeout_ms` | `int` | 空闲连接回收阈值。 |
| `enable_auto_grow` | `bool` | 是否允许动态扩容。 |

**`ConnEntry`（池内部）**

| 成员 | 类型 | 用途 |
|---|---|---|
| `ctx` | `redisContext*` | 空闲 Redis 连接。 |
| `last_used` | `steady_clock::time_point` | 上次归还时间，用于空闲回收。 |

**`RedisConnectionPool` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `mutex_` | `std::mutex` | 保护 `idle_`、`total_conns_`、`stopping_`。 |
| `cv_` | `std::condition_variable` | 连接归还或停止时唤醒等待者。 |
| `idle_` | `std::deque<ConnEntry>` | 空闲连接队列。 |
| `total_conns_` | `int` | 当前总连接数。 |
| `stopping_` | `bool` | 池是否正在停止。 |
| `cfg_` | `RedisConfig` | 初始化时保存的配置。 |

### 13.6 Kafka

**`KafkaProducer` 函数**

| 函数 | 一句话作用 |
|---|---|
| `~KafkaProducer()` | 析构时 `flush(3000)`，尽量把缓冲消息发出去。 |
| `Init(brokers, topic)` | 创建全局配置、Producer 和 Topic；设置低延迟参数。 |
| `Send(key, value)` | 异步发送一条消息；失败返回 `false`；`poll(0)` 驱动回调。 |

**`KafkaProducer` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `producer_` | `unique_ptr<RdKafka::Producer>` | librdkafka 生产者。 |
| `topic_` | `unique_ptr<RdKafka::Topic>` | 目标 topic 句柄。 |
| `topic_name_` | `std::string` | topic 名字，用于日志和 client.id。 |

**`KafkaConsumer` 函数**

| 函数 | 一句话作用 |
|---|---|
| `~KafkaConsumer()` | 调用 `Stop()`。 |
| `Init(brokers, group_id, topic, callback, options)` | 创建 KafkaConsumer、订阅 topic、创建 worker 线程池。 |
| `Start()` | 启动 worker 池和消费线程 `Loop()`。 |
| `Stop()` | 停止消费，等待在途任务完成，关闭 consumer。 |
| `Loop()` | 消费主循环：DrainCompletions、背压暂停、`consume(10)`、分发消息。 |
| `HandleMessage(message)` | 把 Kafka 消息封装成 `WorkItem`，按 partition 放入 `pending`。 |
| `SchedulePartition(key)` | 取该 partition 队首任务提交给线程池，保证同一 partition 串行。 |
| `SetPartitionsPaused(paused)` | 在途数达到上限时 pause 全部分区，降下来后 resume。 |
| `DrainCompletions()` | 处理已完成任务：出队、减 inflight、提交 offset、调度下一条。 |

**`KafkaConsumer::Options` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `enable_auto_commit` | `bool` | 是否让 Kafka 自动提交 offset；项目默认 `false`，手动提交。 |
| `auto_commit_interval_ms` | `int` | 自动提交间隔（如果启用）。 |
| `max_poll_interval_ms` | `int` | 两次 poll 的最大间隔，超过会被踢出消费组。 |
| `session_timeout_ms` | `int` | 消费者会话超时。 |
| `auto_offset_reset` | `std::string` | 无 offset 时从哪开始：`earliest` / `latest`。 |
| `worker_threads` | `size_t` | 每个 consumer 的工作线程数。 |
| `max_inflight` | `size_t` | 最大在途消息数，达到后暂停拉取形成背压。 |

**`KafkaConsumer::PartitionKey`**

| 成员 | 类型 | 用途 |
|---|---|---|
| `topic` | `std::string` | topic 名。 |
| `partition` | `int32_t` | 分区号。 |

重载了 `operator<`，所以可以放进 `std::map`，按 `(topic, partition)` 排序。

**`KafkaConsumer::WorkItem`**

| 成员 | 类型 | 用途 |
|---|---|---|
| `partition_key` | `PartitionKey` | 这条消息属于哪个 topic/partition。 |
| `offset` | `int64_t` | Kafka offset，提交时用 `offset + 1`。 |
| `key` | `std::string` | 消息 key。 |
| `value` | `std::string` | 消息体。 |

**`KafkaConsumer::PartitionState`**

| 成员 | 类型 | 用途 |
|---|---|---|
| `pending` | `deque<shared_ptr<WorkItem>>` | 该 partition 待处理的消息队列，保证 FIFO。 |
| `active` | `bool` | 该 partition 是否已有一个任务在工作线程中执行。 |

**`KafkaConsumer` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `consumer_` | `unique_ptr<RdKafka::KafkaConsumer>` | librdkafka 消费者。 |
| `topic_` | `std::string` | 订阅的 topic。 |
| `callback_` | `function<void(const string&, const string&)>` | 业务处理回调，参数是 key/value。 |
| `options_` | `Options` | 初始化时保存的配置。 |
| `workers_` | `unique_ptr<ThreadPool>` | 执行 callback 的工作线程池。 |
| `partitions_` | `map<PartitionKey, PartitionState>` | 每个 partition 的待处理队列和活跃状态。 |
| `inflight_` | `size_t` | 当前正在处理的消息数。 |
| `completion_mutex_` | `mutex` | 保护完成队列。 |
| `completion_cv_` | `condition_variable` | 完成通知。 |
| `completions_` | `deque<PartitionKey>` | 已完成 partition 的队列，等待主循环提交 offset。 |
| `partitions_paused_` | `bool` | 当前是否处于暂停拉取状态。 |
| `running_` | `atomic<bool>` | 是否正在运行。 |
| `thread_` | `std::thread` | 消费线程。 |

### 13.7 timer_wheel 与 local_timer

**`TimerWheel` 函数**

| 函数 | 一句话作用 |
|---|---|
| `instance()` | 返回全局单例时间轮。 |
| `TimerWheel(tick)` | 校验 tick 合法并启动 worker 线程。 |
| `~TimerWheel()` | 停止 worker，释放所有时间轮节点。 |
| `add_timer(delay)` | 创建一个定时器节点，插入时间轮，返回 `TimerToken`。 |
| `worker_loop()` | 等待最近到期时间，推进时间轮，标记到期节点。 |
| `link(bucket, node)` | 把节点尾插到桶链表。 |
| `clear(bucket)` | 摘下桶里的整条链表并重置桶。 |
| `add_node_locked(node, base_tick)` | 按到期 tick 把节点放入 near 或某一层。 |
| `add_node_locked(node)` | 以当前 tick 为基准插入节点。 |
| `move_list_locked(level, index)` | 把某层某槽的节点重新插入时间轮。 |
| `shift_locked()` | 推进一个 tick，并做上层槽的级联。 |
| `collect_current_slot_locked(expired)` | 收集当前 near 槽里到期的节点。 |
| `advance_to_locked(now, expired)` | 把时间轮推进到 `now` 对应的 tick，收集经过的到期节点。 |
| `ticks_from_delay(delay)` | 把毫秒延迟向上取整成 tick 数。 |
| `absolute_tick_from_time(time)` | 把时间点转换成绝对 tick。 |
| `tick_to_time(tick)` | 把相对 tick 转换成时间点。 |
| `absolute_tick_to_time(tick)` | 把绝对 tick 转换成时间点。 |
| `level_cascade_tick(level, index)` | 计算某层某槽下一次级联的 tick。 |
| `next_wait_time_locked()` | 返回下一次需要唤醒的时间；没有任务时返回 `time_point::max()`。 |
| `rebuild_next_wakeup_locked()` | 重新扫描并记录最近的非空槽。 |
| `mark_expired(expired)` | 把到期节点的 `TimerState::ready` 置 true，并释放节点。 |

**`LocalTimer` 函数**

| 函数 | 一句话作用 |
|---|---|
| `LocalTimer()` | 获取全局 `TimerWheel` 单例。 |
| `~LocalTimer()` | 取消尚未触发的定时器。 |
| `schedule(delay, callback)` | 取消旧定时器，注册新延迟和新回调。 |
| `cancel()` | 标记当前定时器取消。 |
| `execute_if_ready()` | 如果到期且未取消，执行回调并返回 `true`。 |
| `pending()` | 是否还有未处理的定时器。 |

**`TimerState` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `ready` | `atomic_bool` | 时间轮是否已经把该定时器标记为到期。 |
| `cancelled` | `atomic_bool` | 调用方是否已经取消该定时器。 |

**`TimerWheel::TimerToken` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `id` | `TimerId` | 定时器编号。 |
| `state` | `shared_ptr<TimerState>` | 与时间轮共享的到期/取消状态。 |

**`TimerWheel::TimerNode` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `next` | `TimerNode*` | 同一桶链表中的下一个节点。 |
| `id` | `TimerId` | 定时器编号。 |
| `expire_tick` | `uint32_t` | 到期 tick。 |
| `state` | `shared_ptr<TimerState>` | 到期状态，供 `LocalTimer` 查询。 |

**`TimerWheel::Bucket` 字段**

| 成员 | 类型 | 用途 |
|---|---|---|
| `head` | `TimerNode` | 哨兵头节点，不存真实数据。 |
| `tail` | `TimerNode*` | 尾指针，支持 O(1) 尾插。 |

**`TimerWheel` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `tick_` | `milliseconds` | 时间轮最小刻度，默认 10ms。 |
| `start_time_` | `steady_clock::time_point` | 时间轮起点，用于绝对 tick 换算。 |
| `mutex_` | `mutex` | 保护所有桶和 tick 状态。 |
| `cv_` | `condition_variable` | 唤醒 worker 线程。 |
| `stopping_` | `bool` | 是否停止。 |
| `near_` | `Bucket[256]` | 最近 256 个 tick 的细粒度槽。 |
| `levels_` | `Bucket[4][64]` | 4 层、每层 64 槽的粗粒度时间轮。 |
| `current_tick_` | `uint32_t` | 当前相对 tick。 |
| `current_absolute_tick_` | `uint64_t` | 当前绝对 tick（从 `start_time_` 起）。 |
| `next_wakeup_tick_` | `uint32_t` | 下一次需要唤醒的 tick。 |
| `has_next_wakeup_` | `bool` | 是否存在下一个唤醒点。 |
| `next_id_` | `atomic<TimerId>` | 定时器编号生成器。 |
| `worker_` | `std::thread` | 时间轮工作线程。 |

**`LocalTimer` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `timer_` | `TimerWheel&` | 全局时间轮引用。 |
| `state_` | `shared_ptr<TimerState>` | 当前定时器的到期/取消状态。 |
| `callback_` | `std::function<void()>` | 到期时执行的回调。 |

### 13.8 http_server_lite

**`HttpRequest` 函数**

| 函数 | 一句话作用 |
|---|---|
| `method()` | 返回请求方法枚举。 |
| `path()` | 返回路径（已去掉 `?query`）。 |
| `body()` | 返回请求体。 |
| `getHeader(name)` | 按 header 名大小写不敏感地取值。 |
| `set_method/set_path/set_body/add_header/clear` | 服务器解析时填充请求，或复用对象前清空。 |

**`HttpResponse` 函数**

| 函数 | 一句话作用 |
|---|---|
| `setStatusCode(...)` | 设置状态码，支持枚举和 `int` 重载。 |
| `setContentType(...)` | 设置 `Content-Type`。 |
| `addHeader(...)` | 追加一个响应头。 |
| `setBody(...)` | 设置响应体，支持拷贝和移动。 |
| `status_code/content_type/body/headers` | 读取响应字段，供序列化使用。 |

**`HttpServerLite` 函数**

| 函数 | 一句话作用 |
|---|---|
| `HttpServerLite(port, name)` | 创建 acceptor，绑定端口并 listen。 |
| `~HttpServerLite()` | 调用 `stop()`。 |
| `setHttpCallback(cb)` | 设置业务请求回调。 |
| `setThreadNum(n)` | 设置 IO 线程数，至少 1。 |
| `run()` | 启动 accept，阻塞在 `io_context` 上直到停止。 |
| `stop()` | 关闭 acceptor 并停止 `io_context`。 |

**内部函数/结构**

| 名称 | 一句话作用 |
|---|---|
| `ToLower(s)` | 把字符串转小写，用于 header 名比较。 |
| `ReasonPhrase(code)` | 状态码转 `OK/Bad Request/...`。 |
| `MapMethod(m)` | llhttp 方法常量转 `HttpRequest::Method`。 |
| `OnMessageBegin/OnUrl/OnHeaderField/OnHeaderValue/OnHeadersComplete/OnBody/OnMessageComplete` | llhttp 回调，增量填充 `ParseCtx` 和 `HttpRequest`。 |
| `BuildResponse(resp, keep_alive)` | 序列化状态行、headers、body，并补 `Content-Length` / `Connection`。 |
| `Session::Start/DoRead` | 读循环：读 socket → `llhttp_execute` → 回调 → 写响应。 |
| `WriteThen(data, keep_alive)` | 异步写响应；keep-alive 则继续读，否则关闭。 |
| `WriteAndClose(data)` | 异步写完并关闭连接。 |
| `DoAccept(impl)` | 递归 accept，新连接创建 `Session`。 |

**`HttpRequest` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `method_` | `Method` | 请求方法，默认 `kInvalid`。 |
| `path_` | `std::string` | 请求路径，不含 query。 |
| `body_` | `std::string` | 请求体。 |
| `headers_` | `vector<pair<string,string>>` | 原始 header 列表，保留顺序。 |

**`HttpResponse` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `status_` | `int` | HTTP 状态码，默认 200。 |
| `content_type_` | `std::string` | `Content-Type`。 |
| `body_` | `std::string` | 响应体。 |
| `headers_` | `vector<pair<string,string>>` | 额外响应头。 |

**`ParseCtx` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `req` | `HttpRequest` | 当前正在解析的请求。 |
| `cur_header_field` | `std::string` | llhttp 分段回调时暂存 header 名。 |
| `cur_header_value` | `std::string` | llhttp 分段回调时暂存 header 值。 |
| `complete` | `bool` | 当前请求是否解析完成。 |
| `keep_alive` | `bool` | 是否保持连接。 |

**`Session` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `socket_` | `asio::ip::tcp::socket` | 客户端 TCP socket。 |
| `callback_` | `HttpCallback` | 业务请求处理函数。 |
| `parser_` | `llhttp_t` | llhttp 解析器实例。 |
| `settings_` | `llhttp_settings_t` | llhttp 回调表。 |
| `ctx_` | `ParseCtx` | 当前请求的解析上下文。 |
| `buf_` | `std::array<char, 8192>` | 读缓冲区。 |

**`HttpServerLite::Impl` 成员**

| 成员 | 类型 | 用途 |
|---|---|---|
| `io_ctx` | `asio::io_context` | 事件循环。 |
| `acceptor` | `asio::ip::tcp::acceptor` | 监听 socket。 |
| `callback` | `HttpCallback` | 业务回调。 |
| `name` | `std::string` | 服务器名字。 |
| `threads` | `int` | IO 线程数。 |
| `stopped` | `atomic<bool>` | 是否停止。 |

### 13.9 signal_handler

| 函数 | 一句话作用 |
|---|---|
| `ShutdownRequested()` | 返回全局 `atomic<bool>` 关闭标志。 |
| `InstallSignalHandler(on_signal)` | 注册 `SIGTERM/SIGINT`：置关闭标志并调用可选回调。 |

## 14. 业务模块结构体与成员字段速查

> 本章覆盖 Comet / Logic / Job 三个业务进程里“项目自己定义”的结构体和关键类成员。protobuf 生成的消息字段放在 14.7。
>
> 读代码时建议配合 **7.9 阶段 4～6**：先看 `ConnContext` 和 `ConnRegistry`，再看 `StreamState`，最后看 Logic 的数据模型。

### 14.1 Comet：连接与心跳

#### `ConnContext`

**作用**：一条 WebSocket 连接的业务上下文。握手 `OnValidate` 时创建，挂到 `WsConnectionBase::context` 上，后续 `OnOpen/OnMessage/OnClose/心跳` 都靠它拿用户和连接信息。

| 成员 | 类型 | 用途 |
|---|---|---|
| `user_id` | `int64_t` | 握手鉴权成功后由 Logic 返回的用户 ID；0 表示尚未鉴权。 |
| `conn_id` | `std::string` | 连接唯一编号，格式 `comet_id:sequence`，如 `comet-1:7`。 |
| `handle` | `ConnHandle` | websocketpp 的连接句柄，用于发消息、ping、close。 |
| `last_pong_ms` | `std::atomic<int64_t>` | 最近一次收到 pong 的毫秒时间戳；心跳线程据此判断是否超时。 |
| `closed` | `std::atomic<bool>` | 是否已经关闭；防止 `OnClose` 和心跳重复清理。 |
| `heartbeat_bucket` | `uint8_t` | 该连接属于 64 个心跳桶中的哪一个，由连接序号取模得到。 |

**生命周期**：

```text
OnValidate 创建 shared_ptr<ConnContext>
   ↓ 挂到 connection 上
OnOpen 加入 ConnRegistry + 心跳桶
   ↓ 收消息 / pong 更新 last_pong_ms
OnClose 标记 closed、移除路由、通知 Logic
   ↓
最后一个 shared_ptr 释放，ConnContext 销毁
```

#### `WsConnectionBase`

**作用**：websocketpp 允许自定义“每条连接都带一个基类对象”。这里把 `std::shared_ptr<ConnContext> context` 塞进去，于是每个 WebSocket 连接都能直接访问自己的业务上下文。

| 成员 | 类型 | 用途 |
|---|---|---|
| `context` | `std::shared_ptr<ConnContext>` | 当前连接的业务上下文；未鉴权完成前可能为空。 |

#### `WsConfig`

**作用**：继承 `websocketpp::config::asio`，只改一件事：把 `connection_base` 换成 `WsConnectionBase`。这样 `websocketpp::server<WsConfig>` 创建出来的连接就自带 `context` 字段。

| 成员/别名 | 类型 | 用途 |
|---|---|---|
| `connection_base` | `WsConnectionBase` | 替换 websocketpp 默认的连接基类，让每条连接携带 `ConnContext`。 |

`using WsServer = websocketpp::server<WsConfig>;` 就是最终使用的 WebSocket 服务器类型。

#### `HeartbeatEntry`

**作用**：心跳桶里保存的一条记录。只存“怎么找到连接”和“怎么看连接状态”，不持有连接生命周期。

| 成员 | 类型 | 用途 |
|---|---|---|
| `handle` | `ConnHandle` | 用来发 ping / close。 |
| `context` | `std::weak_ptr<ConnContext>` | 读取 `last_pong_ms` 和 `closed`；用 `weak_ptr` 避免心跳桶阻止连接销毁。 |

#### `HeartbeatBucket`

**作用**：64 个分片之一。每个桶有自己的锁，心跳线程每 500ms 只扫一个桶，避免一次锁住全部连接。

| 成员 | 类型 | 用途 |
|---|---|---|
| `mutex` | `std::mutex` | 保护该桶的 `entries`；只有 `OnOpen` 插入和心跳扫描会碰它。 |
| `entries` | `std::vector<HeartbeatEntry>` | 该桶当前管理的连接列表。 |

#### `CometServer` 关键成员

| 成员 | 类型 | 用途 |
|---|---|---|
| `cfg_` | `Config` | 当前 Comet 的配置副本。 |
| `ws_` | `WsServer` | websocketpp 服务器，负责监听、握手、收发帧。 |
| `registry_` | `ConnRegistry` | 用户/房间 → 连接句柄的索引。 |
| `io_threads_` | `int` | asio IO 线程数。 |
| `io_workers_` | `vector<std::thread>` | 真正跑 `io_context.run()` 的线程。 |
| `heartbeat_thread_` | `unique_ptr<std::thread>` | 独立心跳线程。 |
| `heartbeat_wait_mutex_` | `mutex` | 心跳线程等待/唤醒用。 |
| `heartbeat_wait_cv_` | `condition_variable` | 停止时立即唤醒心跳线程。 |
| `heartbeat_stopping_` | `atomic<bool>` | 心跳线程是否该退出。 |
| `stopped_` | `atomic<bool>` | 整个 Comet 是否已停止，保证 `Stop()` 幂等。 |
| `logic_stub_` | `unique_ptr<LogicService::Stub>` | 调用 Logic 的 unary gRPC 客户端。 |
| `comet_id_` | `std::string` | 当前节点编号，写入 `conn_id` 和路由。 |
| `grpc_pool_` | `ThreadPool` | 执行 `UserOffline/ReportRoomJoin/ReportRoomLeave/SendUpstreamMessage` 等异步 gRPC 调用。 |
| `use_stream_` | `bool` | 是否使用与 Logic 的双向流。 |
| `stream_count_` | `int` | 双向流条数。 |
| `channel_` | `shared_ptr<grpc::Channel>` | Comet → Logic 的 gRPC 连接通道。 |
| `stream_client_` | `unique_ptr<LogicStreamClient>` | 双向流客户端，管理多条流和读写线程。 |
| `conn_id_counter_` | `atomic<uint64_t>` | 连接序号生成器，生成 `comet_id:seq`。 |
| `heartbeat_buckets_` | `array<HeartbeatBucket, 64>` | 64 个心跳分片桶。 |
| `next_heartbeat_bucket_` | `size_t` | 下一次要扫描的桶下标，循环 0～63。 |

### 14.2 Comet：连接注册表

#### `ConnHandle`

```cpp
using ConnHandle = websocketpp::connection_hdl;
```

本质是 `std::weak_ptr<void>`，用来标识一条 websocketpp 连接。不能直接比较大小，所以需要下面的 `ConnHandleLess`。

#### `ConnHandleLess`

**作用**：给 `std::set<ConnHandle>` 和 `std::map<ConnHandle, ...>` 提供比较器。内部用 `std::owner_less` 按控制块地址比较，保证同一个连接句柄的排序稳定。

| 成员 | 类型 | 用途 |
|---|---|---|
| `operator()` | 比较函数 | 判断两个 `ConnHandle` 的先后顺序。 |

#### `ConnRegistry::UserShard`

**作用**：用户维度索引的一个分片：`user_id -> 该用户的所有连接`。一个用户可能有多设备/多标签页，所以值是 `set<ConnHandle>`。

| 成员 | 类型 | 用途 |
|---|---|---|
| `mutex` | `mutable std::mutex` | 保护 `conns`；`mutable` 让 `const` 查询也能加锁。 |
| `conns` | `unordered_map<int64_t, set<ConnHandle, ConnHandleLess>>` | 用户 ID → 连接集合。 |

#### `ConnRegistry::RoomShard`

**作用**：房间维度索引的一个分片：`room_id -> 房间内用户集合`。用于聊天室广播时快速找到本机要推送的用户。

| 成员 | 类型 | 用途 |
|---|---|---|
| `mutex` | `mutable std::mutex` | 保护 `users`。 |
| `users` | `unordered_map<int64_t, set<int64_t>>` | 房间 ID → 用户 ID 集合。 |

#### `ConnRegistry` 成员

| 成员 | 类型 | 用途 |
|---|---|---|
| `kUserShardCount` | `static constexpr size_t = 64` | 用户索引分片数。 |
| `kRoomShardCount` | `static constexpr size_t = 64` | 房间索引分片数。 |
| `user_shards_` | `array<UserShard, 64>` | 用户索引的 64 个分片。 |
| `room_shards_` | `array<RoomShard, 64>` | 房间索引的 64 个分片。 |
| `handle_mutex_` | `mutable std::mutex` | 保护 `hdl_to_uid_`。 |
| `hdl_to_uid_` | `map<ConnHandle, int64_t, ConnHandleLess>` | 连接句柄 → 用户 ID 的反向索引，`OnClose` 时反查用户。 |

**为什么分 64 片**：所有连接共用一把锁会在高并发下排队；按 `user_id % 64` / `room_id % 64` 拆开，不同分片可以并行读写。

### 14.3 Comet：与 Logic 的双向流客户端

#### `LogicStreamClient::PendingRequest`

**作用**：一条已经放进发送队列、但还没收到响应的请求。

| 成员 | 类型 | 用途 |
|---|---|---|
| `msg` | `StreamMessage` | 要发给 Logic 的 protobuf 消息。 |
| `callback` | `function<void(const StreamResponse&)>` | 收到对应 `request_id` 的响应后执行的回调。 |

#### `LogicStreamClient::StreamState`

**作用**：一条双向流的全部状态。每条流有独立的 gRPC 上下文、读写线程、发送队列和待回调表。

| 成员 | 类型 | 用途 |
|---|---|---|
| `ctx` | `unique_ptr<grpc::ClientContext>` | 该流的 gRPC 客户端上下文，控制生命周期和取消。 |
| `stream` | `unique_ptr<ClientReaderWriter<StreamMessage, StreamResponse>>` | 双向流对象。 |
| `writer_thread` | `std::thread` | 从 `send_queue` 取消息并 `Write()`。 |
| `reader_thread` | `std::thread` | 循环 `Read()` 响应，按 `request_id` 找回调。 |
| `send_queue` | `queue<PendingRequest>` | 待发送请求队列。 |
| `send_queue_mutex` | `std::mutex` | 保护 `send_queue`。 |
| `send_queue_cv` | `condition_variable` | 有新请求时唤醒 writer。 |
| `pending_callbacks_` | `unordered_map<uint64_t, function<void(const StreamResponse&)>>` | `request_id` → 回调；响应回来时取出并执行。 |
| `pending_callbacks_mutex` | `std::mutex` | 保护 `pending_callbacks_`。 |

#### `LogicStreamClient` 成员

| 成员 | 类型 | 用途 |
|---|---|---|
| `stub_` | `LogicService::Stub*` | 非拥有指针，由 `CometServer` 保证生命周期。 |
| `stream_count_` | `int` | 要建立的流条数。 |
| `streams_` | `vector<unique_ptr<StreamState>>` | 所有流的状态。 |
| `stream_running_` | `atomic<bool>` | 是否已成功启动。 |
| `stopped_` | `atomic<bool>` | `Stop()` 防重入标志。 |
| `request_id_counter_` | `atomic<uint64_t>` | 全局请求 ID 生成器，保证不同流之间也不冲突。 |

**工作方式**：

```text
Send(msg, cb)
  → 分配 request_id
  → 轮询选一条流
  → 放入该流 send_queue
  → writer 线程 Write()
  → reader 线程 Read() 到同 request_id 的响应
  → 从 pending_callbacks_ 取出 cb 并执行
```

### 14.4 Logic：数据模型

#### `User`

**作用**：对应 `user` 表的一行用户记录。

| 成员 | 类型 | 用途 |
|---|---|---|
| `id` | `int64_t` | 用户 ID，数据库自增主键。 |
| `account` | `std::string` | 登录账号，唯一。 |
| `name` | `std::string` | 显示昵称。 |
| `password_hash` | `std::string` | 密码哈希（当前项目用 MD5）。 |

#### `SessionType`

**作用**：会话类型枚举，决定 `session_id` 的格式和消息路由方式。

| 枚举值 | 数值 | 含义 |
|---|---|---|
| `kSingle` | 0 | 单聊，`session_id = single:<小uid>:<大uid>`。 |
| `kGroup` | 1 | 群聊。 |
| `kChatroom` | 2 | 聊天室/房间。 |

#### `Session`

**作用**：对应 `session` 表的一行会话记录。

| 成员 | 类型 | 用途 |
|---|---|---|
| `id` | `std::string` | 会话 ID，例如 `single:1:2`、`room:1001`。 |
| `type` | `SessionType` | 会话类型。 |
| `user1_id` | `int64_t` | 单聊用户 A；非单聊为 0。 |
| `user2_id` | `int64_t` | 单聊用户 B；非单聊为 0。 |
| `group_id` | `int64_t` | 群/聊天室 ID；单聊为 0。 |
| `last_msg_seq` | `int64_t` | 该会话当前最大消息序号，分配新序号时 +1。 |

#### `Message`

**作用**：一条聊天消息，对应 `message` 表的一行。

| 成员 | 类型 | 用途 |
|---|---|---|
| `msg_id` | `std::string` | 服务端生成的消息唯一 ID，通常是 `session_id + "-" + seq`。 |
| `session_id` | `std::string` | 所属会话。 |
| `msg_seq` | `int64_t` | 会话内严格递增序号，用于排序、历史拉取、已读计算。 |
| `sender_id` | `int64_t` | 发送者用户 ID。 |
| `timestamp_ms` | `int64_t` | 服务端消息时间戳，毫秒。 |
| `msg_type` | `std::string` | 消息类型：`text/system/danmaku/broadcast` 等。 |
| `content_json` | `std::string` | 消息内容 JSON 字符串。 |
| `client_msg_id` | `std::string` | 客户端生成的 ID，用于 ACK 匹配和去重。 |

#### `UserSessionState`

**作用**：某个用户在某个会话里的已读状态。

| 成员 | 类型 | 用途 |
|---|---|---|
| `read_seq` | `int64_t` | 用户已经读到的最新 `msg_seq`；未读数 = `last_msg_seq - read_seq`。 |

#### `ImGroup`

**作用**：对应 `im_group` 表的一行群/聊天室记录。当前项目主要用 `group_type=1` 表示聊天室。

| 成员 | 类型 | 用途 |
|---|---|---|
| `id` | `int64_t` | 群/聊天室 ID。 |
| `name` | `std::string` | 群/聊天室名称。 |
| `owner_id` | `int64_t` | 创建者/群主用户 ID。 |
| `group_type` | `int` | 群类型；1 表示聊天室。 |

#### `DanmakuItem`

**作用**：对应 `video_danmaku` 表的一行弹幕记录。

| 成员 | 类型 | 用途 |
|---|---|---|
| `id` | `int64_t` | 弹幕自增主键。 |
| `video_id` | `std::string` | 视频 ID。 |
| `timeline_ms` | `int64_t` | 弹幕出现在视频时间轴上的毫秒位置。 |
| `sender_id` | `int64_t` | 发送者用户 ID。 |
| `content_json` | `std::string` | 弹幕内容 JSON。 |
| `timestamp_ms` | `int64_t` | 弹幕发送时间戳，毫秒。 |

#### `RedisStore::RouteCacheEntry`

**作用**：Logic 本地缓存“用户路由到哪些 Comet”，减少每次推送都查 Redis。

| 成员 | 类型 | 用途 |
|---|---|---|
| `comets` | `vector<std::string>` | 去重后的 Comet ID 列表。 |
| `expire_time` | `steady_clock::time_point` | 本地缓存过期时间；默认 TTL 约 1 秒。 |

### 14.5 Job：推送流状态

#### `PushHandler::StreamState`

**作用**：Job 到某个 Comet 的一条客户端流（`PushStream`）的全部状态。每个 Comet 一条流，避免每条消息都新建 RPC。

| 成员 | 类型 | 用途 |
|---|---|---|
| `channel` | `shared_ptr<grpc::Channel>` | 到该 Comet 的 gRPC 通道。 |
| `stub` | `unique_ptr<CometService::Stub>` | 该 Comet 的 gRPC 客户端。 |
| `ctx` | `unique_ptr<grpc::ClientContext>` | 这条流的客户端上下文。 |
| `writer` | `unique_ptr<grpc::ClientWriter<PushToCometRequest>>` | 客户端流写入器。 |
| `response` | `PushStreamReply` | 流结束时 Comet 返回的响应，里面有 `received_count`。 |
| `writer_thread` | `std::thread` | 从 `send_queue` 取消息并 `Write()`。 |
| `send_queue` | `queue<PushToCometRequest>` | 待推送消息队列。 |
| `queue_mutex` | `std::mutex` | 保护 `send_queue`。 |
| `queue_cv` | `condition_variable` | 新消息到达时唤醒 writer。 |

#### `PushHandler` 关键成员

| 成员 | 类型 | 用途 |
|---|---|---|
| `cfg_` | `Config` | Job 配置副本。 |
| `push_consumer_` | `KafkaConsumer` | 消费 `push_to_comet` topic，负责实时推送。 |
| `broadcast_consumer_` | `KafkaConsumer` | 消费广播任务 topic。 |
| `comet_addrs_` | `unordered_map<string, string>` | `comet_id -> host:port`。 |
| `comet_stubs_` | `unordered_map<string, unique_ptr<CometService::Stub>>` | 每个 Comet 的 unary stub。 |
| `stub_mu_` | `std::mutex` | 保护 `comet_stubs_` 的创建和查找。 |
| `use_stream_` | `bool` | 是否启用客户端流模式。 |
| `stream_running_` | `atomic<bool>` | 流模式是否运行中。 |
| `streams_` | `unordered_map<string, unique_ptr<StreamState>>` | 每个 Comet 一条流的状态。 |

#### `PersistHandler` 关键成员

| 成员 | 类型 | 用途 |
|---|---|---|
| `cfg_` | `Config` | Job 配置副本。 |
| `persist_consumer_` | `KafkaConsumer` | 持久化消费组，消费同一 topic 但独立 offset。 |
| `mysql_pool_` | `unique_ptr<MySqlConnectionPool>` | 写 MySQL 的连接池。 |

### 14.6 关键服务类成员

#### `HttpApiServer`

| 成员 | 类型 | 用途 |
|---|---|---|
| `store_` | `ConversationStore*` | 会话/消息/未读的统一入口。 |
| `user_dao_` | `UserDao*` | 用户注册、登录查询。 |
| `group_dao_` | `GroupDao*` | 聊天室创建、查询。 |
| `group_member_dao_` | `GroupMemberDao*` | 聊天室成员关系。 |
| `redis_store_` | `RedisStore*` | token、路由、序号、房间在线状态。 |
| `kafka_producer_` | `KafkaProducer*` | 普通消息推送任务的生产者。 |
| `broadcast_producer_` | `KafkaProducer*` | 广播任务的生产者。 |
| `danmaku_dao_` | `DanmakuDao*` | 弹幕读写。 |
| `admin_account_` | `std::string` | 管理员账号。 |
| `admin_password_hash_` | `std::string` | 管理员密码哈希。 |
| `admin_user_id_` | `int64_t` | 管理员用户 ID。 |
| `server_` | `net::HttpServerLite` | 底层 HTTP 服务器。 |

#### `LogicServiceImpl`

| 成员 | 类型 | 用途 |
|---|---|---|
| `store_` | `ConversationStore*` | 会话、消息、序号、已读。 |
| `group_member_dao_` | `GroupMemberDao*` | 校验用户是否在房间/群里。 |
| `user_dao_` | `UserDao*` | 按 token 查用户。 |
| `producer_` | `KafkaProducer*` | 写推送任务 topic。 |
| `broadcast_producer_` | `KafkaProducer*` | 写广播任务 topic。 |
| `redis_store_` | `RedisStore*` | token 校验、路由注册/清理、房间在线状态。 |

#### `ConversationStore`

| 成员 | 类型 | 用途 |
|---|---|---|
| `session_dao_` | `SessionDao*` | 会话表的读写。 |
| `message_dao_` | `MessageDao*` | 消息表的读写。 |
| `state_dao_` | `UserSessionStateDao*` | 已读游标的持久化。 |
| `redis_store_` | `RedisStore*` | 消息序号分配、已读缓存、未读计算。 |

### 14.7 protobuf 核心消息字段

> 这些不是手写结构体，而是 `proto/spark_push.proto` 生成的消息类。C++ 里用 `msg.field()` 读、`msg.set_field()` 写。

#### `ErrorInfo`

| 字段 | 类型 | 用途 |
|---|---|---|
| `code` | `int32` | 0 表示成功；非 0 是业务错误码（如 401、403）。 |
| `message` | `string` | 错误原因。 |

#### `ChatMessage`

| 字段 | 类型 | 用途 |
|---|---|---|
| `msg_id` | `string` | 服务端消息唯一 ID。 |
| `session_id` | `string` | 会话 ID。 |
| `msg_seq` | `int64` | 会话内递增序号。 |
| `sender_id` | `int64` | 发送者 ID。 |
| `timestamp_ms` | `int64` | 消息时间戳。 |
| `msg_type` | `MsgType` | 消息类型枚举。 |
| `content_json` | `string` | 消息内容 JSON。 |
| `client_msg_id` | `string` | 客户端消息 ID，用于 ACK。 |

#### `VerifyTokenRequest` / `VerifyTokenReply`

| 消息 | 字段 | 类型 | 用途 |
|---|---|---|---|
| Request | `token` | `string` | 客户端握手带的 token。 |
| Request | `comet_id` | `string` | 来源 Comet 编号。 |
| Request | `conn_id` | `string` | 本次连接编号。 |
| Reply | `error` | `ErrorInfo` | 业务错误；成功时 `code=0`。 |
| Reply | `user_id` | `int64` | 校验成功后的用户 ID。 |

#### `UpstreamMessageRequest`

| 字段 | 类型 | 用途 |
|---|---|---|
| `from_user_id` | `int64` | 发送者 ID，由 Comet 从连接上下文填入。 |
| `client_msg_id` | `string` | 客户端消息 ID。 |
| `scene` | `Scene` | `single/chatroom/danmaku`。 |
| `to_user_id` | `int64` | 单聊目标用户。 |
| `group_id` | `int64` | 聊天室/群 ID。 |
| `video_id` | `string` | 弹幕视频 ID。 |
| `timeline_ms` | `int64` | 弹幕时间轴位置。 |
| `content_json` | `string` | 消息内容。 |
| `comet_id` | `string` | 来源 Comet。 |
| `conn_id` | `string` | 来源连接。 |
| `received_time_ms` | `int64` | Comet 收到消息的时间。 |

#### `PushToCometRequest`

| 字段 | 类型 | 用途 |
|---|---|---|
| `comet_id` | `string` | 目标 Comet。 |
| `message` | `ChatMessage` | 要推送的消息。 |
| `targets` | `repeated FanoutTarget` | 单聊目标用户列表；聊天室广播时为空，由 Comet 按房间查。 |
| `scene` | `Scene` | 消息场景。 |
| `to_user_id` | `int64` | 单聊目标。 |
| `room_id` | `int64` | 聊天室/弹幕房 ID。 |
| `need_persist` | `bool` | 是否需要 PersistHandler 落 MySQL。 |
| `persist_only` | `bool` | 只落盘不推送。 |

#### `StreamMessage` / `StreamResponse`

| 消息 | 字段 | 类型 | 用途 |
|---|---|---|---|
| `StreamMessage` | `request_id` | `uint64` | 请求 ID，用于匹配响应。 |
| `StreamMessage` | `payload` | `oneof` | 四选一：`upstream`、`offline`、`room_join`、`room_leave`。 |
| `StreamResponse` | `request_id` | `uint64` | 对应请求的 ID。 |
| `StreamResponse` | `error` | `ErrorInfo` | 业务错误。 |
| `StreamResponse` | `payload` | `oneof` | `upstream_reply` 或 `simple_reply`。 |

#### 其他常用消息

| 消息 | 关键字段 | 用途 |
|---|---|---|
| `UserOfflineRequest` | `user_id`、`comet_id`、`conn_id` | 连接关闭时清理该连接的路由。 |
| `RoomReportRequest` | `user_id`、`room_id`、`comet_id` | 上报房间加入/离开。 |
| `BroadcastRequest` | `scope`、`group_id`、`content_json` | HTTP 管理端触发广播。 |
| `BroadcastTaskRequest` | `task_id`、`scope`、`group_id`、`content_json` | Logic 写 Kafka、Job 消费的广播任务。 |
| `FanoutTarget` | `user_id` | 单聊推送目标。 |
| `PushStreamReply` | `received_count` | Job 客户端流结束时 Comet 返回的接收条数。 |

## 结语

这个项目的价值不在于“功能多”，而在于它把**长连接、异步解耦、可靠投递、路由、序号、连接池、并发控制**这些后端核心问题做了一次完整而克制的实现。建议的学习姿态是：

1. **先当用户**：把它跑起来，玩一遍。
2. **再当读者**：按第 7 章的阶段读，每读一段就画图。
3. **然后当侦探**：用 Redis/Kafka/MySQL/日志验证你的理解。
4. **最后当作者**：从 Level 1 的小改动开始，逐步做 Level 2/3。

只要你能独立讲清楚下面三个问题，这个项目就算学透了：

- 一条单聊消息从 alice 的浏览器到 bob 的浏览器，中间每一步发生了什么？失败了会怎样？
- 一个用户上线/下线时，Redis、Comet 内存、MySQL 分别发生了什么变化？
- 如果要把系统扩展成 3 个 Comet、2 个 Logic、4 个 Job，哪些地方会先成为瓶颈？为什么？

祝学习顺利。
