# MsgRail · 讯驿

> 一个基于 C++17 的分布式实时消息推送系统。  
> 浏览器通过 WebSocket 接入 Comet，消息经 Logic 路由后写入 Kafka，由 Job 异步推送与落盘。

MsgRail（讯驿）是一个类 IM 的消息推送系统，重点不在聊天界面，而在**长连接管理、消息路由、异步解耦和可靠投递**这套后端骨架。

## 功能

- **单聊**：用户之间的实时消息收发
- **聊天室**：房间加入/离开、房间广播、在线人数
- **弹幕**：按视频时间轴发送和拉取
- **广播**：全站广播、房间广播
- **HTTP API**：注册、登录、发消息、历史消息、未读/已读、会话列表、聊天室、弹幕、管理后台
- **可靠投递**：Kafka 分区有序、手动提交 offset、失败重试、死信日志
- **异步落盘**：MySQL 幂等写入，消息与会话状态保持一致

## 架构

```text
                        HTTP 9101
  浏览器 ──────────────────────────────► Logic ──────────► Kafka
     │                                      ▲               │
     │ WebSocket 9200                       │ gRPC          │
     ▼                                      │               ▼
   Comet ────────── gRPC MessageStream ─────┘              Job
     ▲                                                     │
     └────────── gRPC PushToComet / PushStream ◄───────────┤
                                                           │
                                                           └──► MySQL
```

一条单聊消息的完整链路：

```text
浏览器 A
  └─ WebSocket → Comet
       └─ gRPC → Logic
            ├─ Redis：鉴权、路由、会话序号
            ├─ Kafka：写入 push_to_comet
            └─ Job：消费 Kafka
                 ├─ gRPC → 目标 Comet → 浏览器 B
                 └─ MySQL：异步持久化
```

## 技术栈

| 层次 | 技术 |
|---|---|
| 语言 / 构建 | C++17、CMake |
| 网络 I/O | standalone Asio |
| WebSocket | websocketpp |
| HTTP | 自研 HTTP/1.1 服务器（Asio + llhttp） |
| RPC | gRPC + Protobuf |
| 消息队列 | Kafka（KRaft + librdkafka） |
| 缓存 | Redis + hiredis |
| 数据库 | MySQL 8 + mysqlclient |
| 日志 | spdlog |
| 测试 | doctest |

## 目录结构

```text
MsgRail/
├── proto/          # gRPC / Protobuf 协议定义
├── common/         # 配置、日志、线程池、Kafka、Redis、MySQL、HTTP 基础设施
├── logic/          # 业务层：HTTP API、LogicService、Redis 路由、DAO
├── comet/          # 接入层：WebSocket 网关、连接管理、CometService
├── job/            # 异步层：Kafka 消费、推送 Comet、持久化 MySQL
├── conf/           # logic.conf / comet.conf / job.conf
├── sql/            # 建库建表与测试数据
├── scripts/        # 一键启动、停止、状态检查
├── tests/          # 单元测试
├── load_test/      # WebSocket / HTTP 压测与命令行客户端
├── web_demo/       # 静态演示页面
└── docs/           # 源码解读、消息字段、请求清单等文档
```

## 快速开始

### 1. 环境要求

- Linux（开发环境为 Ubuntu 22.04）
- CMake >= 3.14、C++17 编译器
- gRPC、Protobuf、librdkafka、hiredis、mysqlclient
- MySQL 8、Redis、Kafka 4.x（KRaft，JDK 17+）
- `spdlog`、`llhttp`、`asio`、`websocketpp`、`nlohmann_json` 已安装到 `/usr/local`

> 根 `CMakeLists.txt` 锁定了 llhttp 9.2.1、Asio 1.30.2 和 websocketpp 0.8.2 的版本。

### 2. 初始化数据库

```bash
mysql -uroot -p123456 < sql/schema.sql
```

### 3. 编译

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

### 4. 启动服务

确保 MySQL 已启动，然后执行：

```bash
./scripts/start_all.sh
```

脚本会自动启动 Redis 和 Kafka（KRaft），创建 topic，并依次启动 Logic、Comet、Job 与 Web Demo。

```bash
./scripts/status.sh          # 查看服务状态
./scripts/stop_all.sh        # 只停止应用服务
./scripts/stop_all.sh --all  # 同时停止 Kafka 和 Redis
```

Kafka 默认路径是 `~/software/kafka_2.13-4.1.2`，可以通过环境变量覆盖：

```bash
KAFKA_HOME=/path/to/kafka ./scripts/start_all.sh
```

### 5. 打开演示页面

```text
http://127.0.0.1:9001/index.html
```

测试账号：`alice`、`bob`、`carol`、`dave`，密码均为 `123456`。

## 服务端口

| 服务 | 端口 | 协议 |
|---|---:|---|
| Logic gRPC | 9100 | gRPC |
| Logic HTTP | 9101 | HTTP |
| Comet WebSocket | 9200 | WebSocket |
| Comet gRPC | 9205 | gRPC |
| Web Demo | 9001 | HTTP |
| Kafka | 9092 | Kafka |
| Redis | 6379 | Redis |
| MySQL | 3306 | MySQL |

## HTTP API

| 分类 | 接口 |
|---|---|
| 认证 | `POST /api/login`、`POST /api/register` |
| 消息 | `POST /api/message/send`、`GET /api/session/history`、`GET /api/session/unread`、`POST /api/session/mark_read`、`GET /api/session/list_single` |
| 聊天室 | `/api/chatroom/join`、`/leave`、`/unsubscribe`、`/online_count`、`/list` |
| 弹幕 | `POST /api/danmaku/send`、`GET /api/danmaku/list` |
| 管理 | `/api/admin/chatroom/create`、`/api/admin/chatroom/list`、`/api/admin/broadcast` |

登录示例（密码字段传 MD5）：

```bash
curl -s http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}'
```

拿到 token 后建立 WebSocket 连接：

```text
ws://127.0.0.1:9200/ws?token=<token>
```

## 配置

| 文件 | 说明 |
|---|---|
| `conf/logic.conf` | Logic 的 HTTP/gRPC 端口、Kafka、Redis、MySQL、HTTP 线程数 |
| `conf/comet.conf` | WebSocket/gRPC 端口、Logic 地址、Comet ID、IO 线程数、gRPC 流开关 |
| `conf/job.conf` | Kafka 消费组、Comet 目标、消费线程数、在途上限、推送模式、MySQL |

关键开关：

- `use_grpc_stream`：Comet → Logic 使用长连接双向流
- `use_push_stream`：Job → Comet 使用客户端流；默认 `false`，使用带重试的 unary RPC
- `job_worker_threads`：Kafka 消费的工作线程数
- `job_max_inflight`：单 consumer 最大在途消息数，用于背压

## 可靠性设计

- 同一 Kafka partition 严格串行，不同 partition 并行处理
- 业务回调完成后才手动提交 offset，提供 **at-least-once** 语义
- 推送和持久化使用独立消费组，互不影响
- MySQL 使用 `INSERT ... ON DUPLICATE KEY UPDATE` 保证重复消费幂等
- 推送失败按 `100ms → 400ms → 1600ms` 退避重试，耗尽后记录 `[DLQ]` 日志
- Redis 保存 token、用户路由、会话序号和房间在线状态

## 测试

```bash
cd build
ctest --output-on-failure
```

压测和命令行客户端见 [`load_test/README.md`](load_test/README.md)。

## 文档

- [`docs/PROJECT_GUIDE_BEGINNER.md`](docs/PROJECT_GUIDE_BEGINNER.md)：新手版源码阅读指南
- [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md)：完整项目解析与二次开发路线
- [`docs/message.md`](docs/message.md)：消息字段全链路说明
- [`docs/requests.md`](docs/requests.md)：gRPC / HTTP / Kafka 请求清单与调用关系
- [`docs/protocol_ws_http.md`](docs/protocol_ws_http.md)：WebSocket / HTTP 实际传输内容与头字段详解
- [`docs/INTERVIEW_QUESTIONS.md`](docs/INTERVIEW_QUESTIONS.md)：项目面试问题清单与高频追问点
- [`docs/review.md`](docs/review.md)：模拟面试复盘、回答纠错与下一轮行动项
- [`docs/lock.md`](docs/lock.md)：全项目锁、条件变量与原子变量清单
- [`docs/store.md`](docs/store.md)：Redis 与 MySQL 实际存储清单
- [`docs/media_upload_design.md`](docs/media_upload_design.md)：图片/视频/语音上传与模糊预览设计草案
- [`docs/media_api_spec.md`](docs/media_api_spec.md)：图片媒体消息第一阶段的接口契约草案

---

> 说明：当前代码中的 CMake 工程名、C++ namespace 和数据库名仍沿用 `spark_push` / `sparkpush`，对外项目名称使用 **MsgRail / 讯驿**。
