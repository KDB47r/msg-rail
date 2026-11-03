# SparkPush 请求清单

本文按 **“谁发起 → 调用哪个函数 → 谁执行 → 执行函数内部做什么”** 整理 SparkPush 的主要请求链路，重点覆盖 gRPC，同时附上 HTTP 和 Kafka 异步消息。

- 调用处：发起请求的代码位置
- 调用函数：客户端实际调用的函数
- 执行处：服务端接收请求的代码位置
- 执行函数：服务端真正处理请求的函数

---

## 目录

1. [请求总图](#1-请求总图)
2. [gRPC 服务注册](#2-grpc-服务注册)
3. [Comet → Logic：LogicService](#3-comet--logiclogicservice)
4. [Job → Comet：CometService](#4-job--cometcometservice)
5. [浏览器 → Logic：HTTP 请求](#5-浏览器--logichttp-请求)
6. [Logic → Kafka → Job：异步消息](#6-logic--kafka--job异步消息)
7. [两条完整链路示例](#7-两条完整链路示例)
8. [读表注意点](#8-读表注意点)

---

## 1. 请求总图

```text
浏览器 ──HTTP(JSON)──▶ Logic :9101
浏览器 ──WebSocket────▶ Comet :9200 ──gRPC(LogicService)──▶ Logic :9100
                                                        │
                                                        ├─ Kafka: push_to_comet ──▶ Job
                                                        │        └─ gRPC(CometService) ──▶ Comet :9205
                                                        │                                   └─ WebSocket ──▶ 浏览器
                                                        │
                                                        └─ Kafka: broadcast_task ──▶ Job
                                                                 └─ gRPC(CometService) ──▶ Comet
```

端口来自：

- `conf/logic.conf`：gRPC `9100`，HTTP `9101`
- `conf/comet.conf`：WebSocket `9200`，Comet gRPC `9205`，Logic gRPC 目标 `127.0.0.1:9100`
- `conf/job.conf`：`comet_targets=comet-1=127.0.0.1:9205`

---

## 2. gRPC 服务注册

| gRPC 服务 | 注册处 | 监听端口 | 客户端 |
|---|---|---|---|
| `LogicService` | `logic/main.cpp:137-147` | 9100 | Comet |
| `CometService` | `comet/app.cpp:26-30` | 9205 | Job |
| `JobService` | 只有 proto 定义，仓库内无实现 | — | — |

服务定义在 `proto/spark_push.proto`：

- `LogicService`：`:161-178`
- `CometService`：`:225-232`
- `JobService`：`:220-223`

---

## 3. Comet → Logic：`LogicService`

### 3.1 请求总表

| RPC | 调用处 | 调用函数 | 执行处 | 执行函数 | 触发 / 模式 |
|---|---|---|---|---|---|
| `VerifyToken`√ | `comet/comet_server.cpp:136` `CometServer::OnValidate`，调用在 `:175` | `logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep)` | `logic/grpc_service.cpp:47` | `LogicServiceImpl::VerifyToken` | WS 握手 `/ws?token=...`，同步阻塞 |
| `SendUpstreamMessage`√ | `comet/comet_server.cpp:425` `HandleTextMessage`，调用在 `:556` | `stub->SendUpstreamMessage(&rpc_ctx, req, &rep)` | `logic/grpc_service.cpp:78` → `:241` | `LogicServiceImpl::SendUpstreamMessage` → `HandleUpstreamMessage` | `use_grpc_stream=false`，unary |
| `MessageStream` √| `comet/logic_stream_client.cpp:33` `LogicStreamClient::Start`，由 `comet/comet_server.cpp:87` 启动 | `stub_->MessageStream(ctx)` | `logic/grpc_service.cpp:512` | `LogicServiceImpl::MessageStream` | `use_grpc_stream=true`，双向流 |
| `UserOffline`√ | `comet/comet_server.cpp:214` `OnClose` → `:379` `NotifyUserOffline` | `stub->UserOffline(&ctx, req, &rep)` | `logic/grpc_service.cpp:87` → `:443` | `LogicServiceImpl::UserOffline` → `HandleUserOffline` | WS 断线，`grpc_pool_` 异步 |
| `ReportRoomJoin` | `comet/comet_server.cpp:357` `AddUserToRoom` → `:402` `NotifyRoomJoin` | `stub->ReportRoomJoin(&ctx, req, &rep)` | `logic/grpc_service.cpp:95` → `:462` | `LogicServiceImpl::ReportRoomJoin` → `HandleRoomJoin` | 收到 `chatroom_join`，异步 |
| `ReportRoomLeave` | `comet/comet_server.cpp:363` / `:215` → `:418` `NotifyRoomLeave` | `stub->ReportRoomLeave(&ctx, req, &rep)` | `logic/grpc_service.cpp:103` → `:485` | `LogicServiceImpl::ReportRoomLeave` → `HandleRoomLeave` | `chatroom_leave` / 断线，异步 |
| `Broadcast` | **仓库内无调用方** | — | `logic/grpc_service.cpp:111` | `LogicServiceImpl::Broadcast` | 预留 RPC；HTTP 管理端直接写 Kafka |

### 3.2 `MessageStream` 单独展开

双向流不是“一次调用就结束”，而是三个阶段：

| 阶段 | 调用处 | 调用函数 | 执行处 | 执行函数 |
|---|---|---|---|---|
| 建立流 | `comet/logic_stream_client.cpp:33` | `stub_->MessageStream(ctx)` | `logic/grpc_service.cpp:512` | `LogicServiceImpl::MessageStream` |
| 发送一条上行消息(xiezheyixiedongxishiweilehaokan) | `comet/comet_server.cpp:528` `stream_client_->Send(...)` → `comet/logic_stream_client.cpp:118` | `LogicStreamClient::Send` → `ClientReaderWriter::Write` | `logic/grpc_service.cpp:517` `stream->Read(&msg)` → `:524` | `MessageStream` 内 `Read` → `HandleUpstreamMessage` |
| 接收响应 | `comet/logic_stream_client.cpp:131` `stream->Read(&resp)` → `:144` 回调 | `ClientReaderWriter::Read` | `logic/grpc_service.cpp:555` `stream->Write(resp)` | `MessageStream` 内 `Write` |

关键点：

- `MessageStream` 内部支持 `upstream`、`offline`、`room_join`、`room_leave` 四种 oneof（`logic/grpc_service.cpp:521-548`）；
- 但当前 Comet **只通过流发送 `upstream`**（`comet/comet_server.cpp:526-528`）；
- `offline`、`room_join`、`room_leave` 仍走 unary 的 `UserOffline` / `ReportRoomJoin` / `ReportRoomLeave`；
- 每条流在 Logic 侧是一个长期占用的 gRPC 工作线程，因为 `MessageStream` 里是 `while (stream->Read(&msg))`。

### 3.3 执行函数内部做什么

| 执行函数 | 关键内部调用 | 结果 |
|---|---|---|
| `LogicServiceImpl::VerifyToken` | `redis_store_->GetUserIdByToken`（`logic/redis_store.cpp:83`）→ `AddRoute`（`:106`） | 校验 token，返回 `user_id`，写 `route:user:{uid}` |
| `LogicServiceImpl::HandleUpstreamMessage` | `GetUserRoutes` / `GetRoomComets` → `store_->AllocateMessageSeq` → `producer_->Send` | 生成 `session_id`、`msg_seq`、`msg_id`，投递 `push_to_comet` |
| `LogicServiceImpl::HandleUserOffline` | `redis_store_->RemoveRoute` | 按 `user_id + conn_id` 删除路由 |
| `LogicServiceImpl::HandleRoomJoin` | `IncrRoomOnlineCount`、`RoomJoinAtomic` | 房间在线数 +1，登记 Comet |
| `LogicServiceImpl::HandleRoomLeave` | `IncrRoomOnlineCount(-1)`、`RoomLeaveAtomic` | 房间在线数 -1，移除 Comet |
| `LogicServiceImpl::Broadcast` | 房间：`GetRoomComets` + `producer_->Send`；全站：`broadcast_producer_->Send` | 房间广播走 push topic，全站广播走 `broadcast_task` |

### 3.4 调用方式：同步还是异步

| RPC | Comet 侧调用方式 | 说明 |
|---|---|---|
| `VerifyToken` | 同步阻塞 | 在 Asio IO 线程里直接调用（`comet_server.cpp:174` 注释），握手期间会占住该线程 |
| `SendUpstreamMessage` | 异步提交到 `grpc_pool_` | `comet_server.cpp:553-556`，工作线程同步等待 RPC 返回 |
| `MessageStream` | 入队后立即返回 | `LogicStreamClient::Send` 只把请求放进 `send_queue`，writer 线程负责 `Write` |
| `UserOffline` | 异步提交到 `grpc_pool_` | `comet_server.cpp:372-379` |
| `ReportRoomJoin` | 异步提交到 `grpc_pool_` | `comet_server.cpp:395-402` |
| `ReportRoomLeave` | 异步提交到 `grpc_pool_` | `comet_server.cpp:411-418` |

---

## 4. Job → Comet：`CometService`

| RPC | 调用处 | 调用函数 | 执行处 | 执行函数 | 触发 / 模式 |
|---|---|---|---|---|---|
| `PushToComet` | `job/push_handler.cpp:305` `ProcessPushRequest`，调用在 `:321`；广播在 `:386` | `stub->PushToComet(&ctx, req, &reply)` | `comet/comet_grpc_service.cpp:52` → `:12` | `CometServiceImpl::PushToComet` → `ProcessPushRequest` | `use_push_stream=false` 的在线推送 / 广播兜底 |
| `PushStream` | `job/push_handler.cpp:67` `InitStreams`，调用在 `:81`；重连 `:119`；写消息 `:157` | `stub->PushStream(ctx, &response)` → `writer->Write(req)` | `comet/comet_grpc_service.cpp:64` → `:75` / `:78` | `CometServiceImpl::PushStream` → `reader->Read` → `ProcessPushRequest` | `use_push_stream=true`，每个 Comet 一条客户端流 |
| `EnqueuePushTask` / `EnqueueBroadcastTask` | **仓库内无调用方** | — | **无实现** | — | proto 预留；实际 Logic→Job 走 Kafka |

执行函数内部：

```text
CometServiceImpl::ProcessPushRequest   // comet/comet_grpc_service.cpp:12
  ├─ targets_size() > 0  → CometServer::PushToUsers
  ├─ room_id() > 0       → CometServer::PushToRoom
  ├─ session_id 前缀 room:/danmaku: → PushToRoom
  └─ session_id == "broadcast"      → PushToAll
```

调用目标：

- `CometServer::PushToUsers`：`comet/comet_server.cpp:581`
- `CometServer::PushToRoom`：`:589`
- `CometServer::PushToAll`：`:595`

注意：

- `PushStream` 会跳过 `persist_only` 消息（`comet/comet_grpc_service.cpp:77`）；
- unary `PushToComet` 在 Job 侧带 3 秒 deadline 和最多 3 次重试（`job/push_handler.cpp:316-333`）；
- 广播 `HandleBroadcastTask` 固定走 unary `PushToComet`（`job/push_handler.cpp:386`），不走流；
- `comet/comet_grpc_service.cpp:52-61` 的 `PushToComet` 目前始终返回 `grpc::Status::OK`，业务错误码固定为 0。

---

## 5. 浏览器 → Logic：HTTP 请求

所有 HTTP 请求统一入口：

```text
浏览器 / curl
   → HttpApiServer::onRequest        // logic/http_server.cpp:222
   → 路由表 kRouteTable              // :235-253
   → (this->*handler)(req, resp)     // :270
   → 具体 handleXxx
```

| HTTP 路径 | 执行函数 | 文件:行 |
|---|---|---|
| `/api/login` | `HttpApiServer::handleLogin` | `logic/http_handlers_auth.cpp:16` |
| `/api/register` | `HttpApiServer::handleRegister` | `logic/http_handlers_auth.cpp:99` |
| `/api/message/send` | `HttpApiServer::handleSendMessage` | `logic/http_handlers_message.cpp:31` |
| `/api/session/history` | `HttpApiServer::handleHistory` | `logic/http_handlers_message.cpp:202` |
| `/api/session/mark_read` | `HttpApiServer::handleMarkRead` | `logic/http_handlers_message.cpp:281` |
| `/api/session/list_single` | `HttpApiServer::handleSingleSessionList` | `logic/http_handlers_message.cpp:377` |
| `/api/session/unread` | `HttpApiServer::handleUnread` | `logic/http_handlers_message.cpp:329` |
| `/api/chatroom/join` | `HttpApiServer::handleChatroomJoin` | `logic/http_handlers_chatroom.cpp:68` |
| `/api/chatroom/leave` | `HttpApiServer::handleChatroomLeave` | `logic/http_handlers_chatroom.cpp:121` |
| `/api/chatroom/unsubscribe` | `HttpApiServer::handleChatroomUnsubscribe` | `logic/http_handlers_chatroom.cpp:158` |
| `/api/chatroom/online_count` | `HttpApiServer::handleChatroomOnlineCount` | `logic/http_handlers_chatroom.cpp:200` |
| `/api/chatroom/list` | `HttpApiServer::handleChatroomList` | `logic/http_handlers_chatroom.cpp:18` |
| `/api/danmaku/send` | `HttpApiServer::handleDanmakuSend` | `logic/http_handlers_danmaku.cpp:21` |
| `/api/danmaku/list` | `HttpApiServer::handleDanmakuList` | `logic/http_handlers_danmaku.cpp:192` |
| `/api/admin/chatroom/create` | `HttpApiServer::handleAdminCreateChatroom` | `logic/http_handlers_admin.cpp:21` |
| `/api/admin/chatroom/list` | `HttpApiServer::handleAdminListChatroom` | `logic/http_handlers_admin.cpp:71` |
| `/api/admin/broadcast` | `HttpApiServer::handleAdminBroadcast` | `logic/http_handlers_admin.cpp:120` |

补充：

- `OPTIONS` 预检请求在 `logic/http_server.cpp:226-232` 直接返回，不进业务路由。
- `/health`、`/metrics` 只在 `logic/http_server.cpp:258-259` 的注释和文档中出现，当前 `onRequest` 没有实际分支，访问会落到 404。这是代码和文档不一致的地方。

---

## 6. Logic → Kafka → Job：异步消息

Kafka 不是 gRPC，但它是“Logic 把任务抛给 Job”的真正通道。

| Topic | 生产处 | 生产函数 | 消费处 | 消费函数 | 用途 |
|---|---|---|---|---|---|
| `push_to_comet` | `logic/grpc_service.cpp:406` | `producer_->Send` | `job/push_handler.cpp:273` | `PushHandler::HandleMessage` | gRPC 上行消息在线推送 |
| `push_to_comet` | `logic/grpc_service.cpp:429` | `producer_->Send` | 同上 | 同上 | 离线消息 `persist_only` |
| `push_to_comet` | `logic/http_handlers_message.cpp:152` | `kafka_producer_->Send` | 同上 | 同上 | HTTP 发送在线推送 |
| `push_to_comet` | `logic/http_handlers_message.cpp:183` | `kafka_producer_->Send` | 同上 | 同上 | HTTP 发送离线 `persist_only` |
| `push_to_comet` | `logic/grpc_service.cpp:178` | `producer_->Send` | 同上 | 同上 | gRPC 房间广播 |
| `push_to_comet` | `logic/http_handlers_admin.cpp:237` | `kafka_producer_->Send` | 同上 | 同上 | HTTP 管理员房间广播 |
| `push_to_comet` | `logic/http_handlers_danmaku.cpp:182` | `kafka_producer_->Send` | 同上 | 同上 | 弹幕消息 |
| `push_to_comet` | 同上 | 同上 | `job/persist_handler.cpp:164` | `PersistHandler::HandleMessage` | 独立消费组异步落 MySQL，只处理 `need_persist` |
| `broadcast_task` | `logic/grpc_service.cpp:218` | `broadcast_producer_->Send` | `job/push_handler.cpp:345` | `PushHandler::HandleBroadcastTask` | 全站广播 |
| `broadcast_task` | `logic/http_handlers_admin.cpp:275` | `broadcast_producer_->Send` | 同上 | 同上 | HTTP 管理员全站广播 |

注意：

> `KafkaProducer::Send` 返回 `true` 只代表**本地入队成功**，不代表 broker 已确认，更不代表 MySQL 已落库。

---

## 7. 两条完整链路示例

### 7.1 token 验证

```text
浏览器
  GET /ws?token=xxx
    ↓
CometServer::OnValidate                 comet/comet_server.cpp:136
    ↓
logic_stub_->VerifyToken(...)           comet/comet_server.cpp:175
    ↓
LogicServiceImpl::VerifyToken           logic/grpc_service.cpp:47
    ├─ RedisStore::GetUserIdByToken     logic/redis_store.cpp:83
    │    └─ GET token:{token}
    └─ RedisStore::AddRoute             logic/redis_store.cpp:106
         └─ HSET route:user:{uid} conn_id comet_id
    ↓
VerifyTokenReply{user_id}
    ↓
Comet 保存 ctx->user_id                 comet/comet_server.cpp:188
```

### 7.2 单聊消息发送

```text
浏览器 WS {"type":"single_chat", "to_user_id":..., "content":...}
    ↓
CometServer::HandleTextMessage                      comet/comet_server.cpp:425
    ↓
[默认流模式]
LogicStreamClient::Send                             comet/comet_server.cpp:528
    ↓
MessageStream: stream->Write                        comet/logic_stream_client.cpp:118
    ↓
LogicServiceImpl::MessageStream                     logic/grpc_service.cpp:512
    ↓
HandleUpstreamMessage                               logic/grpc_service.cpp:241
    ├─ GetUserRoutes / GetRoomComets
    ├─ AllocateMessageSeq（Redis INCR）
    └─ producer_->Send("session_id|comet_id", ...)
    ↓
Kafka push_to_comet
    ↓
PushHandler::HandleMessage                          job/push_handler.cpp:273
    ↓
[默认 unary 模式]
PushToComet                                         job/push_handler.cpp:321
    ↓
CometServiceImpl::PushToComet                       comet/comet_grpc_service.cpp:52
    ↓
CometServer::PushToUsers                            comet/comet_server.cpp:581
    ↓
WebSocket 发送 content_json 给浏览器
```

---

## 8. 读表注意点

### 8.1 gRPC 状态和业务错误是两层

- `VerifyToken` 对无效 token 返回 `grpc::Status::OK`，但 `error.code()=401`（`logic/grpc_service.cpp:60-64`）；
- 调用处必须同时检查 `status.ok()` 和 `vrep.error().code()`（`comet/comet_server.cpp:179`）；
- `SendUpstreamMessage` 也是同样的模式：gRPC 层 OK，业务错误放在 `error.code()`。

### 8.2 同步/异步看调用方式，不看 RPC 名字

- `VerifyToken`：在 Comet 的 Asio IO 线程里同步阻塞（`comet/comet_server.cpp:174` 注释）；
- `SendUpstreamMessage` / `UserOffline` / `ReportRoom*`：提交到 `grpc_pool_`，工作线程阻塞；
- `MessageStream`：Comet 侧 `Send` 只是入队，writer/reader 线程负责 `Write/Read`；
- Job `PushToComet`：Kafka 消费线程同步调用，带 deadline 和重试。

### 8.3 当前配置下的实际路径

- `conf/comet.conf:8`：`use_grpc_stream=true` → Comet→Logic 上行默认走 `MessageStream`；
- `conf/comet.conf:9`：`grpc_stream_count=4` → 默认建立 4 条双向流；
- `conf/job.conf:13`：`use_push_stream=false` → Job→Comet 默认走 unary `PushToComet`。

### 8.4 未使用/预留的接口

- `LogicService::Broadcast`：仓库内无调用方；
- `JobService::EnqueuePushTask` / `EnqueueBroadcastTask`：只有 proto，没有实现和调用；
- `MessageStream` 的 `offline` / `room_join` / `room_leave` 分支：协议支持，但 Comet 当前没走；
- `/health`、`/metrics`：文档和注释里有，当前代码没有实际路由分支。
