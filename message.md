# SparkPush 消息字段全链路说明

> 本文按“一条单聊消息从浏览器到另一个浏览器”的顺序，说明每个字段的含义、由谁填充、在哪个文件哪一行填充，以及最终被谁消费。
>
> 记录时间：2026-11-02

## 目录

- [总规则](#总规则)
- [全链路](#全链路)
- [第一站：浏览器发出的 WS JSON](#第一站浏览器发出的-ws-json)
- [第二站：Comet → Logic 的 UpstreamMessageRequest](#第二站comet--logic-的-upstreammessagerequest)
- [第三站：Logic 内部的 ChatMessage](#第三站logic-内部的-chatmessage)
- [第四站：Logic → Kafka 的 PushToCometRequest](#第四站logic--kafka-的-pushtocometrequest)
- [第五站：Comet → 浏览器](#第五站comet--浏览器实际只发-content_json)
- [专表：所有 user_id 类字段](#专表所有-user_id-类字段)
- [ACK 与确认字段](#ack-与确认字段)
- [落库时字段怎么对应](#落库时字段怎么对应)
- [最容易混的 5 组字段](#最后最容易混的-5-组字段)

---

## 总规则

先记住一条总规则：

> **客户端只能决定“发给谁、发什么、用哪个临时 ID”；发送者是谁、会话 ID、消息序号、消息 ID、时间戳，全部由服务端填。**

---

## 全链路

```
浏览器 ──WS JSON──▶ Comet ──UpstreamMessageRequest──▶ Logic
                                                        │
                                                        ├─ ChatMessage（内部/落库模型）
                                                        └─ PushToCometRequest
                                                              │ Kafka: push_to_comet
                                                              ▼
                                                            Job（只搬运，不改身份字段）
                                                              │ gRPC: PushToComet / PushStream
                                                              ▼
浏览器 ◀──WS JSON（就是 content_json）── Comet
```

---

## 第一站：浏览器发出的 WS JSON

发送代码在 `web_demo/static/index.html:970-976`：

```json
{
  "type": "single_chat",
  "to_user_id": 2,
  "client_msg_id": "1730000000000",
  "content": { "text": "hello" }
}
```

| 字段 | 含义 | 谁填 | 后续去向 |
|---|---|---|---|
| `type` | 前端协议里的动作类型：`single_chat` / `chatroom` / `danmaku` / `chatroom_join`… | 前端 `index.html:971` | Comet 用它映射成 `scene`，`comet_server.cpp:435-464` |
| `to_user_id` | 单聊接收者 | 前端 `index.html:972` | 校验 >0 后复制进 `UpstreamMessageRequest.to_user_id`，`comet_server.cpp:436,503` |
| `client_msg_id` | 客户端生成的临时 ID，用来把服务端 ACK 对回这条消息 | 前端 `index.html:973`（`Date.now()`） | 复制进请求 `:498`，再进 `ChatMessage.client_msg_id`，`grpc_service.cpp:378` |
| `content` | 业务内容，前端传的是对象 `{text}` | 前端 `index.html:974` | **Comet 完全不解析**，整条原始 JSON 原样塞进 `content_json`，`comet_server.cpp:512` |
| `group_id` / `room_id` | 聊天室、弹幕房 ID，两个名字都兼容 | 聊天室/弹幕前端 | `websocket_utils.cpp:66-68` 解析 |
| `video_id`、`timeline_ms` | 弹幕视频 ID、时间轴位置 | 弹幕前端 | `websocket_utils.cpp:69-70`，`comet_server.cpp:508-509` |

注意：**浏览器不发 `from_user_id`，也不发 `session_id`、`msg_seq`。** 这三个都是服务端生成的，客户端伪造不了。

---

## 第二站：Comet → Logic 的 `UpstreamMessageRequest`

定义在 `proto/spark_push.proto:92-104`，填充在 `comet/comet_server.cpp:495-512`。

| 字段 | 含义 | 值从哪里来 | 填充位置 |
|---|---|---|---|
| `from_user_id` | 发送者用户 ID | **不是浏览器传的**，来自 `ctx.user_id`；而 `ctx.user_id` 是 `VerifyToken` 成功后由 Logic 返回的 | `comet_server.cpp:496`；`ctx.user_id` 赋值在 `:188` |
| `client_msg_id` | 客户端临时 ID | 从 WS JSON 解析出的 `meta.client_msg_id` | `:498` |
| `scene` | 消息场景：单聊/聊天室/弹幕 | 由 `type` 字符串映射成枚举 | `:497`，映射在 `:435-464` |
| `to_user_id` | 单聊接收者 | 浏览器传的 `to_user_id` | `:503` |
| `group_id` | 聊天室/弹幕房 ID | 浏览器传的 `group_id`/`room_id` | `:505,507` |
| `video_id` | 弹幕视频 ID | 浏览器传的 `video_id` | `:508` |
| `timeline_ms` | 弹幕出现在视频的第几毫秒 | 浏览器传的 `timeline_ms` | `:509` |
| `content_json` | 消息内容 | **整条 WS 原始 JSON 原样放进去**，包括 `type`、`content`、`client_msg_id` | `:512` |
| `comet_id` | **来源** Comet 节点 ID | 本机配置的 `comet_id_` | `:499` |
| `conn_id` | 来源连接 ID，形如 `comet-1:7` | 握手时生成的连接上下文 | `:500`，生成于 `:157-159` |
| `received_time_ms` | Comet 收到消息的本机时间 | Comet 当前时间 | `:490-493,501` |

两个容易误解的点：

1. **`from_user_id` 的信任链**：浏览器只传 token → Logic 查 Redis `token:{token}` 得到 `user_id`（`grpc_service.cpp:62-68`）→ 返回给 Comet 存进 `ctx.user_id`（`comet_server.cpp:188`）→ 发消息时再填进 `from_user_id`。所以这是整条链路里唯一可信的身份来源。
2. `UpstreamMessageRequest.comet_id` / `conn_id` / `received_time_ms` 在 `HandleUpstreamMessage` 里**目前没有被读取**；消息主链路只依赖 `from_user_id`。它们主要留给路由注册、下线清理和排查问题使用。

---

## 第三站：Logic 内部的 `ChatMessage`

定义在 `proto/spark_push.proto:45-54`，是“消息的标准内部模型”，也是最终落 MySQL 的结构。

| 字段 | 含义 | 谁填 / 怎么算 | 填充位置 |
|---|---|---|---|
| `msg_id` | 服务端消息唯一 ID | `session_id + "-" + msg_seq` | HTTP `http_handlers_message.cpp:101,134`；gRPC `grpc_service.cpp:348,371` |
| `session_id` | 会话 ID | 单聊：`single:min(uid1,uid2):max(uid1,uid2)`；聊天室：`room:<rid>`；弹幕：`danmaku:<vid>` | HTTP `:83-86,135`；gRPC `:276-279,296,317,372` |
| `msg_seq` | 会话内严格递增序号 | `ConversationStore::AllocateMessageSeq`：Redis INCR，首次/丢失时用 MySQL 兜底 | HTTP `:90-98,136`；gRPC `:334-342,373` |
| `sender_id` | 发送者 ID | 就是 `from_user`：HTTP 从 token 来，gRPC 从 `request.from_user_id()` 来 | HTTP `:36,137`；gRPC `:245,374` |
| `timestamp_ms` | 消息时间 | **Logic 的当前时间**，不是 Comet 的 `received_time_ms` | HTTP `:103-105,138`；gRPC `:344-346,375` |
| `msg_type` | 消息类型枚举 | 单聊/聊天室 → `MSG_TYPE_TEXT`；弹幕 → `MSG_TYPE_DANMAKU`；广播 → `MSG_TYPE_BROADCAST` | HTTP `:139`；gRPC `:350-351,376`；管理员广播 `http_handlers_admin.cpp:218` |
| `content_json` | 最终下发给浏览器的内容 | **两条入口不一样**，见下 | HTTP `:108-116,140`；gRPC `:353-362,377` |
| `client_msg_id` | 客户端临时 ID | gRPC 路径从请求复制；**HTTP 路径没有设置，是空串** | gRPC `:378` |

`content_json` 的差异很关键：

- **WS 路径（gRPC 入口）**：把浏览器原始 JSON 解析后，注入/覆盖 `msg_id`、`create_time`、`from_user_id`；如果原来没有 `client_msg_id` 再补上，然后重新 dump（`grpc_service.cpp:353-362`）。
- **HTTP `/api/message/send` 路径**：重新构造一个 JSON，字段是 `type/msg_id/msg_seq/from_user_id/to_user_id/content/create_time`（`http_handlers_message.cpp:108-116`）。注意这里 `content` 是**字符串**，而 WS 路径的 `content` 是前端传的对象。

---

## 第四站：Logic → Kafka 的 `PushToCometRequest`

定义在 `proto/spark_push.proto:190-201`，是“投递给某个 Comet 的搬运单”。

| 字段 | 含义 | 谁填 | 填充位置 |
|---|---|---|---|
| `comet_id` | **目标** Comet 节点 | 单聊：`GetUserRoutes(to_user)` 查 Redis 得到；聊天室：`GetRoomComets(room_id)` | HTTP `:119-128`；gRPC `:283-288,386-388` |
| `message` | 完整的 `ChatMessage` | 复制上面构造好的消息 | HTTP `:133-140`；gRPC `:389` |
| `targets[].user_id` | 实际要推给哪些用户 | 单聊填接收者 `to_user`；聊天室为空 | HTTP `:142-143`；gRPC `:390-393` |
| `scene` | 场景 | 从入口继承 | HTTP `:129,163`；gRPC `:394,420` |
| `to_user_id` | 单聊接收者 | 从入口继承 | HTTP `:130,164`；gRPC `:395,421` |
| `room_id` | 聊天室/弹幕房 ID | 聊天室/弹幕填；单聊为 0 | gRPC `:396,422` |
| `need_persist` | 是否需要落 MySQL | 正常消息为 `true`；管理员房间广播为 `false` | HTTP `:131,165`；gRPC `:397,423`；广播 `http_handlers_admin.cpp:227` |
| `persist_only` | 只落盘、不推送 | 接收方不在线时置 `true` | HTTP `:162`；gRPC `:418` |

`persist_only` 的效果：

- Job 的推送消费者看到它直接 `return`，不发给 Comet（`job/push_handler.cpp:286`）。
- Job 的持久化消费者只要 `need_persist == true` 就落库（`job/persist_handler.cpp:173`）。
- Comet 的流入口还额外做了一层防御（`comet/comet_grpc_service.cpp:77`）。

Kafka key 也有含义：

- 在线：`session_id + "|" + comet_id`（HTTP `:151`，gRPC `:405`）→ 同一会话到同一 Comet 的消息进同一分区，保证顺序。
- 离线：只用 `session_id`（HTTP `:183`，gRPC `:429`）→ 不同会话分散到不同分区，避免单个 key 形成热点。

---

## 第五站：Comet → 浏览器，实际只发 `content_json`

Comet 收到 `PushToCometRequest` 后：

1. 有 `targets` → 推给这些用户在本机的连接（`comet/comet_grpc_service.cpp:14-21`）；
2. 有 `room_id` → 推给本机该房间的所有连接（`:22-24`）；
3. 兜底看 `session_id` 前缀（`:25-47`）。

真正发出去的 payload 由 `PayloadFor` 决定（`comet/comet_server.cpp:574-578`）：

```cpp
return msg.content_json().empty()
           ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
           : msg.content_json();
```

也就是说：

> **`ChatMessage` 里的 `msg_seq`、`session_id`、`sender_id`、`msg_type`、`timestamp_ms` 不会自动出现在浏览器收到的 JSON 里；浏览器收到的就是 `content_json` 那一坨。**

所以 WS 路径下，接收方看到的 JSON 大致是：

```json
{
  "type": "single_chat",
  "to_user_id": 2,
  "client_msg_id": "1730000000000",
  "content": { "text": "hello" },
  "msg_id": "single:1:2-7",
  "create_time": 1730000000123,
  "from_user_id": 1
}
```

其中 `msg_id`、`create_time`、`from_user_id` 是 Logic 后注入的；`session_id` 和 `msg_seq` 不在顶层，前端是用 `from_user_id` + `to_user_id` 自己算出会话的（`index.html:687-696`）。

HTTP 路径下，接收方看到的是：

```json
{
  "type": "single_chat",
  "msg_id": "single:1:2-8",
  "msg_seq": 8,
  "from_user_id": 1,
  "to_user_id": 2,
  "content": "hello",
  "create_time": 1730000000456
}
```

对比着看，就能明白两条入口的字段差异。

---

## 专表：所有 `user_id` 类字段

| 字段 | 出现位置 | 谁填 | 含义 / 信任级别 |
|---|---|---|---|
| `VerifyTokenReply.user_id` | Comet ↔ Logic 握手 | Logic 用 token 查 Redis 后填（`grpc_service.cpp:62-68`） | **根身份**，后面所有 sender 都由它派生 |
| `from_user_id` | `UpstreamMessageRequest` | Comet 从 `ctx.user_id` 填（`comet_server.cpp:496`） | 发送者；浏览器改不了 |
| `sender_id` | `ChatMessage` | Logic 从 `from_user_id` 复制（`http:137` / `grpc:374`） | 落库和展示用的发送者 |
| `to_user_id` | WS JSON、`UpstreamMessageRequest`、`PushToCometRequest` | 浏览器填，Comet/Logic 校验后透传 | 接收者；决定 `session_id` 和路由查询 |
| `targets[].user_id` | `PushToCometRequest` | Logic 按接收者填（`http:142-143` / `grpc:390-393`） | 实际 fanout 列表；聊天室为空 |
| `UserOfflineRequest.user_id` | Comet → Logic 下线 | Comet 从连接注册表取（`comet_server.cpp:209-214`） | 要清理哪条连接的路由 |
| `RoomReportRequest.user_id` | Comet → Logic 房间上报 | Comet 从 `ctx.user_id` 填（`:398,414`） | 谁加入/离开了房间 |
| HTTP handler 里的 `user_id` | `/api/...` | `AuthenticateRequest` 从 token 解析 | HTTP 请求者；绝不从 body 取 |
| `admin_user_id` | Logic 配置 | 启动时从 `logic.conf` 读 | 管理员身份 |

---

## ACK 与确认字段

Comet 收到 Logic 的响应后，会给浏览器回一条：

```json
{ "type": "ack", "client_msg_id": "...", "msg_id": "single:1:2-7" }
```

失败时：

```json
{ "type": "error", "client_msg_id": "...", "message": "send failed" }
```

填充位置：流模式 `comet_server.cpp:532-542`，一元模式 `:557-569`。

这里用 `client_msg_id` 把 ACK 对回前端那条消息。当前前端收到 `type=ack` 基本忽略（`index.html:662-663`）。

另外，proto 里的 `AckMessageRequest` / `AckMessageReply`（`spark_push.proto:67-75`）**目前没有被任何代码使用**；真正的“已读”是 `/api/session/mark_read` 更新 `read_seq`，不是逐条消息 ACK。

---

## 落库时字段怎么对应

`job/persist_handler.cpp:239-257` 把 `ChatMessage` 写进 `message` 表（表结构 `sql/schema.sql:38-49`）：

| `ChatMessage` | MySQL `message` 列 |
|---|---|
| `session_id` | `session_id` |
| `msg_seq` | `msg_seq` |
| `sender_id` | `sender_id` |
| `msg_type`（枚举） | `msg_type`（字符串，`MsgTypeToString`） |
| `content_json` | `content_json` |
| `timestamp_ms` | `timestamp_ms` |
| `client_msg_id` | `client_msg_id` |

`scene` + `to_user_id` / `room_id` 用来 upsert `session` 表（`persist_handler.cpp:200-236`）。唯一键 `(session_id, msg_seq)` 保证重复消费时幂等。

---

## 最后：最容易混的 5 组字段

1. **`type` vs `msg_type`**：`type` 是浏览器协议里的字符串（`single_chat`），还会原样出现在下发 JSON 里；`msg_type` 是 `ChatMessage` 的枚举，只用于内部和落库。
2. **`from_user_id` vs `sender_id`**：同一个人的两个层次。前者是 Comet→Logic 的线上字段，后者是 Logic 内部/落库字段。
3. **`to_user_id` vs `targets[].user_id`**：前者是业务语义“发给谁”，后者是 fanout 语义“这个 Comet 上具体推给谁”。聊天室有 `to_user_id=0`、`targets` 为空，靠 `room_id` 广播。
4. **两个 `comet_id` 方向相反**：`UpstreamMessageRequest.comet_id` 是**来源** Comet；`PushToCometRequest.comet_id` 是**目标** Comet。
5. **`msg_id` vs `msg_seq`**：`msg_id` 是字符串全局唯一 ID，用于 ACK/排查；`msg_seq` 是会话内递增整数，用于排序、分页、未读计算。两者都由 Logic 生成。
