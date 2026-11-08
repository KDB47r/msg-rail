# MsgRail 存储层清单：Redis 与 MySQL

> 本文以当前代码为准，整理 Redis 和 MySQL **实际读写的 key / table / field**，以及它们各自承担的作用。  
> MySQL 的 `sql/schema.sql` 里还有少量“已建表但当前代码未使用”的对象，文中有单独标注。

## 0. 一句话结论

- **Redis**：运行期状态。存登录 token、用户连接路由、会话消息序号、已读游标、房间在线路由和在线人数；**不存消息正文和历史**。
- **MySQL**：持久化事实源。存用户、群组/成员、会话元数据、消息正文、用户已读游标和弹幕；**不存 token、实时路由、Comet 地址**。
- **Kafka**：消息通道，不是供业务查询的存储；本清单不展开 Kafka 字段。
- **Comet**：不直接访问 Redis/MySQL。连接鉴权、路由登记和房间状态都通过 gRPC 交给 Logic，持久化由 Job 完成。

---

## 1. 总览

```text
                    ┌──────────────────────────────┐
                    │            Redis             │
                    │ token / route / seq /        │
                    │ read_seq / room routing      │
                    └──────────────▲───────────────┘
                                   │
浏览器 ──HTTP/WS──► Comet ──gRPC──► Logic ──Kafka──► Job
                                   │                  │
                                   ▼                  ▼
                    ┌──────────────────────────────┐
                    │            MySQL             │
                    │ user / im_group / group_member│
                    │ session / message /           │
                    │ user_session_state / danmaku  │
                    └──────────────────────────────┘
```

| 存储 | 主要访问者 | 核心数据 | 数据性质 |
|---|---|---|---|
| Redis | Logic | token、连接路由、消息序号、已读游标、房间路由/在线人数 | 运行期状态，可重建或可降级 |
| MySQL | Logic、Job | 用户、群组/成员、会话元数据、消息正文、已读游标、弹幕 | 持久化事实源 |
| Kafka | Logic、Job | `PushToCometRequest` 等 protobuf 消息 | 异步消息通道 |

---

## 2. Redis

### 2.1 Key 设计约定

Redis 使用 `redis_db=0`，无统一项目前缀。所有 key 都由 `RedisStore` 统一封装，定义见 `logic/redis_store.h:15` 和 `logic/redis_store.cpp`。

大多 key 使用 `{...}` hash tag：

- 同一会话的 `session:{<sid>}:seq`、`session:{<sid>}:seq_initialized`、`read_seq:{<sid>}:uid:<uid>` 落在同一个 Redis Cluster slot；
- 同一房间的 `room:{<rid>}:comets`、`room:{<rid>}:comet_count:<cid>`、`room:{<rid>}:online_count` 落在同一个 slot；
- 这样 `MGET`、`EVAL` 在 Cluster 模式下仍然可用。

### 2.2 Redis Key 总表

| Key 模式 | 类型 | 内容 / 字段 | TTL | 主要写入 | 主要读取 | 作用 |
|---|---|---|---|---|---|---|
| `token:{<token>}` | STRING | value = `user_id` 十进制字符串 | 24 小时 | 登录/注册/管理员登录 | HTTP 鉴权、Comet 握手 `VerifyToken` | 登录态 |
| `route:user:{<uid>}` | HASH | field = `conn_id`，value = `comet_id` | 无 | WS 握手成功后的 `AddRoute` | 单聊路由 `GetUserRoutes` | 用户连接在哪个 Comet |
| `session:{<sid>}:seq` | STRING | 当前已分配的最大 `msg_seq` | 无 | 发消息时 Lua `INCR` | 序号分配、未读计算、`GetSessionLastSeq` | 会话消息序号 |
| `session:{<sid>}:seq_initialized` | STRING | 固定值 `"1"` | 无 | 首次从 MySQL 恢复序号时 Lua 写入 | 序号分配前检查 | 标记 seq 是否已初始化 |
| `read_seq:{<sid>}:uid:<uid>` | STRING | 用户已读到的 `msg_seq` | 无 | `mark_read` 接口 | 未读计算 `MGET` | 已读游标缓存 |
| `room:{<rid>}:comets` | SET | 成员 = `comet_id` | 无 | `RoomJoinAtomic` / `RoomLeaveAtomic` | 聊天室/弹幕路由 `GetRoomComets` | 哪些 Comet 上有该房间用户 |
| `room:{<rid>}:comet_count:<cid>` | STRING | 该 Comet 上该房间的用户数 | 无 | `RoomJoinAtomic` / `RoomLeaveAtomic` | Lua 内部 | 控制 `comets` 集合的加入/移除 |
| `room:{<rid>}:online_count` | STRING | 房间在线人数 | 无 | `IncrRoomOnlineCount` | `/api/chatroom/online_count` | 在线人数统计 |
| `rate:msg:{<uid>}` | ZSET | member = 毫秒时间戳，score = 同一时间戳 | `window_ms + 1000` 毫秒 | `CheckRateLimit` | `CheckRateLimit` | 滑动窗口限流；**当前调用被注释，运行时不写入** |

> `seq_initialized` 没有出现在 `redis_store.h` 顶部的 key 注释里，但代码里实际使用，见 `logic/redis_store.cpp:32-43`。

### 2.3 逐项说明

#### token：登录态

- 登录、注册、管理员登录都会调用：

  ```cpp
  SetToken(token, user_id, 24 * 3600);
  ```

  写入：

  ```text
  SETEX token:{<token>} 86400 <user_id>
  ```

- token 是 24 字节随机数转成的 48 位十六进制字符串；Redis 里存的是明文 token key，value 是用户 ID，不存密码。
- HTTP 请求通过 `Authorization: Bearer <token>` 或 JSON body 的 `token` 字段鉴权。
- WebSocket 握手时，Comet 把 token 通过 gRPC `VerifyToken` 交给 Logic，Logic 查同一个 `token:{<token>}`。
- 访问不会续期，24 小时后过期；Redis 重启或 key 被清除后，所有用户都需要重新登录。
- 管理员账号是内存校验，但 token 仍然写 Redis，value 是管理员 user_id。

#### route:user：单聊连接路由

```text
HSET route:user:{<uid>} <conn_id> <comet_id>
```

- `conn_id` 形如 `comet-1:7`，由 Comet 在握手时生成；
- 同一个用户可以有多条连接（多设备、多标签页），所以 value 是 HASH，不是单个字符串；
- 写入时机：WebSocket 握手 `VerifyToken` 成功后，Logic 执行 `AddRoute`；
- 删除时机：Comet 连接关闭后，通过 gRPC `UserOffline` 调 `RemoveRoute`，执行 `HDEL`；
- 查询：单聊发送时 `GetUserRoutes` 执行 `HVALS`，并对 `comet_id` 去重；
- Logic 进程内还有一层 `route_cache_`，默认缓存 1 秒，`AddRoute` / `RemoveRoute` 时会主动失效。它不是 Redis 数据，但会影响路由读取的实时性。

聊天室发送路径**不读**这个 key；它只用于单聊，以及弹幕在某些回退逻辑里按用户路由扇出。

#### session seq：消息序号

```text
session:{<sid>}:seq
```

- `sid` 例如 `single:1:2`、`room:1001`；
- 发消息时 Logic 调用 `AllocateMessageSeq`，执行 Lua：

  ```lua
  if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
  return redis.call('INCR', KEYS[1])
  ```

- 如果 `seq_initialized` 不存在，说明 Redis 里还没有这个会话的安全序号：
  1. Logic 从 MySQL `message` 表查 `MAX(msg_seq)`；
  2. 调用 `InitAndIncrSessionMsgSeq`，把 `session:{<sid>}:seq` 至少提升到该最大值；
  3. 写入 `session:{<sid>}:seq_initialized = "1"`；
  4. 再执行 `INCR`。

- 序号是会话级全局递增：同一个聊天室的所有用户共用一个 `msg_seq`，不是每个发送者独立编号。
- `msg_id` 不单独存 Redis，也不存 MySQL；它由 `session_id + "-" + msg_seq` 在发送/读取时计算。
- key 没有 TTL。Redis 数据丢失时可以从 MySQL 的 `MAX(msg_seq)` 恢复；但如果只丢了 `seq`、`seq_initialized` 还在，`INCR` 可能从 1 重新开始，存在序号冲突风险。

#### read_seq：已读游标

```text
read_seq:{<sid>}:uid:<uid>
```

- `mark_read` 接口先写 MySQL `user_session_state.read_seq`，再写这个 Redis key；
- 未读计算用一次 `MGET`：

  ```text
  MGET read_seq:{<sid>}:uid:<uid> session:{<sid>}:seq session:{<sid>}:seq_initialized
  ```

- 未读数 = `session_seq - read_seq`，小于 0 时按 0 处理；
- Redis 不可用或序号未初始化时，降级查 MySQL：
  - `user_session_state.read_seq`
  - `session.last_msg_seq`
- 这个 key 没有 TTL，用户不调用 `mark_read` 就会一直保留旧值。

#### room：聊天室/弹幕实时路由

```text
room:{<rid>}:comets              SET<comet_id>
room:{<rid>}:comet_count:<cid>   STRING
room:{<rid>}:online_count        STRING
```

`RoomJoinAtomic` / `RoomLeaveAtomic` 执行同一段 Lua：

```lua
local d = tonumber(ARGV[1])
local n = redis.call('INCRBY', KEYS[1], d)
if d > 0 and n == 1 then
  redis.call('SADD', KEYS[2], ARGV[2])
elseif d < 0 and n <= 0 then
  redis.call('SREM', KEYS[2], ARGV[2])
  if n < 0 then redis.call('SET', KEYS[1], 0); n = 0 end
end
return n
```

语义：

- `comet_count:<cid>` 统计该 Comet 上“加入该房间的用户数”；
- 计数从 0 变成 1 时，把 `comet_id` 加入 `room:{rid}:comets`；
- 计数降到 0 时，把 `comet_id` 从集合移除；
- 计数为 0 时 key 会保留，值被设为 0，不会自动删除。

这些 key 没有 TTL，依赖 `chatroom_leave`、连接关闭上报 `ReportRoomLeave` 来清理。Comet 崩溃、上报丢失或异步乱序时，可能留下过期路由。

`online_count` 是统计值，按“用户 × Comet”累加，不是全站去重人数；同一用户连到两个 Comet 会被计两次。它不参与实时推送路由，路由只看 `room:{rid}:comets`。

#### rate:msg：限流（当前未启用）

实现是滑动窗口 ZSET：

- `ZREMRANGEBYSCORE` 清理窗口外记录；
- `ZCARD` 判断是否达到上限；
- `ZADD` 写入当前时间戳；
- `PEXPIRE` 设置窗口 + 1 秒的 TTL。

HTTP 发送和 gRPC 上行消息里的限流调用目前都被注释掉了，所以正常运行时不会产生这个 key。保留代码用于讲解和后续开启。

### 2.4 Redis 数据示例

假设用户 1 登录、在 `comet-1` 上建立连接 `comet-1:7`，并加入房间 1001：

```text
token:{a1b2c3...}                    = "1"
route:user:{1}                       = { "comet-1:7": "comet-1" }
session:{room:1001}:seq              = "42"
session:{room:1001}:seq_initialized  = "1"
read_seq:{room:1001}:uid:1           = "40"
room:{1001}:comets                    = { "comet-1", "comet-2" }
room:{1001}:comet_count:comet-1       = "2"
room:{1001}:comet_count:comet-2       = "1"
room:{1001}:online_count              = "3"
```

---

## 3. MySQL

### 3.1 数据库与表总览

- 数据库：`spark_push`，字符集 `utf8mb4`。
- 建表文件：`sql/schema.sql`。
- 注意：`schema.sql` 开头执行 `DROP DATABASE IF EXISTS spark_push`，重新初始化会清空现有数据。

| 表 | 作用 | 当前代码是否使用 |
|---|---|---|
| `user` | 用户账号、昵称、密码哈希 | 使用 |
| `session` | 会话元数据、会话最后序号 | 使用（`type=1` 的群聊未使用） |
| `message` | 单聊/聊天室消息正文 | 使用 |
| `user_session_state` | 用户在每个会话的已读游标 | 使用 |
| `im_group` | 群组/聊天室元数据 | 使用（只使用 `group_type=1` 聊天室） |
| `group_member` | 群组成员关系 | 使用 |
| `video_danmaku` | 按视频时间轴存储的弹幕 | 使用 |
| `broadcast` | 广播任务记录 | **schema 定义但当前代码未读写** |

### 3.2 表结构详解

#### 3.2.1 `user`：用户表

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 用户 ID |
| `account` | `VARCHAR(64) NOT NULL UNIQUE` | 登录账号 |
| `name` | `VARCHAR(64) NOT NULL DEFAULT ''` | 昵称 |
| `password_hash` | `VARCHAR(128) NOT NULL` | 密码哈希；当前 demo 约定前端先做 MD5，服务端直接比较 |
| `created_at` | `TIMESTAMP` | 创建时间 |

索引/约束：

- 主键 `id`
- `account` 上有列级 `UNIQUE` 和 `uk_account` 唯一索引（schema 中重复声明了一次）

写入：

- 注册：`UserDao::CreateUser` 执行 `INSERT INTO user(account, name, password_hash)`；
- `schema.sql` 插入了 `alice`、`bob`、`carol`、`dave` 四个测试账号。

读取：

- 登录：`GetUserByAccount`；
- `GetUserById` 已有实现，但当前主流程没有调用。

注意：

- **token 不在 MySQL**，只在 Redis；
- 管理员账号不是通过 `user` 表校验，而是 Logic 内存中的特殊账号；
- `password_hash` 当前没有盐值和迭代哈希，属于 demo 级实现。

#### 3.2.2 `session`：会话元数据

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 内部自增 ID，业务不暴露 |
| `session_id` | `VARCHAR(128) NOT NULL UNIQUE` | 业务会话 ID |
| `type` | `TINYINT` | `0=single`，`1=group`，`2=chatroom` |
| `user1_id` | `BIGINT DEFAULT 0` | 单聊一端 |
| `user2_id` | `BIGINT DEFAULT 0` | 单聊另一端 |
| `group_id` | `BIGINT DEFAULT 0` | 群聊/聊天室 ID |
| `last_msg_seq` | `BIGINT DEFAULT 0` | 已持久化的会话最大消息序号 |
| `created_at` | `TIMESTAMP` | 创建时间 |

索引/约束：

- 主键 `id`
- `uk_session_id (session_id)` 唯一索引
- `idx_user1 (user1_id)`、`idx_user2 (user2_id)`

实际行形态：

```text
单聊：
  session_id = "single:<min_uid>:<max_uid>"
  type = 0
  user1_id / user2_id = 两个用户 ID
  group_id = 0

聊天室：
  session_id = "room:<room_id>"
  type = 2
  user1_id = user2_id = 0
  group_id = room_id
```

`type=1` 的普通群聊当前代码没有使用。

写入：

1. **创建会话行**
   - `SessionDao::EnsureRoomSession`：`INSERT ... VALUES(?, 2, 0, 0, ?, 0)`；
   - `SessionDao::EnsureSingleSession`：`VALUES(?, 0, ?, ?, 0, 0)`；
   - `GetOrCreateRoomSession` 当前会被聊天室列表接口和 HTTP 弹幕发送调用；
   - `GetOrCreateSingleSession` 已有实现，但当前主流程没有调用，单聊会话行通常由 Job 持久化时创建。

2. **更新最后序号**
   - Job 持久化单聊：
     ```sql
     INSERT INTO session (... last_msg_seq)
     VALUES (?, 0, ?, ?, 0, ?)
     ON DUPLICATE KEY UPDATE
       last_msg_seq = GREATEST(last_msg_seq, VALUES(last_msg_seq))
     ```
   - Job 持久化聊天室：
     ```sql
     INSERT INTO session (... last_msg_seq)
     VALUES (?, 2, 0, 0, ?, ?)
     ON DUPLICATE KEY UPDATE
       last_msg_seq = GREATEST(last_msg_seq, VALUES(last_msg_seq))
     ```
   - HTTP 弹幕路径使用 `SessionDao::AllocateMessageSeq`：
     ```sql
     UPDATE session
     SET last_msg_seq = LAST_INSERT_ID(last_msg_seq + 1)
     WHERE session_id = ?
     ```

读取：

- `GetSessionById` 查会话元数据；
- `/api/session/list_single` 查用户单聊会话；
- `/api/chatroom/list` 返回房间的 `session_id`、`group_id`、`last_msg_seq`；
- 未读数降级路径会用 `last_msg_seq` 作为最后序号。

注意：

- `last_msg_seq` 是**持久化的水位**，不保证与 Redis `session:{sid}:seq` 实时相等；
- 聊天室/单聊的主发送路径用 Redis 分配序号，Job 异步更新 MySQL；所以刚发完消息时，`session.last_msg_seq` 可能落后；
- 历史消息本身不依赖 `last_msg_seq`，而是直接查 `message` 表。

#### 3.2.3 `message`：消息正文表

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 内部消息 ID |
| `session_id` | `VARCHAR(128) NOT NULL` | 所属会话 |
| `msg_seq` | `BIGINT NOT NULL` | 会话内消息序号 |
| `sender_id` | `BIGINT NOT NULL` | 发送者用户 ID |
| `msg_type` | `VARCHAR(32) NOT NULL DEFAULT 'text'` | 消息类型，当前有 `text`、`danmaku` |
| `content_json` | `TEXT NOT NULL` | 完整消息内容 JSON |
| `timestamp_ms` | `BIGINT NOT NULL` | 消息时间戳，毫秒 |
| `client_msg_id` | `VARCHAR(128) NOT NULL DEFAULT ''` | 客户端消息 ID，用于前端去重/关联 |

索引/约束：

- 主键 `id`
- `uk_session_seq (session_id, msg_seq)` 唯一索引
- **没有单独的 `msg_id` 列**
- **没有 `to_user_id`、`room_id`、`comet_id` 列**

`msg_id` 的生成方式：

```text
msg_id = session_id + "-" + msg_seq
```

例如：

```text
session_id = "room:1001"
msg_seq    = 42
msg_id     = "room:1001-42"
```

消息所属房间由 `session_id` 或 `session.group_id` 表达；`comet_id` 只存在于 Redis 和 Kafka 路由中，不落 MySQL。

写入：

1. **Job 异步持久化单聊/聊天室**
   `job/persist_handler.cpp` 的 `ExecutePersist` 在事务中：
   - upsert `session`；
   - 执行：
     ```sql
     INSERT INTO message
       (session_id, msg_seq, sender_id, msg_type, content_json, timestamp_ms, client_msg_id)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE session_id = session_id
     ```
   - 提交事务。

   唯一键保证重复消费不会插入重复消息。

2. **HTTP 弹幕同步写入**
   `/api/danmaku/send` 调用 `ConversationStore::AppendMessage`：
   - 先用 `SessionDao::AllocateMessageSeq` 从 MySQL `session.last_msg_seq` 分配序号；
   - 再调用 `MessageDao::InsertMessage` 写 `message`；
   - 这条路径是同步写 MySQL，不经过 Job 的 persist 消费组。

读取：

- `GET/POST /api/session/history`：
  `MessageDao::ListMessages` 按 `session_id` 查询，`ORDER BY msg_seq DESC LIMIT ?`，然后在内存中翻转成升序；
- Redis 序号初始化时：
  `SELECT COALESCE(MAX(msg_seq), 0) FROM message WHERE session_id=?`，作为安全下限。

注意：

- `client_msg_id` 不是唯一键，也没有索引；它只用于客户端侧去重/关联；
- 同一聊天室消息会按在线 Comet 数量被 Job 持久化多次，但 `(session_id, msg_seq)` 唯一键会把它变成幂等写入；
- `content_json` 是完整 JSON 文本，历史接口会把它解析后作为嵌套对象返回，解析失败时降级为字符串；
- `MessageDao::ListMessages` 读取 `content_json` 时使用固定 4096 字节缓冲；代码把 `MYSQL_DATA_TRUNCATED` 也当成功处理，超长内容存在截断/越界读取风险；
- `message` 表没有外键，`session_id` 的合法性由应用层保证。

#### 3.2.4 `user_session_state`：已读游标

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 内部 ID |
| `user_id` | `BIGINT NOT NULL` | 用户 ID |
| `session_id` | `VARCHAR(128) NOT NULL` | 会话 ID |
| `read_seq` | `BIGINT NOT NULL DEFAULT 0` | 已读到的最大 `msg_seq` |
| `last_visit_at` | `TIMESTAMP` | 最后访问时间 |

索引/约束：

- 主键 `id`
- `uk_user_session (user_id, session_id)` 唯一索引
- `idx_session (session_id)`

写入：

- `POST /api/session/mark_read` 调用 `ConversationStore::MarkRead`；
- `UserSessionStateDao::UpsertReadSeq`：
  ```sql
  INSERT INTO user_session_state(user_id, session_id, read_seq)
  VALUES (?, ?, ?)
  ON DUPLICATE KEY UPDATE
    read_seq = GREATEST(read_seq, VALUES(read_seq)),
    last_visit_at = CURRENT_TIMESTAMP
  ```
- 同时更新 Redis `read_seq:{<sid>}:uid:<uid>`。

读取：

- Redis 未命中或不可用时，`GetReadSeq` 查 MySQL，作为未读计算的降级数据源；
- `last_visit_at` 当前只写不读。

作用：

- 一个用户在一个会话里只有一条游标记录；
- 不记录“每条消息是否已读”，只记录一个最大已读序号；
- 未读数由 `last_seq - read_seq` 计算，不单独存 unread 计数。

#### 3.2.5 `im_group`：群组/聊天室元数据

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 群组/聊天室 ID |
| `name` | `VARCHAR(128) NOT NULL` | 名称 |
| `owner_id` | `BIGINT NOT NULL` | 创建者/所有者用户 ID |
| `group_type` | `TINYINT` | `0=normal_group`，`1=chatroom`，`2=danmaku_room` |
| `created_at` | `TIMESTAMP` | 创建时间 |

索引/约束：

- 主键 `id`
- `idx_owner (owner_id)`
- `idx_type_id (group_type, id)`

实际使用：

- `GroupDao::CreateChatroom` 插入 `group_type=1`；
- `GetGroup` 用于聊天室加入校验和房间列表名称；
- `ListChatrooms` 只查 `group_type=1`；
- `group_type=0/2` 在当前主流程中没有实际使用。

注意：

- `owner_id` 只是记录创建者，不会自动把创建者写入 `group_member`；
- 没有外键约束。

#### 3.2.6 `group_member`：群组成员

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 内部 ID |
| `group_id` | `BIGINT NOT NULL` | 群组/聊天室 ID |
| `user_id` | `BIGINT NOT NULL` | 用户 ID |
| `role` | `TINYINT NOT NULL DEFAULT 0` | `0=member`，`1=admin`，`2=owner` |
| `join_at` | `TIMESTAMP` | 加入时间 |

索引/约束：

- 主键 `id`
- `uk_group_user (group_id, user_id)` 唯一索引
- `idx_user (user_id)`

写入：

- `POST /api/chatroom/join`：
  `AddOrUpdateMember(room_id, user_id, 0)`；
- 当前实现使用：
  ```sql
  INSERT IGNORE INTO group_member(group_id, user_id, role)
  VALUES (...)
  ```
  因此重复加入不会报错，但也**不会更新已有 role**；
- `POST /api/chatroom/unsubscribe` 调用 `RemoveMember` 删除记录。

读取：

- `ListUserChatrooms`：用户加入的聊天室列表，`JOIN im_group` 且只保留 `group_type=1`；
- `ListRoomMembers`：
  - 历史消息接口的会话归属校验；
  - `/api/chatroom/online_count` 在 Redis 失败时回退为成员总数；
  - 弹幕在无法使用房间 Comet 路由时按成员逐用户扇出。

注意：

- 这张表是**持久成员关系**，不是实时在线状态；
- WebSocket 的 `chatroom_join` 不会写这张表；
- 当前 `role` 实际总是 0，代码中也没有基于角色的鉴权；
- 没有外键，群组删除不会自动清理成员。

#### 3.2.7 `video_danmaku`：视频弹幕

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 弹幕 ID |
| `video_id` | `VARCHAR(128) NOT NULL` | 视频 ID |
| `timeline_ms` | `BIGINT NOT NULL` | 弹幕出现在视频时间轴上的毫秒位置 |
| `sender_id` | `BIGINT NOT NULL` | 发送者 |
| `content_json` | `TEXT NOT NULL` | 弹幕内容 JSON |
| `timestamp_ms` | `BIGINT NOT NULL` | 落库时间戳 |

索引/约束：

- 主键 `id`
- `idx_video_timeline (video_id, timeline_ms)`

写入：

- `POST /api/danmaku/send` 调用 `DanmakuDao::InsertDanmaku`；
- 写库失败只记日志，不阻断后续实时推送；
- `timestamp_ms` 在 `DanmakuDao` 中使用 `time(nullptr) * 1000` 生成，只有秒级精度。

读取：

- `POST /api/danmaku/list` 调用 `ListDanmaku`；
- 按 `video_id` 查询，可选 `timeline_ms` 范围，按 `timeline_ms ASC` 返回。

注意：

- 当前弹幕 HTTP 发送路径还会通过 `ConversationStore::AppendMessage` 往 `message` 表写一条 `msg_type=danmaku` 的消息，并使用固定的 `room:1001` 作为会话；
- 这条 Kafka 推送没有设置 `need_persist`（默认 false），所以 Job 不会再落盘一次；`message` 行由 HTTP handler 自己同步写入；
- 该路径使用 MySQL `session.last_msg_seq` 分配序号，不更新 Redis `session:{room:1001}:seq`；如果 Redis 计数器已经初始化，后续 WebSocket 聊天室发送可能和这批弹幕序号冲突，详见 5.3；
- `DanmakuDao` 顶部注释提到 `idx_sender`，但实际 `schema.sql` 没有这个索引。

#### 3.2.8 `broadcast`：广播记录（当前未使用）

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | `BIGINT`，自增主键 | 内部 ID |
| `task_id` | `VARCHAR(128) NOT NULL UNIQUE` | 广播任务 ID |
| `title` | `VARCHAR(256) DEFAULT ''` | 标题 |
| `scope` | `VARCHAR(64) DEFAULT 'all'` | 广播范围 |
| `group_id` | `BIGINT DEFAULT 0` | 房间广播时的房间 ID |
| `content_json` | `TEXT NOT NULL` | 广播内容 |
| `created_at` | `TIMESTAMP` | 创建时间 |

索引/约束：

- 主键 `id`
- `uk_task_id (task_id)` 唯一索引
- `idx_group (group_id)`

当前代码没有 DAO 读写这张表。管理员广播实际走 Kafka：

- 全站广播：`broadcast_task` topic；
- 房间广播：`push_to_comet` topic + Redis 房间路由。

不要把它和 Kafka 的 `broadcast_task` topic 混为一谈。

### 3.3 MySQL 数据示例

```text
user
  (1, 'alice', 'Alice', 'e10adc3949ba59abbe56e057f20f883e', ...)

session
  ('room:1001', 2, 0, 0, 1001, 42, ...)
  ('single:1:2', 0, 1, 2, 0, 17, ...)

message
  ('room:1001', 42, 1, 'text',
   '{"type":"chatroom","group_id":1001,"content":{"text":"hello"},
     "from_user_id":1,"msg_id":"room:1001-42","create_time":...}',
   1730000000000, 'c-1')

user_session_state
  (1, 'room:1001', 40, ...)

im_group
  (1001, 'Lobby', 1, 1, ...)

group_member
  (1001, 1, 0, ...)
```

---

## 4. 典型流程的存储读写矩阵

| 流程 | Redis 读/写 | MySQL 读/写 |
|---|---|---|
| 注册 | 写 `token:{token}` | 写 `user` |
| 登录 | 读 `user` 不在 Redis；写 `token` | 读 `user` |
| WebSocket 握手 | 读 `token`；写 `route:user:{uid}` | 无 |
| HTTP 加入聊天室 | 不操作房间 Redis | 读 `im_group`；写 `group_member` |
| WebSocket `chatroom_join` | 写 `room:{rid}:comet_count:<cid>`、`room:{rid}:comets`、`room:{rid}:online_count` | 无 |
| WebSocket `chatroom_leave` | 减 `comet_count`、必要时 SREM、减 `online_count` | 无 |
| HTTP 单聊发送 | 读 token、分配 `session:{sid}:seq`、读 `route:user` | 不直接写；Job 异步写 `session` + `message` |
| WS 单聊/聊天室发送 | 读 token（握手时已完成）；分配 seq；聊天室读 `room:{rid}:comets` | 不直接写；Job 异步写 `session` + `message` |
| 聊天室/单聊历史 | 读 token | 读 `group_member`、`message` |
| `mark_read` | 写 `read_seq:{sid}:uid:<uid>` | 写 `user_session_state` |
| 未读查询 | 读 `read_seq` + `session:{sid}:seq` + `seq_initialized` | Redis 降级时读 `user_session_state`、`session` |
| HTTP 弹幕发送 | 读 token；分配/读取房间路由 | 写 `video_danmaku`；写 `session` + `message`（MySQL 序号） |
| HTTP 弹幕列表 | 读 token | 读 `video_danmaku` |
| 管理员创建聊天室 | 读 token | 写 `im_group` |
| 管理员房间广播 | 读 token；读 `room:{rid}:comets` | 无 |
| 管理员全站广播 | 读 token | 无（写 Kafka `broadcast_task`） |
| 连接断开 | 删 `route:user:{uid}`；删房间计数/集合 | 无 |

---

## 5. 一致性与生命周期

### 5.1 Redis 丢失后的恢复能力

| Redis 数据 | 丢失后的影响 | 恢复方式 |
|---|---|---|
| `token` | HTTP/WS 鉴权失败 | 用户重新登录 |
| `route:user` | 单聊找不到在线 Comet | Comet 重连后再次 `VerifyToken` |
| `session:{sid}:seq` | 无法继续分配序号 | 从 MySQL `MAX(msg_seq)` 重新初始化 |
| `seq_initialized` | 下次分配会重新走初始化 | 从 MySQL 恢复 |
| `read_seq` | 未读游标回到 0 或走 MySQL | `user_session_state` 降级读取 |
| `room:{rid}:comets` | 聊天室无法找到在线 Comet | 用户重新 `chatroom_join`，或等重新上报 |
| `room:online_count` | 在线人数统计不准 | 重新 join/leave 后逐步恢复 |

### 5.2 MySQL 与 Redis 的分工

- **消息历史以 MySQL 为准**：Redis 不存消息正文，历史查询直接读 `message`。
- **序号分配以 Redis 为主**：单聊/聊天室的 gRPC/HTTP 主路径用 `INCR`，MySQL 只提供恢复下限和持久化水位。
- **已读游标以 MySQL 为源、Redis 为缓存**：`mark_read` 先写 MySQL，再写 Redis；未读正常读 Redis，异常降级 MySQL。
- **实时路由只在 Redis**：MySQL 不知道用户当前连接在哪个 Comet，也不存 `comet_id`。

### 5.3 需要注意的最终一致性

1. **推送和持久化是两个 Kafka 消费组**
   - 消息可能已经推送给在线用户，但还没写入 MySQL；
   - 离线用户刚重连立刻查历史，可能暂时看不到最后几条。

2. **`session.last_msg_seq` 可能落后**
   - 主发送路径先分配 Redis seq，再异步由 Job 持久化；
   - 聊天室列表返回的 `last_msg_seq` 是 MySQL 水位，不是 Redis 当前值。

3. **多 Comet 会重复持久化**
   - 房间有多个在线 Comet 时，Logic 为每个 Comet 发一条 `need_persist=true` 的 Kafka 消息；
   - 同一 `(session_id, msg_seq)` 会被 persist 组消费多次，靠唯一键幂等去重。

4. **房间在线状态不是强一致**
   - `ReportRoomJoin/Leave` 通过线程池异步 unary 上报；
   - 用户刚 join 就发消息，Logic 可能还没看到该 Comet；
   - Comet 崩溃或上报丢失会留下过期 `room:{rid}:comets`。

5. **大部分 Redis key 没有 TTL**
   - 只有 `token` 和限流 ZSET 有 TTL；
   - `route`、`seq`、`read_seq`、`room` 都依赖显式删除/清理；
   - 这也是为什么断线清理和房间 leave 上报很重要。

6. **没有外键约束**
   - `message.session_id`、`group_member.group_id` 等关系由应用层维护；
   - 删除群组或用户不会自动级联清理相关行。

7. **HTTP 弹幕和 WebSocket 聊天室使用不同的序号分配器**
   - WebSocket 单聊/聊天室走 Redis `session:{sid}:seq`；
   - HTTP 弹幕走 MySQL `session.last_msg_seq`（`LAST_INSERT_ID(last_msg_seq + 1)`）；
   - HTTP 弹幕不会更新 Redis 计数器。若 `session:{room:1001}:seq` 已初始化，下一批 WebSocket 消息可能分配出与已落库弹幕相同的 `msg_seq`，Job 的 `ON DUPLICATE KEY UPDATE` 会把它当成重复消息忽略；
   - 这是当前 demo 的已知一致性缺口，不是“Redis 与 MySQL 双写”设计。

---

## 6. 快速排查命令

Redis：

```bash
# 登录态
redis-cli -n 0 --scan --pattern 'token:*'
redis-cli -n 0 GET 'token:{<token>}'

# 用户连接路由
redis-cli -n 0 HGETALL 'route:user:{1}'

# 会话序号
redis-cli -n 0 GET 'session:{room:1001}:seq'
redis-cli -n 0 EXISTS 'session:{room:1001}:seq_initialized'

# 已读游标
redis-cli -n 0 GET 'read_seq:{room:1001}:uid:1'

# 房间路由和在线人数
redis-cli -n 0 SMEMBERS 'room:{1001}:comets'
redis-cli -n 0 GET 'room:{1001}:comet_count:comet-1'
redis-cli -n 0 GET 'room:{1001}:online_count'
```

MySQL：

```sql
-- 用户
SELECT id, account, name, created_at FROM user;

-- 会话
SELECT session_id, type, user1_id, user2_id, group_id, last_msg_seq
FROM session
WHERE session_id = 'room:1001';

-- 聊天室消息
SELECT session_id, msg_seq, sender_id, msg_type, timestamp_ms, client_msg_id,
       LEFT(content_json, 200) AS content_preview
FROM message
WHERE session_id = 'room:1001'
ORDER BY msg_seq ASC;

-- 已读游标
SELECT * FROM user_session_state WHERE user_id = 1;

-- 房间和成员
SELECT * FROM im_group WHERE id = 1001;
SELECT * FROM group_member WHERE group_id = 1001;

-- 弹幕
SELECT video_id, timeline_ms, sender_id, timestamp_ms,
       LEFT(content_json, 200) AS content_preview
FROM video_danmaku
WHERE video_id = 'v1'
ORDER BY timeline_ms ASC;
```

---

## 7. 代码索引

| 内容 | 文件 |
|---|---|
| Redis key 封装与 Lua | `logic/redis_store.h`、`logic/redis_store.cpp` |
| 消息序号分配、历史、已读 | `logic/conversation_store.cpp`、`logic/session_dao.cpp`、`logic/message_dao.cpp`、`logic/user_session_state_dao.cpp` |
| 用户表 | `logic/user_dao.cpp` |
| 群组/成员表 | `logic/group_dao.cpp` |
| 弹幕表 | `logic/danmaku_dao.cpp` |
| 聊天室 HTTP 接口 | `logic/http_handlers_chatroom.cpp` |
| 认证与 token | `logic/http_handlers_auth.cpp`、`logic/grpc_service.cpp` |
| 异步持久化 | `job/persist_handler.cpp` |
| 建库建表 | `sql/schema.sql` |
