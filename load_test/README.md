## load_test 测试工具说明

本目录提供多种 C++ 测试/压测客户端：

| 工具 | 用途 | 性能 |
|-----|------|-----|
| `async_ws_bench` | epoll WebSocket 完整 ACK 链路压测 | 以实测 ACK QPS 为准 |
| `http_bench` | 高性能 HTTP API 压测（epoll） | 以当前环境实测为准 |
| `ws_client_cli` | 功能验证用的WebSocket命令行客户端 | - |
| `load_tester` | 多线程压测客户端（WebSocket/HTTP/业务流程） | - |
| `mysql_stmt_bench` | 隔离比较 MySQL 每条 prepare 与 statement 复用 | 约 1,000–1,300 QPS（环境相关） |

### 一、编译方式

在工程根目录下执行：

```bash
cmake -S . -B build \
  -DBUILD_TESTS=OFF \
  -DBUILD_WEB_DEMO=OFF
cmake --build build -j"$(nproc)"
```

正式性能对比建议使用独立的 Release 构建，避免 Debug 容器检查、字符串和锁开销污染结果：

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DBUILD_WEB_DEMO=OFF
cmake --build build-release -j"$(nproc)"
```

主要可执行文件路径：

- `build/load_test/ws_client_cli`
- `build/load_test/load_tester`
- `build/load_test/async_ws_bench`
- `build/load_test/http_bench`
- `build/load_test/mysql_stmt_bench`
- `build/comet/comet_server`

### 二、ws_client_cli：功能验证 WebSocket 客户端

#### 2.1 参数说明

```bash
./build/load_test/ws_client_cli \
  --account <账号> \
  --password <密码> \
  [--logic-host 127.0.0.1] \
  [--logic-port 9101] \
  [--comet-host 127.0.0.1] \
  [--comet-port 9200] \
  [--to-user <uid>] | [--room-id <room_id>]
```

- `--account/--password`：登录账号、密码（与 HTTP `/api/login` 保持一致）。
- `--logic-host/--logic-port`：logic HTTP 地址（默认 `127.0.0.1:9101`）。
- `--comet-host/--comet-port`：comet WebSocket 地址（默认 `127.0.0.1:9200`）。
- `--to-user`：单聊目标用户 ID，进入单聊模式。
- `--room-id`：聊天室 `room_id`，进入聊天室模式。

#### 2.2 使用流程

1. 客户端通过 HTTP 调用 `/api/login` 获取 `user_id/token`。
2. 使用 `token` 建立 `ws://comet_host:comet_port/ws?token=...` 连接。
3. 终端中逐行输入文本，每行会被包装为：
   - 单聊：`{"type":"single_chat","to_user_id":...,"content":{"text":...}}`
   - 聊天室：`{"type":"chatroom","group_id":room_id,"content":{"text":...}}`
4. 收到服务器下行消息会在终端打印 `"[RECV] ..."`。

示例：

```bash
./build/load_test/ws_client_cli \
  --account user1 --password 123456 \
  --to-user 2
```

聊天室示例：

```bash
./build/load_test/ws_client_cli \
  --account user1 --password 123456 \
  --room-id 1001
```

### 三、load_tester：压测客户端

`load_tester` 通过 `--mode` 切换不同压测场景，核心公共参数如下：

```bash
./build/load_test/load_tester \
  --mode <ws_send|http_history|biz_flow> \
  --connections N \
  --io-threads M \
  --messages-per-conn K \
  --interval-ms T \
  [功能相关参数...]
```

- `--connections N`：总连接数 / 虚拟用户数量（默认 10）。
- `--io-threads M`：工作线程数（默认 4）。
- `--messages-per-conn K`：
  - `ws_send`：每连接发送的消息条数。
  - `http_history`：每线程执行的请求次数。
  - `biz_flow`：每个虚拟用户执行完整业务脚本的次数。
- `--interval-ms T`：两次消息/请求之间的睡眠时间（默认 10ms）。

程序结束时会输出：

- `sent`：成功请求/消息总数。
- `failed`：失败次数。
- `duration`：总耗时（秒）。
- `approx QPS`：近似 QPS（`sent / duration`）。

#### 3.1 WebSocket 发消息压测（mode=ws_send）

用途：压测 comet 的 WebSocket 收发性能（单聊/聊天室）。

额外参数（与 `ws_client_cli` 相同风格）：

- `--logic-host/--logic-port`：logic HTTP，用于登录。
- `--comet-host/--comet-port`：comet WebSocket。
- `--account/--password`：账号、密码（所有连接可以共用或配合前缀策略）。
- `--to-user`：单聊目标用户 ID（单聊模式）。
- `--room-id`：聊天室 ID（聊天室模式）。

示例：100 条连接压单聊

```bash
./build/load_test/load_tester \
  --mode ws_send \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --comet-host 127.0.0.1 --comet-port 9200 \
  --account user --password 123456 \
  --to-user 2 \
  --connections 100 --io-threads 4 \
  --messages-per-conn 100 --interval-ms 10
```

示例：聊天室消息压测

```bash
./build/load_test/load_tester \
  --mode ws_send \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --comet-host 127.0.0.1 --comet-port 9200 \
  --account user --password 123456 \
  --room-id 1001 \
  --connections 100 --io-threads 4 \
  --messages-per-conn 100 --interval-ms 10
```

#### 3.2 HTTP 历史接口压测（mode=http_history）

用途：压测 logic 的历史消息拉取能力 `/api/session/history`。

额外参数：

- `--account/--password`：登录账号与密码；工具会先登录一次并复用 token。
- `--history-session-id SID`：会话 ID（必填），如 `single:1:2` 或 `room:1001`。
- `--history-anchor N`：`anchor_seq`（默认 0）。
- `--history-limit N`：每次返回条数（默认 20）。

接口有鉴权和会话归属校验：单聊账号必须是会话双方之一，房间账号必须已加入目标房间。登录失败或无权访问时不能把结果当成有效压测数据。

示例：

```bash
./build/load_test/load_tester \
  --mode http_history \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --account alice --password e10adc3949ba59abbe56e057f20f883e \
  --history-session-id single:1:2 \
  --history-anchor 0 \
  --history-limit 50 \
  --io-threads 4 \
  --messages-per-conn 1000 \
  --interval-ms 5
```

#### 3.3 业务流程压测（mode=biz_flow）

用途：压测完整业务链路（注册 → 登录 → 创建聊天室 → 加入聊天室），主要看 MySQL/Redis/logic 的综合能力。

额外参数：

- `--account <前缀>`：账号前缀，例如 `user_`。
- `--password <密码>`：注册/登录密码。

说明：

- 对第 `i` 个虚拟用户，实际账号为：`<前缀><i>`（例如 `user_0`, `user_1`, ...）。
- 每个虚拟用户每轮脚本包含：
  1. `POST /api/register`（账号已存在会收到 409，视为“注册已完成”）。
  2. `POST /api/login`。
  3. `POST /api/admin/chatroom/create` 创建聊天室。
  4. `POST /api/chatroom/join` 加入刚创建的聊天室。
- 每个步骤成功一次都会被统计为一次 `sent`。

示例：

```bash
./build/load_test/load_tester \
  --mode biz_flow \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --account user_ --password 123456 \
  --connections 50 \
  --io-threads 4 \
  --messages-per-conn 5 \
  --interval-ms 100
```

### 四、async_ws_bench：完整 ACK 链路压测

`async_ws_bench` 使用非阻塞 socket、epoll 和批量发送产生高并发 WebSocket 流量，适合比较 Comet→Logic 的 unary RPC 与 gRPC 双向流模式。与 `load_tester --mode ws_send` 不同，它不会把客户端 `send()` 成功直接当作业务完成，而是等待 Comet 返回携带 `client_msg_id` 的 ACK。

完整测量链路为：

```text
压测客户端 → WebSocket → Comet → gRPC → Logic → Redis/Kafka
           ← ACK       ←       ← Logic 响应
```

#### 4.1 当前测试生命周期

1. 逐个注册或复用 `<account-prefix><index>` 账号，并登录获得 token。HTTP 登录按响应 `Content-Length` 读取完整 JSON，不依赖服务端关闭 keep-alive 连接。
2. 启动 worker，建立全部 WebSocket 连接。
3. 等待所有连接收到 `101 Switching Protocols`；建连时间不计入消息吞吐。
4. 同时启用所有 worker 发送业务消息。
5. 所有消息发送完成后保持连接，继续等待 ACK/error 排空。
6. 所有请求完成或超过 `--drain-timeout` 后停止 worker并输出结果。

Comet 的业务 ACK 包含原请求 ID：

```json
{"type":"ack","client_msg_id":"12-9381","msg_id":"single:1:2-12345"}
```

客户端按 JSON `type` 分类：

- `ack`：Logic 成功完成上行处理；
- `error`：业务处理失败；
- 其他文本帧：统计为其他下行帧，不计入 ACK；
- ACK 中的 `client_msg_id` 在当前连接找不到：统计为无法匹配 ACK。

房间模式会先为每条连接发送一次 `chatroom_join`。该加入帧不计入“业务消息发送”和完成 ACK QPS；其不带 `client_msg_id` 的 ACK 会被忽略。

#### 4.2 参数

```bash
./build-release/load_test/async_ws_bench \
  [--logic-host 127.0.0.1] \
  [--logic-port 9101] \
  [--comet-host 127.0.0.1] \
  [--comet-port 9200] \
  [--connections 100] \
  [--workers 4] \
  [--messages 1000] \
  [--connect-timeout 30] \
  [--drain-timeout 30] \
  [--to-user 1 | --room-id ROOM_ID] \
  [--account bench_] \
  [--password bench123]
```

- `--connections`：WebSocket 连接数，也是准备的压测账号数。
- `--workers`：epoll worker 线程数。
- `--messages`：每条成功登录并建连的连接发送的业务消息数；预期总数为 `成功取得 token 的连接数 × messages`。
- `--connect-timeout`：等待全部 WebSocket 握手完成的秒数，默认 30。
- `--drain-timeout`：停止发送后等待 ACK/error 排空的秒数，默认 30。
- `--to-user`：单聊目标用户 ID；未指定房间时默认单聊。
- `--room-id`：大于 0 时切换为聊天室模式。
- `--account`：压测账号前缀。连接 `i` 使用 `<前缀><i>`，工具会先尝试注册再登录。
- `--password`：注册和登录密码。

单聊示例：

```bash
./build-release/load_test/async_ws_bench \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --comet-host 127.0.0.1 --comet-port 9200 \
  --connections 100 --workers 4 --messages 10000 \
  --to-user 999999 \
  --account bench_ --password bench123 \
  --connect-timeout 30 --drain-timeout 60
```

聊天室示例：

```bash
./build-release/load_test/async_ws_bench \
  --connections 100 --workers 4 --messages 10000 \
  --room-id 1001 \
  --account room_bench_ --password bench123 \
  --connect-timeout 30 --drain-timeout 60
```

#### 4.3 输出指标和判读

结果包含：

- `业务消息发送`：已加入发送队列的业务消息数，不包含房间加入帧；
- `ACK`：成功关联到 `client_msg_id` 的业务 ACK；
- `业务错误`：成功关联到请求的 error 响应；
- `未完成`：`发送 - ACK - 业务错误`；
- `其他下行帧`：消息推送等 `type` 不是 `ack/error` 的文本帧；不带 `client_msg_id` 的房间加入 ACK 会被忽略，不进入该计数；
- `无法匹配ACK`：ACK/error 带有 ID，但当前连接中没有对应 pending 请求；
- `发送QPS`：`业务消息发送 / 发送阶段耗时`，主要反映客户端注入速度；
- `完成ACK QPS`：`ACK / 完整链路总耗时`，用于比较 unary 与双向流；
- `ACK延迟 P50/P95/P99`：从消息加入客户端发送批次到收到对应 ACK 的耗时；
- `排空阶段`：停止发送后处理积压请求所需时间。

有效测试应满足：

```text
业务错误 = 0
未完成 = 0
无法匹配ACK = 0
业务消息发送 = 成功取得 token 的连接数 × messages
```

工具在全部已发送消息成功 ACK、无业务错误且发送数达到预期时退出码为 0；存在业务错误、未完成请求或发送数不足时退出码为 2；建连失败或超时退出码为 1。当前 `无法匹配ACK` 只作为诊断指标，不单独改变退出码，但有效 A/B 测试仍应要求它为 0。不要只比较“发送QPS”，应优先比较完成 ACK QPS、P95/P99、排空时间和 CPU。

#### 4.4 unary 与双向流 A/B 对比

两种模式由 `conf/comet.conf` 控制：

```ini
# unary
use_grpc_stream=false
comet_grpc_pool_size=4
```

```ini
# 双向流
use_grpc_stream=true
grpc_stream_count=4
```

每次修改配置后必须重启 `comet_server`，并确认只运行一个 Comet 实例。建议测试步骤：

1. 使用同一份 Release 二进制和同一套 Logic、Redis、Kafka、Job。
2. 先用小规模测试确认 `未完成=0`、`业务错误=0`。
3. 对 `comet_grpc_pool_size` 和 `grpc_stream_count` 分别扫描 `1、2、4、8、16`。
4. 每组参数使用完全相同的连接数、worker、消息数、目标用户和超时时间。
5. 正式结果前先独立运行一轮较小消息数作为预热；当前工具没有内置 `--warmup`，预热轮次应单独执行且不合并进正式结果。
6. 每个配置至少重复 5 次，使用完成 ACK QPS 和 P99 的中位数；建议按 unary→stream→stream→unary 的 ABBA 顺序运行。
7. 同时记录 Comet/Logic CPU、上下文切换、Redis/Kafka 状态，避免把下游波动归因于 gRPC 模式。

最小预热命令：

```bash
./build-release/load_test/async_ws_bench \
  --connections 100 --workers 4 --messages 1000 \
  --to-user 999999 --account bench_ --password bench123 \
  --drain-timeout 60
```

正式命令保持参数一致，只增加消息数：

```bash
./build-release/load_test/async_ws_bench \
  --connections 100 --workers 4 --messages 10000 \
  --to-user 999999 --account bench_ --password bench123 \
  --drain-timeout 60
```

注意：当前 `async_ws_bench` 是“固定消息总量 + 尽快发送”模型，尚不支持固定目标速率或固定测试时长。其延迟包含客户端发送队列等待时间，适合在相同参数下做 A/B 对比，但不能替代独立的服务端处理耗时指标。

### 五、MySQL prepared statement 隔离基准

`mysql_stmt_bench` 用于回答一个单一问题：在其他条件一致时，跨消息复用 prepared statement 能带来多少收益。

两种模式使用同一个 MySQL 连接、相同 INSERT、参数绑定和 autocommit，仅改变 statement 生命周期：

- `prepare_each`：每条记录都执行 `init → prepare → bind → execute → close`。
- `reuse`：每轮只 prepare 一次，每条记录仅 `bind → execute`。

工具会创建独立的 `persist_stmt_bench` 表，预热后交替执行两种模式，报告每轮 QPS、单操作 P95、中位数及加速比，正常或测试失败退出前都会尝试删除该表，不修改业务 `message` 数据。

```bash
MYSQL_PASSWORD=123456 ./build/load_test/mysql_stmt_bench \
  --host 127.0.0.1 --port 3306 --user root --database spark_push \
  --operations 2000 --warmup 500 --rounds 7
```

也可以使用 `--password`，但环境变量可避免密码出现在 shell 历史和进程参数中。基准结果会受磁盘、MySQL 配置和同机负载影响，应至少独立运行两次并比较中位数，不要只挑最好的一轮。

### 六、建议的使用顺序

1. 使用 `ws_client_cli` 手动验证登录、单聊、聊天室功能是否正常。
2. 用 `load_tester --mode ws_send` 做小规模发送功能验证；其 QPS 是客户端 `send()` 口径，不用于判断完整链路吞吐。
3. 用 `async_ws_bench` 等待并关联业务 ACK，比较 unary 与双向流的完成 ACK QPS、P99 和排空时间。
4. 用 `load_tester --mode http_history` 压 `/api/session/history`，观察 MySQL/Redis 的读性能与缓存效果。
5. 用 `load_tester --mode biz_flow` 压注册/登录/建聊天室流程，验证数据库写入能力与业务正确性。
6. 用 `mysql_stmt_bench` 隔离验证 MySQL statement 生命周期优化，避免把 Kafka 波动误归因给数据库。
7. 在压测过程中结合 Logic/Comet/Job 输出、Kafka lag、MySQL 行数、Redis 状态和 CPU 指标一起分析瓶颈。 


