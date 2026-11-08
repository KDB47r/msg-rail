# WebSocket 与 HTTP 在本项目里到底传了什么

> 适用项目：MsgRail / 讯驿（代码内仍使用 `spark_push` / `sparkpush`）
>
> 对应代码：`common/net/http_server_lite.*`、`logic/http_server.cpp`、`logic/http_handlers_*.cpp`、`comet/comet_server.*`、`comet/websocket_utils.cpp`、`proto/spark_push.proto`
>
> 相关文档：[`message.md`](message.md)、[`requests.md`](requests.md)、[`media_upload_design.md`](media_upload_design.md)、[`media_api_spec.md`](media_api_spec.md)

---

## 0. 先给结论

一条消息在本项目里并不是“从头到尾都是 WebSocket”，也不是“HTTP 和 WebSocket 各传一套业务字段”。实际是：

```text
浏览器
  │  HTTP/1.1：文本请求行 + 文本 Header + JSON Body
  │  或 WebSocket：先 HTTP Upgrade 握手，之后是二进制帧 + JSON 文本
  ▼
Comet / Logic
  │  gRPC over HTTP/2：二进制帧 + protobuf
  ▼
Kafka
  │  Kafka 二进制协议：key + value（value 是 protobuf）
  ▼
Job / Comet
  │  gRPC over HTTP/2：二进制帧 + protobuf
  ▼
浏览器
     WebSocket 文本帧，payload 就是 content_json
```

三个最容易混淆的点：

1. **HTTP 没有“一个固定的二进制包头”**。HTTP/1.1 是文本协议：请求行/状态行、若干 `Header: Value`、一个空行、Body。所谓“HTTP 头”就是空行之前的那些 ASCII 文本。
2. **WebSocket 握手阶段是 HTTP，握手成功后就不是 HTTP 了**。`101 Switching Protocols` 之后，双方发送的是 WebSocket 帧；每条消息不再带 `Host`、`Content-Type`、`Authorization` 这些 HTTP Header。
3. **WebSocket 帧头是二进制的，且很短**。最短 2 字节，客户端到服务端因为要带 4 字节掩码，最短 6 字节。它只描述“这帧多长、是什么类型、是否分片、是否掩码”，不描述业务字段。

当前项目**没有启用 TLS**：

- HTTP 是 `http://`，不是 `https://`
- WebSocket 是 `ws://`，不是 `wss://`
- gRPC 使用 `grpc::InsecureChannelCredentials()`

所以上述 Header、Token、JSON 内容在网络上都是明文。生产环境必须补 TLS。

---

## 1. 分层：先搞清楚“头”在哪一层

浏览器到服务端，实际经过的是这样一条栈：

```text
应用层：HTTP / WebSocket / gRPC / Kafka 协议
   │
传输层：TCP（源端口、目的端口、seq、ack、flags、window……）
   │
网络层：IP（源 IP、目的 IP、TTL、协议号、分片信息……）
   │
链路层：Ethernet / Wi-Fi（MAC 地址、EtherType、FCS……）
```

本机开发时，浏览器、Logic、Comet 可能都在同一台机器上，走的是 loopback（`lo`），不一定有真实的 Ethernet 帧；但 TCP/IP 仍然存在。

“头”要分四种看：

| 你看到的“头” | 属于哪一层 | 本项目里的实际内容 |
|---|---|---|
| HTTP 请求行 / 状态行 + Header | 应用层 | 方法、路径、状态码、`Host`、`Content-Type`、`Authorization`、CORS 等 |
| WebSocket 握手 Header | 应用层（HTTP） | `Upgrade`、`Connection`、`Sec-WebSocket-Key`、`Sec-WebSocket-Version`、`Origin` |
| WebSocket 帧头 | 应用层（WebSocket） | `FIN`、`RSV1~3`、`opcode`、`MASK`、`Payload length`、`Masking-key` |
| TCP / IP 头 | 传输层 / 网络层 | 端口、序列号、确认号、TCP flags、IP 地址、TTL 等 |

本文重点讲前三种；最后用一节把 TCP/IP 头也列出来，方便你建立完整的“线上到底有什么字节”的图景。

---

## 2. HTTP/1.1：本项目真正传的是一段文本 + 一段 JSON

### 2.1 HTTP 报文的形状

一个 HTTP/1.1 请求长这样：

```http
POST /api/login HTTP/1.1\r\n
Host: 127.0.0.1:9101\r\n
Content-Type: application/json\r\n
Content-Length: 65\r\n
Connection: keep-alive\r\n
\r\n
{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}
```

把它拆开：

```text
┌──────────────────────────────────────────────┐
│ 请求行：POST /api/login HTTP/1.1              │
├──────────────────────────────────────────────┤
│ Header：Host: ...                            │
│ Header：Content-Type: application/json       │
│ Header：Content-Length: 65                   │
│ Header：Connection: keep-alive               │
├──────────────────────────────────────────────┤
│ 空行：\r\n                                   │
├──────────────────────────────────────────────┤
│ Body：65 字节 JSON                           │
└──────────────────────────────────────────────┘
```

对应字段：

| 部分 | 格式 | 本项目例子 | 作用 |
|---|---|---|---|
| 请求行 | `Method SP Request-Target SP HTTP-Version CRLF` | `POST /api/login HTTP/1.1` | 告诉服务端“用什么方法、访问哪个路径、用哪个 HTTP 版本” |
| Header | `Field-Name: Field-Value CRLF` | `Content-Length: 65` | 描述 Body 和连接行为 |
| 空行 | `CRLF` | 一个额外的 `\r\n` | Header 结束、Body 开始的边界 |
| Body | 任意字节 | JSON UTF-8 | 业务参数 |

响应也是同样的结构：

```http
HTTP/1.1 200 OK\r\n
Content-Type: application/json; charset=utf-8\r\n
Content-Length: 120\r\n
Connection: keep-alive\r\n
\r\n
{"code":0,"message":"ok","data":{...}}
```

| 部分 | 格式 | 本项目例子 |
|---|---|---|
| 状态行 | `HTTP-Version SP Status-Code SP Reason-Phrase CRLF` | `HTTP/1.1 200 OK` |
| Header | 同上 | `Content-Type`、`Content-Length`、CORS |
| 空行 | `CRLF` | Header 结束 |
| Body | 任意字节 | 统一 JSON 信封 |

> `\r\n` 是实际字节 `0D 0A`，不是两个可见字符。本文为了看清结构，在代码块里显式写出来；真实抓包时看到的是换行。

### 2.2 本项目实际会出现的请求 Header

| Header | 例子 | 本项目用不用 | 说明 |
|---|---|---|---|
| `Host` | `127.0.0.1:9101` | 浏览器必带；服务端不主动校验 | HTTP/1.1 要求，虚拟主机/反代也会用 |
| `Content-Type` | `application/json` | **服务端不校验** | Handler 直接按 JSON 解析 Body，不管 Header 写什么 |
| `Content-Length` | `65` | **用** | llhttp 用它判断 Body 何时结束；本项目响应也总是带 |
| `Authorization` | `Bearer <token>` | **用** | `AuthenticateRequest` 优先从这里取 token，见 `logic/http_server.cpp:306` |
| `Origin` | `http://127.0.0.1:9001` | 浏览器 CORS 用；服务端不用于鉴权 | 跨端口访问 9101 时浏览器会带 |
| `Connection` | `keep-alive` / `close` | **用** | 决定响应后是否继续复用 TCP 连接 |
| `Accept` | `*/*` | 不用 | 浏览器自动带 |
| `User-Agent` | Chrome/... | 不用 | 浏览器自动带 |
| `Cookie` | — | 不用 | 本项目不用 Cookie 保存登录态 |
| `Range` | `bytes=0-1023` | 只有 Web Demo 静态文件服务器用 | `web_demo/web_server.cpp:218` 支持 `206 Partial Content` |
| `Transfer-Encoding` | `chunked` | 请求端 llhttp 可以解析，但业务层没有流式接口 | 本项目响应端不生成 chunked |
| `Content-Encoding` | — | 不用 | 没有 gzip/br 压缩 |
| `Expect` | `100-continue` | 没有显式处理 | 大文件上传不要走这条链路 |

几个项目细节：

- Header 名**大小写不敏感**。`HttpRequest::getHeader()` 会把名字转小写后比较，见 `common/net/http_server_lite.cpp:81`。
- 同名 Header 出现多次时，`getHeader()` 返回**第一个**，因为 `headers_` 是 `vector<pair<...>>`。
- `Authorization` 的前缀判断是精确的 `"Bearer "`，大小写不匹配不会被识别；取不到时再降级去 JSON Body 里找 `"token"` 字段。
- 浏览器 `fetch` 不能随便设置自定义 Header；`Authorization` 是 CORS 允许的请求头之一，所以跨端口调用 Logic 可以带。

### 2.3 本项目实际会出现的响应 Header

`common/net/http_server_lite.cpp:190` 的 `BuildResponse()` 负责把 `HttpResponse` 序列化成字节：

```cpp
oss << "HTTP/1.1 " << code << ' ' << ReasonPhrase(code) << "\r\n";
if (!resp.content_type().empty()) {
    oss << "Content-Type: " << resp.content_type() << "\r\n";
}
for (const auto& kv : resp.headers()) {
    oss << kv.first << ": " << kv.second << "\r\n";
}
if (!has_content_length) {
    oss << "Content-Length: " << resp.body().size() << "\r\n";
}
oss << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n";
oss << "\r\n";
oss << resp.body();
```

所以本项目的响应头主要是：

| Header | 例子 | 谁设置 | 说明 |
|---|---|---|---|
| `Content-Type` | `application/json; charset=utf-8` | `WriteJson` | JSON API 固定这个值 |
| `Content-Length` | `120` | `BuildResponse` 自动补 | 等于 Body 的**字节数**，不是字符数 |
| `Connection` | `keep-alive` / `close` | `BuildResponse` 自动补 | 由 `llhttp_should_keep_alive()` 决定 |
| `Access-Control-Allow-Origin` | `*`（默认） | `AddCORSHeaders` | 可通过 `cors_allow_origin` 配置 |
| `Access-Control-Allow-Methods` | `GET, POST, OPTIONS` | `AddCORSHeaders` | 注意：**没有 PUT/DELETE** |
| `Access-Control-Allow-Headers` | `Content-Type, Authorization` | `AddCORSHeaders` | 只允许这两个请求头 |
| `Access-Control-Max-Age` | `86400` | `AddCORSHeaders` | 预检结果缓存 24 小时 |

本项目**不会自动生成**这些常见 Header：

- `Date`
- `Server`
- `Set-Cookie`
- `Cache-Control`
- `ETag`
- `Last-Modified`
- `Content-Encoding`

JSON API 也**不设置** `Cache-Control`。静态文件服务器会额外设置 `Accept-Ranges`、`Content-Range`，但那是 9001 端口的 `web_demo/web_server.cpp`，不是 9101 的业务 API。

### 2.4 登录请求/响应：一条完整的 HTTP 报文

浏览器实际会多带 `Accept`、`User-Agent`、`Origin`、`Sec-Fetch-*` 等，下面只保留关键头。

请求：

```http
POST /api/login HTTP/1.1\r\n
Host: 127.0.0.1:9101\r\n
Content-Type: application/json\r\n
Content-Length: 65\r\n
Connection: keep-alive\r\n
\r\n
{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}
```

Body 正好 65 字节。服务端 `handleLogin` 做的事：

1. `RequirePost()` 检查方法必须是 POST；
2. `ParseJsonAccountPassword()` 解析 `account`、`password`；
3. 查用户、比较密码；
4. 生成 24 字节随机 token，转成 48 位十六进制字符串；
5. 写 Redis `token:{token} -> user_id`，TTL 24 小时；
6. 用统一 JSON 信封返回。

响应（Body 120 字节）：

```http
HTTP/1.1 200 OK\r\n
Content-Type: application/json; charset=utf-8\r\n
Access-Control-Allow-Origin: *\r\n
Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n
Access-Control-Allow-Headers: Content-Type, Authorization\r\n
Access-Control-Max-Age: 86400\r\n
Content-Length: 120\r\n
Connection: keep-alive\r\n
\r\n
{"code":0,"message":"ok","data":{"user_id":1,"token":"0123456789abcdef0123456789abcdef0123456789abcdef","name":"Alice"}}
```

注意：

- HTTP 状态码是 `200`，Body 里的业务码是 `"code": 0`。本项目把两者分开：HTTP 表示传输/接口层结果，`code` 表示业务结果。
- 错误时通常 HTTP 状态码也映射成 400/401/403/404/500 等；例如 `{"code":401,...}` 会配 `HTTP/1.1 401 Unauthorized`。
- 405、409 这类不在枚举里的状态码通过 `setStatusCode(int)` 透传；`ReasonPhrase()` 支持 405/409。
- `ReasonPhrase()` 目前没有 202/413/415/429；如果返回这些码，原因短语会显示成 `OK`，这是当前实现的一个小缺陷（`common/net/http_server_lite.cpp:28`）。

### 2.5 带鉴权的业务请求

发消息：

```http
POST /api/message/send HTTP/1.1\r\n
Host: 127.0.0.1:9101\r\n
Authorization: Bearer 0123456789abcdef0123456789abcdef0123456789abcdef\r\n
Content-Type: application/json\r\n
Content-Length: 34\r\n
Connection: keep-alive\r\n
\r\n
{"to_user_id":2,"content":"hello"}
```

服务端 `AuthenticateRequest()` 优先取 `Authorization: Bearer ...`，取不到再解析 JSON Body 的 `"token"` 字段。`from_user` 由 token 决定，Body 里没有 `from_user_id` 可伪造。

响应：

```json
{"code":0,"message":"ok","data":{"session_id":"single:1:2","msg_id":"single:1:2-7","msg_seq":7}}
```

Body 96 字节。

查历史：

```http
POST /api/session/history HTTP/1.1\r\n
Host: 127.0.0.1:9101\r\n
Authorization: Bearer <token>\r\n
Content-Type: application/json\r\n
Content-Length: 53\r\n
\r\n
{"session_id":"single:1:2","anchor_seq":0,"limit":20}
```

注意这里 `session_id` 放在 **JSON Body**，不是 query string。原因见下一节：本项目会把 URL 的 query 部分直接截掉。

### 2.6 本项目的 HTTP 语义细节（很重要）

#### 路径匹配是精确匹配，没有路径参数

`logic/http_server.cpp:235` 的路由表是：

```cpp
static const std::unordered_map<std::string, Handler> kRouteTable = {
    {"/api/login", &HttpApiServer::handleLogin},
    {"/api/message/send", &HttpApiServer::handleSendMessage},
    {"/api/session/history", &HttpApiServer::handleHistory},
    ...
};
```

`onRequest()` 直接 `kRouteTable.find(path)`：

- 只支持精确路径；
- 不支持 `/api/media/{media_id}` 这样的路径参数；
- 不支持通配符、正则、前缀匹配；
- 未命中直接返回 404。

这对媒体二次开发有直接影响：`docs/media_api_spec.md` 里的 `GET /api/media/{media_id}` 目前**不能直接挂到这张表上**，需要新增路径解析，或者把 `media_id` 放进 query/body。但 query 又会被截掉（见下一条），所以要么改解析器，要么改成 POST + JSON Body。

#### URL 的 query 会被截掉

`common/net/http_server_lite.cpp:131` 的 `OnUrl()`：

```cpp
std::string full(at, len);
auto qpos = full.find('?');
if (qpos != std::string::npos) full.resize(qpos);
c->req.set_path(std::move(full));
```

所以：

```text
GET /api/session/history?session_id=single:1:2&limit=20
                ↓
handler 看到的 path() 是 "/api/session/history"
query 完全丢失
```

当前所有业务接口都把参数放在 JSON Body 或 `Authorization` Header 里，正是为了绕开这个限制。

#### Body 是全量缓冲，不是流式

`OnBody()` 把每次收到的 Body 片段追加到 `std::string`：

```cpp
auto b = c->req.body();
b.append(at, len);
c->req.set_body(std::move(b));
```

`HttpServerLite` 只有在 `ctx_.complete == true` 时才调用业务回调：

```cpp
if (ctx_.complete) {
    HttpResponse resp;
    if (callback_) callback_(ctx_.req, &resp);
    ...
}
```

这意味着：

- 请求 Body 必须完整收完，handler 才能开始执行；
- 没有 multipart 解析器；
- 没有“边收边处理”的流式 API；
- 没有显式的 Body 大小上限，`Content-Length` 很大时会在内存里一直增长；
- handler 在 Asio IO 线程里同步执行，慢 SQL/慢 Redis 会阻塞该 IO 线程。

所以**图片/视频字节不应该走 Logic 的这条 HTTP 链路**。这也是媒体方案选择 MinIO 直传的根本原因。

#### 没有 `Content-Type` 校验

`handleLogin`、`handleSendMessage` 等只检查 Body 是否是合法 JSON，不检查 `Content-Type`。即使客户端发 `Content-Type: text/plain`，只要 Body 是 JSON，也能成功。

这在学习项目里没问题；生产接口应该：

- 强制 `Content-Type: application/json`；
- 不匹配返回 415；
- 限制 Body 大小，超限返回 413。

#### CORS 预检

浏览器页面在 `http://127.0.0.1:9001`，Logic 在 `http://127.0.0.1:9101`，属于跨源。对于 `Content-Type: application/json` 或 `Authorization` 这类非简单请求，浏览器会先发：

```http
OPTIONS /api/message/send HTTP/1.1
Host: 127.0.0.1:9101
Origin: http://127.0.0.1:9001
Access-Control-Request-Method: POST
Access-Control-Request-Headers: content-type,authorization
```

`logic/http_server.cpp:225` 对**任意路径**的 `OPTIONS` 都直接返回：

```http
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 86400
```

预检不鉴权、不走业务路由。浏览器看到这些头后，才发真正的 POST。

注意：

- `Access-Control-Allow-Methods` 没有 `PUT`。如果以后要浏览器直接 `PUT` 到 MinIO，CORS 由 **MinIO 桶配置**负责，不是 Logic 这段代码。
- `Access-Control-Allow-Headers` 只有 `Content-Type`、`Authorization`。如果以后加 `X-Request-Id`、`X-CSRF-Token`，预检会失败，需要同步加。

#### HTTP/1.1 keep-alive

`OnMessageComplete()` 用 `llhttp_should_keep_alive()` 记录是否保持连接；`BuildResponse()` 总是写 `Connection: keep-alive` 或 `close`。

- 浏览器和 Logic 之间可以复用同一条 TCP 连接发多个 HTTP 请求；
- 每个请求仍然有自己的请求行、Header、空行、Body；
- 这不是 WebSocket，仍然是“请求-响应”；
- 响应必须能界定 Body 长度（本项目总是 `Content-Length`），否则下一个响应无法定位。

#### 没有 HTTP/2、没有 TLS、没有 WebSocket 之外的推送

- `HttpServerLite` 只实现 HTTP/1.1；
- 端口 9101 是明文 `http://`；
- 没有 `https://`，没有 HTTP/2；
- 服务端主动推送走 9200 的 WebSocket，不走 HTTP 长轮询。

#### 静态文件服务器是另一套行为

`web_demo/web_server.cpp` 也复用 `HttpServerLite`，但只接受 GET/HEAD，并且支持：

- `Accept-Ranges: bytes`
- `Range: bytes=start-end`
- `206 Partial Content`
- `Content-Range: bytes start-end/total`
- `416 Range Not Satisfiable`
- 按扩展名猜 `Content-Type`：`.html`、`.js`、`.css`、`.png`、`.jpg/.jpeg`、`.mp4`

它只服务 `web_demo/static` 下的演示页面，不是用户上传文件的存储服务，也没有鉴权。后续媒体文件应该走 MinIO/S3/CDN，而不是把文件塞进这个目录。

---

## 3. WebSocket：先 HTTP 握手，之后是二进制帧

### 3.1 为什么用它

HTTP 是“客户端问、服务端答”。如果服务端要主动把消息推给浏览器，HTTP/1.1 只能：

- 轮询：浏览器每隔几秒问一次；
- 长轮询：挂住一个请求，等有消息再返回；
- SSE：服务端单向推送。

本项目需要**双向、低延迟、服务端主动推送**，所以浏览器和 Comet 之间用 WebSocket：

```text
浏览器 A ──WS──▶ Comet ──gRPC──▶ Logic ──Kafka──▶ Job ──gRPC──▶ Comet ──WS──▶ 浏览器 B
```

WebSocket 只负责“浏览器 ↔ Comet”这一段；Comet 之后全部是内部 RPC/消息队列。

### 3.2 握手：一条标准的 HTTP/1.1 Upgrade 请求

浏览器代码在 `web_demo/static/index.html:645`：

```js
const socket = new WebSocket(cometWsUrl + '?token=' + encodeURIComponent(token));
```

假设 `cometWsUrl = ws://127.0.0.1:9200/ws`，浏览器发出的握手请求大致是：

```http
GET /ws?token=0123456789abcdef0123456789abcdef0123456789abcdef HTTP/1.1\r\n
Host: 127.0.0.1:9200\r\n
Upgrade: websocket\r\n
Connection: Upgrade\r\n
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n
Sec-WebSocket-Version: 13\r\n
Origin: http://127.0.0.1:9001\r\n
Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n
User-Agent: Mozilla/5.0 ...\r\n
\r\n
```

关键 Header：

| Header | 例子 | 作用 |
|---|---|---|
| `Host` | `127.0.0.1:9200` | HTTP/1.1 目标主机 |
| `Upgrade` | `websocket` | 告诉服务端“我想升级到 WebSocket” |
| `Connection` | `Upgrade` | 逐跳头，表示本次连接要升级 |
| `Sec-WebSocket-Key` | 16 字节随机数的 Base64 | 握手随机数，**不是密码、不是 token** |
| `Sec-WebSocket-Version` | `13` | 使用 RFC 6455 的版本 13 |
| `Origin` | `http://127.0.0.1:9001` | 浏览器页面来源；服务端可用来做来源校验 |
| `Sec-WebSocket-Extensions` | `permessage-deflate; ...` | 浏览器希望协商压缩扩展 |
| `Sec-WebSocket-Protocol` | 可选 | 子协议；本项目没有设置 |
| `Authorization` | **没有** | 浏览器的 `new WebSocket()` 不能自定义这个头 |

**为什么 token 在 URL 里？**

因为浏览器原生 `WebSocket` 构造函数只接受 URL 和可选的子协议，不能像 `fetch` 那样自由设置 `Authorization` Header。常见做法有：

1. 把 token 放 query：本项目采用；
2. 用 Cookie：需要处理跨域、CSRF；
3. 先 HTTP 换一个一次性 ticket，再拿 ticket 连 WS；
4. 用 `Sec-WebSocket-Protocol` 夹带 token（不推荐，语义不对）。

本项目的实现：

```cpp
// comet/comet_server.cpp:15
std::string ExtractTokenFromResource(const std::string& resource) {
    auto qpos = resource.find("token=");
    if (qpos == std::string::npos) return {};
    qpos += 6;
    std::string token = resource.substr(qpos);
    auto amp = token.find('&');
    if (amp != std::string::npos) token = token.substr(0, amp);
    return token;
}
```

它有两个特点：

- 只做 `token=` 子串查找，**不做 URL 解码**；
- 只截到 `&` 为止，不校验 `token=` 是否真的在 query 参数位置。

当前 token 是 48 位十六进制，`encodeURIComponent` 不会改变它，所以能用。如果以后 token 里出现 `%`、`+`、`&` 等字符，这段代码会出错。

### 3.3 握手成功：`101 Switching Protocols`

服务端返回：

```http
HTTP/1.1 101 Switching Protocols\r\n
Upgrade: websocket\r\n
Connection: Upgrade\r\n
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n
\r\n
```

响应 Header：

| Header | 例子 | 说明 |
|---|---|---|
| 状态行 | `HTTP/1.1 101 Switching Protocols` | 101 表示协议切换，不是普通的 200 |
| `Upgrade` | `websocket` | 确认升级 |
| `Connection` | `Upgrade` | 确认升级 |
| `Sec-WebSocket-Accept` | Base64(SHA1(key + GUID)) | 对客户端 `Sec-WebSocket-Key` 的应答 |
| `Sec-WebSocket-Extensions` | **没有** | 本项目没有启用 `permessage-deflate` |
| `Sec-WebSocket-Protocol` | **没有** | 本项目没有设置子协议 |
| `Server` | **没有** | websocketpp 没有设置 user agent 时不会加 |
| `Date` | **没有** | websocketpp 不会自动加 |
| `Content-Length` | **没有** | 101 没有 Body，也不需要 Content-Length |

`Sec-WebSocket-Accept` 的计算：

```text
GUID = 258EAFA5-E914-47DA-95CA-C5AB0DC85B11
Accept = Base64( SHA1( Sec-WebSocket-Key + GUID ) )
```

以 `Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==` 为例，结果是：

```text
s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

这个值证明服务端真的理解 WebSocket 握手，**不是身份认证**。认证靠的是 URL 里的 token，以及 `OnValidate()` 调 Logic 的 `VerifyToken`。

### 3.4 本项目的握手鉴权流程

`comet/comet_server.cpp:136` 的 `OnValidate()`：

```text
浏览器 GET /ws?token=...
  ↓
websocketpp 解析 HTTP 请求
  ↓
OnValidate()
  ├─ 从 resource 里截 token
  ├─ 没有 token → return false → 握手失败
  ├─ 生成 conn_id，形如 comet-1:7
  ├─ 同步调用 Logic gRPC VerifyToken(token, comet_id, conn_id)
  │    ├─ Redis GET token:{token} → user_id
  │    └─ Redis HSET route:user:{uid} conn_id comet_id
  ├─ gRPC 失败或 error.code()!=0 → return false
  └─ 成功 → ctx->user_id = user_id，挂到连接上
  ↓
OnOpen()：加入本机连接注册表和心跳桶
```

几个关键点：

- **同步阻塞**：`VerifyToken` 是在 Asio IO 线程里同步调的，Logic 慢会卡住该线程上的握手。
- **失败返回 400**：`OnValidate` 返回 false 时，websocketpp 默认回 `HTTP/1.1 400 Bad Request`，不是 401，也不会返回 JSON 错误体。浏览器看到的是 `WebSocket connection failed`。
- **握手后不再校验 token**：连接建立后，消息只依赖 `ctx->user_id`；token 过期不会主动断开已有连接。
- **不校验 Origin**：项目没有检查 `Origin` 是否来自允许的页面。
- **没有 TLS**：`WsConfig : websocketpp::config::asio`，即 `asio_no_tls`，所以只能是 `ws://`。
- **没有子协议**：没有 `Sec-WebSocket-Protocol` 协商。

### 3.5 握手之后：WebSocket 帧

`101` 之后，双方不再发送 HTTP 报文，而是发送 WebSocket 帧。帧是二进制的，结构如下：

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |                               |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - - +-------------------------------+
|                               |Masking-key, if MASK set to 1  |
+-------------------------------+-------------------------------+
| Masking-key (continued)       |          Payload Data         |
+-------------------------------- - - - - - - - - - - - - - - - +
:                     Payload Data continued ...                :
+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
|                     Payload Data continued ...                |
+---------------------------------------------------------------+
```

帧头字段：

| 字段 | 位数 | 含义 | 本项目实际情况 |
|---|---:|---|---|
| `FIN` | 1 | 是否消息的最后一帧 | 一条 JSON 通常一帧，`FIN=1` |
| `RSV1` | 1 | 扩展位 1 | 没有启用压缩，必须为 0 |
| `RSV2` | 1 | 扩展位 2 | 0 |
| `RSV3` | 1 | 扩展位 3 | 0 |
| `opcode` | 4 | 帧类型 | 文本 `0x1`、二进制 `0x2`、关闭 `0x8`、ping `0x9`、pong `0xA` |
| `MASK` | 1 | 客户端到服务端是否掩码 | 浏览器→Comet 必须为 1；Comet→浏览器为 0 |
| `Payload len` | 7 | 载荷长度 | 0~125 直接表示；126/127 表示后面还有扩展长度 |
| `Extended payload length` | 16 或 64 | 更大的长度 | 126 → 后 2 字节；127 → 后 8 字节 |
| `Masking-key` | 32 | 掩码密钥 | 仅 `MASK=1` 时存在，4 字节 |
| `Payload Data` | 可变 | 应用数据 | 本项目是 UTF-8 JSON 文本 |

opcode 取值：

| opcode | 名称 | 本项目用途 |
|---:|---|---|
| `0x0` | continuation | 分片消息的后续帧；websocketpp 会重组 |
| `0x1` | text | **业务 JSON 全部用它** |
| `0x2` | binary | 本项目不处理，`OnMessage` 直接 return |
| `0x8` | close | 关闭连接，可带状态码和原因 |
| `0x9` | ping | 心跳，服务端发 `"ping"` |
| `0xA` | pong | 心跳应答，浏览器自动回 |

控制帧（close/ping/pong）的规则：

- 载荷长度不能超过 125 字节；
- 不能被分片；
- 可以插在分片消息的中间。

### 3.6 帧头到底有多长

因为 `MASK` 和 `Payload len` 的存在，帧头不是固定的 2 字节：

| 方向 | Payload 长度 | 帧头长度 | 组成 |
|---|---:|---:|---|
| 客户端→服务端 | 0~125 | 6 | 2 + 4 字节 mask |
| 客户端→服务端 | 126~65535 | 8 | 2 + 2 扩展长度 + 4 mask |
| 客户端→服务端 | >65535 | 14 | 2 + 8 扩展长度 + 4 mask |
| 服务端→客户端 | 0~125 | 2 | 2 |
| 服务端→客户端 | 126~65535 | 4 | 2 + 2 扩展长度 |
| 服务端→客户端 | >65535 | 10 | 2 + 8 扩展长度 |

本项目业务 JSON 一般只有几十到几百字节：

- 客户端发一帧 96 字节的 `single_chat`：帧头 6 字节；
- 服务端回 70 字节的 ACK：帧头 2 字节；
- 服务端下发 165 字节的单聊消息：帧头 4 字节（因为 165 > 125，要用 16 位扩展长度）。

### 3.7 字节级例子

#### 例 1：客户端发送文本 `"hi"`

假设 4 字节掩码是 `37 FA 21 3D`：

```text
81 82 37 FA 21 3D 5F 93
```

拆解：

```text
81        FIN=1, RSV=000, opcode=0001(text)
82        MASK=1, Payload len=2
37 FA 21 3D   4 字节 Masking-key
5F 93     掩码后的 payload
```

原始 payload：

```text
'h' = 0x68，0x68 XOR 0x37 = 0x5F
'i' = 0x69，0x69 XOR 0xFA = 0x93
```

服务端收到后按 `Masking-key` 异或还原成 `"hi"`。

#### 例 2：客户端发 96 字节的单聊 JSON

```text
81 E0 <4 字节 mask> <96 字节掩码后的 JSON>
```

- `0x81`：FIN=1，text；
- `0xE0 = 0x80 | 96`：MASK=1，长度 96；
- 6 字节帧头，后面是掩码后的 JSON。

#### 例 3：服务端回 70 字节 ACK

```text
81 46 <70 字节 JSON>
```

- `0x81`：FIN=1，text；
- `0x46 = 70`：服务端不掩码，长度 70；
- 2 字节帧头。

#### 例 4：服务端下发 165 字节消息

```text
81 7E 00 A5 <165 字节 JSON>
```

- `0x81`：FIN=1，text；
- `0x7E`：表示后面有 2 字节扩展长度；
- `00 A5` = 165；
- 4 字节帧头。

#### 例 5：服务端 ping

`ws_.ping(handle, "ping")` 发出的帧：

```text
89 04 70 69 6E 67
```

- `0x89`：FIN=1，opcode=0x9(ping)；
- `0x04`：长度 4；
- `70 69 6E 67`：`"ping"`。

浏览器收到 ping 后自动回 pong，业务代码不需要处理。

#### 例 6：关闭帧

服务端心跳超时会调用：

```cpp
ws_.close(handle, websocketpp::close::status::normal,
          "heartbeat timeout", ec);
```

发出的 close 帧大致是：

```text
88 <len> 03 E8 68 65 61 72 74 62 65 61 74 20 74 69 6D 65 6F 75 74
```

- `0x88`：FIN=1，opcode=0x8(close)；
- `03 E8`：状态码 1000（normal）；
- 后面是 UTF-8 原因 `"heartbeat timeout"`。

---

## 4. 本项目 WebSocket 上真正传的业务 JSON

### 4.1 浏览器发：`single_chat`

`web_demo/static/index.html:971`：

```json
{
  "type": "single_chat",
  "to_user_id": 2,
  "client_msg_id": "1710000000000",
  "content": { "text": "hello" }
}
```

紧凑形式 96 字节。字段含义：

| 字段 | 谁填 | Comet 怎么处理 |
|---|---|---|
| `type` | 浏览器 | 映射成 `scene`：`single_chat` → `single` |
| `to_user_id` | 浏览器 | 校验 >0，复制到 `UpstreamMessageRequest.to_user_id` |
| `client_msg_id` | 浏览器 | 复制到 `UpstreamMessageRequest.client_msg_id`，用于 ACK 对账 |
| `content` | 浏览器 | **Comet 完全不解析**，整条原始 JSON 原样放进 `content_json` |
| `from_user_id` | **不传** | Comet 用握手时 `ctx->user_id` 填，浏览器伪造不了 |
| `session_id` | **不传** | Logic 根据双方 user_id 计算 |
| `msg_seq` | **不传** | Logic 用 Redis `INCR` 分配 |

Comet 的 `HandleTextMessage()` 只解析 `type`、`to_user_id`、`client_msg_id`、`group_id`、`video_id`、`timeline_ms` 这些元数据；`content` 原样保留。

### 4.2 Comet → Logic：`UpstreamMessageRequest`

Comet 把原始 JSON 放进 protobuf 的 `content_json` 字段：

```protobuf
message UpstreamMessageRequest {
  int64 from_user_id = 1;
  string client_msg_id = 2;
  Scene scene = 3;
  int64 to_user_id = 4;
  int64 group_id = 5;
  string video_id = 6;
  int64 timeline_ms = 7;
  string content_json = 8;    // 整条原始 WS JSON
  string comet_id = 9;
  string conn_id = 10;
  int64 received_time_ms = 11;
}
```

这一段走 gRPC，不再是 WebSocket。`content_json` 是字符串字段，里面装的是刚才那整段 JSON 文本。

### 4.3 Logic 注入字段，生成最终下发 JSON

`logic/grpc_service.cpp:353` 左右：

```cpp
std::string push_json = request.content_json();
auto j = nlohmann::json::parse(push_json);
j["msg_id"] = msg_id;
j["create_time"] = now_ms;
j["from_user_id"] = from_user;
if (!j.contains("client_msg_id")) {
    j["client_msg_id"] = request.client_msg_id();
}
push_json = j.dump();
```

最终 `content_json` 大致是：

```json
{
  "type": "single_chat",
  "to_user_id": 2,
  "client_msg_id": "1710000000000",
  "content": { "text": "hello" },
  "msg_id": "single:1:2-7",
  "create_time": 1710000000123,
  "from_user_id": 1
}
```

紧凑形式 165 字节。注意：

- `msg_id`、`create_time`、`from_user_id` 是 Logic 注入的；
- `msg_seq` **没有**出现在顶层 JSON 里（它只在 `ChatMessage` protobuf 和 MySQL 里）；
- `session_id` **没有**出现在顶层 JSON 里；
- 前端是用 `from_user_id + to_user_id` 自己算会话的。

### 4.4 Comet → 浏览器：只发 `content_json`

Job 通过 gRPC 调 Comet 的 `PushToComet`，Comet 最终调用 `PayloadFor()`（`comet/comet_server.cpp:576`）：

```cpp
static std::string PayloadFor(const ChatMessage& msg) {
    return msg.content_json().empty()
               ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
               : msg.content_json();
}
```

然后：

```cpp
ws_.send(handle, payload, websocketpp::frame::opcode::text, ec);
```

所以浏览器收到的就是上面那段 `content_json`，作为一个 **WebSocket text 帧**。`ChatMessage` 里的 `msg_seq`、`session_id`、`sender_id`、`msg_type`、`timestamp_ms` 不会自动变成 WS 帧的 Header，也不会自动出现在 JSON 里。

### 4.5 ACK

Comet 在 Logic 返回成功后发：

```json
{"client_msg_id":"1710000000000","type":"ack","msg_id":"single:1:2-7"}
```

紧凑形式 70 字节。

失败时：

```json
{"client_msg_id":"1710000000000","type":"error","message":"send failed"}
```

如果消息格式不对，Comet 直接发：

```json
{"type":"error","message":"invalid message format"}
```

如果类型不支持：

```json
{"type":"error","message":"unsupported type"}
```

### 4.6 聊天室加入/离开

浏览器发：

```json
{"type":"chatroom_join","group_id":1}
```

Comet 本机加入房间，然后回：

```json
{"type":"ack","op":"chatroom_join"}
```

离开同理：

```json
{"type":"chatroom_leave","group_id":1}
```

回：

```json
{"type":"ack","op":"chatroom_leave"}
```

这两个 ACK 没有 `client_msg_id`，因为加入/离开不是一条持久化消息。

### 4.7 WebSocket 路径和 HTTP 路径的差异

同样是“发一条单聊消息”，两条入口的 `content_json` 不完全一样：

| 项目 | WebSocket 路径 | HTTP `/api/message/send` 路径 |
|---|---|---|
| 入口 | Comet → Logic gRPC | Logic 的 HTTP handler |
| 鉴权 | 握手时 URL token，之后靠 `ctx->user_id` | 每个请求 `Authorization` 或 Body token |
| `content` 类型 | 前端传对象 `{ "text": "hello" }` | 前端传字符串 `"hello"` |
| `msg_seq` 是否在 JSON 顶层 | 否 | 是 |
| `client_msg_id` 是否在 JSON 顶层 | 是 | 否（HTTP handler 没有设置） |
| 发送结果 | WebSocket ACK | HTTP JSON 响应 |
| `msg_type` | `TEXT`（弹幕为 `DANMAKU`） | 目前总是 `TEXT` |

这也是为什么 `docs/message.md` 里要分别列两条路径。协议层看都是 JSON，但字段构造逻辑不同。

### 4.8 二进制帧为什么不用

`OnMessage()` 只接受 text：

```cpp
if (msg->get_opcode() != websocketpp::frame::opcode::text) return;
```

所以浏览器如果发 `ArrayBuffer`/`Blob`，Comet 会**静默忽略**，不会回错误。当前项目没有二进制应用协议。

这对媒体功能的含义：

- 不要把图片/视频字节塞进 WebSocket；
- WebSocket 只适合传“媒体消息控制信息”（`media_id`、宽高、缩略图 URL 等）；
- 真正的文件字节走 HTTP `PUT` 到 MinIO/S3。

### 4.9 心跳与超时

`comet/comet_server.h:123`：

```cpp
static constexpr size_t kHeartbeatBucketCount = 64;
static constexpr auto kHeartbeatTick = std::chrono::milliseconds(500);
static constexpr auto kHeartbeatTimeout = std::chrono::seconds(60);
```

`ScanNextHeartbeatBucket()` 每 500ms 扫描一个桶，64 个桶轮一圈约 32 秒。每条连接按 `conn_id` 序号固定分到某个桶，所以：

- 每条连接大约每 32 秒被扫描一次；
- 扫描时如果 `now - last_pong > 60s`，关闭连接；
- 否则发送一个 ping（payload `"ping"`）；
- 浏览器自动回 pong；
- `OnPong()` 刷新 `last_pong_ms`。

实际超时检测不是精确 60 秒，而是 60~92 秒左右（取决于连接所在桶下一次被扫描的时间）。这个设计是为了避免一次扫描遍历所有连接。

连接关闭后 `OnClose()` 会：

- 从本机 `ConnRegistry` 移除；
- 调 Logic 的 `UserOffline` 清理 Redis 路由；
- 对加入过的房间上报 `ReportRoomLeave`。

---

## 5. 同一条消息在各段链路上到底长什么样

### 5.1 总表

| 链路 | 协议 | “头”是什么 | 实际载荷 |
|---|---|---|---|
| 浏览器 → Comet | TCP + WebSocket | WS 帧头（2~14 字节） | UTF-8 JSON text 帧 |
| Comet → Logic | TCP + HTTP/2 + gRPC | HTTP/2 HEADERS（HPACK 压缩）+ gRPC 5 字节前缀 | protobuf `UpstreamMessageRequest` |
| Logic → Kafka | TCP + Kafka 协议 | Kafka 请求头 + Record Batch 头 | key 字符串 + value 字节（protobuf `PushToCometRequest`） |
| Kafka → Job | 同上 | 同上 | 同上 |
| Job → Comet | TCP + HTTP/2 + gRPC | HTTP/2 HEADERS + gRPC 前缀 | protobuf `PushToCometRequest` |
| Comet → 浏览器 | TCP + WebSocket | WS 帧头 | `content_json` 文本帧 |
| Job → MySQL | TCP + MySQL 协议 | MySQL 协议包头 | SQL 参数绑定值 |

### 5.2 gRPC 这一段

Comet 调 Logic 默认走 `MessageStream` 双向流（`conf/comet.conf` 里 `use_grpc_stream=true`），也可以走 unary `SendUpstreamMessage`。无论哪种，底层都是 HTTP/2：

```text
HEADERS 帧（HPACK 压缩）：
  :method: POST
  :scheme: http
  :path: /sparkpush.LogicService/MessageStream
  :authority: 127.0.0.1:9100
  content-type: application/grpc
  te: trailers
  grpc-timeout: ...
  user-agent: grpc-c++/...
```

然后是 DATA 帧，里面是 gRPC 消息：

```text
+-------------------------------+-------------------------------+
| 1 字节压缩标志（0=不压缩）      | 4 字节大端消息长度             |
+-------------------------------+-------------------------------+
| protobuf 序列化后的字节                                        |
+---------------------------------------------------------------+
```

响应末尾的 trailers 里会有：

```text
grpc-status: 0
grpc-message: ...
```

注意：**gRPC 的 `error.code()` 是 protobuf 里的业务错误，不是 HTTP 状态码**。`VerifyToken` 对无效 token 常常返回 `grpc-status: 0`，但 `ErrorInfo.code = 401`；所以 Comet 要同时检查 `status.ok()` 和 `vrep.error().code()`。

### 5.3 Kafka 这一段

Logic 发送的是：

```text
topic: push_to_comet
key:   "single:1:2|comet-1"        // 在线
value: protobuf(PushToCometRequest)
```

离线只落盘时：

```text
key:   "single:1:2"                // 只按会话分区
value: protobuf(PushToCometRequest) // persist_only=true
```

Kafka 记录本身有：

- `key` 字节串；
- `value` 字节串；
- 可选的 record headers；
- topic / partition / offset / timestamp。

本项目主要利用 key 做分区有序：同一个 `session_id|comet_id` 进同一分区，保证同一会话到同一 Comet 的消息有序。protobuf 的具体字段见 `docs/message.md`。

### 5.4 为什么内部不用 HTTP/1.1

- gRPC 有强类型 protobuf schema，字段演进比手写 JSON 稳；
- HTTP/2 支持多路复用，一个连接上可以并发多个 RPC；
- gRPC 支持 streaming，Comet 和 Logic 之间可以跑双向流；
- 二进制比 JSON 紧凑，适合内部高频调用；
- 但浏览器原生不支持直接发 gRPC，所以浏览器这一段仍然是 HTTP/JSON + WebSocket/JSON。

---

## 6. 再往下看一层：TCP/IP 头

如果你抓包，会看到 HTTP/WS 字节外面还有 TCP/IP 头。它们和业务字段无关，但面试里经常问“一个 HTTP 请求在网络上到底发什么”。

### 6.1 IP 头（20~60 字节）

| 字段 | 位数 | 作用 |
|---|---:|---|
| Version | 4 | IPv4 = 4 |
| IHL | 4 | IP 头长度，单位 4 字节 |
| DSCP / ECN | 8 | 服务类型/拥塞标记 |
| Total Length | 16 | 整个 IP 包长度 |
| Identification | 16 | 分片标识 |
| Flags | 3 | 是否分片、是否最后一片 |
| Fragment Offset | 13 | 分片偏移 |
| TTL | 8 | 经过路由器的跳数上限 |
| Protocol | 8 | 6 = TCP，17 = UDP |
| Header Checksum | 16 | 首部校验和 |
| Source Address | 32 | 源 IP |
| Destination Address | 32 | 目的 IP |
| Options | 可变 | 可选 |

### 6.2 TCP 头（20~60 字节）

| 字段 | 位数 | 作用 |
|---|---:|---|
| Source Port | 16 | 源端口，例如浏览器随机端口 |
| Destination Port | 16 | 目的端口，例如 9101 / 9200 |
| Sequence Number | 32 | 本端字节流序号 |
| Acknowledgment Number | 32 | 期望收到的下一个字节序号 |
| Data Offset | 4 | TCP 头长度 |
| Reserved | 3 | 保留 |
| Flags | 9 | NS/CWR/ECE/URG/ACK/PSH/RST/SYN/FIN |
| Window | 16 | 接收窗口，流控 |
| Checksum | 16 | 校验和 |
| Urgent Pointer | 16 | 紧急数据指针 |
| Options | 可变 | MSS、窗口缩放、SACK、时间戳等 |

### 6.3 Ethernet 头（14 字节 + 4 字节 FCS）

| 字段 | 位数 | 作用 |
|---|---:|---|
| Destination MAC | 48 | 目的网卡地址 |
| Source MAC | 48 | 源网卡地址 |
| EtherType | 16 | 0x0800 = IPv4 |
| Payload | 可变 | IP 包 |
| FCS | 32 | 帧校验序列，通常由网卡处理 |

本机 loopback 抓包通常看不到 Ethernet 头，因为不走物理网卡。

### 6.4 关键：TCP 是字节流，不保留 HTTP/WS 的消息边界

HTTP 的一个请求可能被拆成多个 TCP 段，也可能多个请求合在一个 TCP 段里。接收端靠：

- HTTP：`Content-Length` 或 `Transfer-Encoding: chunked` 判断 Body 结束；
- WebSocket：帧头里的 `Payload length` 判断一帧结束；
- TCP：只负责按序、可靠地送字节，不关心里面是 HTTP 还是 WS。

所以“一个 HTTP 请求就是一个 TCP 包”是错的；“一个 WebSocket 帧就是一个 TCP 包”也是错的。抓包时要按协议重新组装。

---

## 7. 本项目协议层的安全与健壮性问题

这部分不是让你现在改代码，而是让你知道当前实现的边界，面试时也能主动说出来。

### 7.1 没有 TLS

- `http://127.0.0.1:9101`、`ws://127.0.0.1:9200` 都是明文；
- token、消息内容、`Authorization` 都能被中间人看到；
- 生产环境应使用 `https://` 和 `wss://`；
- WebSocket 的 `wss://` 需要 websocketpp 的 TLS 配置（`asio_tls`）和证书，当前用的是 `asio_no_tls`。

### 7.2 Token 放在 WebSocket URL query 里

风险：

- 可能进入反向代理访问日志、APM、抓包记录；
- 浏览器开发者工具、错误上报可能带上完整 URL；
- 没有 URL 解码，token 字符集受限；
- 不支持 `Authorization` Header 是浏览器 WebSocket API 的限制，但可以用一次性 ticket 缓解。

改进方向：

1. HTTP 先换一个短 TTL、一次性的 `ws_ticket`；
2. WebSocket 握手只带 ticket；
3. Comet 用 ticket 换 user_id，ticket 立即失效；
4. 生产环境配合 `wss://`。

### 7.3 不校验 Origin

`OnValidate()` 只检查 token，不检查 `Origin`。非浏览器客户端可以伪造 Origin，浏览器客户端则可能被恶意页面利用（如果 token 通过 XSS 泄露）。

改进方向：

- 配置允许的 Origin 白名单；
- 在 `OnValidate()` 里比较 `conn->get_origin()`；
- 不匹配直接拒绝握手。

### 7.4 握手后不再校验 token

连接建立后，token 过期、Redis 删除 token，已有连接仍然能发消息。当前依赖：

- token TTL 24 小时；
- 连接断开时 `UserOffline` 清理路由；
- 没有主动踢人接口。

改进方向：

- 定期或在每次上行消息时检查 token 状态；
- 提供管理端“踢下线”能力；
- 连接上绑定 token 过期时间，到期主动 close。

### 7.5 HTTP Body 没有上限

`HttpServerLite` 没有检查 `Content-Length` 上限，攻击者可以发一个超大 Body 让 `std::string` 持续增长。

改进方向：

- 配置 `max_body_size`；
- 超过返回 `413 Payload Too Large`；
- 当前 `ReasonPhrase()` 还没有 413，需要补；
- 对 `Expect: 100-continue` 做处理，避免客户端先发大 Body。

### 7.6 `Content-Type` 不校验

当前 JSON API 不检查 `Content-Type`。生产接口应：

- 只接受 `application/json`；
- 不匹配返回 415；
- 当前 `ReasonPhrase()` 也没有 415。

### 7.7 CORS 过于宽松

默认 `Access-Control-Allow-Origin: *`，允许任意页面跨域调用。生产环境应改成明确的前端域名，并考虑：

- 是否允许 `Authorization`；
- 是否需要 `Access-Control-Allow-Credentials`；
- 是否限制 `Access-Control-Allow-Methods`。

### 7.8 限流被注释

`handleSendMessage` 和 `HandleUpstreamMessage` 里的 Redis 限流代码目前是注释状态，压测时关闭了。生产环境需要恢复，并补 429 响应。

### 7.9 WebSocket 二进制帧被静默忽略

`OnMessage()` 对非 text 帧直接 return，不给客户端任何错误。这在当前“只传 JSON”的设计下没问题，但调试时容易困惑。可以改成：

- 收到 binary 回 `{"type":"error","message":"binary not supported"}`；
- 或者明确支持二进制协议。

### 7.10 没有压缩

`websocketpp::config::core` 默认的 `permessage_deflate_type` 是 `disabled`，所以：

- 浏览器即使发 `Sec-WebSocket-Extensions: permessage-deflate`，服务端也不会协商；
- 帧的 `RSV1` 必须为 0；
- 业务 JSON 小，不压缩影响不大；
- 如果以后要传较大的 JSON，可以启用压缩，但要处理 `RSV1` 和内存。

---

## 8. 调试方法

### 8.1 看 HTTP

```bash
# 登录，打印响应头和 Body
curl -i -v http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"alice","password":"e10adc3949ba59abbe56e057f20f883e"}'

# 带 token 发消息
curl -i http://127.0.0.1:9101/api/message/send \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"to_user_id":2,"content":"hello"}'
```

`curl -v` 会显示：

```text
> POST /api/login HTTP/1.1
> Host: 127.0.0.1:9101
> Content-Type: application/json
> Content-Length: 65
>
< HTTP/1.1 200 OK
< Content-Type: application/json; charset=utf-8
< Content-Length: 120
< Connection: keep-alive
```

### 8.2 看 WebSocket 握手和帧

浏览器开发者工具：

```text
F12 → Network → WS → 选中连接 → Messages
```

可以看到：

- 握手请求/响应 Header；
- 每一帧的 direction、opcode、length、payload；
- text 帧里的 JSON。

命令行工具（需要安装）：

```bash
# websocat
websocat -v 'ws://127.0.0.1:9200/ws?token=<token>'

# 或 wscat
wscat -c 'ws://127.0.0.1:9200/ws?token=<token>'
```

握手失败时重点看：

- token 是否有效；
- 是否 400；
- Comet 日志里 `VerifyToken failed`；
- Logic gRPC 是否可达。

### 8.3 抓 TCP/IP

```bash
# 看 9101 的 HTTP 明文
sudo tcpdump -i lo -A -s0 'tcp port 9101'

# 看 9200 的 WebSocket 握手和帧（二进制部分会用十六进制显示）
sudo tcpdump -i lo -X -s0 'tcp port 9200'
```

因为当前没有 TLS，所以能看到明文 JSON 和 Header。生产上 `wss://` 之后只能看到 TLS 记录头，看不到内容。

---

## 9. 对媒体二次开发的直接影响

结合 [`media_upload_design.md`](media_upload_design.md) 和 [`media_api_spec.md`](media_api_spec.md)：

### 9.1 文件字节不能走现有 HTTP 链路

原因：

- `HttpServerLite` 全量缓冲 Body；
- 没有 multipart；
- 没有流式接口；
- 没有 Body 大小限制；
- handler 在 IO 线程同步执行。

一张 20 MiB 图片如果走 Logic：

```text
浏览器 --HTTP POST 20MiB--> Logic（内存里 20MiB）
  → 再写 MinIO（又一次 20MiB 拷贝/网络传输）
  → Logic 还要处理 JSON、鉴权、限流
```

正确做法：

```text
浏览器 --HTTP PUT 20MiB--> MinIO
浏览器 --HTTP POST 小 JSON--> Logic（只拿预签名 URL / 确认上传）
Logic --HTTP HEAD/元数据--> MinIO（只检查对象，不搬运字节）
```

### 9.2 媒体消息的 JSON 应该很小

`message.content_json` 是 `TEXT`，`MessageDao::ListMessages` 还用了固定 4096 字节的 `content_buf`。媒体消息不要放 base64，应该只放：

```json
{
  "type": "image",
  "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
  "width": 4032,
  "height": 3024,
  "blur_hash": "",
  "dominant_color": "#8a7f72",
  "caption": "今天的晚霞"
}
```

缩略图/原图通过短时签名 URL 按需加载，URL 也不应该长期写死在消息里。

### 9.3 预签名上传是 HTTP PUT

未来浏览器直传 MinIO 的请求大致是：

```http
PUT /spark-push-media/user/1/m_01J9.../original?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Date=...&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=... HTTP/1.1\r\n
Host: 127.0.0.1:9002\r\n
Content-Type: image/jpeg\r\n
Content-Length: 1234567\r\n
\r\n
<图片原始字节>
```

这里：

- 鉴权在 query 的签名参数里（AWS SigV4），不是 `Authorization: Bearer`；
- Body 是纯二进制，不是 JSON；
- `Content-Length` 是图片字节数；
- 响应通常是 `200 OK`，带 `ETag`；
- CORS 由 MinIO 桶配置负责，Logic 的 CORS 不覆盖 9002 端口；
- 浏览器 `fetch` 发 PUT 时，预检会检查 `Access-Control-Allow-Methods` 是否包含 `PUT`，所以 MinIO 的 CORS 规则必须允许 `PUT`。

### 9.4 `GET /api/media/{media_id}` 需要改路由

当前 `kRouteTable` 只支持精确路径，`/api/media/m_xxx` 不会命中。实现媒体 API 时需要：

1. 在 `onRequest()` 里加前缀匹配/路径参数解析；或
2. 改成 `POST /api/media/get`，`media_id` 放 JSON Body；或
3. 用 query，但当前 `OnUrl()` 会截掉 query，需要先改解析器。

这是协议层和路由层的一个实际耦合点。

### 9.5 缩略图/原图的 HTTP 响应

浏览器加载媒体时通常是普通 `GET`：

```http
GET /spark-push-media/user/1/m_01J9.../thumb HTTP/1.1
Host: 127.0.0.1:9002
```

MinIO 返回：

```http
HTTP/1.1 200 OK
Content-Type: image/jpeg
Content-Length: 12345
ETag: "..."
Accept-Ranges: bytes
```

如果是视频，浏览器还会发 `Range: bytes=...`，MinIO 返回 `206 Partial Content` 和 `Content-Range`。这和 `web_demo/web_server.cpp` 的静态文件 Range 逻辑类似，但 MinIO 是对象存储，不应该用 Web Demo 那个无鉴权的静态服务器。

“模糊预览 → 打开变清晰”在协议层就是：

1. 消息 JSON 里先有小尺寸 `micro` 缩略图 URL；
2. 浏览器先 `GET` 小图，CSS `filter: blur(...)`；
3. 用户点开大图，再 `GET` 大图/原图；
4. 大图加载完成后移除 blur。

真正的图片字节始终是 HTTP `GET` 的响应 Body，不是 WebSocket 帧。

---

## 10. 一页速查

### HTTP 请求

```text
POST /api/login HTTP/1.1
Host: 127.0.0.1:9101
Content-Type: application/json
Content-Length: 65
Authorization: Bearer <token>      ← 需要鉴权时
Connection: keep-alive

{JSON Body}
```

### HTTP 响应

```text
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Content-Length: 120
Connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 86400

{"code":0,"message":"ok","data":{...}}
```

### WebSocket 握手请求

```text
GET /ws?token=<token> HTTP/1.1
Host: 127.0.0.1:9200
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: <16 字节随机 Base64>
Sec-WebSocket-Version: 13
Origin: http://127.0.0.1:9001
```

### WebSocket 握手响应

```text
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: <Base64(SHA1(key + GUID))>
```

### WebSocket 帧头

```text
FIN(1) RSV1(1) RSV2(1) RSV3(1) opcode(4)
MASK(1) Payload len(7)
[Extended payload length(16/64)]
[Masking-key(32)]
Payload Data
```

### 本项目业务 opcode

```text
0x1 text   → JSON 业务消息
0x8 close  → 关闭
0x9 ping   → 心跳
0xA pong   → 心跳应答
0x2 binary → 本项目不处理
```

### 一句话记忆

> **HTTP/1.1 是“文本头 + 文本/JSON Body”；WebSocket 是“HTTP 握手 + 二进制帧头 + JSON 文本载荷”；gRPC 是“HTTP/2 头 + protobuf 二进制”；Kafka 是“二进制记录 + key/value”。**

---

## 11. 代码索引

| 内容 | 位置 |
|---|---|
| HTTP 请求行/Header/Body 解析 | `common/net/http_server_lite.cpp:131-188` |
| 去掉 URL query | `common/net/http_server_lite.cpp:131` |
| 全量缓冲 Body | `common/net/http_server_lite.cpp:173` |
| 响应序列化、自动 `Content-Length`/`Connection` | `common/net/http_server_lite.cpp:190` |
| 状态码原因短语 | `common/net/http_server_lite.cpp:28` |
| 业务路由表（精确匹配） | `logic/http_server.cpp:235` |
| CORS 预检 | `logic/http_server.cpp:225` |
| CORS 响应头 | `logic/http_server.cpp:28` |
| 统一 JSON 信封 | `logic/http_server.cpp:170` |
| HTTP Token 鉴权 | `logic/http_server.cpp:306` |
| HTTP 发消息 | `logic/http_handlers_message.cpp:31` |
| HTTP 查历史 | `logic/http_handlers_message.cpp:202` |
| 静态文件 Range | `web_demo/web_server.cpp:218` |
| WS token 提取 | `comet/comet_server.cpp:15` |
| WS 握手鉴权 | `comet/comet_server.cpp:136` |
| WS 只处理 text | `comet/comet_server.cpp:222` |
| WS 发送 text 帧 | `comet/comet_server.cpp:252` |
| WS 心跳扫描 | `comet/comet_server.cpp:273` |
| WS 上行消息处理 | `comet/comet_server.cpp:427` |
| WS 下行 payload 只取 `content_json` | `comet/comet_server.cpp:576` |
| 心跳参数 | `comet/comet_server.h:123` |
| WS 无 TLS 配置 | `comet/comet_server.h:39` |
| protobuf 消息定义 | `proto/spark_push.proto` |
| 浏览器 WS 发送示例 | `web_demo/static/index.html:645`、`:971` |
| 浏览器 WS 接收示例 | `web_demo/static/index.html:658` |

---

## 12. 下一步

这份文档只解释“协议层到底传了什么”。媒体功能的下一步仍然是先定契约和存储，不写代码：

1. 确认 `docs/media_api_spec.md` 的字段和限制；
2. 写 `docs/media_storage_schema.md`：表结构、索引、状态机、清理策略；
3. 写 `docs/media_impl_plan.md`：逐文件改动、依赖、测试计划；
4. 再开始实现 P1（单聊图片 + MinIO 直传 + micro/CSS blur）。

如果你希望，我下一步可以把“HTTP/WS 报文抓包示例”补成真实运行后的 `tcpdump` 输出，或者单独写一份“gRPC/HTTP2 与 Kafka 协议”文档。
