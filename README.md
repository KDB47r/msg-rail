# MsgRail · 讯驿

> 一个基于 C++17 的分布式实时消息推送系统。  
> 浏览器通过 WebSocket 接入 Comet，消息经 Logic 路由后写入 Kafka，由 Job 异步推送与落盘。

MsgRail（讯驿）是一个类 IM 的消息推送系统，重点不在聊天界面，而在**长连接管理、消息路由、异步解耦和可靠投递**这套后端骨架。

## 功能

- **单聊**：用户之间的实时消息收发
- **消息转发**：右键/长按消息，转发文本、图片、视频、文件到其他单聊；媒体复用对象和变体，不重新上传/解码
- **图片消息**：MinIO/S3 浏览器直传、异步生成缩略图、聊天气泡清晰预览、点击查看大图
- **文件消息**：≤ 50 MiB 任意文件直传，卡片展示，签名 URL + `Content-Disposition` 强制下载
- **视频消息**：≤ 100 MiB MP4/WebM 分片直传（支持断点续传），worker 用 ffprobe 校验、ffmpeg 抽 JPEG 封面；MP4 + HEVC 自动转码为 H.264/AAC，聊天卡片 + 弹窗播放
- **媒体去重**：图片/视频服务端 SHA-256 内容寻址；跨用户重复上传复用同一 asset 和变体，跳过解码/转码
- **聊天室**：房间加入/离开、房间广播、在线人数
- **弹幕**：按视频时间轴发送和拉取
- **广播**：全站广播、房间广播
- **HTTP API**：注册、登录、发消息、媒体上传/读取、历史消息、未读/已读、会话列表、聊天室、弹幕、管理后台
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
| 对象存储 | MinIO / S3（预签名直传） |
| 图片处理 | libjpeg-turbo、libpng、libwebp |
| 视频处理 | FFmpeg / ffprobe（探测元数据、抽封面；MP4 + HEVC 转码为 H.264/AAC） |
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
├── media/          # 媒体子系统：S3 直传元数据、图片/视频处理 worker
├── conf/           # logic.conf / comet.conf / job.conf / media_worker.conf
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
- 图片/文件消息依赖：`pkg-config`、`libjpeg-turbo8-dev`、`libpng-dev`、`libwebp-dev`
- 视频消息依赖：`ffmpeg`、`ffprobe`（worker 启动时检查；未安装时视频任务失败，图片/文件不受影响）
- 对象存储：MinIO（可选；未安装时文字聊天仍可用，媒体接口返回 503）。社区版二进制已不从 `dl.min.io` 分发，可使用 GitHub release `RELEASE.2025-09-07T16-13-09Z`；浏览器直传 CORS 由启动参数 `MINIO_API_CORS_ALLOW_ORIGIN` 控制。
- `spdlog`、`llhttp`、`asio`、`websocketpp`、`nlohmann_json` 已安装到 `/usr/local`

> 根 `CMakeLists.txt` 锁定了 llhttp 9.2.1、Asio 1.30.2 和 websocketpp 0.8.2 的版本。

视频消息还需要系统级 FFmpeg（项目不会自动下载）：

```bash
sudo apt update
sudo apt install -y ffmpeg
```

### 2. 初始化数据库

```bash
# 首次初始化：会 DROP DATABASE spark_push，清空现有数据
mysql -uroot -p123456 < sql/schema.sql

# 已有数据库只补媒体表：不删除现有用户/消息，可重复执行
mysql -uroot -p123456 spark_push < sql/migrations/001_media_tables.sql

# 视频 multipart 上传会话表（2026-11-18）
mysql -uroot -p123456 spark_push < sql/migrations/004_media_multipart_upload.sql

# 媒体内容寻址 asset 表（2026-11-20）
mysql -uroot -p123456 spark_push < sql/migrations/005_media_asset.sql
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

脚本会自动启动 Redis、Kafka（KRaft）和 MinIO（若已安装），创建 topic/bucket，并依次启动 Logic、Comet、Job、media_worker 与 Web Demo。

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
| MinIO S3 | 9002 | HTTP（S3 API） |
| MinIO Console | 9003 | HTTP |
| Kafka | 9092 | Kafka |
| Redis | 6379 | Redis |
| MySQL | 3306 | MySQL |

## HTTP API

| 分类 | 接口 |
|---|---|
| 认证 | `POST /api/login`、`POST /api/register` |
| 消息 | `POST /api/message/send`、`POST /api/message/forward`、`GET /api/session/history`、`GET /api/session/unread`、`POST /api/session/mark_read`、`GET /api/session/list_single` |
| 媒体 | `POST /api/media/upload/init`、`POST /api/media/upload/complete`、`GET /api/media/{media_id}` |
| 视频分片 | `POST /api/media/upload/multipart/part_urls`、`POST /api/media/upload/multipart/status`、`POST /api/media/upload/abort` |
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
| `conf/media_worker.conf` | 媒体库连接、S3、worker 线程数、图片/视频处理限制、ffmpeg/ffprobe 路径与临时目录 |

视频消息默认限制：

- 大小 ≤ 100 MiB，时长 ≤ 10 分钟，最大边长 3840，像素数 ≤ 8,294,400
- MP4：H.264 + AAC（或无音频）直接播放；HEVC 自动转码为 H.264/AAC（最大边长 1920，CRF 23）
- WebM：VP8/VP9 + Opus/Vorbis（或无音频）
- 服务端提取元数据并抽一张 JPEG 封面；转码输出超过 100 MiB 或转码失败时任务失败
- 原始 HEVC 文件保留为 `original.mp4`，播放对象为 `playback.mp4`

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
- 视频 multipart 由服务端 `ListParts` 校验分片；complete 幂等，崩溃后用对象 HEAD 恢复
- media_worker 定期清理过期 multipart 和长期 `init` 的单次 PUT 孤儿
- Redis 保存 token、用户路由、会话序号和房间在线状态；媒体元数据缓存故障时回退 MySQL

## 图片上传与异步处理性能优化

测试环境：WSL Ubuntu 22.04，16 核，17GiB 内存；真实图片来自本地 `tests/data/`。
图片处理数据用 `image_pipeline_bench --process-only` 测量（一次一张图、独立进程）；上传 E2E 用 `tests/media_upload_bench.py` 走真实 HTTP + MinIO + media_worker。

> `tests/data/` 中的大图只保留在本地，已从 Git 跟踪中移除；复现 benchmark 时需自行准备测试图片。

### 优化项

| 环节 | 优化 |
|---|---|
| JPEG 解码 | 按 preview 最长边 1280 选择 DCT 缩放 `1/1~1/8`，不再全量解码 |
| WebP 解码 | `WebPDecodeRGBAInto` 零拷贝；lossy 大图走 YUV 域缩小 |
| PNG 解码 | 大图低层逐行解码 + box 降采样，只保留 preview RGBA |
| WebP 编码 | preview `method=2`，thumb/micro `method=0` |
| 内存控制 | `MemoryLimiter`，图片任务共享 1GiB 预算；lossless/progressive 单独估算 |
| 任务队列 | 图片/视频拆成 3+1 lane，`task_type` 过滤 + 复合索引 |
| 任务延迟 | `media_worker_poll_ms` 500→100 |
| 视频上传 | S3 multipart 分片 + localStorage 断点续传 + 过期上传清理 |
| 读路径 | history 内嵌媒体元数据；会话列表头像批量查询 |
| 读缓存 | `media:meta:v1:{id}` Redis 缓存；ready 600s、处理中 1s；绑定/解绑主动失效 |
| 变体 CDN | micro/thumb/preview/poster 稳定公开 URL + 1 年 `Cache-Control`；原图/文件仍预签名 |
| 读接口并发 | 阻塞 handler 移入 8 线程业务池，4 个 IO 线程只负责收发 |

### 优化前后数据

| 场景 | 优化前 | 优化后 | 说明 |
|---|---:|---:|---|
| JPEG 30.8MP（LRC）端到端 | 851ms / 230MiB | **543ms / 38MiB** | DCT 1/4 + method |
| JPEG 45.4MP（ISS）端到端 | 805ms / 313MiB | **286ms / 29MiB** | DCT 1/4 + method |
| JPEG 44.8MP progressive 端到端 | 1515ms / 412MiB | **1136ms / 292MiB** | 整幅系数缓冲仍高 |
| JPEG 2.5MP（KSC-JBS）端到端 | 192ms / 21MiB | **112ms / 21MiB** | method 收益 |
| PNG 45.4MP 端到端 | 1890.5ms / 264MiB | **1875.7ms / 91MiB** | 内存 -66%，时间持平 |
| PNG 12MP 端到端 | 600.8ms / 83MiB | **647.2ms / 37MiB** | 内存 -55%，时间 +8% |
| PNG 8MP 端到端 | 427.0ms / 59MiB | **444.4ms / 28MiB** | 内存 -53%，时间 +4% |
| WebP 45.4MP lossy 端到端 | 945.6ms / 355MiB | **800.7ms / 74MiB** | 零拷贝 + YUV 缩小 |
| WebP 12MP lossy 端到端 | 379.4ms / 97MiB | **266.6ms / 23MiB** | 零拷贝 + YUV 缩小 |
| WebP preview 编码（LRC） | 157ms / 70.9KiB | **59.7ms / 74.1KiB** | method=2，体积 +4.5% |
| 历史 21 张图读路径 | 26.8ms / 21 次 media GET / 约 44 SQL | **约 9ms / 0 次额外 GET / 4 SQL** | history 内嵌 media |
| 历史 32 个媒体读路径（冷→热缓存） | 8.2ms | **5.3ms** | Redis 热缓存，媒体 SQL 2→0 |
| 单张 media GET（热缓存） | 1~3ms | **1.1ms** | Redis 命中 |

队列与上传延迟：

| 场景 | 优化前 | 优化后 | 说明 |
|---|---:|---:|---|
| 视频先到，图片 P95（调度仿真） | 22.27s | **5.00s** | 3 图片 + 1 视频 lane |
| 8MP PNG ready | 623.0ms | **312.5ms** | poll 500→100ms |
| 45MP WebP lossy ready | 622.4ms | **518.2ms** | poll 500→100ms |
| 12MP PNG 截图 ready | 418.1ms | 414.9ms | 解码主导，轮询影响小 |

真实上传 E2E（100ms poll，本地 MinIO）：

| 图片 | 大小 | init | PUT | complete | ready | total |
|---|---:|---:|---:|---:|---:|---:|
| 8MP PNG | 17.93MiB | 7.0ms | 53.0ms | 11.1ms | 312.5ms | **383.6ms** |
| 12MP PNG 截图 | 19.67MiB | 5.6ms | 54.0ms | 13.3ms | 414.9ms | **487.8ms** |
| 45MP WebP lossy | 3.27MiB | 4.9ms | 17.2ms | 10.4ms | 518.2ms | **550.7ms** |
| 12MP WebP lossless | 12.25MiB | 5.8ms | 39.4ms | 20.7ms | 517.9ms | **583.9ms** |
| 12MP WebP alpha | 0.75MiB | 5.0ms | 10.8ms | 10.2ms | 208.1ms | **234.2ms** |

worker 峰值 RSS：500ms poll 下约 192MiB，100ms poll 下约 154MiB（5 张顺序上传）。

Logic history 读接口并发（C++ `load_tester`，客户端与服务端同机 16 核）：

| 客户端线程 | 同步（4 IO 线程） | 异步（4 IO + 8 worker） | 提升 |
|---:|---:|---:|---:|
| 8 | 661.2 req/s | **897.9 req/s** | +35.8% |
| 16 | 669.5 req/s | **936.2 req/s** | +39.9% |
| 32 | 652.8 req/s | **1009.2 req/s** | +54.6% |

视频 multipart 上传（18.73 MiB / 8 MiB 分片 / 3 片）：

| 场景 | 上传总计 | 说明 |
|---|---:|---|
| single PUT | 71.5ms | 旧链路 |
| multipart full | 93.8ms | 多约 22ms 请求开销 |
| multipart resume（已传第 1 片） | **57.4ms** | 只补剩余 2 片，少约 39% |

媒体读缓存与 CDN：

- history 32 个媒体：冷缓存 8.2ms → 热缓存 **5.3ms**，媒体批量 SQL 2→0；单张 media GET 热缓存 **1.1ms**；
- C++ history 压测：关闭缓存 → 热缓存，8 线程 1039→**1344 req/s**（+29%），32 线程 1184→**1602 req/s**（+35%）；
- `micro/thumb/preview/poster` 用稳定公开 URL，可接 CDN；原图、文件、`playback` 仍用 15 分钟预签名；
- 公开变体是“不可猜 URL”而不是强鉴权，拿到 URL 的人可匿名访问缩略图/预览图/封面；生产可再换 CDN 签名；
- 变体上传带 `Cache-Control: public, max-age=31536000, immutable`；旧对象需要重新处理或由 CDN 响应头覆盖。

说明：

- 45MP PNG 文件约 81MiB，超过生产 20MiB 限制，只用于压力测试；真实可上传样本是 8~12MP PNG。
- PNG 这轮主要降内存，解码时间基本不变；要降时间需要 libspng/zlib-ng 等新依赖。
- 这些数据不包含前端 500ms 轮询；浏览器用户可见延迟还会叠加最多 500ms。
- 同步/异步对照只切换 HTTP 回调，其余代码和数据相同；Python 混合压测会先被客户端 GIL 限制，服务端能力以 C++ 压测为准。
- 详细数据见 [`docs/benchmark.md`](docs/benchmark.md) 第 12~20 节、[`docs/media_redis_cache_cdn_design.md`](docs/media_redis_cache_cdn_design.md)、[`docs/video_multipart_resumable_design.md`](docs/video_multipart_resumable_design.md)、[`docs/png_webp_decode_optimization_design.md`](docs/png_webp_decode_optimization_design.md)、[`docs/media_queue_split_design.md`](docs/media_queue_split_design.md)。

## 视频分片上传与断点续传

视频 ≤ 100 MiB 走 MinIO/S3 multipart；图片和普通文件仍走单次 PUT。旧客户端不传 `multipart:true` 时也保持单次 PUT，兼容现有调用。

### 上传流程

```text
video init(multipart=true)
  → media_object(status=init)
  → S3 CreateMultipartUpload → upload_id
  → media_multipart_upload(part_size/part_count/expires_at)
  → 返回 media_id + part_size + part_count

浏览器
  → 请求缺失分片的预签名 URL
  → 3 并发 PUT 分片到 MinIO
  → 失败重试；URL 过期(403)重新取 URL

complete
  → 服务端 ListParts（不信任客户端 ETag）
  → 校验分片数量/大小/总大小
  → CompleteMultipartUpload
  → HEAD 校验
  → MarkUploaded + EnqueueTask(process_video)
  → 删除 media_multipart_upload
```

### 接口

`POST /api/media/upload/init`

```json
{
  "kind": "video",
  "mime_type": "video/mp4",
  "size_bytes": 19639683,
  "filename": "demo.mp4",
  "multipart": true
}
```

分片响应：

```json
{
  "code": 0,
  "data": {
    "media_id": "m_...",
    "status": "init",
    "upload_method": "multipart",
    "part_size": 8388608,
    "part_count": 3,
    "expires_at": 1795060000,
    "max_bytes": 104857600
  }
}
```

不返回 S3 `upload_id`，它只保存在服务端 `media_multipart_upload` 表。

`POST /api/media/upload/multipart/part_urls`

```json
{ "media_id": "m_...", "part_numbers": [1, 2, 3] }
```

返回每个 part 的短期预签名 UploadPart URL：

```json
{
  "code": 0,
  "data": {
    "media_id": "m_...",
    "parts": [
      { "part_number": 1, "url": "http://127.0.0.1:9002/...", "expires_at": 1795060900 }
    ]
  }
}
```

`POST /api/media/upload/multipart/status`

```json
{ "media_id": "m_..." }
```

```json
{
  "code": 0,
  "data": {
    "media_id": "m_...",
    "status": "init",
    "part_size": 8388608,
    "part_count": 3,
    "uploaded_parts": [
      { "part_number": 1, "size_bytes": 8388608 }
    ],
    "expires_at": 1795060000
  }
}
```

- `status=init`：还有分片没传；
- `status=uploaded`：S3 已完成但 DB 可能还没入队，前端再调一次 complete；
- `status=processing/ready`：直接轮询媒体状态；
- 404/409：会话不存在或已过期，前端清理本地状态后重新 init。

`POST /api/media/upload/complete`

请求只需要：

```json
{ "media_id": "m_..." }
```

服务端自己 `ListParts` 并完成 multipart，不需要浏览器提交 ETag。缺片返回 409，前端查 status 后补传。

`POST /api/media/upload/abort`

```json
{ "media_id": "m_..." }
```

取消未完成上传：S3 AbortMultipartUpload + 删除会话 + media 标记 deleted。

### 断点续传

前端在 localStorage 保存：

```json
{
  "media_id": "m_...",
  "part_size": 8388608,
  "part_count": 3,
  "expires_at": 1795060000,
  "file_name": "demo.mp4",
  "file_size": 19639683,
  "last_modified": 1795000000000
}
```

- key 使用 `文件名 + 大小 + lastModified` 指纹；
- 中断后重新选择同一个文件，先调 status 拿已上传分片，只补缺失部分；
- 3 个分片并发上传；
- 每片最多重试 3 次；403（URL 过期）会重新请求该片 URL；
- complete 返回 409 缺片时，自动查状态、补传、再 complete；
- 上传成功、abort 或过期后清理本地状态。

限制：浏览器刷新后拿不到原 File 对象，**必须由用户重新选择同一个文件**才能续传；不做跨设备续传。

### 过期上传清理

`media_worker` maintenance 线程定期执行：

1. 过期 `media_multipart_upload`：
   - HEAD 对象，若已存在且大小匹配 → 补 `MarkUploaded + EnqueueTask`，避免误删已完成上传；
   - 否则 AbortMultipartUpload、删除会话、标记 deleted；
2. 过期 `status=init` 且没有 multipart 会话的单次 PUT 孤儿：
   - 删除对象（不存在也继续）、标记 deleted。

生产环境建议再配 S3/MinIO lifecycle `AbortIncompleteMultipartUpload` 作为兜底。

### 配置

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `media_multipart_part_size_bytes` | 8 MiB | 分片大小，最小 5 MiB |
| `media_multipart_upload_ttl_seconds` | 86400 | 上传会话有效期 |
| `media_upload_url_ttl_seconds` | 900 | 分片 URL 有效期 |
| `media_multipart_cleanup_interval_seconds` | 3600 | worker 清理周期 |
| `media_multipart_cleanup_batch` | 100 | 每轮清理数量 |
| `media_upload_orphan_ttl_seconds` | 86400 | init 孤儿媒体 TTL |

### 兼容与限制

- 视频上限仍是 **100 MiB**，分片不改变大小限制；
- 图片/文件不进入 multipart；
- 不传 `multipart:true` 的旧客户端继续走单次 PUT；
- 分片上传需要 MinIO CORS 允许 PUT；`scripts/start_all.sh` 默认设置 `MINIO_API_CORS_ALLOW_ORIGIN=*`；
- 一次上传只对应一个 media，浏览器同一时间仍只允许一个附件上传任务；
- 未做服务端 sha256 强校验，worker 仍会读取视频头并校验格式/时长/像素。

### 验证

```bash
ctest --test-dir build --output-on-failure
python3 tests/video_multipart_e2e.py
python3 tests/video_multipart_bench.py
python3 tests/media_message_regression_e2e.py   # 旧单次 PUT 回归
```

当前结果：`ctest` 1/1、multipart E2E 13/13、媒体 E2E 23/23、用户功能 E2E 67/67。18.73 MiB / 3 片的本地 benchmark：单次 PUT 71.5ms、multipart 全量 93.8ms、续传阶段 57.4ms（少约 39%）。详见 [`docs/benchmark.md`](docs/benchmark.md) 第 20 节。

### 代码位置

| 模块 | 位置 |
|---|---|
| S3 multipart | `common/s3_client.cpp` |
| 上传会话表 | `sql/migrations/004_media_multipart_upload.sql` |
| 后端接口 | `logic/http_handlers_media.cpp` |
| 前端续传 | `web_demo/static/index.html` |
| 过期清理 | `media/media_worker_main.cpp` |
| 设计文档 | [`docs/video_multipart_resumable_design.md`](docs/video_multipart_resumable_design.md) |

## 媒体去重（图片/视频）

同一份图片/视频被重复上传时，通过内容寻址复用已有 asset：

```text
worker 下载原文件
→ 服务端计算 SHA-256
→ 查 media_asset(sha256, kind) ready
   ├─ 命中：链接已有 asset，复制变体，跳过解码/转码
   └─ 未命中：正常处理，创建 asset + asset_variant
```

数据模型：

```text
media_asset           同一份字节 + 一套变体
media_asset_variant   canonical 变体
media_object          每次上传/转发独立的 media
media_object_asset    media_id -> asset_id 映射
```

特点：

- 跨用户去重：不同用户上传相同字节，底层 asset 只有一份；
- 转发 clone 继承 asset 映射，不重复解码；
- 每条消息仍有独立 `media_id`，权限和历史行为不变；
- 重复上传仍会先传字节（首期不做客户端 hash 秒传）；
- 当前覆盖图片和视频，文件不参与；
- asset 预留 `ref_count`，暂不做 GC/历史 backfill。

测试：

```bash
python3 tests/media_dedup_e2e.py
```

## 消息转发

单聊消息支持右键/长按转发：

- 桌面端：右键消息 → “转发”；
- 移动端：长按消息 → “转发”；
- 弹窗里选择已有单聊，或直接输入目标 UserID；
- 原样保留源消息内容和 caption，不附加新说明；
- 支持文本、图片、视频、文件。

接口：

```http
POST /api/message/forward
{
  "source_session_id": "single:19:20",
  "source_msg_seq": 33,
  "to_user_id": 25
}
```

媒体消息采用 **media clone**：

```text
新 media_id
+ 复制源 media 的 object_key / mime / 宽高 / 时长 / sha256
+ 复制 media_variant
+ status=ready，owner=转发者
```

特点：

- 不重新上传字节；
- 不重新解码/转码；
- 不产生 media_task；
- 与原 media 共用 S3 对象和变体；
- 当前没有媒体删除 API，所以暂时不存在共享对象被误删的问题；后续做删除/去重时需要 asset/ref_count。

当前不做：

- 聊天室转发；
- 转发附加说明；
- 媒体 asset 去重（重复上传仍会传字节，只是后续可继续做去重）。

测试：

```bash
python3 tests/message_forward_e2e.py
```

## 测试

```bash
cd build
ctest --output-on-failure
```

服务全部启动后，可运行真实链路 E2E（脚本只新增测试数据，不删除已有数据）：

```bash
# 用户资料 / 好友 / 昵称搜索 / 未读 / 头像（67 项）
python3 tests/api_user_feature_e2e.py

# 原生 WebSocket 单聊实时收发 / 非好友拒绝 / 聊天室广播（20 项）
python3 tests/ws_user_feature_e2e.py

# 文件 / 图片 / 视频消息与历史分页回归（23 项）
python3 tests/media_message_regression_e2e.py

# 真实图片上传链路 benchmark（init → PUT → complete → ready）
python3 tests/media_upload_bench.py

# 历史读路径 benchmark（history + 逐张/并发 media GET，只读）
python3 tests/history_read_bench.py

# 媒体 Redis 缓存 + CDN benchmark（冷/热缓存、SQL 次数、公开 URL，只读）
python3 tests/media_cache_cdn_bench.py

# 视频 multipart / 断点续传 / abort E2E
python3 tests/video_multipart_e2e.py

# 视频单次 PUT vs multipart vs 续传耗时
python3 tests/video_multipart_bench.py

# 单聊消息转发（文本/图片/视频/文件、权限）
python3 tests/message_forward_e2e.py

# 图片/视频跨用户内容去重
python3 tests/media_dedup_e2e.py
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
- [`docs/media_implementation.md`](docs/media_implementation.md)：图片/文件消息与 MinIO 直传实现文档（实际代码、流程、原理、测试、限制）
- [`docs/video_message_implementation.md`](docs/video_message_implementation.md)：视频消息设计、ffprobe/ffmpeg 处理、前端播放与测试记录
- [`docs/video_transcoding_design.md`](docs/video_transcoding_design.md)：HEVC MP4 自动转码方案、输出规格、失败处理与验证记录
- [`docs/media_image_video_chain_comparison.md`](docs/media_image_video_chain_comparison.md)：图片与视频从上传、异步处理到前端播放的链路对比
- [`docs/image_thumbnail_clarity_design.md`](docs/image_thumbnail_clarity_design.md)：聊天气泡图片清晰度调整方案与验证记录
- [`docs/media_load_test_plan.md`](docs/media_load_test_plan.md)：媒体链路压测设计、场景矩阵、指标与瓶颈分析方法
- [`docs/image_processing_optimization_design.md`](docs/image_processing_optimization_design.md)：图片处理性能优化（JPEG DCT 缩放、WebP method、内存信号量）
- [`docs/media_queue_split_design.md`](docs/media_queue_split_design.md)：图片/视频 worker 队列拆分（3 图片 + 1 视频 lane、task_type 领取、索引）
- [`docs/queue_split_benchmark.md`](docs/queue_split_benchmark.md)：旧共享池 vs 新 lane 的调度仿真结果与解读
- [`docs/png_webp_decode_optimization_design.md`](docs/png_webp_decode_optimization_design.md)：PNG/WebP 解码优化（WebP 零拷贝、YUV 域缩小、PNG 逐行降采样）
- [`docs/benchmark.md`](docs/benchmark.md)：图片处理管线 benchmark 命令、真实图片结果与优化前后对比
- [`docs/media_redis_cache_cdn_design.md`](docs/media_redis_cache_cdn_design.md)：媒体元数据 Redis 缓存与变体 CDN 方案、验证结果
- [`docs/video_multipart_resumable_design.md`](docs/video_multipart_resumable_design.md)：视频 multipart 分片上传、断点续传、过期上传清理方案与验证
- [`docs/message_forward_design.md`](docs/message_forward_design.md)：单聊消息转发、右键/长按菜单、media clone 方案
- [`docs/media_dedup_forward_design.md`](docs/media_dedup_forward_design.md)：图片/视频内容寻址去重、asset 层、转发共享 asset 方案
- [`docs/media_upload_design.md`](docs/media_upload_design.md)：图片/视频/语音上传与模糊预览设计草案
- [`docs/media_api_spec.md`](docs/media_api_spec.md)：历史接口契约草案；图片/文件以实现文档为准，视频见视频实现文档
- [`docs/user_profile_friend_unread_design.md`](docs/user_profile_friend_unread_design.md)：用户资料、好友关系、昵称/头像与未读消息技术方案（含数据模型、接口契约与状态机）
- [`docs/user_profile_friend_unread_implementation.md`](docs/user_profile_friend_unread_implementation.md)：上述功能的实际实现、代码位置、验证记录与启用步骤（迁移已执行，HTTP/WS/媒体回归已通过；浏览器 E2E 未执行）
- [`docs/profile_contacts_search_summary.md`](docs/profile_contacts_search_summary.md)：私聊搜索、个人资料页、联系人实现速览（先看技术框架，每项需求 ≤100 字）

---

> 说明：当前代码中的 CMake 工程名、C++ namespace 和数据库名仍沿用 `spark_push` / `sparkpush`，对外项目名称使用 **MsgRail / 讯驿**。
