# 媒体消息与文件上传设计（草案 v0.1）

> 状态：设计讨论稿，**本文不包含实现代码**。  
> 日期：2026-11-08  
> 项目：MsgRail / SparkPush 二次开发  
> 目标：先打通“图片 / 视频 / 后续语音、文件”的完整媒体消息链路，再逐步补齐模糊预览、断点续传、转码等能力；重点是理解业务与架构，而不是堆功能。

---

## 0. 结论先行

### 0.1 推荐方案一句话

> **文件字节不经过 Logic、不进 Kafka、不进 MySQL、不走 WebSocket。**  
> 浏览器通过 Logic 申请“预签名上传 URL”，直接把文件传到对象存储（MinIO/S3）；Logic 只保存 `media_id` 和元数据；独立的 `media_worker` 异步生成缩略图、模糊占位图、视频封面；消息体里只放 `media_id` 和少量展示元数据。客户端按 **模糊占位 → 小图 → 预览图 → 原图** 渐进加载。

### 0.2 推荐链路

```text
浏览器
  │
  ├─ 1. POST /api/media/upload/init        （Logic：鉴权、限额、建 media 记录、签发 URL）
  │       └─ 返回 media_id + 预签名 PUT URL / multipart 分片 URL
  │
  ├─ 2. PUT/POST 文件字节 ────────────────▶ MinIO / S3（不经过 Logic）
  │
  ├─ 3. POST /api/media/upload/complete    （Logic：HEAD 校验、入处理队列）
  │       └─ 返回 media_id，status=processing
  │
  ├─ 4. media_worker（独立进程）
  │       ├─ 校验文件、探测尺寸/时长
  │       ├─ 生成 micro / thumb / preview / poster
  │       ├─ 生成 blurhash / 主色
  │       └─ 更新 media_object.status=ready
  │
  ├─ 5. POST /api/message/send             （Logic：校验 media 属于自己且 ready，构造消息）
  │       └─ Kafka：push_to_comet（仍然只传 JSON 元数据）
  │
  └─ 6. 接收方收到消息
          ├─ 先用 blurhash / 主色 / 宽高渲染模糊占位
          ├─ GET /api/media/{media_id} 拿短期签名 URL
          └─ 按 thumb → preview → original 渐进加载
```

### 0.3 五个核心原则

1. **控制面与数据面分离**：JSON 控制走 Logic；文件字节走对象存储。
2. **消息只引用，不搬运**：`content_json` 里只放 `media_id`、宽高、blurhash、时长等元数据，绝不放 base64。
3. **上传与处理异步化**：上传完成不等于可用；`processing → ready` 是显式状态机。
4. **服务端权威**：宽高、blurhash、MIME、大小等由服务端探测/生成，客户端传的只作为提示。
5. **渐进加载**：模糊占位先出现，再逐步换成清晰图；视频先出封面，点击再加载视频。

### 0.4 已确认决策（2026-11-08）

| 编号 | 决策 | 结论 | 状态 |
|---|---|---|---|
| D1 | 文件存储 | MinIO/S3 直传 | **已确认** |
| D2 | 上传数据面 | 浏览器直传对象存储，Logic 不碰文件字节 | **由 D1 确定** |
| D3 | 服务边界 | Logic 做媒体控制面；`media_worker` 独立进程做处理 | 暂定，待细化 |
| D4 | 第一阶段范围 | 先做单聊图片，视频放 P3 | **已确认** |
| D5 | 发送时机 | 媒体必须 `ready` 后才能发消息 | 暂定，待细化 |
| D6 | 模糊预览 | `micro` 缩略图 + CSS blur，BlurHash 后续再加 | **已确认** |
| D7 | 视频 v1 | MP4 直链 + 封面，不做 HLS/转码 | 暂定，P3 再定 |
| D8 | 发送入口 | 媒体消息先走 HTTP `/api/message/send` | **已确认** |
| D9 | 分片上传 | v1 不做，P4 再加 | 暂定 |

> 这次确认把“最小可用链路”收敛为：**单聊图片 + MinIO 直传 + Logic 控制面 + media_worker + HTTP 发送 + micro/CSS 模糊预览**。  
> 视频、语音、文件、聊天室、分片、HLS 都作为后续阶段，不进入第一个里程碑。

---

## 1. 需求与边界

### 1.1 功能需求

| 能力 | 第一阶段 | 后续 |
|---|---|---|
| 图片 | JPG/PNG/WebP 上传、缩略图、模糊预览、点击看原图 | HEIC、GIF、AVIF、多图、图片编辑 |
| 视频 | MP4/WebM 上传、封面、时长/宽高、点击播放 | 转码、HLS、多码率、CDN |
| 语音 | — | 录制、时长、波形、Opus/AAC、播放 |
| 文件 | — | 任意文件、图标、下载、病毒扫描 |
| 场景 | 单聊 | 聊天室、弹幕、群聊 |
| 上传 | 单文件、进度、取消 | 分片、断点续传、秒传/去重 |
| 预览 | 模糊占位 → 小图 → 预览图 → 原图 | 手势缩放、视频拖动预览 |

### 1.2 非功能需求

- **正确性**：消息引用的媒体必须存在、属于发送者、已处理完成；不能出现“消息已发但图片永远转圈”。
- **可靠性**：上传可重试；处理任务可重试；进程崩溃后状态可恢复；孤儿文件可清理。
- **安全性**：未登录不能上传；不能引用别人的媒体；不能越权读取会话媒体；上传内容要做类型/大小/解码安全校验。
- **可扩展**：Logic/worker 可以多实例；不能依赖本地磁盘；对象存储可替换为 S3/OSS/MinIO。
- **可观测**：上传成功率、处理耗时、队列积压、失败原因、存储用量可查。
- **学习价值**：能讲清楚 HTTP、对象存储、预签名、状态机、异步任务、幂等、渐进加载、视频 Range/HLS 等知识点。

### 1.3 本期明确不做

- 不做端到端加密。
- 不做完整 CDN/多地域容灾。
- 不做复杂的视频剪辑、滤镜、AI 审核。
- 不做大规模推荐/搜索。
- 不把现有系统改造成微服务全集；按阶段演进。

---

## 2. 现有代码约束（为什么不能“直接在 Logic 里收文件”）

这部分很关键。当前项目不是“加一个 multipart 接口”就能收视频的，现有 HTTP 基础设施有几个硬约束。

| 现状 | 代码位置 | 对媒体上传的影响 |
|---|---|---|
| HTTP 请求体整体缓存在 `std::string body_` | `common/net/http_server_lite.h` | 大文件会全部进内存；1 GB 视频直接打爆 |
| 只有请求完整解析后才回调业务 | `common/net/http_server_lite.cpp` `OnMessageComplete` / `DoRead` | 无法边收边写磁盘；没有流式上传 |
| 没有 multipart/form-data 解析 | 同上 | 浏览器表单上传不能直接用 |
| 业务回调在 Asio IO 线程上同步执行 | `http_server.cpp` `onRequest` | 图片处理/视频探测会阻塞 IO 线程 |
| `HttpResponse` 的 body 是完整字符串 | `http_server_lite.h` | 没有 chunked/流式响应；大文件下载不适合走 Logic |
| 路由表是精确路径匹配 | `http_server.cpp` `kRouteTable` | `/api/media/{id}` 这种路径参数需要扩展路由 |
| `message.content_json` 是 `TEXT` | `sql/schema.sql` | 能放元数据，但绝不能放文件字节/base64 |
| 读历史时 `content_json` 用固定 4096 字节缓冲 | `logic/message_dao.cpp:235` | 媒体消息的 JSON 必须保持小；否则会被截断/越界风险 |
| `msg_type` 枚举只有 text/system/danmaku/broadcast | `proto/spark_push.proto` | 需要新增 image/video/voice/file 类型 |
| Kafka 消息体默认不适合大文件 | `common/kafka_producer.cpp` | 文件不能进 Kafka；Kafka 只传 `media_id` |
| WebSocket 是消息通道，不是文件通道 | `comet/comet_server.cpp` | 不适合传大二进制；没有断点续传/Range/CDN |
| 静态文件服务器只服务 web_demo 的 doc_root | `web_demo/web_server.cpp` | 不能做鉴权媒体存储；也不是多实例共享存储 |
| 当前没有对象存储 | — | 需要引入 MinIO/S3，或实现本地存储适配器 |

### 2.1 结论

- **图片小文件**可以勉强走“Logic 缓冲后写本地磁盘”，但这会养成错误架构习惯。
- **视频/语音/大文件**必须走“浏览器 → 对象存储”的直传路径，或者新增一个真正支持流式上传的媒体服务。
- 当前最划算的方案是：**Logic 只做控制面，MinIO 做数据面，media_worker 做异步处理**。

---

## 3. 方案对比

### 3.1 方案 A：Logic 接收 multipart，写本地磁盘

```text
浏览器 ──multipart──▶ Logic ──写本地文件──▶ 本地磁盘
                         └─ 同步生成缩略图
```

优点：

- 不引入对象存储，部署最简单。
- 能学习 multipart、文件落盘、图片处理。

缺点：

- 现有 HTTP 服务器要改成流式收包 + multipart 解析，工作量大。
- 大文件占内存/阻塞 IO 线程。
- Logic 多实例时本地磁盘不共享，需要 NFS/共享盘。
- 下载要自己实现鉴权、Range、缓存、CDN。
- 视频处理会拖垮业务进程。

**适合**：只做小图片 Demo；不适合作为长期方案。

### 3.2 方案 B：Logic 接收文件流，转存 MinIO

```text
浏览器 ──multipart/stream──▶ Logic ──S3 SDK──▶ MinIO
```

优点：

- 文件不落本地磁盘，存储可扩展。
- 可以统一做鉴权、限流、病毒扫描。

缺点：

- 仍然要改 HTTP 服务器支持流式上传。
- Logic 成为文件数据面的带宽瓶颈。
- 大文件上传占用 Logic 连接和线程。
- 需要处理背压、临时文件、超时、断点续传。

**适合**：需要对上传内容做强制服务端预处理的场景；当前项目不必要。

### 3.3 方案 C：浏览器直传对象存储（推荐）

```text
浏览器 ──1. init──▶ Logic ──签发预签名 URL──▶ 浏览器
浏览器 ──2. PUT 文件字节────────────────────▶ MinIO
浏览器 ──3. complete──▶ Logic ──HEAD 校验/入队──▶ media_worker
```

优点：

- 文件字节不经过 Logic，Logic 只处理小 JSON。
- MinIO 原生支持大文件、Range、分片、生命周期、CORS。
- 容易扩展到多 Logic、多 worker、CDN。
- 预签名 URL 是对象存储的标准玩法，面试可讲。
- 处理与上传解耦，视频可以慢慢转。

缺点：

- 引入 MinIO/S3 依赖。
- 需要处理预签名、CORS、URL 过期、上传后校验。
- 客户端要配合直传（XHR/fetch）。
- 需要防止客户端伪造大小/类型，complete 时仍要服务端校验。

**推荐作为主方案。**

### 3.4 方案 D：WebSocket 传文件

不推荐。原因：

- WebSocket 帧没有 HTTP 的 `Content-Length`、Range、缓存、CDN 语义。
- 大二进制会占用 Comet 内存和连接；Comet 是长连接网关，不该做文件网关。
- 断线重连后无法方便续传。
- 现有消息链路是 JSON 文本，混入二进制会破坏协议。

WebSocket 只负责“消息通知/信令”，文件通道仍走 HTTP。

### 3.5 方案对比表

| 维度 | A 本地磁盘 | B Logic 转存 | C 预签名直传 | D WebSocket |
|---|---|---|---|---|
| 改动量 | 中 | 大 | 中 | 大 |
| 大文件支持 | 差 | 中 | 好 | 差 |
| 多实例 | 差 | 好 | 好 | 差 |
| 带宽经过 Logic | 是 | 是 | 否 | 是 |
| 断点续传 | 难 | 中 | 原生支持 | 难 |
| Range/视频播放 | 自己实现 | MinIO 支持 | MinIO 支持 | 不适合 |
| 安全校验 | 自己实现 | 自己实现 | complete 时校验 | 自己实现 |
| 学习价值 | HTTP/文件 | HTTP/S3 | S3/预签名/直传 | 低 |
| 推荐度 | 仅小图 Demo | 特定场景 | **推荐** | 不推荐 |

---

## 4. 总体架构

### 4.1 组件职责

| 组件 | 职责 | 不做什么 |
|---|---|---|
| 浏览器 | 选择文件、直传、展示进度、渐进加载、发送消息 | 不决定媒体归属/大小/类型 |
| Logic HTTP 9101 | 鉴权、init/complete、媒体元数据、消息发送校验 | 不接收文件字节、不做转码 |
| MinIO/S3 | 存原文件、变体、视频、分片；提供 Range/生命周期 | 不做业务鉴权（靠预签名） |
| `media_worker` | 消费处理任务、探测、生成缩略图/封面、更新状态 | 不处理聊天消息、不接 WebSocket |
| MySQL | `media_object`、`media_variant`、`media_task`、消息表 | 不存文件字节 |
| Redis | 上传限流、幂等键、短期状态缓存（可选） | 不作为媒体最终存储 |
| Kafka/Job | 原消息链路不变，只传 `media_id` 元数据 | 不传文件 |
| Comet | WebSocket 推送消息 | 不传文件 |

### 4.2 推荐的服务边界

**v1 推荐：Logic 内做媒体控制面 + 独立 `media_worker` 进程。**

理由：

- Logic 已经有 token 鉴权、MySQL、Redis、HTTP 路由，复用成本最低。
- 上传/查询接口都是小 JSON，现有 `HttpServerLite` 够用（加上路由参数和 body 限制即可）。
- 图片/视频处理是 CPU/内存密集型，必须从 Logic 拆出去，避免拖垮聊天业务。
- `media_worker` 可以独立扩容、独立重启、独立限流。

**v2 可选：抽出独立 `media_server`。**

当出现以下情况再拆：

- 媒体接口 QPS/带宽明显上升，Logic 路由和线程被占满。
- 媒体团队/模块需要独立部署。
- 需要独立的鉴权、配额、审核体系。
- `media_worker` 已经稳定，控制面也需要独立扩缩容。

> 不要一开始就上“媒体微服务”。先把控制面放 Logic、处理放 worker，理解清楚边界后再拆，学习收益更高。

### 4.3 为什么对象存储，而不是 MySQL/Kafka/本地磁盘

| 存储 | 为什么不用于文件 |
|---|---|
| MySQL | BLOB 会让表/备份/复制膨胀；没有 Range/CDN；连接池和事务不适合大对象；当前 `content_json TEXT` 只有 64 KB |
| Kafka | 消息体默认约 1 MB，保留策略不适合长期存文件；消费者还要下载；没有随机读取 |
| 本地磁盘 | 多实例不共享；扩容/迁移/备份麻烦；Logic 重启可能丢 |
| WebSocket | 不是存储，也不是文件传输协议 |
| 对象存储 | 大对象、Range、分片、生命周期、CORS、预签名、CDN 都原生支持 |

### 4.4 端口与部署（开发环境建议）

| 组件 | 端口 | 说明 |
|---|---:|---|
| Logic HTTP | 9101 | 控制面 API |
| Logic gRPC | 9100 | 不变 |
| Comet WS | 9200 | 不变 |
| Comet gRPC | 9205 | 不变 |
| Web Demo | 9001 | 不变 |
| MinIO S3 API | 9002 | 避开 Web Demo 的 9001 |
| MinIO Console | 9003 | 管理界面 |
| `media_worker` | 无监听端口 | 只消费 DB 任务 |
| MySQL | 3306 | 新增媒体表 |
| Redis | 6379 | 限流/幂等 |
| Kafka | 9092 | 消息链路不变 |

> MinIO 的端口可以按实际环境调整；关键是不要让浏览器直传地址和 Web Demo 冲突。

---

## 5. 数据模型

> 下面是设计草案，不是最终迁移脚本。字段名/类型在实现前还要和现有 DAO 风格对齐。

### 5.1 `media_object`：一个媒体资产

```sql
-- 草案：媒体主表
CREATE TABLE media_object (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  media_id        CHAR(26)        NOT NULL,          -- 对外 ID，ULID/随机，不可猜测
  owner_id        BIGINT          NOT NULL,          -- 上传者
  kind            VARCHAR(16)     NOT NULL,          -- image/video/voice/file
  status          VARCHAR(16)     NOT NULL,          -- init/uploaded/processing/ready/failed/deleted
  original_name   VARCHAR(255)    NOT NULL DEFAULT '',
  mime_type       VARCHAR(128)    NOT NULL DEFAULT '',
  size_bytes      BIGINT          NOT NULL DEFAULT 0,
  sha256          CHAR(64)        NOT NULL DEFAULT '',
  width           INT             NOT NULL DEFAULT 0,
  height          INT             NOT NULL DEFAULT 0,
  duration_ms     BIGINT          NOT NULL DEFAULT 0,
  blur_hash       VARCHAR(128)    NOT NULL DEFAULT '',
  dominant_color  CHAR(7)         NOT NULL DEFAULT '',
  storage_bucket  VARCHAR(64)     NOT NULL DEFAULT '',
  object_key      VARCHAR(512)    NOT NULL DEFAULT '',
  session_id      VARCHAR(128)    NOT NULL DEFAULT '', -- 绑定到哪个会话（发送后填）
  message_id      VARCHAR(160)    NOT NULL DEFAULT '', -- 绑定到哪条消息（发送后填）
  error_message   VARCHAR(512)    NOT NULL DEFAULT '',
  created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at      TIMESTAMP       NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_media_id (media_id),
  KEY idx_owner_created (owner_id, created_at),
  KEY idx_status_created (status, created_at),
  KEY idx_sha256 (sha256)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

设计点：

- `media_id` 是对外 ID，不要暴露自增主键；用 ULID/雪花/随机 ID。
- `status` 是状态机核心，所有更新都要带条件，避免并发覆盖。
- `session_id/message_id` 用于“发送后”的访问控制；上传但未发送时只有 owner 能看。
- `sha256` 可选：用于去重、完整性校验；大文件计算成本高，可异步算。
- `object_key` 只存原始对象 key；变体在 `media_variant` 表。

### 5.2 `media_variant`：派生文件

```sql
-- 草案：同一媒体的不同展示变体
CREATE TABLE media_variant (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  media_id    CHAR(26)        NOT NULL,
  variant     VARCHAR(32)     NOT NULL,   -- micro/thumb/preview/poster/playback/hls_360p...
  format      VARCHAR(16)     NOT NULL,   -- webp/jpeg/mp4/m3u8...
  width       INT             NOT NULL DEFAULT 0,
  height      INT             NOT NULL DEFAULT 0,
  size_bytes  BIGINT          NOT NULL DEFAULT 0,
  bitrate_kbps INT            NOT NULL DEFAULT 0,
  object_key  VARCHAR(512)    NOT NULL,
  created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_media_variant_format (media_id, variant, format),
  KEY idx_media (media_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

变体命名建议：

| variant | 用途 | 典型尺寸 |
|---|---|---|
| `micro` | 模糊占位/首帧 | 32px 级别 |
| `thumb` | 聊天气泡 | 320px 级别 |
| `preview` | 点开后的首屏清晰图 | 1280px 级别 |
| `poster` | 视频封面 | 640px 级别 |
| `playback` | 可直接播放的 MP4 | 原分辨率或转码后 |
| `hls_360p` / `hls_720p` | HLS 码率 | 后续 |

### 5.3 `media_task`：异步处理任务

```sql
-- 草案：DB 任务队列（v1）
CREATE TABLE media_task (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  media_id      CHAR(26)        NOT NULL,
  task_type     VARCHAR(32)     NOT NULL,   -- process_image/process_video/process_voice
  status        VARCHAR(16)     NOT NULL,   -- pending/running/done/failed
  attempts      INT             NOT NULL DEFAULT 0,
  max_attempts  INT             NOT NULL DEFAULT 3,
  next_run_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_by     VARCHAR(64)     NOT NULL DEFAULT '',
  locked_at     TIMESTAMP       NULL DEFAULT NULL,
  last_error    VARCHAR(512)    NOT NULL DEFAULT '',
  created_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_media_task (media_id, task_type),
  KEY idx_status_next (status, next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

为什么 v1 用 DB 队列：

- 不引入新 Kafka topic，部署简单。
- MySQL 8 支持 `SELECT ... FOR UPDATE SKIP LOCKED`，多 worker 可以安全抢任务。
- 任务状态、重试次数、错误信息都能直接查。
- 对学习来说，状态机和幂等逻辑更直观。

什么时候换 Kafka：

- 任务量很大、DB 轮询成为瓶颈。
- 需要跨系统分发、削峰、独立重放。
- 需要更细的分区并行/死信队列。

### 5.4 `media_upload` / `media_upload_part`：分片上传（第二阶段）

如果 v1 只做单次 PUT，可以先不建这两张表；分片阶段再加。

```sql
-- 草案：分片上传会话
CREATE TABLE media_upload (
  upload_id      CHAR(26)     NOT NULL,
  media_id       CHAR(26)     NOT NULL,
  owner_id       BIGINT       NOT NULL,
  total_size     BIGINT       NOT NULL,
  part_size      BIGINT       NOT NULL,
  s3_upload_id   VARCHAR(256) NOT NULL DEFAULT '',
  status         VARCHAR(16)  NOT NULL, -- init/uploading/completed/aborted
  expires_at     TIMESTAMP    NOT NULL,
  created_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (upload_id),
  KEY idx_media (media_id),
  KEY idx_expire (status, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 5.5 消息与媒体的关联

推荐两种做法，按阶段选择：

**v1：在 `media_object` 上直接绑定**

- 发送消息时，Logic 校验 `owner_id`、`status=ready`、`kind`，然后写入 `session_id`、`message_id`。
- 简单，查询少。
- 缺点：一个媒体只能绑定一条消息，转发/多会话需要再设计。

**v2：`media_ref` 引用表**

```sql
CREATE TABLE media_ref (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  media_id   CHAR(26) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  message_id VARCHAR(160) NOT NULL,
  ref_type   VARCHAR(16) NOT NULL DEFAULT 'message',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_media_message (media_id, message_id),
  KEY idx_message (message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- 支持转发、多会话引用、引用计数、删除保护。
- 代价是每次发送多一次写入/查询。

> **v1 建议先用 `media_object` 绑定，理解清楚后再引入 `media_ref`。** 不要一开始就做完整引用计数。

### 5.6 状态机

```text
                 complete + HEAD 校验通过
INIT ─────────────────────────────────────▶ UPLOADED
  │                                             │
  │ 超时未完成                                   │ 入队
  ▼                                             ▼
EXPIRED                                      PROCESSING
                                                │
                          ┌─────────────────────┴─────────────────────┐
                          │ 成功                                      │ 失败/重试耗尽
                          ▼                                           ▼
                        READY ─────────────── 软删除 ───────────────▶ DELETED
                          │
                          └─ 发送消息时绑定 session_id/message_id
```

状态约束：

- `INIT`：已创建记录，但对象未确认；可被清理。
- `UPLOADED`：对象存在且大小/类型通过初检；等待处理。
- `PROCESSING`：worker 已领取任务；同一媒体只允许一个处理任务。
- `READY`：所有必要变体已生成；可以被消息引用。
- `FAILED`：处理失败；保留错误信息，允许重试或重新上传。
- `DELETED`：软删除；对象由 GC 异步清理。

### 5.7 对象 key 规范

```text
media/{media_id}/original.{ext}
media/{media_id}/micro.webp
media/{media_id}/thumb.webp
media/{media_id}/preview.webp
media/{media_id}/poster.jpg
media/{media_id}/playback.mp4
media/{media_id}/hls/master.m3u8
media/{media_id}/hls/360p/segment_00001.ts
```

原则：

- **绝不使用客户端文件名作为 key**；防止路径穿越、重名、特殊字符。
- `media_id` 不可猜测，但不要把它当唯一权限凭证；还要有服务端鉴权。
- 同一媒体变体 key 固定；重试时覆盖写入，天然幂等。
- 原文件扩展名只允许白名单；实际类型以服务端探测为准。

---

## 6. API 设计

> 所有业务 API 保持现有 `{code,message,data}` 格式；成功 `code=0`。  
> 鉴权统一使用 `Authorization: Bearer <token>`。  
> 以下路径是草案，最终以 `docs/media_api_spec.md` 为准。

### 6.1 `POST /api/media/upload/init`

**作用**：创建媒体记录、校验限额、签发上传 URL。

请求：

```json
{
  "kind": "image",
  "file_name": "IMG_0001.HEIC",
  "mime_type": "image/heic",
  "size": 2456789,
  "sha256": "可选的客户端计算结果",
  "scene": "single",
  "session_id": "single:1:2"
}
```

响应（单次 PUT）：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
    "upload": {
      "mode": "single",
      "method": "PUT",
      "url": "http://127.0.0.1:9002/sparkpush-media/media/m_.../original.heic?X-Amz-Algorithm=...",
      "headers": {
        "Content-Type": "image/heic"
      },
      "expires_at": 1760000000
    }
  }
}
```

响应（分片，第二阶段）：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_...",
    "upload": {
      "mode": "multipart",
      "upload_id": "u_...",
      "part_size": 5242880,
      "parts": [
        { "part_number": 1, "url": "http://127.0.0.1:9002/...partNumber=1..." },
        { "part_number": 2, "url": "http://127.0.0.1:9002/...partNumber=2..." }
      ],
      "expires_at": 1760000000
    }
  }
}
```

服务端校验：

- token 有效，`owner_id` 从 token 取，不能由客户端传。
- `kind` 合法；`mime_type` 在允许列表；`size` 不超过该 kind 上限。
- 限流/配额：Redis 计数器；单用户并发上传数、日上传字节数。
- 创建 `media_object`，`status=init`，生成不可猜测的 `media_id`。
- 生成 `object_key`，不要用客户端文件名。
- 签发预签名 URL；URL 只对指定 method/key 有效，TTL 短（例如 15 分钟）。
- 如果客户端传了 `sha256`，记录为候选值；final 仍以服务端计算为准。

### 6.2 浏览器直传

**单次 PUT：**

```http
PUT /sparkpush-media/media/m_.../original.jpg?X-Amz-...
Content-Type: image/jpeg

<二进制文件>
```

- 浏览器用 `XMLHttpRequest` 获取上传进度；`fetch` 没有 upload progress。
- 需要 MinIO bucket CORS 允许 Web Demo 的 origin、`PUT/GET/HEAD`、`Content-Type`，并暴露 `ETag`。
- 成功后拿到 `ETag`（分片时每个 part 都要）。
- 失败可重试；单次 PUT 的幂等性由对象 key 固定保证（重试覆盖）。

**分片：**

1. `init` 返回 `upload_id` + 每个 part 的预签名 URL。
2. 浏览器并发上传 part（建议 3 个并发），记录每个 part 的 `ETag`。
3. 全部完成后调用 `complete`，提交 part 列表。
4. 失败可查询已上传 part，跳过已完成分片。

### 6.3 `POST /api/media/upload/complete`

请求：

```json
{
  "media_id": "m_...",
  "upload_id": "u_...",
  "parts": [
    { "part_number": 1, "etag": "\"abc\"" },
    { "part_number": 2, "etag": "\"def\"" }
  ],
  "sha256": "客户端计算值，可选"
}
```

响应：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_...",
    "status": "processing"
  }
}
```

服务端动作：

1. 校验 `media_id` 属于当前用户，状态是 `init`。
2. 单次模式：`HEAD` 对象，检查存在、大小、Content-Type；分片模式：先完成 S3 multipart，再 `HEAD`。
3. 校验实际大小不超过 init 声明上限；最好下载前若干字节做 magic number 检查，或让 worker 做。
4. 更新 `status=uploaded`。
5. 插入 `media_task`（`UNIQUE(media_id, task_type)` 防重复）。
6. 返回 `processing`；不要阻塞等图片处理完。

幂等：

- 重复 `complete` 返回当前状态，不重复入队。
- 如果对象不存在/大小不符，返回 400/409，并把记录置为 `failed` 或保留 `init` 供重试。
- 如果 worker 已处理完，直接返回 `ready`。

### 6.4 `GET /api/media/{media_id}`

**作用**：获取媒体元数据和短期签名 URL。

响应（图片）：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_...",
    "kind": "image",
    "status": "ready",
    "width": 4032,
    "height": 3024,
    "mime_type": "image/jpeg",
    "size_bytes": 2456789,
    "blur_hash": "LKO2?U%2Tw=w]~RBVZRi};RPxuwH",
    "dominant_color": "#8a7f72",
    "variants": {
      "micro":   { "url": "http://...", "width": 32,  "height": 24,  "expires_at": 1760000000 },
      "thumb":   { "url": "http://...", "width": 320, "height": 240, "expires_at": 1760000000 },
      "preview": { "url": "http://...", "width": 1280,"height": 960, "expires_at": 1760000000 },
      "original":{ "url": "http://...", "width": 4032,"height": 3024,"expires_at": 1760000000 }
    }
  }
}
```

响应（视频）：

```json
{
  "code": 0,
  "data": {
    "media_id": "m_...",
    "kind": "video",
    "status": "ready",
    "width": 1920,
    "height": 1080,
    "duration_ms": 15300,
    "blur_hash": "...",
    "dominant_color": "#101010",
    "variants": {
      "poster":   { "url": "...", "width": 640, "height": 360 },
      "playback": { "url": "...", "mime_type": "video/mp4", "width": 1920, "height": 1080 }
    }
  }
}
```

服务端校验：

- 未登录 → 401。
- `status != ready` → 409（或返回 `processing`，由客户端轮询）。
- 未绑定会话：只有 `owner_id == 当前用户` 可读。
- 已绑定会话：`CheckSessionAccess(current_user, session_id)` 通过才可读。
- 单聊/聊天室/弹幕的权限规则复用现有 `CheckSessionAccess`。
- 不要返回 bucket 内部 key；只返回签名 URL。

URL 过期：

- 客户端缓存 `media_id → metadata`，在 `expires_at` 前复用。
- 收到 403/过期时重新请求 metadata。
- 不要把签名 URL 写进 `message.content_json`；历史消息里的 URL 会过期。
- 消息里只存稳定元数据，URL 每次按需换取。

### 6.5 `POST /api/message/send` 扩展

现有接口只接受：

```json
{ "to_user_id": 2, "content": "文本" }
```

建议扩展为：

```json
{
  "to_user_id": 2,
  "msg_type": "image",
  "content": {
    "media_id": "m_...",
    "caption": "今天的晚霞"
  }
}
```

兼容策略：

- `content` 既可以是字符串（旧客户端），也可以是对象（新客户端）。
- `msg_type` 省略时默认 `text`。
- 旧客户端行为完全不变。
- 媒体消息只让客户端传 `media_id` 和可选 `caption`；**宽高、blurhash、MIME、大小由服务端从 `media_object` 读取并注入**，防止伪造。
- 服务端构造最终 `content_json`：

```json
{
  "type": "image",
  "media_id": "m_...",
  "width": 4032,
  "height": 3024,
  "blur_hash": "...",
  "dominant_color": "#8a7f72",
  "caption": "今天的晚霞"
}
```

校验顺序：

1. 鉴权，拿到 `from_user`。
2. 解析 `msg_type`/`content`。
3. 如果是媒体类型：
   - `media_id` 必填且格式合法。
   - 查询 `media_object`：存在、`owner_id == from_user`、`status == ready`、`kind` 与 `msg_type` 匹配。
   - 如果 `session_id/message_id` 已绑定到别的消息：v1 返回 409（防止重复引用/重复发送）；后续用 `media_ref` 支持转发。
   - 校验 `session` 访问权限（单聊目标必须是自己参与的会话）。
4. 分配 `msg_seq`、构造 `ChatMessage`，`msg_type` 用新枚举。
5. 绑定 `media_object.session_id/message_id`（可在 Kafka 发送前后按一致性策略处理，见 §9）。
6. 写入 Kafka；后续 Job 持久化不变。

错误：

| 场景 | HTTP | body code | 说明 |
|---|---:|---:|---|
| media 不存在 | 404 | 404 | 不泄露是否存在 |
| media 不属于当前用户 | 403 | 403 | 防止盗用 |
| media 未 ready | 409 | 409 | 客户端轮询/重试 |
| media 已被其他消息绑定 | 409 | 409 | v1 不允许复用 |
| kind 与 msg_type 不匹配 | 400 | 400 | 例如 image 消息引用 video |
| caption 过长 | 400 | 400 | 限制 500 字符 |
| 不支持的类型 | 415 | 415 | 需要扩展 `ReasonPhrase` |

### 6.6 WebSocket 媒体消息（第二阶段）

v1 建议媒体消息先走 HTTP `/api/message/send`：上传本来就走 HTTP，发送也走 HTTP 最自然；实时下发仍由 Kafka/Job/Comet 完成。

第二阶段再扩展 WS：

```json
{
  "type": "single_chat",
  "to_user_id": 2,
  "client_msg_id": "1730000000000",
  "msg_type": "image",
  "content": { "media_id": "m_...", "caption": "..." }
}
```

需要改：

- `websocket_utils` 解析 `msg_type`（或从 `content.type` 推断）。
- `UpstreamMessageRequest` 增加 `MsgType` 字段，或 Logic 解析 `content_json` 的 `type`。
- Logic 做与 HTTP 相同的媒体校验/注入。
- 不建议让 Comet 直接查媒体表；Comet 只透传。

### 6.7 错误码与 HTTP 状态

现有 `HttpServerLite::ReasonPhrase` 只有部分状态码；媒体接口需要补：

- `202 Accepted`：上传已接收、处理中。
- `413 Payload Too Large`：超过大小限制。
- `415 Unsupported Media Type`：类型不支持。
- `422 Unprocessable Entity`：文件损坏/无法解码（可选，也可用 400）。
- `429 Too Many Requests`：限流。

业务 `code` 与 HTTP 状态保持现有风格：成功 0，失败用 4xx/5xx。

---

## 7. 图片链路

### 7.1 完整时序

```text
用户选择图片
  │
  ├─ 浏览器本地 URL.createObjectURL(file) 立即显示本地预览
  ├─ 读取宽高/大小；可选计算 SHA-256
  │
  ├─ POST /api/media/upload/init
  │     ├─ Logic 鉴权、限额
  │     ├─ INSERT media_object(status=init)
  │     └─ 返回 media_id + 预签名 PUT URL
  │
  ├─ PUT file ─────────────────────────────▶ MinIO
  │     └─ XHR onprogress 更新进度条
  │
  ├─ POST /api/media/upload/complete
  │     ├─ HEAD 对象，校验大小/类型
  │     ├─ UPDATE status=uploaded
  │     ├─ INSERT media_task(process_image)
  │     └─ 返回 processing
  │
  ├─ 客户端轮询 GET /api/media/{id}
  │     └─ 直到 status=ready 或 failed
  │
  ├─ POST /api/message/send
  │     ├─ 校验 media ready + owner + kind
  │     ├─ 注入宽高/blurhash/主色
  │     ├─ 分配 msg_seq
  │     └─ Kafka push_to_comet
  │
  └─ 接收方：
        ├─ 先渲染 blurhash/主色占位
        ├─ GET /api/media/{id} 拿签名 URL
        ├─ 加载 micro/thumb
        └─ 点击后加载 preview/original
```

### 7.2 处理流水线

`media_worker` 领取 `process_image` 任务后：

1. 从 MinIO 下载原图到临时目录（或流式读取）。
2. 用 `vipsheader` / `ffprobe` 读取尺寸、格式、EXIF orientation。
3. 安全校验：
   - 最大像素数（例如 50 MP），防止解压炸弹。
   - 最大文件大小；MIME 与 magic number 一致。
   - 拒绝 SVG（XSS 风险）或做严格清洗。
   - 损坏文件直接 `failed`。
4. 自动 orientation，转 sRGB。
5. 生成变体：
   - `micro`：最长边 32px，WebP 质量约 50；用于模糊占位。
   - `thumb`：最长边 320px，WebP 质量约 80；聊天气泡。
   - `preview`：最长边 1280px，WebP 质量约 82；点开首屏。
   - `original`：可选“去 EXIF 的原图”或保留原文件；见 §10 隐私。
6. 计算 `blur_hash`（可选）和 `dominant_color`。
7. 上传变体到 MinIO，key 固定。
8. 在事务中：
   - 插入/更新 `media_variant` 行。
   - 更新 `media_object.status=ready`，写入宽高、blurhash、主色。
   - `media_task.status=done`。
9. 失败：`attempts++`，`next_run_at` 退避；超过上限 `failed`，记录 `last_error`。

### 7.3 变体尺寸建议

| 变体 | 最长边 | 格式 | 质量 | 用途 |
|---|---:|---|---:|---|
| `micro` | 32 | WebP | 50 | 模糊占位，先出现 |
| `thumb` | 320 | WebP | 80 | 聊天气泡 |
| `preview` | 1280 | WebP | 82 | 点开首屏 |
| `original` | 原始 | 原格式/JPEG | — | 点击查看/保存 |

补充：

- 竖图/横图都按“最长边”缩放，保持宽高比。
- 客户端用 `width/height` 设置 `aspect-ratio`，避免布局跳动。
- 如果浏览器兼容性有要求，可额外生成 JPEG 变体；v1 可先 WebP + original。
- 动图 GIF：v1 可保留原图、缩略图取首帧；或者直接拒绝 GIF，避免 scope 膨胀。
- HEIC：依赖 libvips 的 HEIF 支持；没有就 415，提示用户转 JPG。

### 7.4 模糊预览：三种实现

#### 方案 1：micro 缩略图 + CSS blur（v1 推荐）

- 服务端生成 32px `micro` WebP。
- 客户端先加载 micro（通常几百字节到 1 KB），设置：
  ```css
  filter: blur(12px);
  transform: scale(1.05);
  ```
- 同时用 `dominant_color` 作为背景色。
- thumb/preview 加载完成后淡入覆盖，形成“先模糊后清晰”。

优点：

- 实现简单，C++ 侧只需要图片缩放，不需要 DCT/编码算法。
- 视觉效果接近微信：先看到模糊轮廓，再变清晰。
- 不依赖额外库。

缺点：

- 需要一次网络请求拿 micro；如果消息里不带 URL，还要先请求 metadata。
- 极端弱网下仍可能先空白。

#### 方案 2：BlurHash / ThumbHash（可选增强）

- 服务端从 micro 计算一个短字符串（几十字节），直接放进消息 `content_json`。
- 客户端用 JS 库解码到 canvas，**零网络请求**立即显示模糊图。
- BlurHash 生态成熟、C 实现多；ThumbHash 质量更好、支持 alpha，但 C++ 库少一些。

优点：

- 消息到达即可显示，不依赖额外请求。
- 字符串很短，适合 Kafka/MySQL。
- 面试可讲“LQIP/占位图”。

缺点：

- 需要引入编码库或自己实现 DCT。
- 模糊图只是氛围，不是精确缩略图。
- 仍需 micro/thumb 作为真实图像。

#### 方案 3：渐进式 JPEG/WebP

- 原图用 progressive JPEG，浏览器先显示低清再逐步清晰。
- 优点：不需要额外变体。
- 缺点：不可控，不同浏览器/格式行为不同；大图仍要下载完整文件；不适合作为主方案。

**推荐组合：**

> **v1：micro + dominant_color + CSS blur；v2：再加 BlurHash/ThumbHash 作为零请求占位。**  
> 这样先打通业务，不被算法库卡住；后续增强不影响消息协议（`blur_hash` 字段可空）。

### 7.5 客户端渐进加载

推荐状态：

```text
[本地预览] → [上传中 %] → [处理中 spinner] → [模糊占位] → [thumb] → [preview] → [original]
```

实现要点：

- 发送方：选图后立刻 `URL.createObjectURL(file)` 显示本地图，不等上传。
- 上传：`XMLHttpRequest.upload.onprogress`；支持取消。
- 处理：轮询 `GET /api/media/{id}`，指数退避（300ms、600ms、1.2s...），最多 30s；失败提示重试。
- 接收方：
  - 消息到达先按 `width/height` 占位，背景用 `dominant_color`。
  - 有 `blur_hash` 就先画 canvas；否则加载 `micro`。
  - 进入视口再加载 `thumb`（`IntersectionObserver` + `loading="lazy"`）。
  - 点击才加载 `preview`/`original`。
- 图片元素：
  ```html
  <img
    src="thumb-url"
    width="320" height="240"
    style="aspect-ratio: 4 / 3; filter: blur(12px); transition: filter .25s"
    decoding="async"
    loading="lazy"
    alt="图片消息"
  >
  ```
  加载完成后去掉 `filter`，或叠加第二张图淡入。
- 全屏查看：点击打开 viewer，加载 preview，再按需加载 original；支持缩放/保存。
- 失败：显示“图片加载失败，点击重试”，不要一直转圈。

### 7.6 上传进度与取消

- `fetch` 不支持上传进度，使用 `XMLHttpRequest`。
- 取消：
  - 单次 PUT：`xhr.abort()`；服务端可能已写入部分对象，complete 未调用则视为孤儿，由清理任务处理。
  - 分片：abort multipart upload，删除已上传分片。
- 重试：
  - 网络错误可重试同一 URL（未过期）。
  - URL 过期重新 init 或重新签发。
  - complete 幂等，可安全重试。

---

## 8. 视频链路

### 8.1 v1 范围（建议先窄）

- 输入：MP4（H.264 + AAC）为主，WebM 可选。
- 最大：200 MB、10 分钟、1080p（可配置）。
- 处理：`ffprobe` 探测 + 生成封面 + 可选 faststart remux。
- 播放：直接 MP4 + Range；不做 HLS、不做多码率转码。
- 不支持 HEVC/ProRes 等浏览器兼容性差的格式；返回 415，后续再转码。

为什么先不做 HLS：

- HLS 需要切片、码率决策、manifest、分片签名、播放器（hls.js）、清理策略。
- 先打通“上传 → 封面 → 播放 → 消息展示”，再升级 HLS，学习曲线更平滑。

### 8.2 视频处理流水线

```text
complete
  │
  ├─ ffprobe -show_format -show_streams -of json
  │     ├─ 宽高、时长、码率、codec、rotation
  │     └─ 校验时长/分辨率/编码
  │
  ├─ 选取封面时间点（1s 或 10%，避开黑帧）
  ├─ ffmpeg -ss ... -i input -frames:v 1 -vf scale=640:-1 poster.jpg
  ├─ 生成 micro poster（32px）
  ├─ 计算 blurhash/主色
  ├─ 可选：ffmpeg -c copy -movflags +faststart playback.mp4
  └─ 更新 media_object ready + media_variant
```

关键点：

- `ffprobe` 输出 JSON，解析时只取需要的字段；不要用 shell 拼接文件名，防止命令注入。
- 使用 `fork/exec` + argv 数组；设置超时、CPU/内存限制；临时目录隔离。
- 封面时间点：
  - 固定 1s 可能在黑帧；可尝试 10% 处，或抽 3 帧选亮度最高。
  - v1 固定 1s + 失败重试 0s 即可。
- `faststart`：把 moov atom 移到文件头，浏览器可以边下边播；不转码，速度快。
- 如果原文件不是 H.264/AAC，v1 直接拒绝；v2 再 `libx264` 转码。

### 8.3 视频消息内容

```json
{
  "type": "video",
  "media_id": "m_...",
  "width": 1920,
  "height": 1080,
  "duration_ms": 15300,
  "blur_hash": "...",
  "dominant_color": "#101010",
  "caption": "旅行片段"
}
```

客户端：

```html
<video
  controls
  playsinline
  preload="metadata"
  poster="poster-url"
  width="640" height="360">
  <source src="playback-url" type="video/mp4">
</video>
```

- 聊天窗口里只显示封面 + 时长 + 播放按钮，不要自动播放。
- 点击后再加载视频；`preload="metadata"` 只取头部信息。
- 使用签名 URL 播放时，确保 MinIO/S3 支持 Range；浏览器会发 `Range: bytes=...`，服务端返回 206。
- URL 过期会导致播放中断；签名 TTL 要覆盖一次观看（例如 1 小时），或在过期时刷新 `src`。

### 8.4 视频封面与模糊预览

- 生成 `poster`（640px）和 `micro`（32px）。
- `blur_hash` 从 micro poster 计算。
- 聊天窗口先显示模糊 poster，点击后加载 `<video>`。
- 视频加载前显示时长；加载失败显示“视频加载失败”。

### 8.5 HLS 演进（后续）

```text
ffmpeg -i input.mp4 \
  -vf scale=... -c:v libx264 -b:v ... \
  -hls_time 6 -hls_playlist_type vod \
  -hls_segment_filename ... master_%03d.ts
```

需要补齐：

- 多码率（360p/720p/1080p）与 master playlist。
- 分片/播放列表的签名 URL；或 CDN signed cookie。
- 播放器：原生 Safari HLS；Chrome 用 hls.js。
- 清理策略：转码产物、临时文件、失败分片。
- 成本：CPU 转码时间可能远大于视频时长；需要独立 worker 池和队列。

---

## 9. 语音与文件扩展

### 9.1 语音

目标链路：

```text
浏览器 MediaRecorder
  → webm/opus 或 mp4/aac
  → 上传
  → ffprobe 时长
  → 可选转码为 Opus/AAC
  → 生成波形 peaks
  → media_object kind=voice
  → 消息 content 带 duration_ms + waveform
```

消息内容：

```json
{
  "type": "voice",
  "media_id": "m_...",
  "duration_ms": 8000,
  "waveform": [0, 12, 30, 18, 42, 8],
  "caption": ""
}
```

要点：

- 最长 60 秒，最大 10 MB（可配置）。
- 波形数组不要太大，例如 40–80 个峰值，直接放 `content_json`。
- 播放用 `<audio controls preload="metadata">`。
- 录制格式跨浏览器不同，服务端统一转码后再存。
- 语音没有“模糊预览”，用波形/时长占位。

### 9.2 通用文件

- `kind=file`，保存原始文件名、大小、MIME。
- 不生成图片/视频变体；生成图标类型。
- 下载使用 `Content-Disposition: attachment`；文件名做 RFC 5987 编码。
- 病毒扫描（ClamAV）可作为后续；没有扫描前不要允许公开下载。
- 最大 50 MB（v1），后续按需放宽。
- 文件名只用于展示和下载头，不用于对象 key。

### 9.3 扩展点

建议抽象：

```text
MediaProcessor 接口
  ├─ ImageProcessor   (libvips)
  ├─ VideoProcessor   (ffprobe/ffmpeg)
  ├─ VoiceProcessor   (ffmpeg + waveform)
  └─ FileProcessor    (无处理/病毒扫描)
```

`media_worker` 根据 `kind` 选择处理器；新增类型只加处理器和 `media_variant` 约定，不改消息主链路。

---

## 10. 可靠性、一致性与失败处理

### 10.1 上传幂等

- `init` 可以重复调用；每次生成新的 `media_id`，旧记录由清理任务处理。
- `complete` 必须幂等：同一 `media_id` 重复调用返回当前状态。
- 单次 PUT 覆盖同一 key；分片 complete 重复提交同一 part 列表应成功或返回已完成。
- 客户端重试使用同一个 `media_id`/`upload_id`，不要重新 init。

### 10.2 任务队列

v1 用 MySQL 任务表：

```sql
-- 概念示意，不是最终 SQL
START TRANSACTION;
SELECT id FROM media_task
 WHERE status='pending' AND next_run_at <= NOW()
 ORDER BY id
 LIMIT 1
 FOR UPDATE SKIP LOCKED;
UPDATE media_task
   SET status='running', locked_by=?, locked_at=NOW(), attempts=attempts+1
 WHERE id=?;
COMMIT;
```

- 多个 worker 不会抢到同一任务。
- worker 崩溃后，`running` 超时的任务可以被重新领取（`locked_at` 超时）。
- 处理逻辑必须幂等：变体 key 固定，重复生成覆盖即可。
- 重试退避：1s、5s、30s；超过 `max_attempts` 置 `failed`。
- 视频任务和图片任务分开 worker/并发，避免长视频阻塞图片。

### 10.3 孤儿清理

| 场景 | 清理策略 |
|---|---|
| init 后从未 complete | 24h 后标记 expired，删除对象（如果存在） |
| complete 后对象不存在 | 标记 failed，通知客户端重传 |
| 上传成功但从未发消息 | 7 天后软删除 + 对象 GC（可配置） |
| 处理失败 | 保留 24h 供重试，之后清理 |
| 消息删除 | 软删除 media，引用计数为 0 后 GC |
| 分片上传中断 | MinIO lifecycle 自动 abort incomplete multipart |

### 10.4 消息与媒体的一致性

没有分布式事务，采用“先校验、后发送、最终清理”：

1. 发送前校验 `media_object`：owner、ready、kind。
2. 绑定 `session_id/message_id`，再写 Kafka。
3. 如果 Kafka 写失败：媒体保持 ready 但已绑定；可作为孤儿清理，或允许客户端重试发送（v1 可返回 409，提示重新上传；更好的做法是绑定放在 Kafka 成功后，但会有接收方短暂读不到媒体的窗口）。
4. 接收方读取媒体时，如果 `status=deleted` 或对象不存在，显示“媒体已失效”，不要让消息链路崩溃。

**v1 推荐**：

- 绑定放在 Kafka 发送成功之后；接收方通常有几毫秒到几百毫秒延迟，足够绑定完成。
- 如果极端情况下接收方先到，`GET /api/media` 对 owner 放行；对接收方暂时返回 409/404，客户端重试一次即可。
- 后续用 `media_ref` + outbox 做严格一致。

### 10.5 失败矩阵

| 故障 | 上传 | 处理 | 发送消息 | 接收方展示 |
|---|---|---|---|---|
| MinIO 挂 | init 可成功但 PUT 失败 | worker 下载失败，重试 | 可发但媒体不可读 | 占位后失败 |
| media_worker 挂 | 上传/complete 正常 | 任务积压，status=processing | 未 ready 不能发（v1） | 无 |
| MySQL 挂 | init/complete 失败 | 任务无法领取 | 发送失败 | 历史/媒体查询失败 |
| Logic 挂 | init/complete 失败 | worker 继续处理 | 发送失败 | 已推送消息仍可显示；新消息延迟 |
| Redis 挂 | 限流/幂等降级 | 不受影响 | token 校验失败 | 取决于 token |
| 浏览器崩溃 | 上传中断，complete 未调用 | 无任务 | 无消息 | 孤儿清理 |
| 对象被删 | complete 失败 | 处理失败 | 不能发 | 显示失效 |

### 10.6 多实例

- Logic 多实例：媒体控制面必须无状态；签名 URL 由任意实例生成；不要写本地磁盘。
- `media_worker` 多实例：DB 队列 `SKIP LOCKED` 保证不重复领取；处理幂等。
- 对象存储：共享；bucket 全局唯一。
- 如果使用本地磁盘，多实例会立刻出问题，这是推荐对象存储的核心原因之一。

---

## 11. 安全设计

### 11.1 权限矩阵

| 操作 | 谁可以 | 校验点 |
|---|---|---|
| init | 登录用户 | token → owner_id；限流/配额 |
| complete | 该 media 的 owner | `media_object.owner_id` |
| 读取 metadata | owner；或已绑定会话的成员 | `owner_id` 或 `CheckSessionAccess` |
| 获取签名 URL | 同上 | 只对通过 metadata 校验的人签发 |
| 删除 | owner，且未绑定消息 | 绑定后只能走消息删除流程 |
| 直传对象 | 持有预签名 URL 的人 | URL 短期有效、method/key 限定 |
| 下载对象 | 持有预签名 URL 的人 | URL 短期有效；bucket 私有 |

### 11.2 预签名 URL

- bucket 必须私有，不能 public read。
- URL TTL 短：上传 15 分钟；下载 10–60 分钟（视频可更长）。
- 只签指定 method（PUT/GET/HEAD）和指定 key。
- 不要把 access key/secret 下发到浏览器；只下发签名 URL。
- 日志里不要打印完整 URL 的签名参数。
- CORS 只允许 Web Demo 的 origin；不要 `*` 配合凭证。
- 分片上传要暴露 `ETag` 给浏览器。

### 11.3 内容校验

- 不信任客户端 `Content-Type`；下载后看 magic number。
- 图片：
  - 最大像素数、最大文件大小、最大尺寸。
  - 拒绝 SVG（XSS）或做严格 sanitize。
  - 使用 libvips 的安全加载选项；限制内存/并发。
  - 去除 EXIF GPS/设备信息（隐私）。
- 视频：
  - 限制时长、分辨率、文件大小、codec。
  - ffmpeg/ffprobe 子进程设置超时和资源限制；不要 shell 拼接。
  - 不处理未知容器/损坏文件。
- 文件：
  - 文件名 sanitize；不要用于路径。
  - 下载加 `Content-Disposition: attachment` 和 `X-Content-Type-Options: nosniff`。
  - 病毒扫描后再允许公开下载（后续）。

### 11.4 防滥用

- 每用户：
  - 每分钟 init 次数。
  - 并发上传数。
  - 每日上传字节数/文件数。
  - 单文件大小/总存储配额。
- Redis key 示例：`rate:upload:init:{uid}`、`quota:upload:bytes:{uid}:{yyyymmdd}`。
- 超限返回 429；不要静默丢弃。
- 对异常 MIME、超大尺寸、频繁失败做告警。
- 对象存储 lifecycle 清理未完成分片和过期对象，防止成本失控。

### 11.5 隐私

- 图片默认去除 EXIF GPS；如果业务需要保留，必须明确告知。
- 删除消息/账号时，媒体要软删除并最终 GC。
- 签名 URL 可能被转发；TTL 短，且不包含用户身份。
- 日志脱敏：不打印 token、完整签名 URL、文件内容。

---

## 12. 可观测性与运维

### 12.1 指标

| 指标 | 说明 |
|---|---|
| `media_upload_init_total{kind}` | 上传初始化次数 |
| `media_upload_complete_total{kind,status}` | 完成成功/失败 |
| `media_upload_bytes_total{kind}` | 上传字节数 |
| `media_process_duration_seconds{kind}` | 处理耗时 |
| `media_process_failures_total{kind,reason}` | 处理失败 |
| `media_ready_total{kind}` | 可用媒体数 |
| `media_task_queue_depth{task_type}` | 队列积压 |
| `media_storage_bytes{kind}` | 存储用量 |
| `media_presign_errors_total` | 签名失败 |
| `media_signed_url_expired_total` | URL 过期/刷新次数 |

不要给指标加 `user_id`、`media_id` 这种高基数标签。

### 12.2 日志

建议结构化字段：

```text
media_id=... upload_id=... user_id=... kind=image stage=complete
size=... mime=... duration_ms=... error=...
```

关键阶段：

- init 成功/限额拒绝
- complete HEAD 校验
- worker 领取/完成/失败/重试
- 消息绑定成功/冲突
- 签名 URL 生成失败

### 12.3 配置草案

`conf/logic.conf` 增加：

```ini
# 媒体存储
media_storage=s3
media_s3_endpoint=http://127.0.0.1:9002
media_s3_region=us-east-1
media_s3_bucket=sparkpush-media
media_s3_access_key=...
media_s3_secret_key=...
media_signed_url_ttl_seconds=900

# 限制
media_max_image_bytes=20971520
media_max_video_bytes=209715200
media_max_voice_bytes=10485760
media_max_file_bytes=52428800
media_image_max_pixels=50000000
```

`conf/media_worker.conf`：

```ini
mysql_host=localhost
mysql_port=3306
mysql_user=root
mysql_password=123456
mysql_db=spark_push
mysql_pool_size=4

media_s3_endpoint=http://127.0.0.1:9002
media_s3_bucket=sparkpush-media
media_s3_access_key=...
media_s3_secret_key=...

media_temp_dir=/tmp/sparkpush-media
media_image_workers=2
media_video_workers=1
media_task_poll_ms=500
media_max_attempts=3
```

> 生产环境不要把密钥写进仓库；用环境变量/密钥管理。学习环境可以先用本地配置，但文档要标注。

### 12.4 MinIO 初始化

需要：

- 创建私有 bucket：`sparkpush-media`。
- 配置 CORS：
  - AllowedOrigin: `http://127.0.0.1:9001`
  - AllowedMethod: `GET, PUT, HEAD, POST`
  - AllowedHeader: `*`
  - ExposeHeader: `ETag`
- 配置 lifecycle：
  - 未完成 multipart 上传 1 天自动 abort。
  - 临时/失败对象 7 天清理（按前缀）。
- 不要把 bucket 设为 public；下载全部走预签名。

---

## 13. 开发难度与阶段计划

> 难度是相对当前项目（已有 Logic/Comet/Job/Kafka/MySQL/Redis）的估计；对初学者，实际时间通常要乘 1.5–2。

### 13.1 总览

| 阶段 | 内容 | 新增代码 | 难度 | 风险 | 学习收益 |
|---|---|---|---|---|---|
| P0 | 方案确认 + MinIO/预签名 spike | 少量 | 中高 | 签名/CORS | 对象存储、SigV4 |
| P1 | 图片上传 MVP（单次 PUT） | 中 | 中 | HTTP 路由、HEAD 校验 | 直传、状态机 |
| P2 | 图片缩略图 + 模糊预览 | 中 | 中 | libvips/前端渐进 | 图片处理、LQIP |
| P3 | 视频上传 + 封面 + MP4 播放 | 中大 | 高 | ffmpeg、Range | 视频元数据、流播放 |
| P4 | 分片/断点续传 + 可靠性 | 大 | 高 | multipart、清理 | 断点续传、幂等 |
| P5 | 语音/文件/HLS/转码 | 大 | 高 | 波形、HLS、成本 | 音频、流媒体 |

### 13.2 P0：方案与存储 spike（建议先做）

目标：验证“浏览器能否直传 MinIO、C++ 能否签发 URL、MinIO 能否 CORS/Range”。

交付：

- 本地 MinIO 启动脚本/说明。
- 一个最小 C++ 签名工具或 SDK 验证程序。
- 浏览器 `fetch PUT` 上传一个 1 MB 文件成功。
- 浏览器 `GET` 签名 URL 成功；视频 Range 返回 206。
- 记录签名、CORS、时钟偏差、URL 过期等坑。

完成标准：

- 不写业务代码，只验证基础设施。
- 确定使用 MinIO C++ SDK 还是自实现 SigV4。

### 13.3 P1：图片上传 MVP

交付：

- `media_object`/`media_task` 表。
- Logic：init/complete/metadata 三个接口。
- 单次 PUT 直传。
- `media_worker`：只做“下载 + 生成 320px thumb + 更新 ready”，先不做 blurhash。
- `/api/message/send` 支持 `msg_type=image`，只传 `media_id`。
- Web Demo：选图、上传进度、发送、接收方显示 thumb。
- 失败/重试/超时提示。

完成标准：

- 两个浏览器单聊，A 发图，B 能看到 thumb；刷新历史仍能看到。
- 上传 10 MB 图片不阻塞聊天；Logic 内存不随文件大小增长。
- 未 ready/非 owner 的 media 不能发送。

### 13.4 P2：模糊预览与体验

交付：

- `micro` + `dominant_color`（可选 blurhash）。
- 前端模糊占位 → thumb → preview → original。
- 宽高/aspect-ratio，避免布局跳动。
- 图片全屏查看、加载失败重试。
- EXIF 去除、orientation 处理。

完成标准：

- 弱网/慢网下先看到模糊图，再清晰。
- 长列表滚动不一次性加载所有原图。
- 大图/超大像素图片被拒绝或安全处理。

### 13.5 P3：视频 MVP

交付：

- 视频 init/complete（可先单次 PUT，限制 200 MB）。
- `ffprobe` 元数据、封面、micro poster。
- MP4 直链播放 + Range。
- 消息 type=video，聊天窗口显示封面/时长，点击播放。
- 不支持格式返回 415。

完成标准：

- 上传 50 MB MP4，生成封面，接收方点击可播放、可拖动进度。
- 视频处理不阻塞图片任务（独立并发/队列）。
- 失败视频有明确状态和错误提示。

### 13.6 P4：分片与可靠性

交付：

- S3 multipart init/part/complete/abort。
- 断点续传：查询已上传 part，跳过完成分片。
- `media_upload`/`media_upload_part` 表或 Redis 状态。
- 孤儿清理、重试、失败告警。
- 限流/配额。

完成标准：

- 上传 500 MB 视频中断后可以继续。
- worker 崩溃后任务可重新领取，不重复产生脏数据。
- 过期上传不泄漏对象。

### 13.7 P5：语音/文件/HLS

按兴趣选择，不建议一次全做：

- 语音：MediaRecorder + ffprobe + 波形 + `<audio>`。
- 文件：上传/下载/图标/病毒扫描。
- HLS：多码率、切片、播放器、签名、清理。

### 13.8 难度判断

**最容易踩坑的不是 C++ 业务代码，而是：**

1. 预签名 URL 的 SigV4/CORS/时钟偏差。
2. 现有 HTTP 服务器不适合大文件，必须绕开。
3. 图片解码安全（解压炸弹、EXIF、SVG）。
4. 视频 ffmpeg 的资源/超时/格式兼容。
5. 前端渐进加载和 URL 过期处理。
6. 状态机/幂等/孤儿清理，而不是“上传成功就完事”。

---

## 14. 测试计划

### 14.1 单元测试

- `media_id` 生成/格式校验。
- 状态机合法/非法转换。
- 预签名 URL 生成（固定测试向量；注意时钟）。
- 文件类型 magic number 校验。
- `content_json` 构造与大小限制。
- 任务重试/退避计算。

### 14.2 集成测试

- MinIO 测试容器/本地实例。
- init → PUT → complete → worker → metadata。
- 重复 complete、重复任务、worker 崩溃恢复。
- 未授权读取、跨用户 media_id、过期 URL。
- 大文件/分片/中断续传。

### 14.3 E2E

- 两个浏览器单聊发图片：
  - 发送方本地预览 → 上传 → 处理 → 发送 → 接收方模糊 → 清晰。
- 离线：B 不在线，A 发图，B 上线拉历史仍能显示。
- 视频：封面、播放、拖动。
- 失败：上传中断、处理失败、URL 过期、媒体删除。

### 14.4 安全/异常样本

准备 fixtures：

- 正常 JPEG/PNG/WebP。
- 超大像素 JPEG（解压炸弹）。
- 带 GPS EXIF 的 JPEG。
- 损坏 JPEG。
- SVG（应拒绝）。
- HEIC（支持/拒绝按实现）。
- 正常 MP4、HEVC MP4、损坏 MP4、超长视频。
- 伪造 Content-Type 的文件。
- 文件名含 `../`、中文、emoji。

### 14.5 性能

- 10 MB 图片上传时 Logic 内存/延迟。
- 100 张图片并发处理，worker 队列/CPU。
- 50 MB 视频处理耗时、临时磁盘。
- 历史列表 100 条媒体消息，前端请求数/加载策略。
- 签名 URL 生成 QPS。

---

## 15. 需要你拍板的决策

先不要写代码，下面这些确认后再进入详细设计。

| 编号 | 决策 | 选项 | 推荐 | 影响 |
|---|---|---|---|---|
| D1 | 文件存储 | MinIO/S3 vs 本地磁盘 | **MinIO/S3** | 决定是否引入对象存储、能否多实例 |
| D2 | 上传数据面 | 浏览器直传 vs Logic 代理 | **浏览器直传** | 决定是否要改流式 HTTP |
| D3 | 服务边界 | Logic 控制面 + media_worker vs 独立 media_server | **Logic + worker（v1）** | 决定部署/代码结构 |
| D4 | 第一阶段范围 | 只做单聊图片 vs 图片+视频 | **先图片** | 决定首个里程碑 |
| D5 | 发送时机 | 必须 ready 才能发 vs 允许 processing 占位 | **必须 ready** | 决定消息状态/客户端复杂度 |
| D6 | 模糊预览 | micro+CSS vs BlurHash/ThumbHash | **micro+CSS（v1）** | 决定依赖和消息字段 |
| D7 | 视频 v1 | MP4 直链+封面 vs HLS 转码 | **MP4 直链+封面** | 决定转码/播放器复杂度 |
| D8 | 媒体消息入口 | 先 HTTP 发送 vs 同时改 WS | **先 HTTP** | 决定 Comet/proto 改动量 |
| D9 | 分片上传 | v1 不做 vs 一开始就做 | **v1 不做，P4 再加** | 决定上传基础设施复杂度 |

### 15.1 我建议你先确认的最小组合

如果目标是“尽快跑通并学到东西”，建议先确认：

1. **D1 = MinIO/S3**
2. **D2 = 浏览器直传**
3. **D3 = Logic 控制面 + media_worker**
4. **D4 = 先只做单聊图片**
5. **D5 = 必须 ready 才能发**
6. **D6 = micro + CSS blur（BlurHash 后续）**
7. **D7 = 视频 P3 再做，MP4 直链**
8. **D8 = 媒体先走 HTTP，WS 第二阶段**
9. **D9 = 分片 P4 再做**

---

## 16. 下一步文档计划

确认上面的决策后，再写三份更细的文档，仍然不写实现代码：

1. `docs/media_api_spec.md`
   - 每个接口的完整请求/响应/错误码/幂等语义。
   - 消息 content schema 的版本化。
   - 前端状态机。
2. `docs/media_storage_schema.md`
   - 最终 DDL、索引、迁移顺序。
   - `media_object`/`media_variant`/`media_task` 的字段级说明。
   - 对象 key、生命周期、清理 SQL。
3. `docs/media_impl_plan.md`
   - 按文件列出改动：新增 `logic/http_handlers_media.cpp`、`media_dao`、`media_worker`、`s3_presigner`、前端上传模块。
   - 编译依赖（libvips/ffmpeg/MinIO SDK/OpenSSL）。
   - 分阶段验收标准。
   - 测试清单。

最后再开始 P0/P1 实现。

---

## 附录 A：图片消息 content 示例

```json
{
  "type": "image",
  "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
  "width": 4032,
  "height": 3024,
  "blur_hash": "LKO2?U%2Tw=w]~RBVZRi};RPxuwH",
  "dominant_color": "#8a7f72",
  "caption": "今天的晚霞"
}
```

注意：

- 不包含签名 URL；URL 由 `GET /api/media/{id}` 动态返回。
- 不包含 base64。
- 目标大小 < 1 KB；当前历史读取的 4096 字节缓冲可以容纳，但仍建议后续改成动态缓冲。
- `msg_type=image`，MySQL `message.msg_type='image'`。

## 附录 B：建议的代码改动点

> 仅列位置，不代表最终实现；细节在 `media_impl_plan.md` 中定。

| 文件/模块 | 改动 |
|---|---|
| `proto/spark_push.proto` | `MsgType` 增加 image/video/voice/file |
| `proto/proto_enums.h` | 字符串 ↔ 枚举映射 |
| `logic/http_server.h` | 增加媒体 handler 声明 |
| `logic/http_server.cpp` | 路由增加 `/api/media/...`；路径参数；`ReasonPhrase` 补 202/413/415/429 |
| `logic/http_handlers_media.cpp` | 新增 init/complete/metadata 实现 |
| `logic/http_handlers_message.cpp` | 支持 `msg_type`、对象 content、媒体校验/注入 |
| `logic/media_dao.h/.cpp` | 新增媒体表 DAO |
| `logic/media_service.h/.cpp` | 媒体业务逻辑（可选，避免 handler 过胖） |
| `common/object_storage.h/.cpp` 或 `media/s3_presigner.*` | S3/MinIO 抽象与签名 |
| `media_worker/` | 新进程：任务领取、图片/视频处理 |
| `sql/schema.sql` | 新增媒体表（迁移脚本单独） |
| `conf/logic.conf` | 媒体存储/大小限制配置 |
| `conf/media_worker.conf` | worker 配置 |
| `scripts/start_all.sh` / `stop_all.sh` / `status.sh` | 启动/停止/状态加入 media_worker（MinIO 可单独管理） |
| `web_demo/static/index.html` | 选图/上传/进度/渐进渲染/视频播放 |
| `logic/message_dao.cpp` | 历史读取缓冲改成动态/足够大（媒体内容虽小，但 caption 等仍可能变长） |
| `CMakeLists.txt` / `logic/CMakeLists.txt` | 新源文件、libvips/ffmpeg/OpenSSL 依赖 |

## 附录 C：面试可讲点

1. **为什么文件不经过 Logic/Kafka/MySQL**：控制面/数据面分离；大对象不适合消息队列和关系库。
2. **预签名 URL 的原理**：服务端用 access key/secret 签 method、key、expires、headers；浏览器只拿到短期 URL，不拿密钥。
3. **直传 vs 代理**：直传省带宽、易扩展；代理可控但成为瓶颈。
4. **上传状态机**：init → uploaded → processing → ready；处理失败/重试/孤儿清理。
5. **幂等**：complete 幂等、任务唯一键、变体 key 固定、消息媒体绑定冲突处理。
6. **模糊预览**：LQIP/micro 缩略图 + CSS blur，或 BlurHash/ThumbHash；宽高防布局抖动。
7. **视频 Range**：`Range: bytes=...` → 206；对象存储/CDN 原生支持；HLS 的演进。
8. **安全**：私有 bucket、短 TTL、magic number、EXIF 去除、解压炸弹、ffmpeg 子进程限制。
9. **一致性**：没有分布式事务，靠“发送前校验 + 最终清理”；消息只引用 `media_id`。
10. **多实例**：无状态 Logic、共享对象存储、DB 队列 `SKIP LOCKED`、worker 幂等。

---

## 附录 D：一句话总结

> **把“文件”当成对象存储里的资产，把“消息”当成对资产的引用；上传、处理、发送三条链路解耦，用状态机和幂等保证最终可用，用 micro/blurhash + 多级变体实现微信式渐进预览。**
