# 图片媒体消息 API 规格（草案 v0.1）

> 状态：设计讨论稿，**不包含实现代码**。  
> 日期：2026-11-08  
> 范围：只覆盖已确认的第一阶段——**单聊图片 + MinIO 直传 + HTTP 发送 + micro/CSS 模糊预览**。  
> 视频、语音、文件、聊天室、分片上传、HLS 不在本规格内，后续单独扩展。

---

## 1. 本阶段要跑通的链路

```text
A 选择图片
  │
  ├─ 本地预览 URL.createObjectURL
  ├─ POST /api/media/upload/init
  ├─ PUT 文件 ─────────────────────▶ MinIO
  ├─ POST /api/media/upload/complete
  ├─ 轮询 GET /api/media/{id} 直到 ready
  ├─ POST /api/message/send（msg_type=image, content.media_id）
  │
  └─ Kafka → Job → Comet → B

B 收到消息
  ├─ 先显示模糊占位（micro + dominant_color）
  ├─ GET /api/media/{id} 拿签名 URL
  ├─ 加载 thumb
  └─ 点击加载 preview/original
```

本阶段**不改变**：

- Comet 的 WebSocket 消息格式。
- Kafka `push_to_comet` 的语义。
- Job 的推送/持久化流程。
- 文本消息的现有行为。

本阶段**新增/修改**：

- Logic HTTP 的媒体控制面接口。
- `media_object` / `media_variant` / `media_task` 表。
- `media_worker` 进程。
- `/api/message/send` 支持 `msg_type=image`。
- Web Demo 的选图/上传/渐进展示。

---

## 2. 通用约定

### 2.1 响应格式

沿用现有 API：

```json
{
  "code": 0,
  "message": "ok",
  "data": {}
}
```

- `code == 0` 表示业务成功。
- 失败时 HTTP 状态码与 `code` 尽量一致；现有 `WriteError` 会把 `code` 映射到 HTTP 状态。
- 所有时间戳使用 Unix 毫秒，字段名 `*_ms`；URL 过期时间用 Unix 秒，字段名 `expires_at`。

### 2.2 鉴权

- 统一使用 `Authorization: Bearer <token>`。
- 现有 `AuthenticateRequest` 还支持 JSON body 的 `token` 字段；媒体接口可以继续兼容，但新客户端只用 header。
- `owner_id` 永远来自 token，不接受客户端传入。

### 2.3 ID

- `media_id`：`m_` + 26 位 Crockford Base32（ULID 风格），例如 `m_01J9Z8YQ3K5V7M2C4T6R8W0XAB`。
- `upload_id`：分片阶段使用，`u_` + 26 位；本阶段单次 PUT 不需要。
- 不对外暴露 MySQL 自增 ID。

### 2.4 本阶段默认限制

以下值先作为默认值，后续可配置：

| 项 | 默认值 | 说明 |
|---|---:|---|
| 单张图片最大 | 20 MiB | `media_max_image_bytes` |
| 允许格式 | JPEG / PNG / WebP | v1 不支持 GIF/HEIC/SVG |
| 最大像素 | 50 MP | 防止解压炸弹 |
| 最大边长 | 12000 px | 可按需调整 |
| caption 最大长度 | 500 字符 | UTF-8 字符数 |
| 上传 URL TTL | 15 分钟 | 过期重新 init 或重新签发 |
| 下载 URL TTL | 15 分钟 | 客户端按需刷新 |
| 处理超时 | 60 秒/图 | worker 超时置失败 |
| 单用户并发上传 | 3 | Redis 计数 |
| 单用户日上传量 | 500 MiB | 可配置 |

### 2.5 状态

`media_object.status`：

```text
init → uploaded → processing → ready
                         └──→ failed
ready → deleted（软删除）
```

本阶段只有 `image` 一种 kind；`process_image` 一种任务。

---

## 3. 接口总表

| 方法 | 路径 | 作用 | 需要鉴权 |
|---|---|---|---|
| POST | `/api/media/upload/init` | 创建媒体记录并签发上传 URL | 是 |
| PUT | 预签名 MinIO URL | 浏览器直传文件字节 | URL 自身签名 |
| POST | `/api/media/upload/complete` | 确认对象存在、入处理队列 | 是 |
| GET | `/api/media/{media_id}` | 查元数据 + 签名下载 URL | 是 |
| POST | `/api/message/send` | 发送图片消息 | 是 |

内部接口（不对浏览器暴露）：

- `media_worker` 从 MySQL `media_task` 领取任务。
- worker 直接访问 MinIO/MySQL，不经过 Logic HTTP。

---

## 4. `POST /api/media/upload/init`

### 4.1 请求

```json
{
  "kind": "image",
  "file_name": "IMG_0001.jpg",
  "mime_type": "image/jpeg",
  "size": 2456789,
  "sha256": "可选，64 位十六进制"
}
```

字段：

| 字段 | 必填 | 类型 | 说明 |
|---|---|---|---|
| `kind` | 是 | string | 本阶段只能是 `image` |
| `file_name` | 否 | string | 仅展示/生成扩展名，不用于对象 key |
| `mime_type` | 是 | string | 允许 `image/jpeg`、`image/png`、`image/webp` |
| `size` | 是 | int64 | 字节数；必须 >0 且 <= 20 MiB |
| `sha256` | 否 | string | 客户端可选计算；服务端最终以自己计算为准 |

### 4.2 服务端校验

1. token 有效，得到 `owner_id`。
2. `kind == image`。
3. `mime_type` 在白名单内。
4. `size` 在范围内。
5. 限流：
   - `rate:upload:init:{uid}`：每分钟 init 次数。
   - `upload:concurrent:{uid}`：并发上传数。
   - `quota:upload:bytes:{uid}:{yyyymmdd}`：日上传字节数。
6. 生成 `media_id`、`object_key`。
7. 插入 `media_object`：
   - `owner_id`
   - `kind='image'`
   - `status='init'`
   - `original_name`、`mime_type`、`size_bytes`
   - `storage_bucket`、`object_key`
   - `sha256`（如果客户端提供，先记录候选值）
8. 签发 MinIO 预签名 PUT URL。

### 4.3 响应

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
    "upload": {
      "mode": "single",
      "method": "PUT",
      "url": "http://127.0.0.1:9002/sparkpush-media/media/m_01J.../original.jpg?X-Amz-Algorithm=...",
      "headers": {
        "Content-Type": "image/jpeg"
      },
      "expires_at": 1760000000
    }
  }
}
```

说明：

- `url` 是短期签名 URL，不要缓存到消息里。
- `headers` 必须由浏览器原样带上；如果签名包含 `Content-Type`，不一致会 403。
- 本阶段 `mode` 固定 `single`；分片阶段再增加 `multipart`。

### 4.4 错误

| 场景 | HTTP | code | 说明 |
|---|---:|---:|---|
| 未登录/无效 token | 401 | 401 | |
| kind 不支持 | 400 | 400 | 本阶段只支持 image |
| MIME 不支持 | 415 | 415 | 例如 GIF/HEIC/SVG |
| 文件过大 | 413 | 413 | |
| 超过限流/配额 | 429 | 429 | `Retry-After` 可选 |
| MinIO 签名失败 | 503 | 503 | 不创建半成品记录，或标记 init 清理 |
| DB 写入失败 | 500 | 500 | |

---

## 5. 浏览器直传

### 5.1 单次 PUT

```http
PUT /sparkpush-media/media/m_01J.../original.jpg?X-Amz-Algorithm=...
Content-Type: image/jpeg

<二进制文件>
```

要求：

- 使用 `XMLHttpRequest` 以便拿到 `upload.onprogress`。
- 请求头与 init 返回的 `headers` 一致。
- 成功状态：`200` 或 `204`。
- 记录响应头 `ETag`（本阶段不一定用，分片时必需）。
- 网络失败可重试同一 URL；URL 未过期时直接重试。
- 取消：`xhr.abort()`；未 complete 的对象由清理任务处理。

### 5.2 CORS 要求

MinIO bucket 需要允许 Web Demo 的 origin：

```text
AllowedOrigin: http://127.0.0.1:9001
AllowedMethod: GET, PUT, HEAD
AllowedHeader: *
ExposeHeader: ETag
```

如果使用 `fetch` 而不是 XHR，仍需要 CORS；但 `fetch` 没有上传进度。

### 5.3 客户端不要做的事

- 不要把文件转 base64 再发 JSON。
- 不要把 access key/secret 放进前端。
- 不要用客户端文件名拼对象 key。
- 不要相信客户端的 `mime_type`；服务端 complete/worker 仍要校验。

---

## 6. `POST /api/media/upload/complete`

### 6.1 请求

```json
{
  "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB"
}
```

本阶段单次 PUT 不需要 `parts`。分片阶段再扩展：

```json
{
  "media_id": "m_...",
  "upload_id": "u_...",
  "parts": [
    { "part_number": 1, "etag": "\"abc\"" }
  ]
}
```

### 6.2 服务端动作

1. 鉴权，取 `owner_id`。
2. 查 `media_object`：
   - 不存在/不属于当前用户 → 404/403。
   - 已 `ready` → 直接返回 `ready`（幂等）。
   - 已 `deleted` → 410/409。
3. 对 `object_key` 做 `HEAD`：
   - 对象不存在 → 400/409。
   - 实际大小 > 限制或与 init 差异过大 → 413/400。
   - 内容类型不可信，worker 再做 magic number。
4. 更新 `status='uploaded'`。
5. 插入 `media_task(media_id, task_type='process_image', status='pending')`：
   - `UNIQUE(media_id, task_type)` 防重复入队。
6. 返回 `processing`。

### 6.3 响应

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

如果已经处理完：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_...",
    "status": "ready"
  }
}
```

### 6.4 幂等语义

- 同一 `media_id` 多次 complete：
  - 第一次入队；
  - 后续返回当前状态，不重复插入任务。
- worker 还没处理完：返回 `processing`。
- worker 失败：返回 `failed`，可提供 `error_message`（不暴露内部堆栈）。
- 如果对象不存在：返回 409，允许客户端重新 PUT 后再次 complete。

### 6.5 错误

| 场景 | HTTP | code |
|---|---:|---:|
| media 不存在/非 owner | 404/403 | 404/403 |
| 对象不存在 | 409 | 409 |
| 大小超限 | 413 | 413 |
| 处理中重复调用 | 200 | 0，status=processing |
| 已 ready 重复调用 | 200 | 0，status=ready |
| 已 failed | 200 | 0，status=failed（或 422） |
| DB/队列失败 | 500 | 500 |

---

## 7. `GET /api/media/{media_id}`

### 7.1 请求

```http
GET /api/media/m_01J9Z8YQ3K5V7M2C4T6R8W0XAB
Authorization: Bearer <token>
```

> 当前 `HttpServerLite` 路由是精确匹配；实现时需要增加 `/api/media/` 前缀匹配和路径参数解析。这是本阶段必须改的基础设施之一。

### 7.2 权限

| 状态 | 谁可读 |
|---|---|
| `init` / `uploaded` / `processing` | 只有 owner |
| `ready` 且未绑定会话 | 只有 owner |
| `ready` 且已绑定会话 | owner + 会话成员（复用 `CheckSessionAccess`） |
| `failed` / `deleted` | owner（deleted 可 404） |

- 未登录 → 401。
- 非 owner 且不满足会话权限 → 403 或 404（建议 404，避免泄露存在性）。
- 不要返回 bucket、object_key、access key。

### 7.3 响应（ready）

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
    "kind": "image",
    "status": "ready",
    "width": 4032,
    "height": 3024,
    "mime_type": "image/jpeg",
    "size_bytes": 2456789,
    "blur_hash": "",
    "dominant_color": "#8a7f72",
    "variants": {
      "thumb": {
        "url": "http://127.0.0.1:9002/sparkpush-media/media/m_.../thumb.webp?X-Amz-...",
        "width": 320,
        "height": 240,
        "expires_at": 1760000000
      },
      "original": {
        "url": "http://127.0.0.1:9002/sparkpush-media/media/m_.../original.jpg?X-Amz-...",
        "width": 4032,
        "height": 3024,
        "expires_at": 1760000000
      }
    }
  }
}
```

P1 阶段只要求 `thumb` 和 `original`；P2 增加：

```json
"micro": { "url": "...", "width": 32, "height": 24 },
"preview": { "url": "...", "width": 1280, "height": 960 }
```

`blur_hash` 在 P1 为空字符串；P2 可以填入 BlurHash/ThumbHash。客户端必须容忍字段缺失/为空。

### 7.4 响应（处理中）

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "media_id": "m_...",
    "kind": "image",
    "status": "processing",
    "width": 0,
    "height": 0,
    "variants": {}
  }
}
```

客户端轮询策略：

- 300ms → 600ms → 1.2s → 2.4s，最大 30s。
- 超过 30s 显示“处理中，稍后重试”，不要无限转圈。
- `failed` 显示失败原因（脱敏）和重试入口。

### 7.5 URL 过期

- 客户端缓存 `media_id → metadata`，在 `expires_at` 前复用。
- 收到 403/过期时重新 GET。
- 不要把签名 URL 写进 `content_json`；历史消息只存稳定元数据。
- 视频/大图播放中 URL 过期会导致中断；P3 视频 TTL 可以更长，或播放器监听错误后刷新。

---

## 8. `POST /api/message/send` 扩展

### 8.1 文本消息（保持兼容）

```json
{
  "to_user_id": 2,
  "content": "hello"
}
```

行为不变：`msg_type` 默认 `text`。

### 8.2 图片消息

```json
{
  "to_user_id": 2,
  "msg_type": "image",
  "content": {
    "media_id": "m_01J9Z8YQ3K5V7M2C4T6R8W0XAB",
    "caption": "今天的晚霞"
  }
}
```

客户端**只传** `media_id` 和可选 `caption`；不要传宽高/blurhash/URL。服务端从 `media_object` 读取权威值并注入。

### 8.3 服务端校验

1. 鉴权得到 `from_user`。
2. `to_user_id > 0`，不能给自己发？现有代码允许；可保持现状或新增限制（待定）。
3. `msg_type == image`。
4. `content` 是对象，`media_id` 必填。
5. 查 `media_object`：
   - 存在；
   - `owner_id == from_user`；
   - `status == ready`；
   - `kind == image`；
   - `session_id/message_id` 为空（v1 不允许重复引用）。
6. 构造 `session_id = single:min:max`（复用现有逻辑）。
7. 检查 `media_object` 如果已绑定到别的 session → 409。
8. 分配 `msg_seq`，构造 `ChatMessage`：
   - `msg_type = MSG_TYPE_IMAGE`
   - `content_json =` 服务端构造的规范 JSON。
9. 绑定 `media_object.session_id/message_id`（建议 Kafka 发送成功后更新；见 §10）。
10. 发送 Kafka（在线/离线路径复用现有逻辑）。
11. 返回 `msg_id/msg_seq/session_id`。

### 8.4 最终 `content_json`

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

约束：

- 目标 < 1 KB。
- 不含签名 URL。
- 不含 base64。
- `type` 与 `msg_type` 一致。
- `caption` 最长 500 字符；超长 400。

### 8.5 响应

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "session_id": "single:1:2",
    "msg_id": "single:1:2-42",
    "msg_seq": 42,
    "media_id": "m_...",
    "status": "ready"
  }
}
```

### 8.6 错误

| 场景 | HTTP | code | 说明 |
|---|---:|---:|---|
| media 不存在 | 404 | 404 | |
| 非 owner | 403 | 403 | |
| 未 ready | 409 | 409 | 客户端应等待 |
| 已被其他消息绑定 | 409 | 409 | v1 不允许复用 |
| kind 不匹配 | 400 | 400 | 例如 image 消息引用 video |
| caption 超长 | 400 | 400 | |
| Kafka 发送失败 | 500 | 500 | 媒体仍 ready，可重试发送 |

### 8.7 幂等与重复发送

v1 的重复保护：

- 同一个 `media_id` 只能绑定一条消息；第二次发送返回 409。
- 客户端发送成功后禁用发送按钮。
- 网络超时导致“客户端不知道成功”的情况，v1 可能返回 409；用户可刷新历史确认。
- 后续可加 `client_msg_id` 唯一约束/幂等表，支持安全重试。

### 8.8 WebSocket（本阶段不做）

- 本阶段媒体消息只走 HTTP。
- 文本 WS 消息保持原样。
- 第二阶段再让 Comet 解析 `msg_type`，Logic 复用同一套媒体校验。

---

## 9. `media_worker` 处理规格

### 9.1 领取任务

- 轮询 `media_task`，`status='pending' AND next_run_at <= NOW()`。
- 使用 `FOR UPDATE SKIP LOCKED` 保证多 worker 不重复领取。
- 图片并发默认 2，视频并发后续单独配置。

### 9.2 图片处理

1. 从 MinIO 下载 `original` 到临时文件。
2. 校验：
   - 文件大小；
   - magic number 与 MIME 一致；
   - 像素数 <= 50 MP；
   - 不是 SVG/未知格式；
   - 可正常解码。
3. 自动 orientation，转 sRGB。
4. 生成 P1 必需：
   - `thumb`：最长边 320px，WebP，质量 80。
   - `original`：P1 可以直接保留原对象；P2 再决定是否去 EXIF 重编码。
5. P2 增加：
   - `micro`：最长边 32px，WebP，质量 50。
   - `preview`：最长边 1280px，WebP，质量 82。
   - `dominant_color`、`blur_hash`（可选）。
6. 上传变体到固定 key：
   - `media/{media_id}/thumb.webp`
   - `media/{media_id}/micro.webp`
   - `media/{media_id}/preview.webp`
7. 事务更新：
   - `media_variant` 插入/更新；
   - `media_object.status='ready'`、宽高、主色、blurhash；
   - `media_task.status='done'`。
8. 失败：`attempts++`、`next_run_at` 退避；超过上限 `failed`，写 `last_error`。

### 9.3 幂等

- 变体 key 固定，重复生成覆盖。
- 任务唯一键防重复。
- 更新 status 带条件：
  - 只有 `processing` 才能变 `ready`；
  - 避免旧 worker 覆盖新状态。

### 9.4 失败清理

- 临时文件 `finally` 删除。
- 变体上传失败：保留已上传的部分，重试覆盖。
- 超过重试：`failed`；对象保留 24h 供排查，之后 GC。

---

## 10. 消息与媒体绑定的一致性

推荐顺序（v1）：

```text
校验 media ready/owner
  → 分配 msg_seq
  → 构造 ChatMessage
  → 写 Kafka
  → 成功后 UPDATE media_object SET session_id=?, message_id=?
  → 返回成功
```

风险：

- Kafka 成功后、DB 绑定前进程崩溃：消息已发出，接收方 GET metadata 时媒体仍未绑定；owner 可读，接收方可能 403。
- 缓解：
  - 接收方客户端对 403/404 做一次短重试；
  - 或者把绑定放在 Kafka 前，失败时媒体成为孤儿，由清理任务处理；
  - 后续用 outbox/`media_ref` 做严格一致。

v1 建议：

- 绑定放在 Kafka 成功后。
- metadata 对“已 ready 但未绑定”的媒体，允许 owner 读取；接收方由客户端重试。
- 记录日志 `media_bind_failed` 便于排查。

---

## 11. 前端状态机

```text
IDLE
  │ 选择文件
  ▼
LOCAL_PREVIEW
  │ init 成功
  ▼
UPLOADING ──失败──▶ UPLOAD_FAILED ──重试──▶ UPLOADING
  │ PUT 成功
  ▼
COMPLETING
  │ complete 成功
  ▼
PROCESSING ──失败──▶ PROCESS_FAILED ──重试──▶ PROCESSING
  │ ready
  ▼
READY
  │ 发送
  ▼
SENDING ──失败──▶ SEND_FAILED ──重试──▶ SENDING
  │ ack
  ▼
SENT
```

接收方：

```text
MESSAGE_RECEIVED
  │ 先用 width/height/dominant_color 占位
  ▼
PLACEHOLDER（micro/blurhash/CSS blur）
  │ thumb 加载
  ▼
THUMB
  │ 点击
  ▼
PREVIEW
  │ 点击原图
  ▼
ORIGINAL
```

---

## 12. 验收标准（P1 + P2）

### 12.1 P1 图片上传 MVP

- [ ] A 选择 JPG/PNG/WebP（<=20 MiB），看到本地预览。
- [ ] 上传进度可见；可取消；失败可重试。
- [ ] complete 后状态 `processing`，worker 生成 thumb 后变 `ready`。
- [ ] A 发送图片消息，B 实时收到。
- [ ] B 能看到 thumb；点击能看到 original。
- [ ] B 离线后重新上线，拉历史仍能看到图片。
- [ ] 非 owner 不能读取 media；未 ready 不能发送。
- [ ] 超过 20 MiB 返回 413；GIF/HEIC/SVG 返回 415。
- [ ] Logic 内存不随文件大小线性增长（文件不经过 Logic）。
- [ ] MinIO 挂掉时，init/complete 返回可理解的错误，不产生“假成功”。

### 12.2 P2 模糊预览

- [ ] 消息到达时先出现 `dominant_color`/模糊占位，不出现大块空白。
- [ ] `micro` 加载后显示模糊轮廓；thumb 加载后淡入清晰。
- [ ] 点击后加载 preview，再点击/缩放加载 original。
- [ ] 长列表使用 lazy load，不一次性加载全部原图。
- [ ] 宽高/aspect-ratio 正确，图片加载不导致列表跳动。
- [ ] 带 EXIF GPS 的图片，服务端处理后不泄露 GPS。
- [ ] 超大像素图片被拒绝/安全处理，不拖垮 worker。

---

## 13. 待确认的细节

这些不阻塞设计，但实现前需要定：

1. **图片大小上限**：20 MiB 是否合适？还是 10/50 MiB？
2. **格式范围**：v1 只支持 JPEG/PNG/WebP，GIF/HEIC 后续，是否同意？
3. **caption**：图片消息是否需要文字说明？默认允许 500 字符，是否保留？
4. **缩略图尺寸**：micro 32 / thumb 320 / preview 1280 是否合适？
5. **media_worker 形态**：独立进程（推荐）还是先放 Logic 线程池？
6. **消息发送入口**：本阶段只走 HTTP，WS 第二阶段，是否同意？
7. **原图隐私**：是否默认去除 EXIF 并重编码？还是保留原图仅去预览元数据？
8. **历史图片懒加载**：是否需要 P1 就做，还是 P2 再做？

---

## 14. 下一步

确认 §13 后，再写：

1. `docs/media_storage_schema.md`：最终 DDL、索引、迁移顺序、清理 SQL。
2. `docs/media_impl_plan.md`：按文件列出改动、依赖、编译、启动脚本、测试。
3. 然后开始 P0/P1 实现。

> 在 API 契约、状态机、错误码和存储模型定稿前，不写业务代码。
