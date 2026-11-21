-- 自动创建数据库并初始化所有表
-- 支持重复执行（幂等）
DROP DATABASE IF EXISTS spark_push;
CREATE DATABASE IF NOT EXISTS `spark_push`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `spark_push`;

-- 用户表：账户与密码（后续可扩展 profile 字段、盐值和密码哈希）
CREATE TABLE IF NOT EXISTS `user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `account` VARCHAR(64) NOT NULL UNIQUE,
  `name` VARCHAR(64) NOT NULL DEFAULT '',
  `avatar_media_id` VARCHAR(32) NOT NULL DEFAULT '',
  `password_hash` VARCHAR(128) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  KEY `idx_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 好友申请表：一对用户只保留一条记录，status 0=pending,1=accepted,2=rejected
CREATE TABLE IF NOT EXISTS `friend_request` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `from_user_id` BIGINT NOT NULL,
  `to_user_id` BIGINT NOT NULL,
  `status` TINYINT NOT NULL DEFAULT 0,
  `message` VARCHAR(255) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_from_to` (`from_user_id`, `to_user_id`),
  KEY `idx_to_status` (`to_user_id`, `status`),
  KEY `idx_from_status` (`from_user_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 好友关系表：双向各存一行，查询“我的好友”只走 user_id 索引
CREATE TABLE IF NOT EXISTS `friend` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL,
  `friend_user_id` BIGINT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_friend` (`user_id`, `friend_user_id`),
  KEY `idx_friend_user` (`friend_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话表：单聊/群聊/聊天室等（当前代码主要使用单聊）
CREATE TABLE IF NOT EXISTS `session` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,      -- 业务层的 session_id，如 s_<uid1>_<uid2>
  `type` TINYINT NOT NULL DEFAULT 0,       -- 0=single,1=group,2=chatroom
  `user1_id` BIGINT DEFAULT 0,             -- 单聊一端
  `user2_id` BIGINT DEFAULT 0,             -- 单聊另一端
  `group_id` BIGINT DEFAULT 0,             -- 群聊/聊天室 id
  `last_msg_seq` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_id` (`session_id`),
  KEY `idx_user1` (`user1_id`),
  KEY `idx_user2` (`user2_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消息表：单聊/群聊/聊天室消息
CREATE TABLE IF NOT EXISTS `message` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,
  `msg_seq` BIGINT NOT NULL,             -- 会话内自增序号
  `sender_id` BIGINT NOT NULL,
  `msg_type` VARCHAR(32) NOT NULL DEFAULT 'text',
  `content_json` TEXT NOT NULL,
  `timestamp_ms` BIGINT NOT NULL,        -- 发送时间（毫秒）
  `client_msg_id` VARCHAR(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_seq` (`session_id`, `msg_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户会话状态表：记录每个用户在每个会话的已读游标
CREATE TABLE IF NOT EXISTS `user_session_state` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL,
  `session_id` VARCHAR(128) NOT NULL,
  `read_seq` BIGINT NOT NULL DEFAULT 0,     -- 已读到的最大 msg_seq
  `last_visit_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_session` (`user_id`, `session_id`),
  KEY `idx_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 群组表（后续 V2/V3 可用：normal_group / chatroom / danmaku_room）
CREATE TABLE IF NOT EXISTS `im_group` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(128) NOT NULL,
  `owner_id` BIGINT NOT NULL,
  `group_type` TINYINT NOT NULL DEFAULT 0,   -- 0=normal_group,1=chatroom,2=danmaku_room
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_owner` (`owner_id`),
  KEY `idx_type_id` (`group_type`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 群成员表
CREATE TABLE IF NOT EXISTS `group_member` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `group_id` BIGINT NOT NULL,
  `user_id` BIGINT NOT NULL,
  `role` TINYINT NOT NULL DEFAULT 0,         -- 0=member,1=admin,2=owner
  `join_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_user` (`group_id`, `user_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 弹幕表（V3：按视频时间轴拉取）
CREATE TABLE IF NOT EXISTS `video_danmaku` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `video_id` VARCHAR(128) NOT NULL,
  `timeline_ms` BIGINT NOT NULL,
  `sender_id` BIGINT NOT NULL,
  `content_json` TEXT NOT NULL,
  `timestamp_ms` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_video_timeline` (`video_id`, `timeline_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 广播任务表（可用于记录系统广播）
CREATE TABLE IF NOT EXISTS `broadcast` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `task_id` VARCHAR(128) NOT NULL,
  `title` VARCHAR(256) DEFAULT '',
  `scope` VARCHAR(64) DEFAULT 'all',
  `group_id` BIGINT DEFAULT 0,
  `content_json` TEXT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_id` (`task_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 媒体资产：图片/视频/语音/文件的对象存储元数据
-- 文件字节不进入 MySQL，只保存 media_id、对象 key、状态和处理结果
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `media_object` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,             -- 对外 ID：m_ + 26 位 Crockford Base32
  `owner_id` BIGINT NOT NULL,                  -- 上传者
  `kind` VARCHAR(16) NOT NULL,                 -- image/video/voice/file
  `status` VARCHAR(16) NOT NULL DEFAULT 'init',-- init/uploaded/processing/ready/failed/deleted
  `original_name` VARCHAR(255) NOT NULL DEFAULT '',
  `mime_type` VARCHAR(128) NOT NULL DEFAULT '',
  `size_bytes` BIGINT NOT NULL DEFAULT 0,
  `sha256` CHAR(64) NOT NULL DEFAULT '',
  `width` INT NOT NULL DEFAULT 0,
  `height` INT NOT NULL DEFAULT 0,
  `duration_ms` BIGINT NOT NULL DEFAULT 0,
  `blur_hash` VARCHAR(128) NOT NULL DEFAULT '',
  `dominant_color` CHAR(7) NOT NULL DEFAULT '',
  `storage_bucket` VARCHAR(64) NOT NULL DEFAULT '',
  `object_key` VARCHAR(512) NOT NULL DEFAULT '',
  `etag` VARCHAR(128) NOT NULL DEFAULT '',
  `session_id` VARCHAR(128) NOT NULL DEFAULT '', -- 发送后绑定到会话
  `message_id` VARCHAR(160) NOT NULL DEFAULT '', -- 发送后绑定到消息
  `error_message` VARCHAR(512) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `deleted_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_media_id` (`media_id`),
  KEY `idx_owner_created` (`owner_id`, `created_at`),
  KEY `idx_status_created` (`status`, `created_at`),
  KEY `idx_sha256` (`sha256`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 派生对象：micro/thumb/preview/poster/playback 等
CREATE TABLE IF NOT EXISTS `media_variant` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,
  `variant` VARCHAR(32) NOT NULL,              -- micro/thumb/preview/poster...
  `format` VARCHAR(16) NOT NULL,                -- webp/jpeg/mp4...
  `width` INT NOT NULL DEFAULT 0,
  `height` INT NOT NULL DEFAULT 0,
  `size_bytes` BIGINT NOT NULL DEFAULT 0,
  `bitrate_kbps` INT NOT NULL DEFAULT 0,
  `object_key` VARCHAR(512) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_media_variant` (`media_id`, `variant`),
  KEY `idx_media` (`media_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 异步处理任务（DB 队列）。MySQL 8 使用 FOR UPDATE SKIP LOCKED 支持多 worker
CREATE TABLE IF NOT EXISTS `media_task` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,
  `task_type` VARCHAR(32) NOT NULL,              -- process_image/process_video...
  `status` VARCHAR(16) NOT NULL DEFAULT 'pending', -- pending/running/done/failed
  `attempts` INT NOT NULL DEFAULT 0,
  `max_attempts` INT NOT NULL DEFAULT 3,
  `next_run_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `locked_by` VARCHAR(64) NOT NULL DEFAULT '',
  `locked_at` TIMESTAMP NULL DEFAULT NULL,
  `last_error` VARCHAR(512) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_media_task` (`media_id`, `task_type`),
  KEY `idx_status_next` (`status`, `next_run_at`),
  KEY `idx_type_status_next` (`task_type`, `status`, `next_run_at`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 视频/大文件 multipart 上传会话；complete/abort 后删除。
CREATE TABLE IF NOT EXISTS `media_multipart_upload` (
  `media_id` VARCHAR(32) NOT NULL,
  `upload_id` VARCHAR(512) NOT NULL,
  `part_size` INT NOT NULL,
  `parts_total` INT NOT NULL,
  `expires_at` TIMESTAMP NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`media_id`),
  UNIQUE KEY `uk_upload_id` (`upload_id`),
  KEY `idx_expires` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 内容寻址的媒体资产层；media_object_asset 把每次上传/转发映射到共享 asset。
CREATE TABLE IF NOT EXISTS `media_asset` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `sha256` CHAR(64) NOT NULL,
  `size_bytes` BIGINT NOT NULL,
  `kind` VARCHAR(16) NOT NULL,
  `mime_type` VARCHAR(128) NOT NULL,
  `storage_bucket` VARCHAR(64) NOT NULL,
  `object_key` VARCHAR(512) NOT NULL,
  `etag` VARCHAR(128) NOT NULL DEFAULT '',
  `width` INT NOT NULL DEFAULT 0,
  `height` INT NOT NULL DEFAULT 0,
  `duration_ms` BIGINT NOT NULL DEFAULT 0,
  `blur_hash` VARCHAR(128) NOT NULL DEFAULT '',
  `dominant_color` CHAR(7) NOT NULL DEFAULT '',
  `status` VARCHAR(16) NOT NULL DEFAULT 'ready',
  `ref_count` INT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_asset_content` (`sha256`, `size_bytes`, `kind`),
  KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `media_asset_variant` (
  `asset_id` BIGINT UNSIGNED NOT NULL,
  `variant` VARCHAR(32) NOT NULL,
  `format` VARCHAR(16) NOT NULL,
  `width` INT NOT NULL DEFAULT 0,
  `height` INT NOT NULL DEFAULT 0,
  `size_bytes` BIGINT NOT NULL DEFAULT 0,
  `bitrate_kbps` INT NOT NULL DEFAULT 0,
  `object_key` VARCHAR(512) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`asset_id`, `variant`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `media_object_asset` (
  `media_id` VARCHAR(32) NOT NULL,
  `asset_id` BIGINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`media_id`),
  KEY `idx_asset` (`asset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `user` (`id`, `account`, `name`, `password_hash`) VALUES
(1, 'alice', 'Alice', 'e10adc3949ba59abbe56e057f20f883e'),
(2, 'bob', 'Bob', 'e10adc3949ba59abbe56e057f20f883e'),
(3, 'carol', 'Carol', 'e10adc3949ba59abbe56e057f20f883e'),
(4, 'dave', 'Dave', 'e10adc3949ba59abbe56e057f20f883e');

-- 预置好友关系：alice-bob、carol-dave 互为好友；其余组合用于申请流程测试
INSERT IGNORE INTO `friend` (`user_id`, `friend_user_id`)
SELECT a.id, b.id FROM `user` a, `user` b
WHERE a.account = 'alice' AND b.account = 'bob';
INSERT IGNORE INTO `friend` (`user_id`, `friend_user_id`)
SELECT a.id, b.id FROM `user` a, `user` b
WHERE a.account = 'bob' AND b.account = 'alice';
INSERT IGNORE INTO `friend` (`user_id`, `friend_user_id`)
SELECT a.id, b.id FROM `user` a, `user` b
WHERE a.account = 'carol' AND b.account = 'dave';
INSERT IGNORE INTO `friend` (`user_id`, `friend_user_id`)
SELECT a.id, b.id FROM `user` a, `user` b
WHERE a.account = 'dave' AND b.account = 'carol';
