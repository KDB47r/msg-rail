-- 001_media_tables.sql
-- 给已有 spark_push 数据库补齐媒体相关表。
--
-- 注意：不要用 sql/schema.sql 升级已有库，它开头会 DROP DATABASE。
-- 本文件只包含 CREATE TABLE IF NOT EXISTS，可重复执行。
--
-- 用法：
--   mysql -uroot -p123456 spark_push < sql/migrations/001_media_tables.sql

USE `spark_push`;

CREATE TABLE IF NOT EXISTS `media_object` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,
  `owner_id` BIGINT NOT NULL,
  `kind` VARCHAR(16) NOT NULL,
  `status` VARCHAR(16) NOT NULL DEFAULT 'init',
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
  `session_id` VARCHAR(128) NOT NULL DEFAULT '',
  `message_id` VARCHAR(160) NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS `media_variant` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,
  `variant` VARCHAR(32) NOT NULL,
  `format` VARCHAR(16) NOT NULL,
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

CREATE TABLE IF NOT EXISTS `media_task` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `media_id` VARCHAR(32) NOT NULL,
  `task_type` VARCHAR(32) NOT NULL,
  `status` VARCHAR(16) NOT NULL DEFAULT 'pending',
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
  KEY `idx_status_next` (`status`, `next_run_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
