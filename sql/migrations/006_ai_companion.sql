-- P6 AI Companion: bot users, session_bot consent, ai_job idempotency.
-- Safe to run on an existing database; does not drop data.

USE `spark_push`;

-- 幂等添加 is_bot，避免在已跑过 schema.sql 的库上重复执行时报错。
SET @has_is_bot = (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'user'
    AND COLUMN_NAME = 'is_bot'
);
SET @add_is_bot_sql = IF(
  @has_is_bot > 0,
  'SELECT 1',
  'ALTER TABLE `user` ADD COLUMN `is_bot` TINYINT NOT NULL DEFAULT 0'
);
PREPARE add_is_bot_stmt FROM @add_is_bot_sql;
EXECUTE add_is_bot_stmt;
DEALLOCATE PREPARE add_is_bot_stmt;

CREATE TABLE IF NOT EXISTS `session_bot` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,
  `bot_user_id` BIGINT NOT NULL,
  `status` TINYINT NOT NULL DEFAULT 0,
  `invited_by` BIGINT NOT NULL,
  `activated_seq` BIGINT NOT NULL DEFAULT 0,
  `activated_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_bot` (`session_id`, `bot_user_id`),
  KEY `idx_session_status` (`session_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ai_job` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `job_id` VARCHAR(64) NOT NULL,
  `session_id` VARCHAR(128) NOT NULL,
  `bot_user_id` BIGINT NOT NULL,
  `mode` VARCHAR(16) NOT NULL,
  `trigger_message_id` VARCHAR(160) NOT NULL,
  `trigger_sender_id` BIGINT NOT NULL,
  `trigger_sender_name` VARCHAR(128) NOT NULL DEFAULT '',
  `trigger_content` TEXT NOT NULL,
  `activated_seq` BIGINT NOT NULL DEFAULT 0,
  `status` VARCHAR(16) NOT NULL DEFAULT 'queued',
  `msg_id` VARCHAR(160) NOT NULL DEFAULT '',
  `msg_seq` BIGINT NOT NULL DEFAULT 0,
  `error_code` VARCHAR(64) NOT NULL DEFAULT '',
  `error_message` VARCHAR(512) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ai_job` (`job_id`),
  UNIQUE KEY `uk_trigger_bot` (`session_id`, `trigger_message_id`, `bot_user_id`),
  KEY `idx_ai_job_status` (`status`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `user` (`id`, `account`, `name`, `avatar_media_id`, `password_hash`, `is_bot`)
VALUES
  (910000000000001, 'ai_assistant', '小桁', '', '', 1),
  (910000000000002, 'ai_vibe', '小桁', '', '', 1)
ON DUPLICATE KEY UPDATE
  `name` = VALUES(`name`),
  `is_bot` = 1;
