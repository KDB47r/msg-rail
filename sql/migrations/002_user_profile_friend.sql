-- 002: 用户头像、昵称搜索索引、好友关系与好友申请
-- 只增不删，可对已有 spark_push 库重复执行。
-- 不要用 sql/schema.sql 代替本文件：schema.sql 会 DROP DATABASE。

USE `spark_push`;

-- 1. user.avatar_media_id（幂等）
SET @db_name := DATABASE();
SET @has_avatar_col := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'user'
    AND COLUMN_NAME = 'avatar_media_id'
);
SET @ddl := IF(
  @has_avatar_col = 0,
  'ALTER TABLE `user` ADD COLUMN `avatar_media_id` VARCHAR(32) NOT NULL DEFAULT '''' AFTER `name`',
  'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2. user.name 精确搜索索引（幂等）
SET @has_name_idx := (
  SELECT COUNT(*) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = @db_name
    AND TABLE_NAME = 'user'
    AND INDEX_NAME = 'idx_name'
);
SET @ddl := IF(
  @has_name_idx = 0,
  'ALTER TABLE `user` ADD KEY `idx_name` (`name`)',
  'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 3. 好友申请表
CREATE TABLE IF NOT EXISTS `friend_request` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `from_user_id` BIGINT NOT NULL,
  `to_user_id` BIGINT NOT NULL,
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=pending,1=accepted,2=rejected',
  `message` VARCHAR(255) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_from_to` (`from_user_id`, `to_user_id`),
  KEY `idx_to_status` (`to_user_id`, `status`),
  KEY `idx_from_status` (`from_user_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 好友关系表（双向两行）
CREATE TABLE IF NOT EXISTS `friend` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL,
  `friend_user_id` BIGINT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_friend` (`user_id`, `friend_user_id`),
  KEY `idx_friend_user` (`friend_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. 预置部分好友关系：alice-bob、carol-dave
-- 使用 account 查找，避免依赖自增 ID；测试库没有这些账号时插入 0 行。
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
