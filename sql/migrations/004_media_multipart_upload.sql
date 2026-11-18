-- 视频/大文件 multipart 上传会话。
-- 一行对应一个 media_id 的一次 S3 multipart upload；complete/abort 后删除。
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
