-- 内容寻址的媒体资产层：同一份字节只有一份 asset 和一套变体。
-- media_object 仍是每次上传/转发的独立消息记录，通过 media_object_asset 关联 asset。
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
