-- 图片 / 视频 worker 队列拆分：按 task_type 过滤领取任务。
-- 在已有数据库上执行一次：
--   mysql -uroot -p123456 spark_push < sql/migrations/003_media_task_queue_index.sql

ALTER TABLE `media_task`
  ADD KEY `idx_type_status_next` (`task_type`, `status`, `next_run_at`, `id`);
