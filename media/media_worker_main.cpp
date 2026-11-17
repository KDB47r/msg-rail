#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "config.h"
#include "image_processor.h"
#include "logging.h"
#include "media_dao.h"
#include "memory_limiter.h"
#include "mysql_pool.h"
#include "s3_client.h"
#include "signal_handler.h"
#include "video_processor.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/media_worker.conf。\n",
                 prog);
}

ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    *config_path = "conf/media_worker.conf";
    bool positional_used = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } else {
            if (positional_used) {
                std::fprintf(stderr, "重复的配置文件参数: %s\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = arg;
            positional_used = true;
        }
    }
    return ArgParseResult::kOk;
}

std::string VariantObjectKey(const std::string& media_id,
                             const std::string& variant) {
    return "media/" + media_id + "/" + variant + ".webp";
}

std::string VideoPosterObjectKey(const std::string& media_id) {
    return "media/" + media_id + "/poster.jpg";
}

std::string VideoPlaybackObjectKey(const std::string& media_id) {
    return "media/" + media_id + "/playback.mp4";
}

int64_t FileSizeOrNegative(const std::string& path) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    if (ec) return -1;
    return static_cast<int64_t>(size);
}

// 每个视频任务独占一个临时目录，析构时整体删除。
class TempDir {
   public:
    ~TempDir() {
        if (path_.empty()) return;
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    bool Create(const std::string& base, std::string* err) {
        std::string root = base.empty() ? "/tmp" : base;
        if (root.size() > 1 && root.back() == '/') root.pop_back();
        std::string tmpl = root + "/sparkpush-video-XXXXXX";
        std::vector<char> buffer(tmpl.begin(), tmpl.end());
        buffer.push_back('\0');
        char* made = ::mkdtemp(buffer.data());
        if (!made) {
            if (err) {
                *err = std::string("mkdtemp failed: ") + std::strerror(errno);
            }
            return false;
        }
        path_ = made;
        return true;
    }

    const std::string& path() const { return path_; }

   private:
    std::string path_;
};

bool FailTask(sparkpush::MediaDao* dao, const sparkpush::MediaTask& task,
              const std::string& error, bool force_terminal = false) {
    sparkpush::MediaTask effective = task;
    if (force_terminal) effective.attempts = effective.max_attempts;
    bool terminal = false;
    std::string db_err;
    if (!dao->FailTask(effective, error, &terminal, &db_err)) {
        sparkpush::LogError("FailTask failed for media " + task.media_id +
                            ": " + db_err);
        return false;
    }
    if (terminal) {
        sparkpush::LogError("media " + task.media_id + " failed permanently: " +
                            error);
    } else {
        sparkpush::LogWarn("media " + task.media_id + " will retry: " + error);
    }
    return true;
}

bool ProcessVideoTask(sparkpush::MediaDao* dao, sparkpush::S3Client* s3,  //  video worker 异步处理
                      const sparkpush::Config& cfg,
                      const sparkpush::MediaTask& task,
                      const sparkpush::MediaObject& media) {
    std::string err;
    if (!sparkpush::IsExecutableAvailable(cfg.media_video_ffprobe_path) ||
        !sparkpush::IsExecutableAvailable(cfg.media_video_ffmpeg_path)) {
        FailTask(dao, task,
                 "ffprobe/ffmpeg not found; install ffmpeg on this host", true);
        return false;
    }

    TempDir temp;
    if (!temp.Create(cfg.media_video_temp_dir, &err)) {
        FailTask(dao, task, "create video temp dir failed: " + err);
        return false;
    }
    const std::string input_path = temp.path() + "/original";

    sparkpush::S3GetFileResult object;
    if (!s3->GetObjectToFile(media.object_key, input_path,
                             cfg.media_max_video_bytes,
                             cfg.media_video_process_timeout_seconds, &object,
                             &err)) { // 落临时文件
        FailTask(dao, task, "download video failed: " + err);
        return false;
    }
    if (object.size_bytes <= 0) {
        FailTask(dao, task, "video object is empty", true);
        return false;
    }

    sparkpush::VideoProbeInfo info;
    bool permanent = false;
    if (!sparkpush::ProbeVideoFile(input_path, media.mime_type,
                                   cfg.media_video_ffprobe_path,
                                   cfg.media_video_process_timeout_seconds,
                                   &info, &permanent, &err)) {
        FailTask(dao, task, "probe video failed: " + err, permanent);
        return false;
    }

    sparkpush::VideoLimits limits;
    limits.max_duration_ms = cfg.media_max_video_duration_ms;
    limits.max_pixels = cfg.media_max_video_pixels;
    limits.max_side = cfg.media_max_video_side;
    if (!sparkpush::ValidateVideoLimits(info, limits, &err)) {
        FailTask(dao, task, "validate video failed: " + err, true);
        return false;
    }

    std::string playable_path = input_path;
    sparkpush::VideoProbeInfo display_info = info;
    std::string playback_key = media.object_key;
    int64_t playback_size = object.size_bytes;
    std::string playback_etag =
        object.etag.empty() ? media.etag : object.etag;

    if (info.transcode_required) {
        if (info.container != "mp4" || info.video_codec != "hevc") {
            FailTask(dao, task,
                     "unsupported transcode source: " + info.container + " " +
                         info.video_codec,
                     true);
            return false;
        }
        playable_path = temp.path() + "/playback.mp4";
        sparkpush::VideoTranscodeOptions options;
        options.max_side = cfg.media_video_transcode_max_side;
        options.crf = cfg.media_video_transcode_crf;
        options.preset = cfg.media_video_transcode_preset;
        options.timeout_seconds = cfg.media_video_transcode_timeout_seconds;
        if (!sparkpush::TranscodeVideoToMp4(
                input_path, playable_path, options,
                cfg.media_video_ffmpeg_path, &err)) {
            FailTask(dao, task, "transcode video failed: " + err);
            return false;
        }

        sparkpush::VideoProbeInfo transcoded;
        bool transcode_permanent = false;
        if (!sparkpush::ProbeVideoFile(
                playable_path, "video/mp4", cfg.media_video_ffprobe_path,
                cfg.media_video_process_timeout_seconds, &transcoded,
                &transcode_permanent, &err)) {
            FailTask(dao, task, "probe transcoded video failed: " + err,
                     transcode_permanent);
            return false;
        }
        if (transcoded.transcode_required) {
            FailTask(dao, task,
                     "transcoded video still requires transcoding", true);
            return false;
        }
        if (!sparkpush::ValidateVideoLimits(transcoded, limits, &err)) {
            FailTask(dao, task,
                     "validate transcoded video failed: " + err, true);
            return false;
        }
        const int64_t duration_tolerance =
            std::max<int64_t>(2000, info.duration_ms / 20);
        if (std::llabs(transcoded.duration_ms - info.duration_ms) >
            duration_tolerance) {
            FailTask(dao, task,
                     "transcoded video duration mismatch: " +
                         std::to_string(transcoded.duration_ms) + "ms vs " +
                         std::to_string(info.duration_ms) + "ms",
                     true);
            return false;
        }
        const int64_t transcoded_size = FileSizeOrNegative(playable_path);
        if (transcoded_size <= 0) {
            FailTask(dao, task, "transcoded video file is empty", true);
            return false;
        }
        if (transcoded_size > cfg.media_max_video_bytes) {
            FailTask(dao, task,
                     "transcoded video exceeds byte limit: " +
                         std::to_string(transcoded_size) + " > " +
                         std::to_string(cfg.media_max_video_bytes),
                     true);
            return false;
        }
        display_info = transcoded;
        playback_key = VideoPlaybackObjectKey(media.media_id);
        playback_size = transcoded_size;
    }

    const std::string poster_path = temp.path() + "/poster.jpg";
    int poster_width = 0, poster_height = 0;
    if (!sparkpush::ExtractVideoPoster(
            playable_path, poster_path, display_info.duration_ms,
            display_info.width, display_info.height,
            cfg.media_video_poster_max_width, cfg.media_video_ffmpeg_path,
            cfg.media_video_process_timeout_seconds, &poster_width,
            &poster_height, &err)) {
        FailTask(dao, task, "extract video poster failed: " + err);
        return false;
    }

    if (info.transcode_required) {
        sparkpush::S3PutResult playback_put;
        if (!s3->PutObjectFromFile(
                playback_key, playable_path, "video/mp4",
                cfg.media_max_video_bytes,
                cfg.media_video_transcode_timeout_seconds, &playback_put,
                &err)) {
            FailTask(dao, task, "upload transcoded video failed: " + err);
            return false;
        }
        playback_etag = playback_put.etag;
    }

    std::ifstream poster_stream(poster_path, std::ios::binary);
    if (!poster_stream) {
        FailTask(dao, task, "open video poster failed", true);
        return false;
    }
    std::string poster_bytes((std::istreambuf_iterator<char>(poster_stream)),
                             std::istreambuf_iterator<char>());
    if (poster_bytes.empty() || poster_bytes.size() > 8 * 1024 * 1024) {
        FailTask(dao, task, "video poster size is invalid", true);
        return false;
    }

    const std::string poster_key = VideoPosterObjectKey(media.media_id);
    sparkpush::S3PutResult put_result;
    if (!s3->PutObject(poster_key, poster_bytes, "image/jpeg", &put_result,
                       &err)) {
        FailTask(dao, task, "upload video poster failed: " + err);
        return false;
    }

    sparkpush::MediaVariant variant;
    variant.media_id = media.media_id;
    variant.variant = "poster";
    variant.format = "jpeg";
    variant.width = poster_width;
    variant.height = poster_height;
    variant.size_bytes = static_cast<int64_t>(poster_bytes.size());
    variant.object_key = poster_key;
    std::vector<sparkpush::MediaVariant> variants{std::move(variant)};

    if (!dao->CompleteVideo(media.media_id, display_info.width,
                            display_info.height, display_info.duration_ms,
                            playback_key, playback_size, playback_etag,
                            variants, &err)) {
        FailTask(dao, task, "complete video failed: " + err);
        return false;
    }
    sparkpush::LogInfo(
        "video processed: " + media.media_id + " " +
        std::to_string(display_info.width) + "x" +
        std::to_string(display_info.height) + " duration_ms=" +
        std::to_string(display_info.duration_ms) +
        " transcoded=" + (info.transcode_required ? "yes" : "no"));
    return true;
}

bool ProcessOneTask(sparkpush::MediaDao* dao, sparkpush::S3Client* s3,
                    const sparkpush::Config& cfg,
                    const sparkpush::MediaTask& task,
                    sparkpush::MemoryLimiter* image_limiter) {
    sparkpush::MediaObject media;
    std::string err;
    if (!dao->GetById(task.media_id, &media, &err)) {
        FailTask(dao, task, "load media failed: " + err, true);
        return false;
    }
    if (task.task_type != "process_image" && task.task_type != "process_video") {
        FailTask(dao, task, "unsupported task type: " + task.task_type, true);
        return false;
    }
    if (media.status == "deleted") {
        FailTask(dao, task, "media is deleted", true);
        return false;
    }
    if (media.status == "failed") {
        FailTask(dao, task, "media is already failed", true);
        return false;
    }
    if (media.status == "ready") {
        // 任务重复入队时，直接把已有变体再写一遍并把任务标记 done。
        std::vector<sparkpush::MediaVariant> variants;
        if (!dao->ListVariants(media.media_id, &variants, &err)) {
            FailTask(dao, task, "list existing variants failed: " + err);
            return false;
        }
        const bool video = (task.task_type == "process_video");
        const bool completed =
            video ? dao->CompleteVideo(media.media_id, media.width,
                                       media.height, media.duration_ms,
                                       media.object_key, media.size_bytes,
                                       media.etag, variants, &err)
                  : dao->CompleteImage(media.media_id, media.width,
                                       media.height, media.dominant_color,
                                       variants, &err);
        if (!completed) {
            FailTask(dao, task, "mark ready media done failed: " + err);
            return false;
        }
        return true;
    }

    if (!dao->MarkProcessing(media.media_id, &err)) {
        FailTask(dao, task, "mark processing failed: " + err);
        return false;
    }

    if (task.task_type == "process_video") {
        return ProcessVideoTask(dao, s3, cfg, task, media); //分叉点
    }

    sparkpush::S3GetResult object;
    if (!s3->GetObject(media.object_key, &object, &err)) { //读原图 
        FailTask(dao, task, "download original failed: " + err);
        return false;
    }
    if (object.body.empty()) { 
        FailTask(dao, task, "original object is empty");
        return false;
    }
    if (static_cast<int64_t>(object.body.size()) >
        cfg.media_max_image_bytes) {
        FailTask(dao, task, "original object exceeds byte limit", true);
        return false;
    }

    sparkpush::ImageLimits limits;
    limits.max_pixels = cfg.media_max_pixels;
    limits.max_side = cfg.media_max_side;
    sparkpush::ProcessedImage processed;
    if (!sparkpush::ProcessImage(object.body, media.mime_type, limits,
                                 &processed, &err, image_limiter)) {
        FailTask(dao, task, "process image failed: " + err);
        return false;
    }

    std::vector<sparkpush::MediaVariant> variants;
    variants.reserve(processed.variants.size());
    for (const auto& processed_variant : processed.variants) {
        const std::string key =
            VariantObjectKey(media.media_id, processed_variant.variant);
        sparkpush::S3PutResult put_result;
        if (!s3->PutObject(key, processed_variant.bytes, "image/webp",
                           &put_result, &err)) { // 上传三档webp
            FailTask(dao, task,
                     "upload " + processed_variant.variant + " failed: " + err);
            return false;
        }
        sparkpush::MediaVariant variant;
        variant.media_id = media.media_id;
        variant.variant = processed_variant.variant;
        variant.format = processed_variant.format;
        variant.width = processed_variant.width;
        variant.height = processed_variant.height;
        variant.size_bytes =
            static_cast<int64_t>(processed_variant.bytes.size());
        variant.object_key = key;
        variants.push_back(std::move(variant));
    }

    if (!dao->CompleteImage(media.media_id, processed.width, processed.height, // 落库并标记ready
                            processed.dominant_color, variants, &err)) {
        FailTask(dao, task, "complete image failed: " + err);
        return false;
    }
    sparkpush::LogInfo("media processed: " + media.media_id + " " +
                       std::to_string(processed.width) + "x" +
                       std::to_string(processed.height) + " variants=" +
                       std::to_string(variants.size()));
    return true;
}

}  // namespace

namespace sparkpush {

int RunMediaWorker(const Config& cfg) {
    MySqlConnectionPool mysql_pool;
    MySqlConfig mc;
    mc.host = cfg.mysql_host;
    mc.port = cfg.mysql_port;
    mc.user = cfg.mysql_user;
    mc.password = cfg.mysql_password;
    mc.db = cfg.mysql_db;
    mc.pool_size = cfg.mysql_pool_size;
    mc.min_pool_size =
        (cfg.mysql_pool_min_size > 0) ? cfg.mysql_pool_min_size : mc.pool_size;
    mc.max_pool_size = (cfg.mysql_pool_max_size > 0)
                           ? cfg.mysql_pool_max_size
                           : std::max(mc.min_pool_size, mc.pool_size);
    mc.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
    if (!mysql_pool.Init(mc)) {
        LogError("Failed to init MySQL pool for media_worker");
        return 1;
    }

    S3Config s3_cfg;
    s3_cfg.endpoint = cfg.s3_endpoint;
    s3_cfg.public_endpoint = cfg.s3_public_endpoint;
    s3_cfg.bucket = cfg.s3_bucket;
    s3_cfg.access_key = cfg.s3_access_key;
    s3_cfg.secret_key = cfg.s3_secret_key;
    s3_cfg.region = cfg.s3_region;
    s3_cfg.presign_ttl_seconds = cfg.s3_presign_ttl_seconds;
    s3_cfg.path_style = cfg.s3_path_style;
    S3Client s3;
    std::string err;
    if (!s3.Init(s3_cfg, &err)) {
        LogError("Failed to init S3 client: " + err);
        mysql_pool.Stop();
        return 1;
    }

    MediaDao media_dao(&mysql_pool);
    // 转码可能持续数分钟，恢复阈值必须大于转码超时，避免重复领取。
    const int stale_seconds =
        std::max(300, cfg.media_video_transcode_timeout_seconds + 120);
    if (!media_dao.RecoverStaleRunningTasks(stale_seconds, &err)) {
        LogWarn("RecoverStaleRunningTasks failed: " + err);
    }

    if (!IsExecutableAvailable(cfg.media_video_ffprobe_path) ||
        !IsExecutableAvailable(cfg.media_video_ffmpeg_path)) {
        LogWarn("ffprobe/ffmpeg not found; video tasks will fail. "
                "Install with: sudo apt install -y ffmpeg");
    }

    InstallSignalHandler([]() {});
    const int image_threads = std::max(1, cfg.media_image_worker_threads);
    const int video_threads = std::max(1, cfg.media_video_worker_threads);
    LogInfo("media_worker started, image_threads=" +
            std::to_string(image_threads) + ", video_threads=" +
            std::to_string(video_threads) + ", poll_ms=" +
            std::to_string(cfg.media_worker_poll_ms));

    std::unique_ptr<MemoryLimiter> image_limiter;
    if (cfg.media_memory_budget_mb > 0) {
        const size_t budget =
            static_cast<size_t>(cfg.media_memory_budget_mb) * 1024ULL * 1024ULL;
        image_limiter = std::make_unique<MemoryLimiter>(budget);
        LogInfo("image memory budget: " +
                std::to_string(cfg.media_memory_budget_mb) + " MiB");
    } else {
        LogInfo("image memory budget: disabled");
    }
    MemoryLimiter* image_limiter_ptr = image_limiter.get();

    auto start_lane = [&](const std::string& lane,
                          const std::string& task_type, int thread_count,
                          bool use_image_limiter,
                          std::vector<std::thread>* workers) {
        workers->reserve(static_cast<size_t>(thread_count));
        for (int i = 0; i < thread_count; ++i) {
            workers->emplace_back(
                [&, lane, task_type, i, use_image_limiter,
                 image_limiter_ptr]() {
                    const std::string worker_id =
                        "media-worker-" + std::to_string(::getpid()) + "-" +
                        lane + "-" + std::to_string(i);
                    while (!ShutdownRequested().load(
                        std::memory_order_relaxed)) {
                        MediaTask task;
                        bool found = false;
                        std::string claim_err;
                        if (!media_dao.ClaimNextTask(
                                worker_id, task_type, &task, &found,
                                &claim_err)) {
                            LogError("ClaimNextTask failed: " + claim_err);
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(
                                    std::max(100, cfg.media_worker_poll_ms)));
                            continue;
                        }
                        if (!found) {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(
                                    std::max(50, cfg.media_worker_poll_ms)));
                            continue;
                        }
                        ProcessOneTask(
                            &media_dao, &s3, cfg, task,
                            use_image_limiter ? image_limiter_ptr : nullptr);
                    }
                    LogInfo("media_worker thread stopped: " + worker_id);
                });
        }
    };

    std::vector<std::thread> image_workers;
    std::vector<std::thread> video_workers;
    start_lane("img", "process_image", image_threads, true, &image_workers);
    start_lane("vid", "process_video", video_threads, false, &video_workers);
    LogInfo("image lane started, threads=" + std::to_string(image_threads));
    LogInfo("video lane started, threads=" + std::to_string(video_threads));

    // 定期回收进程崩溃后遗留的 running 任务。
    std::thread maintenance([&media_dao, stale_seconds]() {
        auto last = std::chrono::steady_clock::now();
        while (!ShutdownRequested().load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            const auto now = std::chrono::steady_clock::now();
            if (now - last < std::chrono::seconds(30)) continue;
            last = now;
            std::string recover_err;
            if (!media_dao.RecoverStaleRunningTasks(stale_seconds,
                                                     &recover_err)) {
                LogWarn("RecoverStaleRunningTasks failed: " + recover_err);
            }
        }
    });

    for (auto& worker : image_workers) {
        if (worker.joinable()) worker.join();
    }
    for (auto& worker : video_workers) {
        if (worker.joinable()) worker.join();
    }
    if (maintenance.joinable()) maintenance.join();
    mysql_pool.Stop();
    LogInfo("media_worker shutdown complete");
    return 0;
}

}  // namespace sparkpush

int main(int argc, char** argv) {
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    if (result == ArgParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::InitLogging("media_worker", /*also_stdout=*/false,
                           sparkpush::kDefaultLogRotateBytes, cfg.log_level);
    int ret = sparkpush::RunMediaWorker(cfg);
    sparkpush::ShutdownLogging();
    return ret;
}

/*
  图片分支在：

  media/media_worker_main.cpp:391-441

  GetObject 读原图
  → ProcessImage 生成 micro/thumb/preview
  → PutObject 上传三档 WebP
  → CompleteImage 落库并标记 ready

  视频分支在：

  media/media_worker_main.cpp:138-335

  GetObjectToFile 落临时文件
  → ffprobe 探测
  → 必要时 ffmpeg 转码
  → 抽封面
  → 上传 playback/poster
  → CompleteVideo 落库并标记 ready
*/


/*
三、建议的阅读顺序

  按这个顺序看最顺：

  1. logic/http_handlers_media.cpp:229-346
     看 complete 如何做 HEAD、MarkUploaded、分叉入队。
  2. media/media_dao.cpp:361-390
     看 INSERT INTO media_task 的字段和幂等写法。
  3. sql/migrations/001_media_tables.sql:59-75
     看 media_task 表结构和唯一键。
  4. media/media_worker_main.cpp:455-563
     看 worker 进程初始化、线程创建、维护线程。
  5. media/media_dao.cpp:547-642
     看 FOR UPDATE SKIP LOCKED 如何抢任务。
  6. media/media_worker_main.cpp:337-446
     看 ProcessOneTask 如何分派图片/视频。
  7. 最后分别看：
     - 图片：media/media_worker_main.cpp:391-441
     - 视频：media/media_worker_main.cpp:138-335

  ---

  四、几个容易看错的点

  1. Logic 不执行媒体处理
     Logic 只往 media_task 插一行，然后返回 uploaded。真正的处理在独立的 media_worker 进程。
  2. worker 不依赖 Kafka/Redis
     worker 只用 MySQL 和 MinIO；视频额外依赖本机 ffmpeg/ffprobe 和临时目录。
  3. attempts 是领取时加的
     ClaimNextTask 里 attempts=attempts+1。所以第一次领取后就是 1，不是 0。
  4. 真正生效的 max_attempts 来自 Logic 入队时写入的值
     media_task.max_attempts 是 Logic 在 EnqueueTask 时写入的。worker 配置里虽然也有同名项，但当前 worker 判断重试使用的是任务行里的 max_attempts。
  5. 重复 complete 不会重置失败任务
     ON DUPLICATE KEY UPDATE media_id=VALUES(media_id) 基本是空更新；如果任务已经 failed，再次 complete 不会自动重试。
*/