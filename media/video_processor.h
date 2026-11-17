#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace sparkpush {

struct VideoLimits {
    int64_t max_duration_ms{10LL * 60 * 1000};
    int max_pixels{8294400};  // 3840 * 2160
    int max_side{3840};
};

// ffprobe 解析出的、经过服务端校验的视频元数据。
// width/height 是“显示尺寸”：手机竖拍视频带 rotation 时已经交换宽高。
struct VideoProbeInfo {
    int width{0};
    int height{0};
    int64_t duration_ms{0};
    std::string container;    // mp4 / webm
    std::string video_codec;  // h264 / hevc / vp8 / vp9
    std::string audio_codec;  // aac / opus / vorbis / 空
    // MP4 + HEVC 能通过解析，但浏览器通常不能直接播放，需要 worker 转码。
    bool transcode_required{false};
    std::string transcode_reason;
};

struct VideoTranscodeOptions {
    int max_side{1920};
    int crf{23};
    std::string preset{"veryfast"};
    int timeout_seconds{600};
};

// 构造 ffmpeg 转码 argv；纯函数，便于单元测试。
std::vector<std::string> BuildVideoTranscodeArgv(
    const std::string& input_path, const std::string& output_path,
    const VideoTranscodeOptions& options, const std::string& ffmpeg_path);

// 把 HEVC/H.265 MP4 转成 H.264/AAC MP4。失败时 err 带 ffmpeg stderr 尾部。
bool TranscodeVideoToMp4(const std::string& input_path,
                         const std::string& output_path,
                         const VideoTranscodeOptions& options,
                         const std::string& ffmpeg_path,
                         std::string* err);

// 纯函数：解析 ffprobe JSON，并校验声明 MIME、容器、视频/音频编码。
// 不启动进程，便于单元测试。
bool ParseVideoProbeJson(const std::string& json,
                         const std::string& declared_mime,
                         VideoProbeInfo* out, std::string* err);

// 纯函数：校验时长、边长和像素数。
bool ValidateVideoLimits(const VideoProbeInfo& info, const VideoLimits& limits,
                         std::string* err);

// 启动 ffprobe 探测文件；process 失败与格式不支持通过 permanent 区分：
//   permanent=false：进程启动失败/超时/退出码非 0，可重试
//   permanent=true ：JSON 结构、容器、编码不符合要求，重试也不会成功
bool ProbeVideoFile(const std::string& input_path,
                    const std::string& declared_mime,
                    const std::string& ffprobe_path, int timeout_seconds,
                    VideoProbeInfo* out, bool* permanent, std::string* err);

// 抽一帧生成 JPEG 封面。display_width/height 是旋转后的显示尺寸。
// 返回实际写入 media_variant 的封面宽高。
bool ExtractVideoPoster(const std::string& input_path,
                        const std::string& output_path, int64_t duration_ms,
                        int display_width, int display_height, int max_width,
                        const std::string& ffmpeg_path, int timeout_seconds,
                        int* poster_width, int* poster_height,
                        std::string* err);

// 按 PATH 查找可执行文件（也支持带 '/' 的绝对/相对路径）。
bool IsExecutableAvailable(const std::string& program);

}  // namespace sparkpush
