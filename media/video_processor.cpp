#include "video_processor.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <nlohmann/json.hpp>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace sparkpush {

namespace {

using nlohmann::json;

std::string ToLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return s;
}

bool JsonNumber(const json& obj, const char* key, double* out) {
    if (!obj.is_object() || !obj.contains(key)) return false;
    const json& value = obj.at(key);
    try {
        if (value.is_number()) {
            *out = value.get<double>();
            return std::isfinite(*out);
        }
        if (value.is_string()) {
            const std::string text = value.get<std::string>();
            if (text.empty() || text == "N/A") return false;
            size_t idx = 0;
            const double parsed = std::stod(text, &idx);
            if (idx == 0 || !std::isfinite(parsed)) return false;
            *out = parsed;
            return true;
        }
    } catch (...) {
    }
    return false;
}

bool JsonInt(const json& obj, const char* key, int* out) {
    if (!obj.is_object() || !obj.contains(key)) return false;
    const json& value = obj.at(key);
    try {
        if (value.is_number_integer()) {
            *out = value.get<int>();
            return true;
        }
        if (value.is_string()) {
            *out = std::stoi(value.get<std::string>());
            return true;
        }
    } catch (...) {
    }
    return false;
}

const json* FindStream(const json& root, const char* codec_type,
                       bool skip_attached_pic) {
    if (!root.contains("streams") || !root["streams"].is_array()) return nullptr;
    for (const auto& stream : root["streams"]) {
        if (!stream.is_object() || !stream.contains("codec_type") ||
            !stream["codec_type"].is_string()) {
            continue;
        }
        if (stream["codec_type"].get<std::string>() != codec_type) continue;
        if (skip_attached_pic) {
            int attached = 0;
            if (stream.contains("disposition") &&
                JsonInt(stream["disposition"], "attached_pic", &attached) &&
                attached != 0) {
                continue;
            }
        }
        return &stream;
    }
    return nullptr;
}

// ffprobe 的 rotation 可能在 side_data_list，也可能在 tags.rotate。
int GetRotation(const json& stream) {
    int rotation = 0;
    if (stream.contains("side_data_list") &&
        stream["side_data_list"].is_array()) {
        for (const auto& side_data : stream["side_data_list"]) {
            int value = 0;
            if (side_data.is_object() &&
                JsonInt(side_data, "rotation", &value)) {
                rotation = value;
                break;
            }
        }
    }
    if (rotation == 0 && stream.contains("tags") &&
        stream["tags"].is_object()) {
        int value = 0;
        if (JsonInt(stream["tags"], "rotate", &value)) rotation = value;
    }
    rotation %= 360;
    if (rotation > 180) rotation -= 360;
    if (rotation < -180) rotation += 360;
    return rotation;
}

// 编码策略：H.264/AAC 和 WebM 直接播放；HEVC MP4 标记为需要转码；
// 其他组合仍然拒绝，避免“上传成功但浏览器播不了”。
bool ApplyCodecPolicy(VideoProbeInfo* info, std::string* err) {
    if (info->container == "mp4") {
        if (info->video_codec == "h264") {
            if (!info->audio_codec.empty() && info->audio_codec != "aac") {
                if (err) {
                    *err = "unsupported mp4 audio codec: " + info->audio_codec;
                }
                return false;
            }
            return true;
        }
        if (info->video_codec == "hevc") {
            info->transcode_required = true;
            info->transcode_reason = "mp4 hevc";
            return true;
        }
        if (err) *err = "unsupported mp4 video codec: " + info->video_codec;
        return false;
    }
    if (info->container == "webm") {
        if (info->video_codec != "vp8" && info->video_codec != "vp9") {
            if (err) *err = "unsupported webm video codec: " + info->video_codec;
            return false;
        }
        if (!info->audio_codec.empty() && info->audio_codec != "opus" &&
            info->audio_codec != "vorbis") {
            if (err) *err = "unsupported webm audio codec: " + info->audio_codec;
            return false;
        }
        return true;
    }
    if (err) *err = "unsupported video container: " + info->container;
    return false;
}

struct ProcessResult {
    int exit_code{-1};
    bool timed_out{false};
    std::string stderr_tail;
};

std::string ReadFileTail(const std::string& path, size_t max_bytes) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return {};
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    if (size <= 0) return {};
    const size_t take =
        std::min<size_t>(static_cast<size_t>(size), max_bytes);
    in.seekg(size - static_cast<std::streamoff>(take));
    std::string out(take, '\0');
    in.read(&out[0], static_cast<std::streamsize>(take));
    return out;
}

bool ReadFileToString(const std::string& path, size_t max_bytes,
                      std::string* out, std::string* err) {
    if (!out) return false;
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (err) *err = "open file failed: " + path;
        return false;
    }
    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    if (size < 0) {
        if (err) *err = "get file size failed: " + path;
        return false;
    }
    if (static_cast<size_t>(size) > max_bytes) {
        if (err) *err = "file is too large: " + path;
        return false;
    }
    out->resize(static_cast<size_t>(size));
    in.seekg(0);
    if (size > 0) {
        in.read(&(*out)[0], static_cast<std::streamsize>(size));
        if (!in) {
            if (err) *err = "read file failed: " + path;
            return false;
        }
    }
    return true;
}

// fork + execvp，不经过 shell；超时后杀整个进程组。
bool RunProcess(const std::vector<std::string>& argv, int timeout_seconds,
                const std::string& stdout_path,
                const std::string& stderr_path, ProcessResult* result,
                std::string* err) {
    if (!result || argv.empty()) {
        if (err) *err = "invalid process arguments";
        return false;
    }
    *result = ProcessResult{};
    std::vector<char*> args;
    args.reserve(argv.size() + 1);
    for (const auto& item : argv) {
        args.push_back(const_cast<char*>(item.c_str()));
    }
    args.push_back(nullptr);

    const pid_t pid = ::fork();
    if (pid < 0) {
        if (err) *err = std::string("fork failed: ") + std::strerror(errno);
        return false;
    }
    if (pid == 0) {
        ::setpgid(0, 0);
        const int in_fd = ::open("/dev/null", O_RDONLY);
        const int out_fd = ::open(stdout_path.c_str(),
                                  O_WRONLY | O_CREAT | O_TRUNC, 0600);
        const int err_fd = ::open(stderr_path.c_str(),
                                  O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (in_fd < 0 || out_fd < 0 || err_fd < 0) _exit(126);
        ::dup2(in_fd, STDIN_FILENO);
        ::dup2(out_fd, STDOUT_FILENO);
        ::dup2(err_fd, STDERR_FILENO);
        ::close(in_fd);
        ::close(out_fd);
        ::close(err_fd);
        ::execvp(args[0], args.data());
        _exit(127);
    }

    // 子进程自己也会 setpgid；这里再设置一次以缩小 kill 的竞态窗口。
    ::setpgid(pid, pid);

    int wait_status = 0;
    bool reaped = false;
    const int timeout = std::max(1, timeout_seconds);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout);
    while (!reaped) {
        const pid_t waited = ::waitpid(pid, &wait_status, WNOHANG);
        if (waited == pid) {
            reaped = true;
            break;
        }
        if (waited < 0) {
            if (errno == EINTR) continue;
            ::kill(-pid, SIGKILL);
            ::kill(pid, SIGKILL);
            ::waitpid(pid, &wait_status, 0);
            if (err) {
                *err = std::string("waitpid failed: ") + std::strerror(errno);
            }
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            result->timed_out = true;
            ::kill(-pid, SIGKILL);
            ::kill(pid, SIGKILL);
            ::waitpid(pid, &wait_status, 0);
            reaped = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    result->stderr_tail = ReadFileTail(stderr_path, 4096);
    if (result->timed_out) {
        if (err) {
            *err = "process timed out after " + std::to_string(timeout) + "s";
            if (!result->stderr_tail.empty()) {
                *err += ": " + result->stderr_tail;
            }
        }
        return false;
    }
    if (WIFEXITED(wait_status)) {
        result->exit_code = WEXITSTATUS(wait_status);
    } else if (WIFSIGNALED(wait_status)) {
        result->exit_code = 128 + WTERMSIG(wait_status);
    }
    return true;
}

struct FileGuard {
    std::string path;
    ~FileGuard() {
        if (!path.empty()) std::remove(path.c_str());
    }
};

}  // namespace

bool ParseVideoProbeJson(const std::string& json_text,
                         const std::string& declared_mime, VideoProbeInfo* out,
                         std::string* err) {
    if (!out) {
        if (err) *err = "invalid output argument";
        return false;
    }
    if (declared_mime != "video/mp4" && declared_mime != "video/webm") {
        if (err) *err = "unsupported declared video mime_type: " + declared_mime;
        return false;
    }

    json root;
    try {
        root = json::parse(json_text);
    } catch (const std::exception& e) {
        if (err) *err = std::string("parse ffprobe json failed: ") + e.what();
        return false;
    }
    if (!root.is_object() || !root.contains("format") ||
        !root["format"].is_object()) {
        if (err) *err = "ffprobe json has no format object";
        return false;
    }
    const json& format = root["format"];
    if (!format.contains("format_name") ||
        !format["format_name"].is_string()) {
        if (err) *err = "ffprobe json has no format_name";
        return false;
    }

    const std::string format_name =
        ToLower(format["format_name"].get<std::string>());
    VideoProbeInfo info;
    if (format_name.find("webm") != std::string::npos) {
        info.container = "webm";
    } else if (format_name.find("mp4") != std::string::npos ||
               format_name.find("mov") != std::string::npos) {
        info.container = "mp4";
    } else {
        if (err) *err = "unsupported video container: " + format_name;
        return false;
    }
    const std::string expected_container =
        declared_mime == "video/webm" ? "webm" : "mp4";
    if (info.container != expected_container) {
        if (err) {
            *err = "video container mismatch: declared " + declared_mime +
                   ", actual " + info.container;
        }
        return false;
    }

    const json* video = FindStream(root, "video", true);
    if (!video) {
        if (err) *err = "video stream not found";
        return false;
    }
    if (!video->contains("codec_name") || !(*video)["codec_name"].is_string()) {
        if (err) *err = "video stream has no codec_name";
        return false;
    }
    info.video_codec =
        ToLower((*video)["codec_name"].get<std::string>());
    if (!JsonInt(*video, "width", &info.width) ||
        !JsonInt(*video, "height", &info.height) || info.width <= 0 ||
        info.height <= 0) {
        if (err) *err = "invalid video dimensions";
        return false;
    }
    const int rotation = GetRotation(*video);
    if (rotation == 90 || rotation == -90) {
        std::swap(info.width, info.height);
    }

    const json* audio = FindStream(root, "audio", false);
    if (audio && audio->contains("codec_name") &&
        (*audio)["codec_name"].is_string()) {
        info.audio_codec =
            ToLower((*audio)["codec_name"].get<std::string>());
    }

    double duration_seconds = 0.0;
    bool has_duration = JsonNumber(format, "duration", &duration_seconds) &&
                        duration_seconds > 0.0;
    if (!has_duration) {
        has_duration = JsonNumber(*video, "duration", &duration_seconds) &&
                       duration_seconds > 0.0;
    }
    if (!has_duration) {
        if (err) *err = "video duration is missing";
        return false;
    }
    info.duration_ms = static_cast<int64_t>(
        std::llround(duration_seconds * 1000.0));

    if (!ApplyCodecPolicy(&info, err)) return false;
    *out = std::move(info);
    return true;
}

bool ValidateVideoLimits(const VideoProbeInfo& info, const VideoLimits& limits,
                         std::string* err) {
    if (info.duration_ms <= 0) {
        if (err) *err = "video duration is invalid";
        return false;
    }
    if (info.duration_ms > limits.max_duration_ms) {
        if (err) {
            *err = "video duration exceeds limit: " +
                   std::to_string(info.duration_ms) + "ms > " +
                   std::to_string(limits.max_duration_ms) + "ms";
        }
        return false;
    }
    if (info.width <= 0 || info.height <= 0) {
        if (err) *err = "video dimensions are invalid";
        return false;
    }
    if (std::max(info.width, info.height) > limits.max_side) {
        if (err) {
            *err = "video resolution exceeds limit: " +
                   std::to_string(info.width) + "x" +
                   std::to_string(info.height) + ", max side " +
                   std::to_string(limits.max_side);
        }
        return false;
    }
    const int64_t pixels =
        static_cast<int64_t>(info.width) * static_cast<int64_t>(info.height);
    if (pixels > limits.max_pixels) {
        if (err) {
            *err = "video pixel count exceeds limit: " +
                   std::to_string(pixels) + " > " +
                   std::to_string(limits.max_pixels);
        }
        return false;
    }
    return true;
}

bool ProbeVideoFile(const std::string& input_path,
                    const std::string& declared_mime,
                    const std::string& ffprobe_path, int timeout_seconds,
                    VideoProbeInfo* out, bool* permanent, std::string* err) {
    if (permanent) *permanent = false;
    if (!out) {
        if (err) *err = "invalid output argument";
        return false;
    }
    const std::string json_path = input_path + ".ffprobe.json";
    const std::string stderr_path = input_path + ".ffprobe.err";
    FileGuard json_guard{json_path};
    FileGuard stderr_guard{stderr_path};

    std::vector<std::string> argv{
        ffprobe_path, "-v",   "error",       "-show_format",
        "-show_streams",      "-of", "json", input_path};
    ProcessResult proc;
    std::string run_err;
    if (!RunProcess(argv, timeout_seconds, json_path, stderr_path, &proc,
                    &run_err)) {
        if (err) *err = "run ffprobe failed: " + run_err;
        return false;
    }
    if (proc.exit_code == 127) {
        if (permanent) *permanent = true;
        if (err) {
            *err = "ffprobe not found or not executable: " + ffprobe_path;
        }
        return false;
    }
    if (proc.exit_code != 0) {
        std::ostringstream message;
        message << "ffprobe exited with code " << proc.exit_code;
        if (!proc.stderr_tail.empty()) message << ": " << proc.stderr_tail;
        if (err) *err = message.str();
        return false;
    }

    std::string json_text;
    if (!ReadFileToString(json_path, 4 * 1024 * 1024, &json_text, err)) {
        return false;
    }
    bool parse_permanent = false;
    if (!ParseVideoProbeJson(json_text, declared_mime, out, err)) {
        // 容器/编码/时长格式不符合要求，重试也不会变好。
        parse_permanent = true;
    }
    if (parse_permanent && permanent) *permanent = true;
    return !parse_permanent;
}

bool ExtractVideoPoster(const std::string& input_path,
                        const std::string& output_path, int64_t duration_ms,
                        int display_width, int display_height, int max_width,
                        const std::string& ffmpeg_path, int timeout_seconds,
                        int* poster_width, int* poster_height,
                        std::string* err) {
    if (display_width <= 0 || display_height <= 0 || max_width <= 0) {
        if (err) *err = "invalid poster dimensions";
        return false;
    }
    int width = std::min(display_width, max_width);
    if (width <= 0) width = max_width;
    int height = static_cast<int>(std::llround(
        static_cast<double>(display_height) * width / display_width));
    if (height < 1) height = 1;
    if (height % 2 != 0) ++height;

    const std::string stderr_path = output_path + ".ffmpeg.err";
    const std::string stdout_path = output_path + ".ffmpeg.out";
    FileGuard stderr_guard{stderr_path};
    FileGuard stdout_guard{stdout_path};

    auto run_once = [&](const std::string& seek_seconds, ProcessResult* proc,
                        std::string* run_err) {
        std::vector<std::string> argv{
            ffmpeg_path, "-v",      "error", "-y",    "-ss", seek_seconds,
            "-i",        input_path, "-frames:v", "1", "-vf",
            "scale=" + std::to_string(width) + ":-2", "-q:v", "3",
            output_path};
        return RunProcess(argv, timeout_seconds, stdout_path, stderr_path,
                          proc, run_err);
    };

    int64_t seek_ms = std::min<int64_t>(1000, duration_ms / 3);
    if (seek_ms < 0) seek_ms = 0;
    auto seek_string = [](int64_t ms) {
        std::ostringstream ss;
        ss.setf(std::ios::fixed);
        ss.precision(3);
        ss << (static_cast<double>(ms) / 1000.0);
        return ss.str();
    };

    ProcessResult proc;
    std::string run_err;
    if (!run_once(seek_string(seek_ms), &proc, &run_err)) {
        if (err) *err = "run ffmpeg failed: " + run_err;
        return false;
    }
    if (proc.exit_code != 0) {
        // 某些容器在非关键帧 seek 时会失败，回退到第一帧。
        ProcessResult retry;
        std::string retry_err;
        if (!run_once("0", &retry, &retry_err)) {
            if (err) *err = "run ffmpeg failed: " + retry_err;
            return false;
        }
        if (retry.exit_code != 0) {
            std::ostringstream message;
            message << "ffmpeg exited with code " << retry.exit_code;
            if (!retry.stderr_tail.empty()) message << ": " << retry.stderr_tail;
            if (err) *err = message.str();
            return false;
        }
    }

    std::ifstream check(output_path, std::ios::binary | std::ios::ate);
    if (!check || check.tellg() <= 0) {
        if (err) *err = "ffmpeg produced an empty poster";
        return false;
    }
    check.seekg(0);
    unsigned char magic[3] = {0, 0, 0};
    check.read(reinterpret_cast<char*>(magic), 3);
    if (check.gcount() != 3 || magic[0] != 0xFF || magic[1] != 0xD8 ||
        magic[2] != 0xFF) {
        if (err) *err = "poster is not a valid jpeg";
        return false;
    }
    if (poster_width) *poster_width = width;
    if (poster_height) *poster_height = height;
    return true;
}

std::vector<std::string> BuildVideoTranscodeArgv(
    const std::string& input_path, const std::string& output_path,
    const VideoTranscodeOptions& options, const std::string& ffmpeg_path) {
    const std::string max_side = std::to_string(options.max_side);
    // ffmpeg 的 filtergraph 用逗号分隔滤镜，min() 里的逗号必须转义。
    const std::string scale_filter =
        "scale=w=min(" + max_side + "\\,iw):h=min(" + max_side +
        "\\,ih):force_original_aspect_ratio=decrease:force_divisible_by=2";
    return {
        ffmpeg_path,
        "-v", "error",
        "-y",
        "-i", input_path,
        "-map", "0:v:0",
        "-map", "0:a:0?",
        "-vf", scale_filter,
        "-c:v", "libx264",
        "-preset", options.preset,
        "-crf", std::to_string(options.crf),
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        output_path,
    };
}

bool TranscodeVideoToMp4(const std::string& input_path,
                         const std::string& output_path,
                         const VideoTranscodeOptions& options,
                         const std::string& ffmpeg_path,
                         std::string* err) {
    if (input_path.empty() || output_path.empty() ||
        input_path == output_path) {
        if (err) *err = "invalid transcode input/output path";
        return false;
    }
    if (ffmpeg_path.empty()) {
        if (err) *err = "ffmpeg path is empty";
        return false;
    }
    if (options.max_side <= 0 || options.crf < 0 || options.crf > 51 ||
        options.preset.empty() || options.timeout_seconds <= 0) {
        if (err) *err = "invalid transcode options";
        return false;
    }

    const std::string stdout_path = output_path + ".ffmpeg.out";
    const std::string stderr_path = output_path + ".ffmpeg.err";
    FileGuard stdout_guard{stdout_path};
    FileGuard stderr_guard{stderr_path};
    std::remove(output_path.c_str());

    const std::vector<std::string> argv = BuildVideoTranscodeArgv(
        input_path, output_path, options, ffmpeg_path);
    ProcessResult proc;
    std::string run_err;
    if (!RunProcess(argv, options.timeout_seconds, stdout_path, stderr_path,
                    &proc, &run_err)) {
        if (err) *err = "run ffmpeg transcode failed: " + run_err;
        return false;
    }
    if (proc.exit_code == 127) {
        if (err) *err = "ffmpeg not found or not executable: " + ffmpeg_path;
        return false;
    }
    if (proc.exit_code != 0) {
        std::ostringstream message;
        message << "ffmpeg transcode exited with code " << proc.exit_code;
        if (!proc.stderr_tail.empty()) message << ": " << proc.stderr_tail;
        if (err) *err = message.str();
        return false;
    }

    std::ifstream check(output_path, std::ios::binary | std::ios::ate);
    if (!check || check.tellg() <= 0) {
        if (err) *err = "ffmpeg produced an empty playback file";
        return false;
    }
    return true;
}

bool IsExecutableAvailable(const std::string& program) {
    if (program.empty()) return false;
    if (program.find('/') != std::string::npos) {
        return ::access(program.c_str(), X_OK) == 0;
    }
    const char* path_env = std::getenv("PATH");
    if (!path_env) return false;
    const std::string path(path_env);
    size_t start = 0;
    while (start <= path.size()) {
        const size_t end = path.find(':', start);
        std::string dir =
            path.substr(start, end == std::string::npos ? end : end - start);
        if (dir.empty()) dir = ".";
        const std::string full = dir + "/" + program;
        if (::access(full.c_str(), X_OK) == 0) return true;
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return false;
}

}  // namespace sparkpush
