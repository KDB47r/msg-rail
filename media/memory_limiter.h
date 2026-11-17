#pragma once

// 图片解码的内存预算信号量。
//
// media_worker 的线程数不能直接代表内存占用：一张 45MP JPEG 全量解码
// 峰值可达 300~400MiB，两个线程同时处理就可能接近 800MiB。这个类按
// “预计内存字节数”而不是线程数做并发控制。
//
// 用法：
//   MemoryLimiter limiter(1024 * 1024 * 1024);
//   {
//     auto guard = limiter.Acquire(estimated_bytes);
//     // 解码 / 缩放 / 编码
//   }  // guard 析构自动释放
//
// capacity_bytes <= 0 表示不限制，Acquire 返回空 Guard。

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <utility>

namespace sparkpush {

class MemoryLimiter {
   public:
    explicit MemoryLimiter(size_t capacity_bytes) : capacity_(capacity_bytes) {}

    class Guard {
       public:
        Guard() = default;
        Guard(MemoryLimiter* limiter, size_t bytes)
            : limiter_(limiter), bytes_(bytes) {}

        ~Guard() { Release(); }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

        Guard(Guard&& other) noexcept
            : limiter_(other.limiter_), bytes_(other.bytes_) {
            other.limiter_ = nullptr;
            other.bytes_ = 0;
        }

        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                Release();
                limiter_ = other.limiter_;
                bytes_ = other.bytes_;
                other.limiter_ = nullptr;
                other.bytes_ = 0;
            }
            return *this;
        }

       private:
        void Release() {
            if (limiter_ == nullptr) return;
            limiter_->Release(bytes_);
            limiter_ = nullptr;
            bytes_ = 0;
        }

        MemoryLimiter* limiter_{nullptr};
        size_t bytes_{0};
    };

    Guard Acquire(size_t bytes) {
        // capacity <= 0：不限制。
        if (capacity_ == 0) return Guard();

        std::unique_lock<std::mutex> lock(mutex_);
        // 正常情况下要求 in_use + bytes <= capacity。
        // 单任务比整个预算还大时，允许它等到 in_use == 0 后独占执行，
        // 否则会永远等不到足够空间。
        cv_.wait(lock, [&]() {
            if (in_use_ == 0) return true;
            return in_use_ <= capacity_ && bytes <= capacity_ - in_use_;
        });
        in_use_ += bytes;
        return Guard(this, bytes);
    }

    size_t capacity() const { return capacity_; }

    size_t in_use() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return in_use_;
    }

   private:
    friend class Guard;

    void Release(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (bytes > in_use_) {
                in_use_ = 0;
            } else {
                in_use_ -= bytes;
            }
        }
        cv_.notify_all();
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    size_t capacity_{0};
    size_t in_use_{0};
};

}  // namespace sparkpush
