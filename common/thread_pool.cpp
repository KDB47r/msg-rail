#include "thread_pool.h"

#include "logging.h"

namespace sparkpush {

namespace {
// 线程数为 0 时回退到硬件并发数，并保证至少创建 1 个工作线程。
size_t ResolveThreadNum(size_t thread_num) {
    if (thread_num > 0) return thread_num;
    unsigned hw = std::thread::hardware_concurrency();
    return hw > 0 ? hw : 1;
}
}  // namespace

// 构造线程池，解析线程数并保存名字，工作线程延迟到 Start 时创建。
ThreadPool::ThreadPool(size_t thread_num, const std::string& name)
    : thread_num_(ResolveThreadNum(thread_num)), name_(name) {}

// 析构时调用 Stop，确保所有工作线程退出。
ThreadPool::~ThreadPool() { Stop(); }

// 启动固定数量的工作线程，重复调用不会重复创建。
void ThreadPool::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!workers_.empty()) {
        return;
    }
    stopping_ = false;
    workers_.reserve(thread_num_);
    for (size_t i = 0; i < thread_num_; ++i) {
        workers_.emplace_back([this, i]() { WorkerLoop(i); });
    }
    LogInfo("ThreadPool " + name_ + " started with " +
            std::to_string(thread_num_) + " threads");
}

// 停止接收新任务，等待队列任务执行完后 join 所有工作线程。
void ThreadPool::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
    std::queue<std::function<void()>> empty;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(tasks_, empty);
    }
}

// 提交任务到队列并唤醒一个工作线程；停止过程中会丢弃任务并告警。
void ThreadPool::Submit(std::function<void()> task) {
    if (!task) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            // 停止过程中入队会丢任务：保持公有签名不变（仍返回 void），
            // 但至少记录一条日志，避免任务被静默丢弃而难以排查。
            LogWarn("ThreadPool " + name_ +
                    " Submit dropped a task: pool is stopping");
            return;
        }
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

// 工作线程主循环：等待任务、取出执行，并捕获任务抛出的异常。
void ThreadPool::WorkerLoop(size_t worker_id) {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        try {
            task();
        } catch (const std::exception& ex) {
            LogError("ThreadPool " + name_ + " worker " +
                     std::to_string(worker_id) +
                     " caught exception: " + ex.what());
        } catch (...) {
            LogError("ThreadPool " + name_ + " worker " +
                     std::to_string(worker_id) + " caught unknown exception");
        }
    }
}

}  // namespace sparkpush
