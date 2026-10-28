// ThreadPool 单元测试(纯本地、无外部服务依赖):
//   - 提交大量任务,验证全部被执行(atomic 计数汇总)
//   - thread_num=0 时仍至少有 1 个
//   worker,任务不会永久挂起(hardware_concurrency()==0 保护回归)
//   - 通过 std::promise/future 从任务中回传返回值
//
// 依据 common/thread_pool.h 的真实接口编写:
//   - ThreadPool(size_t thread_num=0, const std::string& name="")
//   - void Start(); void Stop(); void Submit(std::function<void()>); size_t
//   size();
//   - Submit() 返回 void(不带 future),Stop() 会先把队列中剩余任务执行完再 join,
//     因此可作为"全部任务完成"的同步点。
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <vector>

#include "doctest.h"
#include "thread_pool.h"

using namespace sparkpush;

TEST_CASE("ThreadPool 提交大量任务全部执行完毕") {
    constexpr int kTaskCount = 1000;
    std::atomic<int> counter{0};
    {
        ThreadPool pool(4, "bulk");
        pool.Start();
        for (int i = 0; i < kTaskCount; ++i) {
            pool.Submit([&counter]() {
                counter.fetch_add(1, std::memory_order_relaxed);
            });
        }
        // Stop() 会等待队列清空(worker 在 stopping 且队列非空时仍继续消费)后
        // join, 返回时即代表所有已入队任务执行完毕,是可靠的同步点。
        pool.Stop();
    }
    CHECK(counter.load() == kTaskCount);
}

TEST_CASE("ThreadPool 累加求和结果正确(验证每个任务恰好执行一次)") {
    constexpr int kN = 500;
    std::atomic<long long> sum{0};
    ThreadPool pool(3, "sum");
    pool.Start();
    for (int i = 1; i <= kN; ++i) {
        pool.Submit(
            [&sum, i]() { sum.fetch_add(i, std::memory_order_relaxed); });
    }
    pool.Stop();
    // 1..500 的和 = 500*501/2 = 125250;若有任务漏执行或重复执行,总和会偏离。
    CHECK(sum.load() == static_cast<long long>(kN) * (kN + 1) / 2);
}

TEST_CASE("ThreadPool thread_num=0 仍至少有 1 个 worker,任务不会永久挂起") {
    // 回归:std::thread::hardware_concurrency() 在个别环境返回 0,
    // 若不做保底会创建 0 个 worker 导致任务永远无人执行。
    ThreadPool pool(0, "zero");
    CHECK(pool.size() >= 1);

    std::atomic<bool> executed{false};
    std::promise<void> done;
    std::future<void> done_fut = done.get_future();

    pool.Start();
    pool.Submit([&executed, &done]() {
        executed.store(true, std::memory_order_relaxed);
        done.set_value();
    });

    // 用有限超时的 future 等待,即便回归失效也不会让测试进程永久卡死。
    auto status = done_fut.wait_for(std::chrono::seconds(5));
    CHECK(status == std::future_status::ready);
    CHECK(executed.load());
    pool.Stop();
}

TEST_CASE("ThreadPool 通过 promise/future 回传任务返回值") {
    ThreadPool pool(2, "future");
    pool.Start();

    // Submit() 本身返回 void,这里用 packaged_task 包一层,从 future
    // 取回计算结果。
    std::packaged_task<int()> task([]() { return 6 * 7; });
    std::future<int> fut = task.get_future();
    // 用 shared_ptr 持有 packaged_task,保证在异步执行期间对象存活。
    auto task_ptr =
        std::make_shared<std::packaged_task<int()>>(std::move(task));
    pool.Submit([task_ptr]() { (*task_ptr)(); });

    CHECK(fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    CHECK(fut.get() == 42);
    pool.Stop();
}

TEST_CASE("ThreadPool 多轮 Submit 汇总多个 future 返回值") {
    ThreadPool pool(4, "futures");
    pool.Start();

    constexpr int kJobs = 16;
    std::vector<std::future<int>> futures;
    futures.reserve(kJobs);
    for (int i = 0; i < kJobs; ++i) {
        auto pt = std::make_shared<std::packaged_task<int()>>(
            [i]() { return i * i; });
        futures.push_back(pt->get_future());
        pool.Submit([pt]() { (*pt)(); });
    }

    long long total = 0;
    for (auto& f : futures) {
        REQUIRE(f.wait_for(std::chrono::seconds(5)) ==
                std::future_status::ready);
        total += f.get();
    }
    // sum of i*i for i in [0,15] = 1240
    CHECK(total == 1240);
    pool.Stop();
}
