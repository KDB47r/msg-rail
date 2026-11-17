#!/usr/bin/env python3
"""媒体 worker 队列调度仿真：旧共享池 vs 新图片/视频 lane。

这不是完整 E2E，而是离散事件调度仿真：
- 旧实现：N 个线程共享一个 FIFO 队列（ClaimNextTask 不带 task_type）。
- 新实现：图片 lane 和视频 lane 各自 FIFO，按 task_type 领取。
- 任务时长使用真实 benchmark 的近似值，可通过参数覆盖。

为什么用仿真而不是直接回滚旧代码：
- 旧实现已经被替换，回滚/重新编译两套服务成本高；
- 队列拆分的核心差异是“调度”，不是 SQL 执行时间；
- 实际 ClaimNextTask 的 SQL 开销在毫秒级，相对图片 0.5s、视频 10s 可以忽略；
- 新索引已经用 EXPLAIN 验证，不需要在仿真里重复。

运行：
  python3 tests/queue_split_bench.py
  python3 tests/queue_split_bench.py --image-ms 500 --video-ms 10000
  python3 tests/queue_split_bench.py --images 60 --videos 5
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable, List, Tuple


@dataclass
class Task:
    id: int
    kind: str  # "image" / "video"
    arrival: float
    duration: float


@dataclass
class Result:
    task: Task
    start: float
    end: float


def percentile(values: Iterable[float], p: float) -> float:
    data = sorted(values)
    if not data:
        return 0.0
    if len(data) == 1:
        return data[0]
    pos = (len(data) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    frac = pos - lo
    return data[lo] + (data[hi] - data[lo]) * frac


def simulate_shared(tasks: List[Task], threads: int) -> List[Result]:
    """共享池：所有任务按 (arrival, id) FIFO，线程取最早可用的那个。"""
    available = [0.0] * threads
    results: List[Result] = []
    for task in sorted(tasks, key=lambda t: (t.arrival, t.id)):
        idx = min(range(threads), key=lambda i: available[i])
        start = max(available[idx], task.arrival)
        end = start + task.duration
        available[idx] = end
        results.append(Result(task, start, end))
    return results


def simulate_split(tasks: List[Task], image_threads: int,
                   video_threads: int) -> List[Result]:
    """拆分池：图片/视频各自一个 FIFO lane。"""
    available = {
        "image": [0.0] * image_threads,
        "video": [0.0] * video_threads,
    }
    results: List[Result] = []
    for task in sorted(tasks, key=lambda t: (t.arrival, t.id)):
        lane = available[task.kind]
        idx = min(range(len(lane)), key=lambda i: lane[i])
        start = max(lane[idx], task.arrival)
        end = start + task.duration
        lane[idx] = end
        results.append(Result(task, start, end))
    return results


def metrics(results: List[Result]) -> dict:
    images = [r.end for r in results if r.task.kind == "image"]
    videos = [r.end for r in results if r.task.kind == "video"]
    first_video_end = min(videos) if videos else 0.0
    return {
        "image_p50": percentile(images, 50),
        "image_p95": percentile(images, 95),
        "image_max": max(images) if images else 0.0,
        "image_done": max(images) if images else 0.0,
        "video_last": max(videos) if videos else 0.0,
        "makespan": max((r.end for r in results), default=0.0),
        "images_before_first_video": sum(
            1 for x in images if videos and x < first_video_end),
    }


def print_row(name: str, m: dict) -> None:
    print(f"{name:<24} "
          f"{m['image_p50']:>9.2f} "
          f"{m['image_p95']:>9.2f} "
          f"{m['image_max']:>9.2f} "
          f"{m['image_done']:>10.2f} "
          f"{m['video_last']:>10.2f} "
          f"{m['makespan']:>9.2f} "
          f"{m['images_before_first_video']:>12d}")


def run_scenario(title: str, tasks: List[Task], image_ms: float,
                 video_ms: float) -> None:
    print(f"\n=== {title} ===")
    print(f"图片任务 {sum(1 for t in tasks if t.kind == 'image')} 个 × "
          f"{image_ms / 1000:.3f}s；视频任务 "
          f"{sum(1 for t in tasks if t.kind == 'video')} 个 × "
          f"{video_ms / 1000:.3f}s；所有任务 t=0 到达")
    print(f"{'模式':<24} {'图P50':>9} {'图P95':>9} {'图Max':>9} "
          f"{'图完成':>10} {'视频完成':>10} {'总完成':>9} {'首视频前完成':>12}")

    for label, results in (
        ("shared(2) 旧行为", simulate_shared(tasks, 2)),
        ("shared(3) 只加线程", simulate_shared(tasks, 3)),
        ("split(3图片+1视频) 新", simulate_split(tasks, 3, 1)),
        ("split(3图片+2视频)", simulate_split(tasks, 3, 2)),
    ):
        print_row(label, metrics(results))


def build_video_first(images: int, videos: int, image_ms: float,
                      video_ms: float) -> List[Task]:
    tasks: List[Task] = []
    tid = 0
    for _ in range(videos):
        tasks.append(Task(tid, "video", 0.0, video_ms / 1000.0))
        tid += 1
    for _ in range(images):
        tasks.append(Task(tid, "image", 0.0, image_ms / 1000.0))
        tid += 1
    return tasks


def build_mixed(images: int, videos: int, image_ms: float,
                video_ms: float) -> List[Task]:
    total = images + videos
    video_positions = {i * 10 for i in range(videos) if i * 10 < total}
    tasks: List[Task] = []
    for i in range(total):
        kind = "video" if i in video_positions else "image"
        tasks.append(Task(i, kind, 0.0,
                          (video_ms if kind == "video" else image_ms) / 1000.0))
    return tasks


def build_image_only(images: int, image_ms: float) -> List[Task]:
    return [Task(i, "image", 0.0, image_ms / 1000.0) for i in range(images)]


def main() -> None:
    parser = argparse.ArgumentParser(description="媒体 worker 队列调度仿真")
    parser.add_argument("--image-ms", type=float, default=500,
                        help="单张图片处理耗时（毫秒），默认 500")
    parser.add_argument("--video-ms", type=float, default=10000,
                        help="单个视频处理耗时（毫秒），默认 10000")
    parser.add_argument("--images", type=int, default=30,
                        help="图片任务数，默认 30")
    parser.add_argument("--videos", type=int, default=3,
                        help="视频任务数，默认 3")
    args = parser.parse_args()

    print("媒体 worker 队列调度仿真（离散事件，非真实服务 E2E）")
    print(f"图片 {args.image_ms:.0f}ms/个，视频 {args.video_ms:.0f}ms/个")

    run_scenario("场景 A：视频先到，后面一批图片",
                 build_video_first(args.images, args.videos,
                                   args.image_ms, args.video_ms),
                 args.image_ms, args.video_ms)

    run_scenario("场景 B：图片/视频混合到达（视频按间隔插入）",
                 build_mixed(args.images, args.videos,
                             args.image_ms, args.video_ms),
                 args.image_ms, args.video_ms)

    run_scenario("场景 C：只有图片",
                 build_image_only(args.images, args.image_ms),
                 args.image_ms, args.video_ms)

    print("\n说明：")
    print("- shared(2) 是旧默认：2 个线程共享队列。")
    print("- shared(3) 只增加线程数，但仍是共享队列。")
    print("- split(3图片+1视频) 是当前实现；视频 lane 不影响图片 lane。")
    print("- 时间为仿真时间，单位秒；任务时长来自真实 benchmark 的近似值。")
    print("- 真实 ClaimNextTask SQL 开销约毫秒级，相对 0.5s/10s 可忽略；"
          "新索引已用 EXPLAIN 验证。")


if __name__ == "__main__":
    main()
