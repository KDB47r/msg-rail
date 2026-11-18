#!/usr/bin/env python3
"""视频 multipart / 断点续传 / 单次 PUT 上传耗时对比（本地 MinIO）。

只测上传与 ready 时间，不做消息发送。会留下测试 media，末尾列出。
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import video_multipart_e2e as mp  # noqa: E402


def upload_parts(video_path, urls, part_size, part_numbers, workers=3):
    def one(n):
        mp.upload_part(urls[n], mp.read_part(video_path, n, part_size))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, part_numbers))


def measure_single_put(token, video_path, size):
    t0 = time.perf_counter()
    init = mp.http_post("/api/media/upload/init", {
        "kind": "video",
        "mime_type": "video/mp4",
        "size_bytes": size,
        "filename": "single_put_test.mp4",
    }, token)
    init_ms = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    with open(video_path, "rb") as f:
        resp = requests.put(init["upload_url"], data=f, timeout=300)
    if resp.status_code not in (200, 204):
        raise RuntimeError("single PUT HTTP %s" % resp.status_code)
    upload_ms = (time.perf_counter() - t1) * 1000.0

    t2 = time.perf_counter()
    mp.http_post("/api/media/upload/complete",
                 {"media_id": init["media_id"]}, token)
    complete_ms = (time.perf_counter() - t2) * 1000.0

    t3 = time.perf_counter()
    mp.wait_ready(token, init["media_id"])
    ready_ms = (time.perf_counter() - t3) * 1000.0
    return {
        "media_id": init["media_id"],
        "init": init_ms,
        "upload": upload_ms,
        "complete": complete_ms,
        "ready": ready_ms,
        "total": init_ms + upload_ms + complete_ms,
    }


def measure_multipart(token, video_path, size, resume):
    t0 = time.perf_counter()
    init = mp.init_multipart(token, video_path, size,
                             "multipart_resume.mp4" if resume
                             else "multipart_full.mp4")
    media_id = init["media_id"]
    part_size = int(init["part_size"])
    part_count = int(init["part_count"])
    urls = mp.get_part_urls(token, media_id, list(range(1, part_count + 1)))
    setup_ms = (time.perf_counter() - t0) * 1000.0

    if resume:
        # 模拟上一次已经传完第 1 片后中断；本轮只测量“status + 剩余片”。
        upload_parts(video_path, urls, part_size, [1])
        mp.multipart_status(token, media_id)
        t1 = time.perf_counter()
        remaining = list(range(2, part_count + 1))
    else:
        t1 = time.perf_counter()
        remaining = list(range(1, part_count + 1))
    upload_parts(video_path, urls, part_size, remaining)
    upload_ms = (time.perf_counter() - t1) * 1000.0

    t2 = time.perf_counter()
    mp.http_post("/api/media/upload/complete", {"media_id": media_id}, token)
    complete_ms = (time.perf_counter() - t2) * 1000.0

    t3 = time.perf_counter()
    mp.wait_ready(token, media_id)
    ready_ms = (time.perf_counter() - t3) * 1000.0
    return {
        "media_id": media_id,
        "part_count": part_count,
        "setup": setup_ms,
        "upload": upload_ms,
        "complete": complete_ms,
        "ready": ready_ms,
        "total": (upload_ms + complete_ms) if resume
                 else (setup_ms + upload_ms + complete_ms),
    }


def main():
    account = os.environ.get("MULTIPART_ACCOUNT", "alice")
    password = os.environ.get("MULTIPART_PASSWORD", "123456")
    _, token = mp.login(account, password)

    tmpdir = tempfile.mkdtemp(prefix="sparkpush-multipart-bench-")
    video_path = os.path.join(tmpdir, "bench.mp4")
    size = mp.make_video(video_path)
    print("测试视频: %.2f MiB" % (size / 1024.0 / 1024.0))

    runs = [
        ("single PUT", measure_single_put(token, video_path, size)),
        ("multipart full", measure_multipart(token, video_path, size, False)),
        ("multipart resume", measure_multipart(token, video_path, size, True)),
    ]

    print("\n| 场景 | init/中断前 | 上传 | complete | 上传总计 | ready 等待 |")
    print("|---|---:|---:|---:|---:|---:|")
    for name, r in runs:
        setup = r["setup"] if "setup" in r else r["init"]
        print("| %s | %.1fms | %.1fms | %.1fms | **%.1fms** | %.1fms |" %
              (name, setup, r["upload"], r["complete"], r["total"],
               r["ready"]))
    for name, r in runs:
        if "part_count" in r:
            print("%s: part_count=%d" % (name, r["part_count"]))
    print("\nmultipart resume 的上传总计只统计“status + 剩余分片 + complete”，"
          "不包含中断前已传的第 1 片。")

    print("\n测试数据（未清理）:")
    for name, r in runs:
        print("  %s media_id=%s" % (name, r["media_id"]))

    os.remove(video_path)
    os.rmdir(tmpdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
