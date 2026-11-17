#!/usr/bin/env python3
"""真实服务图片上传链路 benchmark。

流程与浏览器一致：
  login -> POST /api/media/upload/init -> PUT 预签名 URL -> complete
  -> GET /api/media/{id} 轮询直到 ready -> 拉取 preview/thumb 签名 URL

只上传媒体，不发送聊天消息，避免污染会话。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import time

import requests

LOGIC_BASE = os.environ.get("LOGIC_BASE", "http://127.0.0.1:9101")
MAX_BYTES = 20 * 1024 * 1024
TIMEOUT = 120


def http_post(path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(LOGIC_BASE + path, json=payload, headers=headers,
                         timeout=TIMEOUT)
    body = resp.json()
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("%s -> HTTP %s code=%s message=%s" %
                           (path, resp.status_code, body.get("code"),
                            body.get("message")))
    return body.get("data") or {}


def http_get(path, token):
    resp = requests.get(LOGIC_BASE + path,
                        headers={"Authorization": "Bearer " + token},
                        timeout=TIMEOUT)
    body = resp.json()
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("%s -> HTTP %s code=%s message=%s" %
                           (path, resp.status_code, body.get("code"),
                            body.get("message")))
    return body.get("data") or {}


def login(account, password):
    data = http_post("/api/login", {
        "account": account,
        "password": hashlib.md5(password.encode()).hexdigest(),
    })
    return data["user_id"], data["token"]


def mime_for(path):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    if ext in (".jpg", ".jpeg"):
        return "image/jpeg"
    return "application/octet-stream"


def worker_peak_mib():
    """读取所有 media_worker 进程的 VmHWM（内核高水位）。"""
    try:
        output = subprocess.check_output(
            ["pgrep", "-f", "media_worker"], text=True)
    except subprocess.CalledProcessError:
        return 0
    peak = 0
    for pid in output.split():
        try:
            with open("/proc/%s/status" % pid) as f:
                for line in f:
                    if line.startswith("VmHWM:"):
                        peak = max(peak, int(line.split()[1]) // 1024)
                        break
        except OSError:
            pass
    return peak


def upload_one(path, token):
    size = os.path.getsize(path)
    if size <= 0 or size > MAX_BYTES:
        print("[SKIP] %s size=%d (limit %d)" % (path, size, MAX_BYTES))
        return None
    mime = mime_for(path)
    data = open(path, "rb").read()

    t0 = time.perf_counter()
    init = http_post("/api/media/upload/init", {
        "kind": "image",
        "mime_type": mime,
        "size_bytes": size,
        "filename": os.path.basename(path),
    }, token)
    t1 = time.perf_counter()

    media_id = init["media_id"]
    put = requests.put(init["upload_url"], data=data,
                       headers={"Content-Type": mime}, timeout=TIMEOUT)
    t2 = time.perf_counter()
    if put.status_code not in (200, 204):
        raise RuntimeError("PUT HTTP %s" % put.status_code)

    http_post("/api/media/upload/complete", {"media_id": media_id}, token)
    t3 = time.perf_counter()

    meta = {}
    deadline = time.time() + 120
    while time.time() < deadline:
        meta = http_get("/api/media/" + media_id, token)
        if meta.get("status") == "ready":
            break
        if meta.get("status") == "failed":
            raise RuntimeError("media failed: %s" % meta.get("error"))
        time.sleep(0.1)
    t4 = time.perf_counter()
    if meta.get("status") != "ready":
        raise RuntimeError("media timeout, status=%s" % meta.get("status"))

    variants = meta.get("variants") or {}
    preview = variants.get("preview") or {}
    thumb = variants.get("thumb") or {}
    fetch_ms = 0
    fetch_bytes = 0
    if preview.get("url"):
        f0 = time.perf_counter()
        resp = requests.get(preview["url"], timeout=TIMEOUT)
        fetch_ms = (time.perf_counter() - f0) * 1000
        fetch_bytes = len(resp.content)

    return {
        "file": os.path.basename(path),
        "size_mib": size / 1024.0 / 1024.0,
        "media_id": media_id,
        "init_ms": (t1 - t0) * 1000,
        "put_ms": (t2 - t1) * 1000,
        "complete_ms": (t3 - t2) * 1000,
        "ready_ms": (t4 - t3) * 1000,
        "total_ms": (t4 - t0) * 1000,
        "preview_kib": preview.get("size_bytes", 0) / 1024.0,
        "thumb_kib": thumb.get("size_bytes", 0) / 1024.0,
        "preview_fetch_ms": fetch_ms,
        "preview_fetch_kib": fetch_bytes / 1024.0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("images", nargs="*", help="图片路径；默认使用 tests/data 样本")
    parser.add_argument("--account", default="alice")
    parser.add_argument("--password", default="123456")
    args = parser.parse_args()

    default_images = [
        "tests/data/png/photo_8mp_landscape.png",
        "tests/data/png/screenshot_12mp.png",
        "tests/data/webp/photo_45mp_q80.webp",
        "tests/data/webp/photo_12mp_lossless_true.webp",
        "tests/data/webp/photo_12mp_alpha_true.webp",
    ]
    images = args.images or default_images
    _, token = login(args.account, args.password)

    before_peak = worker_peak_mib()
    rows = []
    for path in images:
        print("\n=== %s ===" % path, flush=True)
        try:
            row = upload_one(path, token)
            if row:
                rows.append(row)
                print("media_id=%s total=%.1fms (init %.1f, put %.1f, "
                      "complete %.1f, ready %.1f) preview=%.1fKiB "
                      "worker_peak=%dMiB" %
                      (row["media_id"], row["total_ms"], row["init_ms"],
                       row["put_ms"], row["complete_ms"], row["ready_ms"],
                       row["preview_kib"], worker_peak_mib()), flush=True)
        except Exception as exc:
            print("[FAIL] %s: %s" % (path, exc), flush=True)
    after_peak = worker_peak_mib()

    print("\n| file | size MiB | init ms | PUT ms | complete ms | "
          "ready ms | total ms | preview KiB | preview fetch ms |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        print("| %s | %.2f | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f |" %
              (row["file"], row["size_mib"], row["init_ms"], row["put_ms"],
               row["complete_ms"], row["ready_ms"], row["total_ms"],
               row["preview_kib"], row["preview_fetch_ms"]))
    print("\nworker VmHWM: before=%dMiB after=%dMiB" %
          (before_peak, after_peak))
    return 0 if len(rows) == len(images) else 1


if __name__ == "__main__":
    sys.exit(main())
