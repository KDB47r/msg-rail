#!/usr/bin/env python3
"""视频 multipart 上传 / 断点续传 / abort E2E。

覆盖：
  1. init(multipart=true) → 取分片 URL → 先传第 1 片；
  2. status 返回第 1 片已完成（模拟中断后恢复）；
  3. 补传剩余分片 → complete → worker ready；
  4. abort 未完成上传 → media deleted；
  5. 缺片 complete → 409。

会创建一个测试账号/若干 media 和 MinIO 对象，不清理，结果末尾列出。
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import time

import requests

LOGIC_BASE = "http://127.0.0.1:9101"
TIMEOUT = 120


def http_post(path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(LOGIC_BASE + path, json=payload, headers=headers,
                         timeout=TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        body = {}
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("%s -> HTTP %s code=%s message=%s" %
                           (path, resp.status_code, body.get("code"),
                            body.get("message")))
    return body.get("data") or {}


def http_post_status(path, payload, token):
    """返回 (http_status, body)，用于断言 409。"""
    resp = requests.post(
        LOGIC_BASE + path, json=payload,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token},
        timeout=TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body


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
    digest = hashlib.md5(password.encode()).hexdigest()
    data = http_post("/api/login", {"account": account, "password": digest})
    return data["user_id"], data["token"]


def check(name, ok, detail=""):
    print(("[PASS] " if ok else "[FAIL] ") + name +
          ((": " + detail) if detail and not ok else ""))
    if not ok:
        raise AssertionError(name)


def make_video(path, seconds=12):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i",
        "testsrc2=size=1280x720:rate=30:duration=%d" % seconds,
        "-c:v", "libx264", "-preset", "veryfast", "-b:v", "15M",
        "-pix_fmt", "yuv420p", path,
    ]
    subprocess.run(cmd, check=True)
    return os.path.getsize(path)


def read_part(file_path, part_number, part_size):
    with open(file_path, "rb") as f:
        f.seek((part_number - 1) * part_size)
        return f.read(part_size)


def upload_part(url, data):
    resp = requests.put(url, data=data, timeout=TIMEOUT)
    if resp.status_code not in (200, 204):
        raise RuntimeError("UploadPart HTTP %s %s" %
                           (resp.status_code, resp.text[:300]))


def init_multipart(token, file_path, size, filename):
    return http_post("/api/media/upload/init", {
        "kind": "video",
        "mime_type": "video/mp4",
        "size_bytes": size,
        "filename": filename,
        "multipart": True,
    }, token)


def get_part_urls(token, media_id, part_numbers):
    data = http_post("/api/media/upload/multipart/part_urls", {
        "media_id": media_id,
        "part_numbers": part_numbers,
    }, token)
    return {int(p["part_number"]): p["url"] for p in data.get("parts") or []}


def multipart_status(token, media_id):
    return http_post("/api/media/upload/multipart/status",
                     {"media_id": media_id}, token)


def complete(token, media_id):
    return http_post("/api/media/upload/complete",
                     {"media_id": media_id}, token)


def abort(token, media_id):
    return http_post("/api/media/upload/abort",
                     {"media_id": media_id}, token)


def wait_ready(token, media_id, attempts=120):
    for _ in range(attempts):
        data = http_get("/api/media/" + media_id, token)
        if data.get("status") == "ready":
            return data
        if data.get("status") == "failed":
            raise RuntimeError("media failed: %s" % data.get("error"))
        time.sleep(0.5)
    raise RuntimeError("media ready timeout: " + media_id)


def main():
    account = os.environ.get("MULTIPART_ACCOUNT", "alice")
    password = os.environ.get("MULTIPART_PASSWORD", "123456")
    uid, token = login(account, password)
    print("login user_id=%s" % uid)

    tmpdir = tempfile.mkdtemp(prefix="sparkpush-multipart-")
    video_path = os.path.join(tmpdir, "multipart_test.mp4")
    size = make_video(video_path)
    check("生成测试视频", size > 10 * 1024 * 1024, "%d bytes" % size)

    init = init_multipart(token, video_path, size, "multipart_test.mp4")
    media_id = init["media_id"]
    part_size = int(init["part_size"])
    part_count = int(init["part_count"])
    check("multipart init", init.get("upload_method") == "multipart")
    check("分片数 >= 2", part_count >= 2,
          "part_count=%d size=%d" % (part_count, size))

    st = multipart_status(token, media_id)
    check("初始 status 无已上传分片", not st.get("uploaded_parts"))

    urls = get_part_urls(token, media_id, list(range(1, part_count + 1)))
    check("获取全部分片 URL", len(urls) == part_count)

    # 模拟中断：先只上传第 1 片。
    upload_part(urls[1], read_part(video_path, 1, part_size))
    st = multipart_status(token, media_id)
    uploaded = {int(p["part_number"]) for p in st.get("uploaded_parts") or []}
    check("中断后 status 识别第 1 片", uploaded == {1}, str(uploaded))

    # 续传：补传第 2..N 片。
    for n in range(2, part_count + 1):
        upload_part(urls[n], read_part(video_path, n, part_size))
    done = complete(token, media_id)
    check("multipart complete", done.get("status") in ("uploaded", "processing"))

    meta = wait_ready(token, media_id)
    check("视频 ready", meta.get("status") == "ready")
    # 无需转码的视频只有 poster，playback 只在需要转码时生成。
    check("视频包含 poster", "poster" in (meta.get("variants") or {}))
    check("视频原图 URL 存在", bool((meta.get("original") or {}).get("url")))

    # abort：再 init 一个，传第 1 片后取消。
    abort_media = init_multipart(token, video_path, size,
                                 "multipart_abort.mp4")["media_id"]
    abort_urls = get_part_urls(token, abort_media, [1])
    upload_part(abort_urls[1], read_part(video_path, 1, part_size))
    abort(token, abort_media)
    aborted = http_get("/api/media/" + abort_media, token)
    check("abort 后 media deleted", aborted.get("status") == "deleted",
          str(aborted.get("status")))
    abort_status, _ = http_post_status(
        "/api/media/upload/multipart/status", {"media_id": abort_media}, token)
    check("abort 后上传会话不可恢复", abort_status in (404, 409),
          "HTTP %s" % abort_status)

    # 缺片 complete：只传第 1 片就 complete，应 409。
    incomplete_media = init_multipart(token, video_path, size,
                                      "multipart_incomplete.mp4")["media_id"]
    inc_urls = get_part_urls(token, incomplete_media, [1])
    upload_part(inc_urls[1], read_part(video_path, 1, part_size))
    http_status, body = http_post_status(
        "/api/media/upload/complete", {"media_id": incomplete_media}, token)
    check("缺片 complete 返回 409", http_status == 409,
          "HTTP %s %s" % (http_status, body.get("message")))
    abort(token, incomplete_media)

    os.remove(video_path)
    os.rmdir(tmpdir)

    print("\n测试数据（未清理）:")
    print("  ready video media_id=%s" % media_id)
    print("  aborted media_id=%s" % abort_media)
    print("  incomplete-then-aborted media_id=%s" % incomplete_media)
    print("\n全部 multipart E2E 通过")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("[FAIL] %s" % exc, file=sys.stderr)
        raise
