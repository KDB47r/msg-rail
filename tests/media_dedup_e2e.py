#!/usr/bin/env python3
"""图片/视频内容去重 E2E：同一份字节由不同用户重复上传，复用 asset。

前置：start_all.sh 已启动 Logic / media_worker / MinIO / MySQL。
脚本会新增测试用户、media 和 asset，不删除数据。
"""

from __future__ import annotations

import base64
import os
import subprocess
import sys
import tempfile
import time
import urllib.parse

import requests

LOGIC_BASE = os.environ.get("LOGIC_BASE", "http://127.0.0.1:9101")
TIMEOUT = 60
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


def post_raw(path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(LOGIC_BASE + path, json=payload or {},
                         headers=headers, timeout=TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        body = {"message": resp.text[:300]}
    return resp.status_code, body


def ok(path, payload=None, token=None):
    status, body = post_raw(path, payload, token)
    if status != 200 or body.get("code") != 0:
        raise RuntimeError("%s -> HTTP %s code=%s msg=%s" %
                           (path, status, body.get("code"),
                            body.get("message")))
    return body.get("data") or {}


def register(account, password, name):
    return ok("/api/register", {"account": account, "password": password,
                                "name": name})


def login(account, password):
    return ok("/api/login", {"account": account, "password": password})


def upload_media(token, kind, mime, filename, data):
    init = ok("/api/media/upload/init", {
        "kind": kind,
        "mime_type": mime,
        "size_bytes": len(data),
        "filename": filename,
    }, token)
    put = requests.put(init["upload_url"], data=data,
                       headers={"Content-Type": mime}, timeout=120)
    assert put.status_code in (200, 204), put.status_code
    ok("/api/media/upload/complete", {"media_id": init["media_id"]}, token)
    return init["media_id"]


def wait_ready(token, media_id):
    for _ in range(180):
        resp = requests.get(LOGIC_BASE + "/api/media/" + media_id,
                            headers={"Authorization": "Bearer " + token},
                            timeout=TIMEOUT)
        body = resp.json()
        data = body.get("data") or {}
        if data.get("status") == "ready":
            return data
        if data.get("status") == "failed":
            raise RuntimeError("media failed: %s" % data)
        time.sleep(0.25)
    raise RuntimeError("media ready timeout: " + media_id)


def path_of(url):
    return urllib.parse.urlparse(url).path


def mysql_scalar(sql):
    try:
        out = subprocess.run([
            "mysql", "-N", "-B", "-h127.0.0.1", "-P3306",
            "-uroot", "-p123456", "spark_push", "-e", sql,
        ], capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            return None
        return out.stdout.strip()
    except Exception:
        return None


def asset_id_of(media_id):
    raw = mysql_scalar(
        "SELECT asset_id FROM media_object_asset WHERE media_id='%s'" %
        media_id)
    if not raw or not raw.isdigit():
        return 0
    return int(raw)


def main():
    suffix = "%06d%02d" % (int(time.time()) % 1000000, os.getpid() % 100)
    pwd = "dedup-pass-" + suffix
    acc_a, acc_b = "dedup_a_" + suffix, "dedup_b_" + suffix
    ua = register(acc_a, pwd, "DEDUP-A-" + suffix)
    ub = register(acc_b, pwd, "DEDUP-B-" + suffix)
    ta = login(acc_a, pwd)["token"]
    tb = login(acc_b, pwd)["token"]

    # 图片：A、B 上传同一份字节
    img_a = upload_media(ta, "image", "image/png", "same-a.png", PNG_1X1)
    meta_a = wait_ready(ta, img_a)
    img_b = upload_media(tb, "image", "image/png", "same-b.png", PNG_1X1)
    meta_b = wait_ready(tb, img_b)
    asset_a = asset_id_of(img_a)
    asset_b = asset_id_of(img_b)
    assert asset_a > 0 and asset_a == asset_b, (img_a, img_b, asset_a, asset_b)
    assert path_of(meta_a["original"]["url"]) == \
        path_of(meta_b["original"]["url"]), (meta_a, meta_b)
    for name in ("micro", "thumb", "preview"):
        if name in meta_a.get("variants", {}):
            assert name in meta_b.get("variants", {}), meta_b
            assert path_of(meta_a["variants"][name]["url"]) == \
                path_of(meta_b["variants"][name]["url"])
    refs = mysql_scalar(
        "SELECT ref_count FROM media_asset WHERE id=%d" % asset_a)

    # 视频：A、B 上传同一份字节
    video_path = tempfile.mktemp(prefix="dedup-", suffix=".mp4")
    subprocess.run([
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10:duration=1",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", video_path,
    ], check=True)
    with open(video_path, "rb") as f:
        video_bytes = f.read()
    os.remove(video_path)
    vid_a = upload_media(ta, "video", "video/mp4", "same-a.mp4", video_bytes)
    vmeta_a = wait_ready(ta, vid_a)
    vid_b = upload_media(tb, "video", "video/mp4", "same-b.mp4", video_bytes)
    vmeta_b = wait_ready(tb, vid_b)
    vasset_a = asset_id_of(vid_a)
    vasset_b = asset_id_of(vid_b)
    assert vasset_a > 0 and vasset_a == vasset_b, \
        (vid_a, vid_b, vasset_a, vasset_b)
    assert path_of(vmeta_a["original"]["url"]) == \
        path_of(vmeta_b["original"]["url"]), (vmeta_a, vmeta_b)
    assert "poster" in vmeta_a.get("variants", {}), vmeta_a
    assert "poster" in vmeta_b.get("variants", {}), vmeta_b

    print("[PASS] 图片跨用户去重: media=%s/%s asset_id=%d refs=%s" %
          (img_a, img_b, asset_a, refs))
    print("[PASS] 图片 clone 共享 object_key / variants")
    print("[PASS] 视频跨用户去重: media=%s/%s asset_id=%d" %
          (vid_a, vid_b, vasset_a))
    print("[PASS] 视频 clone 共享 object_key")
    print("测试数据（未清理）: users=%s/%s" %
          (ua["user_id"], ub["user_id"]))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("[FAIL] %s" % exc, file=sys.stderr)
        raise
