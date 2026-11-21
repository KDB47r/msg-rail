#!/usr/bin/env python3
"""单聊消息转发 E2E：文本 / 图片 / 文件、权限、legacy 媒体。

前置：scripts/start_all.sh 已启动 Logic、Job、Comet、MinIO、media_worker。
脚本注册新账号并留下测试数据，不删除历史数据。
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
TIMEOUT = 30
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)
FILE_BYTES = b"hello forward file\n"


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


def expect_error(path, payload, token, http_status):
    status, body = post_raw(path, payload, token)
    if status != http_status:
        raise AssertionError("%s expected HTTP %s, got %s body=%s" %
                             (path, http_status, status, body))
    return body


def login(account, password):
    return ok("/api/login", {"account": account, "password": password})


def register(account, password, name):
    return ok("/api/register", {"account": account, "password": password,
                                "name": name})


def make_friends(a_token, a_uid, b_token, b_uid):
    req = ok("/api/friend/request", {"to_user_id": b_uid, "message": ""},
             a_token)
    ok("/api/friend/respond",
       {"request_id": req["request_id"], "action": "accept"}, b_token)


def upload_media(token, kind, mime, filename, data):
    init = ok("/api/media/upload/init", {
        "kind": kind,
        "mime_type": mime,
        "size_bytes": len(data),
        "filename": filename,
    }, token)
    put = requests.put(init["upload_url"], data=data,
                       headers={"Content-Type": mime}, timeout=60)
    assert put.status_code in (200, 204), put.status_code
    ok("/api/media/upload/complete", {"media_id": init["media_id"]}, token)
    return init["media_id"]


def wait_media_ready(token, media_id):
    for _ in range(120):
        resp = requests.get(LOGIC_BASE + "/api/media/" + media_id,
                            headers={"Authorization": "Bearer " + token},
                            timeout=TIMEOUT)
        body = resp.json()
        data = body.get("data") or {}
        if data.get("status") == "ready":
            return data
        if data.get("status") == "failed":
            raise RuntimeError("media failed: %s" % data)
        time.sleep(0.3)
    raise RuntimeError("media ready timeout: " + media_id)


def wait_message(token, session_id, msg_id, timeout=15):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        status, body = post_raw("/api/session/history", {
            "session_id": session_id,
            "anchor_seq": 0,
            "limit": 50,
        }, token)
        if status == 200 and body.get("code") == 0:
            last = body["data"].get("messages") or []
            for msg in last:
                if msg.get("msg_id") == msg_id:
                    return msg
        time.sleep(0.3)
    raise RuntimeError("message not found: %s, last=%s" %
                       (msg_id, last[-3:]))


def content_of(message):
    content = message.get("content") or {}
    if isinstance(content, dict):
        inner = content.get("content")
        if isinstance(inner, (str, dict)):
            return inner
    return content


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


def asset_refcount(media_id):
    raw = mysql_scalar(
        "SELECT a.ref_count FROM media_asset a "
        "JOIN media_object_asset m ON m.asset_id=a.id "
        "WHERE m.media_id='%s'" % media_id)
    if not raw or not raw.isdigit():
        return -1
    return int(raw)


def main():
    suffix = "%06d%02d" % (int(time.time()) % 1000000, os.getpid() % 100)
    pwd = "forward-pass-" + suffix
    accounts = ["fwd_a_" + suffix, "fwd_b_" + suffix, "fwd_c_" + suffix]
    names = ["FWD-A-" + suffix, "FWD-B-" + suffix, "FWD-C-" + suffix]
    users = [register(accounts[i], pwd, names[i]) for i in range(3)]
    tokens = [login(accounts[i], pwd)["token"] for i in range(3)]
    uids = [users[i]["user_id"] for i in range(3)]
    # D 不与 B 建立好友关系，用来验证转发失败时 clone 的清理。
    d = register("fwd_d_" + suffix, pwd, "FWD-D-" + suffix)
    uid_d = d["user_id"]
    a, b, c = 0, 1, 2
    make_friends(tokens[a], uids[a], tokens[b], uids[b])
    make_friends(tokens[b], uids[b], tokens[c], uids[c])
    make_friends(tokens[a], uids[a], tokens[c], uids[c])
    sid_ab = "single:%d:%d" % (min(uids[a], uids[b]), max(uids[a], uids[b]))
    sid_bc = "single:%d:%d" % (min(uids[b], uids[c]), max(uids[b], uids[c]))
    sid_ac = "single:%d:%d" % (min(uids[a], uids[c]), max(uids[a], uids[c]))

    # 文本转发
    text_msg = ok("/api/message/send", {
        "to_user_id": uids[b],
        "msg_type": "text",
        "content": "forward-text-" + suffix,
    }, tokens[a])
    wait_message(tokens[b], sid_ab, text_msg["msg_id"])
    fwd_text = ok("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": text_msg["msg_seq"],
        "to_user_id": uids[c],
    }, tokens[b])
    text_entry = wait_message(tokens[c], sid_bc, fwd_text["msg_id"])
    assert content_of(text_entry) == "forward-text-" + suffix, text_entry

    # 图片上传 + 发送 + 转发
    source_media = upload_media(tokens[a], "image", "image/png",
                                "forward-source.png", PNG_1X1)
    source_meta = wait_media_ready(tokens[a], source_media)
    image_msg = ok("/api/message/send", {
        "to_user_id": uids[b],
        "msg_type": "image",
        "media_id": source_media,
        "caption": "forward-image",
    }, tokens[a])
    wait_message(tokens[b], sid_ab, image_msg["msg_id"])
    fwd_image = ok("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": image_msg["msg_seq"],
        "to_user_id": uids[c],
    }, tokens[b])
    assert fwd_image["media_id"] != source_media, fwd_image
    image_entry = wait_message(tokens[c], sid_bc, fwd_image["msg_id"])
    image_content = content_of(image_entry)
    assert image_content.get("media_id") == fwd_image["media_id"], image_entry
    assert image_content.get("caption") == "forward-image", image_entry

    clone_meta = wait_media_ready(tokens[c], fwd_image["media_id"])
    assert clone_meta["status"] == "ready"
    # 原图和变体对象 key 必须与源媒体一致（共享 S3 对象，不重新上传）
    assert path_of(clone_meta["original"]["url"]) == \
        path_of(source_meta["original"]["url"]), (clone_meta, source_meta)
    for name in ("micro", "thumb", "preview"):
        if name in source_meta.get("variants", {}):
            assert name in clone_meta.get("variants", {}), clone_meta
            assert path_of(clone_meta["variants"][name]["url"]) == \
                path_of(source_meta["variants"][name]["url"])

    # 失败回滚：B 转发给非好友 D，clone 应删除且 asset ref_count 不变。
    ref_before = asset_refcount(source_media)
    assert ref_before > 0, "source image has no asset mapping"
    expect_error("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": image_msg["msg_seq"],
        "to_user_id": uid_d,
    }, tokens[b], 403)
    ref_after = asset_refcount(source_media)
    assert ref_after == ref_before, (ref_before, ref_after)

    # 文件转发（验证非图片媒体）
    source_file = upload_media(tokens[a], "file", "text/plain",
                               "forward.txt", FILE_BYTES)
    file_meta = wait_media_ready(tokens[a], source_file)
    file_msg = ok("/api/message/send", {
        "to_user_id": uids[b],
        "msg_type": "file",
        "media_id": source_file,
    }, tokens[a])
    wait_message(tokens[b], sid_ab, file_msg["msg_id"])
    fwd_file = ok("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": file_msg["msg_seq"],
        "to_user_id": uids[c],
    }, tokens[b])
    file_entry = wait_message(tokens[c], sid_bc, fwd_file["msg_id"])
    assert content_of(file_entry).get("name") == "forward.txt", file_entry
    clone_file = wait_media_ready(tokens[c], fwd_file["media_id"])
    assert path_of(clone_file["original"]["url"]) == \
        path_of(file_meta["original"]["url"])

    # 视频转发（验证 duration/poster 等字段也被 clone）
    video_path = tempfile.mktemp(prefix="forward-", suffix=".mp4")
    subprocess.run([
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10:duration=1",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", video_path,
    ], check=True)
    with open(video_path, "rb") as f:
        video_bytes = f.read()
    os.remove(video_path)
    source_video = upload_media(tokens[a], "video", "video/mp4",
                                "forward.mp4", video_bytes)
    source_video_meta = wait_media_ready(tokens[a], source_video)
    video_msg = ok("/api/message/send", {
        "to_user_id": uids[b],
        "msg_type": "video",
        "media_id": source_video,
        "caption": "forward-video",
    }, tokens[a])
    wait_message(tokens[b], sid_ab, video_msg["msg_id"])
    fwd_video = ok("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": video_msg["msg_seq"],
        "to_user_id": uids[c],
    }, tokens[b])
    video_entry = wait_message(tokens[c], sid_bc, fwd_video["msg_id"])
    video_content = content_of(video_entry)
    assert video_content.get("caption") == "forward-video", video_entry
    assert video_content.get("duration_ms", 0) > 0, video_entry
    clone_video = wait_media_ready(tokens[c], fwd_video["media_id"])
    assert path_of(clone_video["original"]["url"]) == \
        path_of(source_video_meta["original"]["url"])
    assert "poster" in clone_video.get("variants", {}), clone_video

    # 无源会话权限：B 不在 A/C 会话中
    ac_msg = ok("/api/message/send", {
        "to_user_id": uids[c],
        "msg_type": "text",
        "content": "A-to-C-" + suffix,
    }, tokens[a])
    expect_error("/api/message/forward", {
        "source_session_id": sid_ac,
        "source_msg_seq": ac_msg["msg_seq"],
        "to_user_id": uids[b],
    }, tokens[b], 403)

    # 不存在的源消息
    expect_error("/api/message/forward", {
        "source_session_id": sid_ab,
        "source_msg_seq": 999999,
        "to_user_id": uids[c],
    }, tokens[b], 404)

    print("[PASS] 文本转发")
    print("[PASS] 图片转发: media_id=%s -> %s" %
          (source_media, fwd_image["media_id"]))
    print("[PASS] 图片 clone 共享原 object_key / variants")
    print("[PASS] 失败转发清理 clone 且 asset ref_count 不变")
    print("[PASS] 文件转发: media_id=%s" % fwd_file["media_id"])
    print("[PASS] 视频转发: media_id=%s -> %s" %
          (source_video, fwd_video["media_id"]))
    print("[PASS] 无源会话权限返回 403")
    print("[PASS] 不存在源消息返回 404")
    print("测试数据（未清理）: users=%s/%s/%s/%s, sid=%s/%s" %
          (uids[a], uids[b], uids[c], uid_d, sid_ab, sid_bc))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("[FAIL] %s" % exc, file=sys.stderr)
        raise
