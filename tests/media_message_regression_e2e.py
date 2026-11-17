#!/usr/bin/env python3
"""MsgRail 旧功能回归：历史分页 + 文件/图片/视频消息全链路。

前置条件：
  1. logic(9101)、media_worker、MinIO 已启动；
  2. 测试账号 A/B 已是好友（默认复用 api_user_feature_e2e.py 最近一轮数据）。

覆盖：
  - 文件上传 init -> MinIO PUT -> complete -> ready -> /api/message/send
  - 图片消息（不是头像）绑定到消息
  - 视频上传、异步转码变体、发送视频消息
  - 历史分页 anchor_seq：倒序、不重叠、向前翻页
  - 媒体消息计入未读

脚本不删除任何数据：会新增媒体对象和三条消息。
"""

import base64
import os
import subprocess
import time

import requests

LOGIC_BASE = os.environ.get("LOGIC_BASE", "http://127.0.0.1:9101")
A_ACCOUNT = os.environ.get("MEDIA_A_ACCOUNT", "e2e_a_46804654")
A_PASSWORD = os.environ.get("MEDIA_A_PASSWORD", "e2e-pass-46804654")
B_ACCOUNT = os.environ.get("MEDIA_B_ACCOUNT", "e2e_b_46804654")
B_PASSWORD = os.environ.get("MEDIA_B_PASSWORD", "e2e-pass-46804654")

TIMEOUT = 20
PASS = 0
FAIL = 0
FAILED_NAMES = []

PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print("[PASS] " + name)
        return True
    FAIL += 1
    FAILED_NAMES.append(name)
    print("[FAIL] " + name + ((" | " + str(detail)) if detail else ""))
    return False


def http_post(path, payload=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(LOGIC_BASE + path, json=payload or {},
                         headers=headers, timeout=TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError("%s -> HTTP %s %s" %
                           (path, resp.status_code, resp.text[:200]))
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
    data = http_post("/api/login", {"account": account, "password": password})
    return data["user_id"], data["token"]


def upload_media(token, kind, mime, filename, data):
    init = http_post("/api/media/upload/init", {
        "kind": kind,
        "mime_type": mime,
        "size_bytes": len(data),
        "filename": filename,
    }, token)
    media_id = init["media_id"]
    put = requests.put(init["upload_url"], data=data,
                       headers={"Content-Type": mime}, timeout=120)
    check("%s 原文件直传 MinIO" % kind, put.status_code in (200, 204),
          "HTTP %s" % put.status_code)
    http_post("/api/media/upload/complete", {"media_id": media_id}, token)

    meta = {}
    deadline = time.time() + 90
    while time.time() < deadline:
        meta = http_get("/api/media/" + media_id, token)
        if meta.get("status") == "ready":
            break
        if meta.get("status") == "failed":
            raise RuntimeError("%s 处理失败: %s" %
                               (kind, meta.get("error")))
        time.sleep(0.5)
    check("%s 异步处理 ready" % kind, meta.get("status") == "ready",
          "status=%s" % meta.get("status"))
    return media_id, meta


def history(token, session_id, anchor_seq, limit):
    return http_post("/api/session/history", {
        "session_id": session_id,
        "anchor_seq": anchor_seq,
        "limit": limit,
    }, token)


def find_message(page, msg_id):
    for msg in page.get("messages", []):
        if msg.get("msg_id") == msg_id:
            return msg
    return None


def wait_message(token, session_id, msg_id, timeout=15):
    """消息经 Kafka 异步持久化，轮询历史直到该 msg_id 出现。"""
    deadline = time.time() + timeout
    page = {}
    while time.time() < deadline:
        page = history(token, session_id, 0, 50)
        entry = find_message(page, msg_id)
        if entry is not None:
            return entry
        time.sleep(0.3)
    return find_message(page, msg_id)


def media_content(entry):
    """媒体消息的 content_json 形如 {"content": {...}, "msg_type": ...}。"""
    content = (entry or {}).get("content") or {}
    if isinstance(content, dict) and isinstance(content.get("content"), dict):
        return content["content"]
    return content


def main():
    print("Logic HTTP:", LOGIC_BASE)
    uid_a, token_a = login(A_ACCOUNT, A_PASSWORD)
    uid_b, token_b = login(B_ACCOUNT, B_PASSWORD)
    check("A/B 登录成功", uid_a > 0 and uid_b > 0,
          "a=%s b=%s" % (uid_a, uid_b))

    session_id = "single:%d:%d" % (min(uid_a, uid_b), max(uid_a, uid_b))
    before_unread = int(http_post(
        "/api/session/unread", {"session_id": session_id},
        token_b).get("unread", 0))

    # ---------- 文件消息 ----------
    file_name = "regression-%d.txt" % int(time.time())
    file_bytes = ("hello msgrail file regression %d\n" %
                  int(time.time())).encode()
    file_id, _ = upload_media(token_a, "file", "text/plain",
                              file_name, file_bytes)
    file_msg = http_post("/api/message/send", {
        "to_user_id": uid_b,
        "msg_type": "file",
        "media_id": file_id,
        "caption": "file regression",
    }, token_a)
    check("文件消息发送成功",
          file_msg.get("msg_type") == "file" and
          file_msg.get("media_id") == file_id and
          int(file_msg.get("msg_seq", 0)) > 0, file_msg)

    entry = wait_message(token_b, session_id, file_msg.get("msg_id"))
    check("历史可查到文件消息",
          entry is not None and entry.get("msg_type") == "file" and
          media_content(entry).get("name") == file_name,
          entry)

    # ---------- 图片消息 ----------
    image_name = "regression-%d.png" % int(time.time())
    image_id, image_meta = upload_media(
        token_a, "image", "image/png", image_name, PNG_1X1)
    check("图片消息媒体包含缩略图变体",
          bool(image_meta.get("variants")), image_meta)
    image_msg = http_post("/api/message/send", {
        "to_user_id": uid_b,
        "msg_type": "image",
        "media_id": image_id,
        "caption": "image regression",
    }, token_a)
    check("图片消息发送成功",
          image_msg.get("msg_type") == "image" and
          image_msg.get("media_id") == image_id, image_msg)

    entry = wait_message(token_b, session_id, image_msg.get("msg_id"))
    check("历史可查到图片消息",
          entry is not None and entry.get("msg_type") == "image" and
          media_content(entry).get("type") == "image" and
          media_content(entry).get("media_id") == image_id,
          entry)

    # ---------- 视频消息 ----------
    video_path = "/tmp/msgrail_regression_%d.mp4" % int(time.time())
    subprocess.run([
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc=size=64x64:rate=10",
        "-t", "1", "-pix_fmt", "yuv420p", "-c:v", "libx264",
        "-y", video_path,
    ], check=True)
    with open(video_path, "rb") as f:
        video_bytes = f.read()
    check("ffmpeg 生成测试视频", len(video_bytes) > 0,
          "%d bytes" % len(video_bytes))

    video_name = "regression-%d.mp4" % int(time.time())
    video_id, video_meta = upload_media(
        token_a, "video", "video/mp4", video_name, video_bytes)
    check("视频媒体包含变体",
          bool(video_meta.get("variants")), video_meta)
    video_msg = http_post("/api/message/send", {
        "to_user_id": uid_b,
        "msg_type": "video",
        "media_id": video_id,
        "caption": "video regression",
    }, token_a)
    check("视频消息发送成功",
          video_msg.get("msg_type") == "video" and
          video_msg.get("media_id") == video_id, video_msg)

    entry = wait_message(token_b, session_id, video_msg.get("msg_id"))
    video_content = media_content(entry)
    check("历史可查到视频消息",
          entry is not None and entry.get("msg_type") == "video" and
          video_content.get("type") == "video" and
          video_content.get("media_id") == video_id, entry)
    check("视频消息包含时长/宽高",
          int(video_content.get("duration_ms", 0)) > 0 and
          int(video_content.get("width", 0)) > 0 and
          int(video_content.get("height", 0)) > 0, video_content)

    # ---------- 未读 ----------
    after_unread = int(http_post(
        "/api/session/unread", {"session_id": session_id},
        token_b).get("unread", 0))
    check("三条媒体消息计入未读",
          after_unread == before_unread + 3,
          "before=%s after=%s" % (before_unread, after_unread))

    # ---------- 历史分页 ----------
    page1 = history(token_b, session_id, 0, 3)
    seqs1 = [int(m["msg_seq"]) for m in page1.get("messages", [])]
    check("第一页返回最新 3 条且按序号升序",
          len(seqs1) == 3 and seqs1 == sorted(seqs1), seqs1)

    if len(seqs1) == 3:
        page2 = history(token_b, session_id, min(seqs1), 3)
        seqs2 = [int(m["msg_seq"]) for m in page2.get("messages", [])]
        check("第二页向前翻页且按序号升序",
              len(seqs2) == 3 and seqs2 == sorted(seqs2), seqs2)
        check("两页不重叠且严格更旧",
              set(seqs1).isdisjoint(seqs2) and
              (not seqs2 or max(seqs2) < min(seqs1)),
              "p1=%s p2=%s" % (seqs1, seqs2))

        if len(seqs2) == 3:
            page3 = history(token_b, session_id, min(seqs2), 3)
            seqs3 = [int(m["msg_seq"]) for m in page3.get("messages", [])]
            check("第三页继续向前且不重叠",
                  len(seqs3) == 3 and
                  set(seqs3).isdisjoint(seqs2) and
                  (not seqs3 or max(seqs3) < min(seqs2)),
                  "p2=%s p3=%s" % (seqs2, seqs3))

    # 标记已读，保持未读状态干净（保留消息本身）。
    info = http_post("/api/session/unread", {"session_id": session_id},
                     token_b)
    http_post("/api/session/mark_read", {
        "session_id": session_id,
        "read_seq": int(info.get("last_msg_seq", 0)),
    }, token_b)
    check("媒体回归后未读可归零",
          int(http_post("/api/session/unread", {"session_id": session_id},
                        token_b).get("unread", 0)) == 0)

    print("\n" + "=" * 60)
    print("通过 %d 项，失败 %d 项" % (PASS, FAIL))
    if FAILED_NAMES:
        print("失败项：")
        for name in FAILED_NAMES:
            print("  - " + name)
    print("本次新增数据（未清理）：")
    print("  文件消息 media_id=%s msg_id=%s" %
          (file_id, file_msg.get("msg_id")))
    print("  图片消息 media_id=%s msg_id=%s" %
          (image_id, image_msg.get("msg_id")))
    print("  视频消息 media_id=%s msg_id=%s" %
          (video_id, video_msg.get("msg_id")))
    print("  会话 %s" % session_id)
    print("=" * 60)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
