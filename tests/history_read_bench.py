#!/usr/bin/env python3
"""图片读路径现状 benchmark（优化前）。

模拟前端打开一个会话：
  1. POST /api/session/history
  2. 对历史里的每张图片 GET /api/media/{id}
  3. 对比逐张请求和并发请求
  4. 额外测 POST /api/session/list_single

只读，不写数据。
"""

from __future__ import annotations

import argparse
import hashlib
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import requests

LOGIC_BASE = "http://127.0.0.1:9101"
TIMEOUT = 60


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
    digest = hashlib.md5(password.encode()).hexdigest()
    data = http_post("/api/login", {"account": account, "password": digest})
    return data["user_id"], data["token"]


def percentile(values, p):
    if not values:
        return 0.0
    data = sorted(values)
    pos = (len(data) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    frac = pos - lo
    return data[lo] + (data[hi] - data[lo]) * frac


def collect_embedded_media_ids(messages, kinds):
    ids = set()
    for message in messages:
        if message.get("msg_type") not in kinds:
            continue
        media = message.get("media")
        if isinstance(media, dict):
            media_id = media.get("media_id")
            if media_id and media.get("status") in ("ready", "failed"):
                ids.add(media_id)
    return ids


def collect_media_ids(messages, kinds):
    ids = []
    seen = set()
    for message in messages:
        if message.get("msg_type") not in kinds:
            continue
        content = message.get("content")
        if not isinstance(content, dict):
            continue
        # 历史消息 content 可能是 {type, media_id, ...}，
        # 也可能多包一层 {content: {type, media_id, ...}}。
        candidate = content
        if not (candidate.get("type") in kinds and candidate.get("media_id")):
            inner = content.get("content")
            if isinstance(inner, dict):
                candidate = inner
        if candidate.get("type") not in kinds:
            continue
        media_id = candidate.get("media_id")
        if media_id and media_id not in seen:
            seen.add(media_id)
            ids.append(media_id)
    return ids


def fetch_one_media(token, media_id):
    t0 = time.perf_counter()
    data = http_get("/api/media/" + media_id, token)
    return (time.perf_counter() - t0) * 1000.0, data


def run_once(token, session_id, limit, workers, kinds):
    t0 = time.perf_counter()
    history = http_post("/api/session/history", {
        "session_id": session_id,
        "anchor_seq": 0,
        "limit": limit,
    }, token)
    history_ms = (time.perf_counter() - t0) * 1000.0

    messages = history.get("messages") or []
    all_media_ids = collect_media_ids(messages, kinds)
    embedded_ids = collect_embedded_media_ids(messages, kinds)
    media_ids = [media_id for media_id in all_media_ids
                 if media_id not in embedded_ids]
    image_msgs = sum(1 for m in messages if m.get("msg_type") == "image")

    # 逐张请求，模拟最保守的前端实现。
    seq_start = time.perf_counter()
    seq_times = []
    for media_id in media_ids:
        ms, _ = fetch_one_media(token, media_id)
        seq_times.append(ms)
    seq_ms = (time.perf_counter() - seq_start) * 1000.0

    # 并发请求，模拟浏览器同时 hydrate 多张图。
    conc_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_one_media, token, media_id)
                   for media_id in media_ids]
        conc_times = [f.result()[0] for f in futures]
    conc_ms = (time.perf_counter() - conc_start) * 1000.0

    t0 = time.perf_counter()
    sessions = http_post("/api/session/list_single", {}, token)
    session_list_ms = (time.perf_counter() - t0) * 1000.0
    session_items = sessions.get("sessions") or []
    session_count = len(session_items)
    avatar_url_count = sum(1 for s in session_items
                           if s.get("peer_avatar_url"))

    return {
        "history_ms": history_ms,
        "message_count": len(messages),
        "image_msgs": image_msgs,
        "media_count": len(all_media_ids),
        "embedded_count": len(embedded_ids),
        "fetched_count": len(media_ids),
        "media_avg_ms": statistics.mean(seq_times) if seq_times else 0.0,
        "media_p95_ms": percentile(seq_times, 95) if seq_times else 0.0,
        "sequential_ms": seq_ms,
        "concurrent_ms": conc_ms,
        "concurrent_avg_ms": statistics.mean(conc_times) if conc_times else 0.0,
        "session_list_ms": session_list_ms,
        "session_count": session_count,
        "avatar_url_count": avatar_url_count,
        "history_plus_seq_ms": history_ms + seq_ms,
        "history_plus_conc_ms": history_ms + conc_ms,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", default="single:1:2")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--kinds", default="image",
                        help="逗号分隔，如 image,video,file")
    parser.add_argument("--account", default="alice")
    parser.add_argument("--password", default="123456")
    args = parser.parse_args()
    kinds = {x.strip() for x in args.kinds.split(",") if x.strip()}

    _, token = login(args.account, args.password)
    runs = []
    for i in range(args.repeat):
        result = run_once(token, args.session, args.limit, args.workers, kinds)
        runs.append(result)
        print("run %d: history=%.1fms messages=%d images=%d media=%d "
              "embedded=%d fetched=%d media_avg=%.1fms sequential=%.1fms "
              "concurrent=%.1fms history+conc=%.1fms list_single=%.1fms "
              "sessions=%d avatars=%d" %
              (i + 1, result["history_ms"], result["message_count"],
               result["image_msgs"], result["media_count"],
               result["embedded_count"], result["fetched_count"],
               result["media_avg_ms"], result["sequential_ms"],
               result["concurrent_ms"], result["history_plus_conc_ms"],
               result["session_list_ms"], result["session_count"],
               result["avatar_url_count"]),
              flush=True)

    def avg(key):
        return statistics.mean(r[key] for r in runs)

    print("\n| 指标 | 平均 |")
    print("|---|---:|")
    print("| history 消息数 | %.0f |" % avg("message_count"))
    print("| history 图片消息数 | %.0f |" % avg("image_msgs"))
    print("| 去重 media 数 | %.0f |" % avg("media_count"))
    print("| 已内嵌 media 数 | %.0f |" % avg("embedded_count"))
    print("| 仍需 GET 的 media 数 | %.0f |" % avg("fetched_count"))
    print("| history API | %.1fms |" % avg("history_ms"))
    print("| 单张 media API 平均 | %.1fms |" % avg("media_avg_ms"))
    print("| 单张 media API P95 | %.1fms |" % avg("media_p95_ms"))
    print("| 逐张请求总耗时（未内嵌） | %.1fms |" % avg("sequential_ms"))
    print("| 并发 %d 请求总耗时（未内嵌） | %.1fms |" %
          (args.workers, avg("concurrent_ms")))
    print("| history + 逐张 | %.1fms |" % avg("history_plus_seq_ms"))
    print("| history + 并发 | %.1fms |" % avg("history_plus_conc_ms"))
    print("| list_single | %.1fms |" % avg("session_list_ms"))
    print("| 会话数 | %.0f |" % avg("session_count"))
    print("| 头像 URL 数 | %.0f |" % avg("avatar_url_count"))
    print("\n说明：只统计 HTTP 往返；不包含前端渲染、图片字节下载。")
    print("预计 SQL：history 约 2 + 2×media，list_single 约 2 + 2×会话（头像）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
