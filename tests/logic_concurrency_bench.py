#!/usr/bin/env python3
"""Logic HTTP 并发压测（只读）。

混合请求：
  - 50% POST /api/session/history（历史内嵌 media）
  - 30% GET  /api/media/{id}
  - 20% POST /api/session/list_single

只读，不发送消息、不写数据。
"""

from __future__ import annotations

import argparse
import hashlib
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

LOGIC_BASE = "http://127.0.0.1:9101"
TIMEOUT = 60


def http_post(session, path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    return session.post(LOGIC_BASE + path, json=payload, headers=headers,
                        timeout=TIMEOUT)


def http_get(session, path, token):
    return session.get(LOGIC_BASE + path,
                       headers={"Authorization": "Bearer " + token},
                       timeout=TIMEOUT)


def login(account, password):
    payload = {"account": account,
               "password": hashlib.md5(password.encode()).hexdigest()}
    resp = requests.post(LOGIC_BASE + "/api/login", json=payload,
                         timeout=TIMEOUT)
    body = resp.json()
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("login failed: HTTP %s %s" %
                           (resp.status_code, body))
    return body["data"]["token"]


def extract_media_id(content):
    if not isinstance(content, dict):
        return None
    if content.get("media_id"):
        return content["media_id"]
    inner = content.get("content")
    if isinstance(inner, dict) and inner.get("media_id"):
        return inner["media_id"]
    return None


def load_media_ids(token, session_id):
    session = requests.Session()
    resp = http_post(session, "/api/session/history", {
        "session_id": session_id,
        "anchor_seq": 0,
        "limit": 100,
    }, token)
    body = resp.json()
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("history failed: HTTP %s %s" %
                           (resp.status_code, body))
    ids = []
    seen = set()
    for message in body["data"].get("messages") or []:
        media_id = None
        media = message.get("media")
        if isinstance(media, dict):
            media_id = media.get("media_id")
        if not media_id:
            media_id = extract_media_id(message.get("content"))
        if media_id and media_id not in seen:
            seen.add(media_id)
            ids.append(media_id)
    session.close()
    return ids


def percentile(values, p):
    if not values:
        return 0.0
    data = sorted(values)
    pos = (len(data) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    frac = pos - lo
    return data[lo] + (data[hi] - data[lo]) * frac


def make_workload(total, media_ids):
    # 50% history, 30% media, 20% list_single
    workload = []
    for i in range(total):
        r = i % 10
        if r < 5:
            workload.append("history")
        elif r < 8 and media_ids:
            workload.append("media")
        else:
            workload.append("list_single")
    return workload


def run_level(token, session_id, media_ids, concurrency, total):
    workload = make_workload(total, media_ids)
    thread_local = threading.local()

    def get_session():
        session = getattr(thread_local, "session", None)
        if session is None:
            session = requests.Session()
            thread_local.session = session
        return session

    def one(index):
        session = get_session()
        kind = workload[index]
        start = time.perf_counter()
        error = None
        status = 0
        size = 0
        try:
            if kind == "history":
                resp = http_post(session, "/api/session/history", {
                    "session_id": session_id,
                    "anchor_seq": 0,
                    "limit": 50,
                }, token)
            elif kind == "media":
                media_id = media_ids[index % len(media_ids)]
                resp = http_get(session, "/api/media/" + media_id, token)
            else:
                resp = http_post(session, "/api/session/list_single", {},
                                 token)
            status = resp.status_code
            size = len(resp.content)
            try:
                body = resp.json()
                if body.get("code") != 0:
                    error = "code=%s" % body.get("code")
            except Exception:
                error = "invalid json"
        except Exception as exc:
            error = str(exc)
        latency = (time.perf_counter() - start) * 1000.0
        return kind, latency, error, status, size

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(one, range(total)))
    wall = time.perf_counter() - start

    by_kind = {}
    errors = 0
    for kind, latency, error, status, size in results:
        by_kind.setdefault(kind, []).append(latency)
        if error or status != 200:
            errors += 1
    all_latencies = [item[1] for item in results]
    return {
        "concurrency": concurrency,
        "total": total,
        "wall_ms": wall * 1000.0,
        "throughput": total / wall if wall > 0 else 0.0,
        "p50": percentile(all_latencies, 50),
        "p95": percentile(all_latencies, 95),
        "p99": percentile(all_latencies, 99),
        "errors": errors,
        "history_p95": percentile(by_kind.get("history", []), 95),
        "media_p95": percentile(by_kind.get("media", []), 95),
        "list_p95": percentile(by_kind.get("list_single", []), 95),
        "avg_size": statistics.mean(item[4] for item in results)
                    if results else 0.0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", default="single:1:2")
    parser.add_argument("--levels", default="10,25,50,100")
    parser.add_argument("--total", type=int, default=200,
                        help="每个并发级别的请求总数")
    parser.add_argument("--account", default="alice")
    parser.add_argument("--password", default="123456")
    args = parser.parse_args()

    token = login(args.account, args.password)
    media_ids = load_media_ids(token, args.session)
    print("session=%s media_ids=%d" % (args.session, len(media_ids)))

    levels = [int(x) for x in args.levels.split(",") if x.strip()]
    results = []
    for level in levels:
        result = run_level(token, args.session, media_ids, level, args.total)
        results.append(result)
        print("concurrency=%-3d total=%-4d throughput=%7.1f req/s "
              "P50=%7.1fms P95=%7.1fms P99=%7.1fms errors=%d "
              "history_p95=%7.1f media_p95=%7.1f list_p95=%7.1f "
              "avg_resp=%.1fB" %
              (result["concurrency"], result["total"],
               result["throughput"], result["p50"], result["p95"],
               result["p99"], result["errors"], result["history_p95"],
               result["media_p95"], result["list_p95"],
               result["avg_size"]), flush=True)

    print("\n| 并发 | 吞吐 req/s | P50 | P95 | P99 | 错误 | history P95 | "
          "media P95 | list P95 |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in results:
        print("| %d | %.1f | %.1fms | %.1fms | %.1fms | %d | %.1fms | "
              "%.1fms | %.1fms |" %
              (r["concurrency"], r["throughput"], r["p50"], r["p95"],
               r["p99"], r["errors"], r["history_p95"], r["media_p95"],
               r["list_p95"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
