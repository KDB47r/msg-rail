#!/usr/bin/env python3
"""媒体元数据 Redis 缓存 + 变体 CDN benchmark（只读）。

流程：
  1. 登录并拉一次 history，收集媒体 ID；
  2. 删除这些媒体的 Redis 缓存 key，测冷缓存；
  3. 连续请求 history，测热缓存；
  4. 检查 history 内嵌的变体 URL 是否为公开 CDN URL、原图是否仍预签名；
  5. 匿名访问 thumb（应 200）和原图无签名 URL（应 403）；
  6. 读取 Redis / MySQL 统计，估算缓存命中和媒体 SQL 次数。

只删除 media:meta:* 测试缓存 key，不修改 MySQL/对象存储数据。
"""

from __future__ import annotations

import argparse
import hashlib
import statistics
import subprocess
import sys
import time

import requests

LOGIC_BASE = "http://127.0.0.1:9101"
TIMEOUT = 60


def load_conf(path):
    conf = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                conf[key.strip()] = value.strip()
    except OSError:
        pass
    return conf


def percentile(values, p):
    if not values:
        return 0.0
    data = sorted(values)
    pos = (len(data) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    frac = pos - lo
    return data[lo] + (data[hi] - data[lo]) * frac


def redis_cmd(conf):
    cmd = [
        "redis-cli",
        "-h", conf.get("redis_host", "127.0.0.1"),
        "-p", conf.get("redis_port", "6379"),
        "-n", conf.get("redis_db", "0"),
    ]
    password = conf.get("redis_password", "")
    if password:
        cmd.extend(["-a", password])
    return cmd


def redis_run(conf, *args):
    try:
        result = subprocess.run(
            redis_cmd(conf) + list(args),
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def redis_stats(conf):
    out = redis_run(conf, "INFO", "stats")
    stats = {}
    for line in out.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            if key in ("keyspace_hits", "keyspace_misses"):
                try:
                    stats[key] = int(value)
                except ValueError:
                    pass
    return stats or None


def mysql_cmd(conf):
    cmd = [
        "mysql", "-N", "-B",
        "-h", conf.get("mysql_host", "127.0.0.1"),
        "-P", conf.get("mysql_port", "3306"),
        "-u", conf.get("mysql_user", "root"),
    ]
    password = conf.get("mysql_password", "")
    if password:
        cmd.append("-p" + password)
    cmd.append(conf.get("mysql_db", "spark_push"))
    return cmd


def mysql_status_value(conf, name):
    # mysql -N -B 输出形如 "Com_select\t123"。
    try:
        result = subprocess.run(
            mysql_cmd(conf) + ["-e", "SHOW GLOBAL STATUS LIKE '%s'" % name],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None
        line = result.stdout.strip()
        if "\t" not in line:
            return None
        return int(line.split("\t", 1)[1])
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


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
    return http_post("/api/login", {"account": account, "password": digest})


def collect_media_ids(messages):
    ids = []
    seen = set()
    for message in messages:
        media = message.get("media")
        if isinstance(media, dict) and media.get("media_id"):
            media_id = media["media_id"]
        else:
            content = message.get("content")
            if not isinstance(content, dict):
                continue
            candidate = content
            if not candidate.get("media_id") and isinstance(
                    content.get("content"), dict):
                candidate = content["content"]
            media_id = candidate.get("media_id")
        if media_id and media_id not in seen:
            seen.add(media_id)
            ids.append(media_id)
    return ids


def collect_embedded_media(messages):
    media_items = []
    for message in messages:
        media = message.get("media")
        if isinstance(media, dict) and media.get("status") == "ready":
            media_items.append(media)
    return media_items


def cache_key(media_id):
    return "media:meta:v1:{" + media_id + "}"


def timed_history(token, session_id, limit):
    start = time.perf_counter()
    data = http_post("/api/session/history", {
        "session_id": session_id,
        "anchor_seq": 0,
        "limit": limit,
    }, token)
    return (time.perf_counter() - start) * 1000.0, data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="conf/logic.conf")
    parser.add_argument("--session", default="single:1:2")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--warm-runs", type=int, default=8)
    parser.add_argument("--media-runs", type=int, default=20)
    parser.add_argument("--account", default="alice")
    parser.add_argument("--password", default="123456")
    args = parser.parse_args()

    conf = load_conf(args.config)
    login_data = login(args.account, args.password)
    token = login_data["token"]

    _, history = timed_history(token, args.session, args.limit)
    messages = history.get("messages") or []
    media_ids = collect_media_ids(messages)
    if not media_ids:
        print("会话中没有媒体，无法测试缓存", file=sys.stderr)
        return 1

    keys = [cache_key(media_id) for media_id in media_ids]
    deleted = redis_run(conf, "DEL", *keys)
    print("媒体数: %d, 已删除缓存 key: %s" % (len(media_ids), deleted))

    redis0 = redis_stats(conf)
    sql_before_cold = mysql_status_value(conf, "Com_select")

    cold_ms, cold_history = timed_history(token, args.session, args.limit)

    redis1 = redis_stats(conf)
    sql_after_cold = mysql_status_value(conf, "Com_select")
    cold_sql = None
    if sql_before_cold is not None and sql_after_cold is not None:
        cold_sql = sql_after_cold - sql_before_cold

    warm_ms = []
    warm_sqls = []
    warm_history = cold_history
    for _ in range(args.warm_runs):
        before = mysql_status_value(conf, "Com_select")
        ms, warm_history = timed_history(token, args.session, args.limit)
        after = mysql_status_value(conf, "Com_select")
        warm_ms.append(ms)
        if before is not None and after is not None:
            warm_sqls.append(after - before)

    redis2 = redis_stats(conf)

    media_times = []
    for _ in range(args.media_runs):
        start = time.perf_counter()
        http_get("/api/media/" + media_ids[0], token)
        media_times.append((time.perf_counter() - start) * 1000.0)

    embedded = collect_embedded_media(
        warm_history.get("messages") or [])
    variant_urls = []
    original_urls = []
    variants_with_expiry = 0
    for media in embedded:
        for variant in (media.get("variants") or {}).values():
            url = variant.get("url") or ""
            if url:
                variant_urls.append(url)
            if "expires_at" in variant:
                variants_with_expiry += 1
        original = media.get("original") or {}
        if original.get("url"):
            original_urls.append(original["url"])

    public_variants = [u for u in variant_urls if "X-Amz-Signature" not in u]
    presigned_variants = [u for u in variant_urls if "X-Amz-Signature" in u]
    presigned_originals = [u for u in original_urls
                           if "X-Amz-Signature" in u]

    # 优先选最近上传、带 Cache-Control 的 thumb；旧对象可能没有这个头。
    anon_thumb_status = None
    thumb_cache_control = ""
    thumb_candidates = [u for u in public_variants if "/thumb." in u]
    for candidate in reversed(thumb_candidates):
        resp = requests.get(candidate, timeout=TIMEOUT)
        if anon_thumb_status is None:
            anon_thumb_status = resp.status_code
        cache_control = resp.headers.get("Cache-Control", "")
        if resp.status_code == 200 and cache_control:
            anon_thumb_status = resp.status_code
            thumb_cache_control = cache_control
            break

    original_status = None
    if original_urls:
        unsigned_original = original_urls[0].split("?", 1)[0]
        resp = requests.get(unsigned_original, timeout=TIMEOUT)
        original_status = resp.status_code

    ttls = []
    for media_id in media_ids[:5]:
        ttl = redis_run(conf, "TTL", cache_key(media_id))
        if ttl:
            try:
                ttls.append(int(ttl))
            except ValueError:
                pass

    def avg(values):
        return statistics.mean(values) if values else 0.0

    print("\n| 指标 | 结果 |")
    print("|---|---:|")
    print("| 冷缓存 history | %.1fms |" % cold_ms)
    print("| 热缓存 history 平均 | %.1fms |" % avg(warm_ms))
    print("| 热缓存 history P95 | %.1fms |" % percentile(warm_ms, 95))
    print("| 单张 media GET 平均（热缓存） | %.1fms |" % avg(media_times))
    print("| 单张 media GET P95（热缓存） | %.1fms |" %
          percentile(media_times, 95))
    print("| 内嵌媒体数 | %d |" % len(embedded))
    print("| 公开变体 URL 数 | %d |" % len(public_variants))
    print("| 预签名变体 URL 数 | %d |" % len(presigned_variants))
    print("| 带 expires_at 的公开变体数 | %d |" % variants_with_expiry)
    print("| 预签名原图 URL 数 | %d |" % len(presigned_originals))
    print("| 匿名 thumb HTTP | %s |" % anon_thumb_status)
    print("| 无签名原图 HTTP | %s |" % original_status)
    print("| thumb Cache-Control | %s |" % (thumb_cache_control or "-"))
    if ttls:
        print("| 媒体缓存 TTL 范围 | %d~%ds |" % (min(ttls), max(ttls)))

    if redis0 and redis1 and redis2:
        cold_hits = redis1.get("keyspace_hits", 0) - redis0.get("keyspace_hits", 0)
        cold_misses = redis1.get("keyspace_misses", 0) - redis0.get("keyspace_misses", 0)
        warm_hits = redis2.get("keyspace_hits", 0) - redis1.get("keyspace_hits", 0)
        warm_misses = redis2.get("keyspace_misses", 0) - redis1.get("keyspace_misses", 0)
        print("| Redis 冷缓存 hits/misses | %d / %d |" % (cold_hits, cold_misses))
        print("| Redis 热缓存 hits/misses | %d / %d |" % (warm_hits, warm_misses))

    if cold_sql is not None:
        print("| 冷缓存 Com_select 增量 | %d |" % cold_sql)
    if warm_sqls:
        warm_sql_avg = avg(warm_sqls)
        print("| 热缓存 Com_select 平均 | %.1f |" % warm_sql_avg)
        if cold_sql is not None:
            print("| 冷/热差值（约媒体批量查询） | %.1f |" %
                  (cold_sql - warm_sql_avg))

    if not thumb_cache_control:
        print("\n说明：当前会话的 thumb 都是启用 Cache-Control 之前上传的旧对象；"
              "新对象由 E2E 上传后用 curl 验证。")
    print("\n说明：冷缓存通过删除 media:meta:v1:* 测试 key 触发；"
          "Redis 统计包含 token GET；Com_select 包含消息查询和后台轮询，"
          "冷/热差值对应 media_object + media_variant 两条批量查询。")

    cdn_enabled = bool(conf.get("media_variant_cdn_base_url", ""))
    failures = []
    if not embedded:
        failures.append("history 没有内嵌 ready 媒体")
    if cdn_enabled:
        if not public_variants:
            failures.append("CDN 已启用但没有公开变体 URL")
        if presigned_variants:
            failures.append("CDN 已启用但仍有预签名变体 URL")
        if variants_with_expiry:
            failures.append("公开变体不应带 expires_at")
        if anon_thumb_status != 200:
            failures.append("匿名 thumb 访问失败: %s" % anon_thumb_status)
    if original_urls:
        if len(presigned_originals) != len(original_urls):
            failures.append("存在非预签名原图 URL")
        if original_status != 403:
            failures.append("无签名原图不应可访问: %s" % original_status)

    if failures:
        print("\n[FAIL] " + "; ".join(failures), file=sys.stderr)
        return 2
    print("\n[PASS] 缓存与 CDN 校验通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
