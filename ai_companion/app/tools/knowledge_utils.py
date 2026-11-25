"""Wikipedia and RSS tools. No API key is required."""

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx

from app.config import Settings
from app.tools.web_utils import (
    USER_AGENT,
    WebToolError,
    cache_get,
    cache_set,
    validate_public_url,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_RSS_BYTES = 1_000_000
_LANG_RE = re.compile(r"^[a-z]{2,3}$")


def _feeds_path(settings: Settings) -> Path:
    path = Path(settings.rss_feeds_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _load_feeds(settings: Settings) -> list[dict[str, str]]:
    path = _feeds_path(settings)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WebToolError("ERROR[RSS_CONFIG]: RSS 配置读取失败") from exc
    feeds = data.get("feeds", [])
    return [
        {
            "id": str(item.get("id", "")),
            "name": str(item.get("name", "")),
            "url": str(item.get("url", "")),
        }
        for item in feeds
        if isinstance(item, dict) and item.get("id") and item.get("url")
    ]


def rss_list(settings: Settings) -> str:
    feeds = _load_feeds(settings)
    if not feeds:
        return "ERROR[RSS_NOT_CONFIGURED]: 还没有配置 RSS 源，请编辑 config/rss_feeds.json"
    return "可用的 RSS 源：\n" + "\n".join(
        f"- {item['id']}：{item['name']}" for item in feeds
    )


def _strip_html(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _child_text(node: ET.Element, names: set[str]) -> str:
    for child in node:
        if _local_name(child.tag) in names and child.text:
            return child.text.strip()
    return ""


def _parse_feed(data: bytes) -> list[dict[str, str]]:
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise WebToolError("ERROR[RSS_PARSE]: RSS XML 解析失败") from exc

    items = [
        node
        for node in root.iter()
        if _local_name(node.tag) in {"item", "entry"}
    ]

    parsed: list[dict[str, str]] = []
    for item in items:
        title = _child_text(item, {"title"})
        link = _child_text(item, {"link"})
        if not link:
            for child in item:
                if _local_name(child.tag) == "link":
                    link = child.attrib.get("href", "").strip()
                    if link:
                        break
        description = _child_text(item, {"description", "summary", "content"})
        published = _child_text(
            item,
            {"pubdate", "published", "updated", "date"},
        )
        if title or link:
            parsed.append(
                {
                    "title": _strip_html(title),
                    "link": link,
                    "description": _strip_html(description),
                    "published": published,
                }
            )
    return parsed


async def rss_read(feed_id: str, limit: int, settings: Settings) -> str:
    feeds = _load_feeds(settings)
    feed = next((item for item in feeds if item["id"] == feed_id), None)
    if feed is None:
        return f"ERROR[RSS_FEED_NOT_FOUND]: 未配置的 feed_id：{feed_id}"
    limit = max(1, min(limit, 10))
    cache_key = f"rss:{feed_id}:{limit}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    await validate_public_url(feed["url"])
    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = await client.get(feed["url"])
            resp.raise_for_status()
            data = resp.content[:MAX_RSS_BYTES]
    except httpx.TimeoutException as exc:
        raise WebToolError("ERROR[RSS_TIMEOUT]: RSS 请求超时") from exc
    except httpx.HTTPError as exc:
        raise WebToolError("ERROR[RSS_UNAVAILABLE]: RSS 请求失败") from exc

    items = _parse_feed(data)[:limit]
    if not items:
        text = f"{feed['name']} 暂时没有可读取的条目。"
    else:
        lines = [f"{feed['name']} 最新 {len(items)} 条："]
        for index, item in enumerate(items, 1):
            lines.append(f"{index}. {item['title'] or '(无标题)'}")
            if item["link"]:
                lines.append(f"   {item['link']}")
            if item["description"]:
                lines.append(f"   摘要：{item['description'][:300]}")
        text = "\n".join(lines)
    cache_set(cache_key, text, settings.tool_cache_ttl_seconds)
    return text


def _parse_wikipedia_page(payload: dict) -> tuple[str, str, str]:
    pages = payload.get("query", {}).get("pages", {})
    if not pages:
        return "", "", ""
    page = next(iter(pages.values()))
    title = str(page.get("title", ""))
    extract = str(page.get("extract", ""))
    url = str(page.get("fullurl", ""))
    return title, extract, url


async def wikipedia_summary(
    query: str,
    lang: str,
    timeout: float,
    settings: Settings,
) -> str:
    query = query.strip()
    if not query:
        raise WebToolError("ERROR[INVALID_QUERY]: 查询词为空")
    lang = lang if _LANG_RE.match(lang) else settings.wikipedia_lang
    cache_key = f"wiki:{lang}:{query}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    params = {
        "action": "query",
        "generator": "search",
        "gsrsearch": query,
        "gsrlimit": 1,
        "prop": "extracts|info",
        "inprop": "url",
        "exintro": 1,
        "explaintext": 1,
        "redirects": 1,
        "format": "json",
        "utf8": 1,
    }
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            resp = await client.get(
                f"https://{lang}.wikipedia.org/w/api.php",
                params=params,
            )
            resp.raise_for_status()
            payload = resp.json()
    except httpx.TimeoutException as exc:
        raise WebToolError("ERROR[WIKIPEDIA_TIMEOUT]: 维基百科请求超时") from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise WebToolError("ERROR[WIKIPEDIA_UNAVAILABLE]: 维基百科不可用") from exc

    title, extract, url = _parse_wikipedia_page(payload)
    if not title or not extract:
        return f"没有找到与“{query}”相关的维基百科条目。"
    extract = extract.strip()
    if len(extract) > 1500:
        extract = extract[:1500].rstrip() + "……"
    text = (
        f"维基百科：{title}\n"
        f"{extract}\n"
        f"来源：{url or 'https://' + lang + '.wikipedia.org'}"
    )
    cache_set(cache_key, text, max(settings.tool_cache_ttl_seconds, 3600))
    return text
