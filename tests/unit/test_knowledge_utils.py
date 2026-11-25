import json

import pytest

from app.config import Settings
from app.tools.knowledge_utils import _parse_feed, rss_list, rss_read


def test_parse_rss_and_atom():
    rss = """<?xml version="1.0"?><rss><channel>
      <item><title>RSS 标题</title><link>https://example.com/r</link>
      <description>摘要</description><pubDate>Mon, 01 Jan 2026</pubDate></item>
    </channel></rss>""".encode()
    atom = """<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
      <entry><title>Atom 标题</title><link href="https://example.com/a"/>
      <summary>摘要</summary><updated>2026-01-01</updated></entry>
    </feed>""".encode()
    assert _parse_feed(rss)[0]["title"] == "RSS 标题"
    assert _parse_feed(atom)[0]["link"] == "https://example.com/a"


def test_rss_list_and_missing_feed(tmp_path):
    path = tmp_path / "feeds.json"
    path.write_text(
        json.dumps(
            {"feeds": [{"id": "x", "name": "X", "url": "https://example.com/rss"}]}
        ),
        encoding="utf-8",
    )
    settings = Settings(rss_feeds_path=str(path))
    assert "x" in rss_list(settings)


@pytest.mark.asyncio
async def test_rss_read_rejects_unknown_feed(tmp_path):
    path = tmp_path / "feeds.json"
    path.write_text(json.dumps({"feeds": []}), encoding="utf-8")
    settings = Settings(rss_feeds_path=str(path))
    text = await rss_read("missing", 3, settings)
    assert text.startswith("ERROR[RSS_FEED_NOT_FOUND]")
