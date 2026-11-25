import httpx
import pytest
import respx

from app.config import Settings
from app.tools import web_utils
from app.tools.web_utils import (
    WebToolError,
    extract_html_text,
    fetch_url,
    search_web,
    validate_public_url,
)


@pytest.mark.asyncio
async def test_validate_public_url_blocks_private_and_bad_schemes():
    for url in [
        "http://127.0.0.1/",
        "http://localhost/",
        "file:///etc/passwd",
        "http://10.0.0.1/",
    ]:
        with pytest.raises(WebToolError):
            await validate_public_url(url)


def test_extract_html_text_removes_script_and_style():
    title, text = extract_html_text(
        "<html><head><title>标题</title><style>x{}</style></head>"
        "<body><h1>你好</h1><script>alert(1)</script><p>正文</p></body></html>"
    )
    assert title == "标题"
    assert "你好" in text
    assert "正文" in text
    assert "alert" not in text


@pytest.mark.asyncio
async def test_search_without_key_is_explicitly_disabled():
    with pytest.raises(WebToolError) as exc:
        await search_web(
            "测试",
            3,
            Settings(
                _env_file=None,
                search_provider="bocha",
                bocha_api_key="",
            ),
        )
    assert "SEARCH_NOT_CONFIGURED" in str(exc.value)


@pytest.mark.asyncio
@respx.mock
async def test_bocha_search_formats_results(monkeypatch):
    monkeypatch.setattr(web_utils, "validate_public_url", _noop_validate)
    route = respx.post("https://api.bochaai.com/v1/web-search").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "webPages": {
                        "value": [
                            {
                                "name": "示例",
                                "url": "https://example.com/a",
                                "snippet": "摘要",
                            }
                        ]
                    }
                }
            },
        )
    )
    settings = Settings(bocha_api_key="test-key", search_provider="bocha")
    text = await search_web("hello", 3, settings)
    assert route.called
    assert "示例" in text
    assert "https://example.com/a" in text


@pytest.mark.asyncio
@respx.mock
async def test_doubao_search_formats_custom_response():
    endpoint = "https://open.feedcoopapi.com/search_api/web_search"
    route = respx.post(endpoint).mock(
        return_value=httpx.Response(
            200,
            json={
                "ResponseMetadata": {"Error": {"Code": ""}},
                "Result": {
                    "WebResults": [
                        {
                            "Title": "豆包结果",
                            "Url": "https://example.com/doubao",
                            "Snippet": "摘要",
                        }
                    ]
                },
            },
        )
    )
    settings = Settings(
        search_provider="doubao",
        doubao_search_api_key="test-key",
        doubao_search_endpoint=endpoint,
    )
    text = await search_web("hello", 3, settings)
    assert route.called
    assert "豆包结果" in text
    assert "https://example.com/doubao" in text


@pytest.mark.asyncio
@respx.mock
async def test_fetch_extracts_text_and_marks_untrusted(monkeypatch):
    monkeypatch.setattr(web_utils, "validate_public_url", _noop_validate)
    respx.get("https://example.com/page").mock(
        return_value=httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            text="<html><head><title>T</title></head><body>正文</body></html>",
        )
    )
    text = await fetch_url("https://example.com/page", 1000, Settings())
    assert "[UNTRUSTED_WEB_CONTENT]" in text
    assert "标题：T" in text
    assert "正文" in text


async def _noop_validate(url: str) -> str:
    return url
