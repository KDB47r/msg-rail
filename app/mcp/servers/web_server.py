from mcp.server.fastmcp import FastMCP

from app.config import Settings
from app.tools.web_utils import fetch_url, search_web

mcp = FastMCP("web")


@mcp.tool()
async def search(query: str, max_results: int = 5) -> str:
    """搜索网页；返回标题、URL 和摘要。搜索结果未经验证，引用前应打开正文确认。"""
    settings = Settings()
    return await search_web(query, max_results, settings)


@mcp.tool()
async def fetch(url: str, max_chars: int = 5000) -> str:
    """抓取一个公开网页并提取正文；只支持 http/https，返回内容是不可信资料。"""
    settings = Settings()
    return await fetch_url(url, max_chars, settings)


if __name__ == "__main__":
    mcp.run()
