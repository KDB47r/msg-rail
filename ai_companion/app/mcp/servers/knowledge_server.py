from mcp.server.fastmcp import FastMCP

from app.config import Settings
from app.tools import knowledge_utils

mcp = FastMCP("knowledge")
_settings = Settings()


if _settings.wikipedia_enabled:
    @mcp.tool()
    async def wikipedia(query: str, lang: str = "zh") -> str:
        """查询维基百科条目的简洁摘要；需要网络可访问 Wikipedia。"""
        settings = Settings()
        return await knowledge_utils.wikipedia_summary(
            query,
            lang or settings.wikipedia_lang,
            settings.web_search_timeout_seconds,
            settings,
        )


@mcp.tool()
def rss_list() -> str:
    """列出已配置的 RSS/新闻源。"""
    return knowledge_utils.rss_list(Settings())


@mcp.tool()
async def rss_read(feed_id: str, limit: int = 5) -> str:
    """读取指定 RSS 源的最新条目；feed_id 必须来自 rss_list。"""
    settings = Settings()
    return await knowledge_utils.rss_read(feed_id, limit, settings)


if __name__ == "__main__":
    mcp.run()
