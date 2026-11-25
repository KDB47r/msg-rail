from mcp.server.fastmcp import FastMCP

from app.tools.time_utils import format_local_time

mcp = FastMCP("time")


@mcp.tool()
def get_current_time(timezone: str = "Asia/Shanghai") -> str:
    """获取指定 IANA 时区的当前日期、星期、时间和 UTC 偏移。"""
    return format_local_time(timezone)


if __name__ == "__main__":
    mcp.run()
