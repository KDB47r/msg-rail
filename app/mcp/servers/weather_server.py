from mcp.server.fastmcp import FastMCP

from app.tools.weather_utils import fetch_weather

mcp = FastMCP("weather")


@mcp.tool()
async def get_weather(city: str) -> str:
    """查询城市真实当前天气（数据来源：Open-Meteo）。"""
    return await fetch_weather(city)


if __name__ == "__main__":
    mcp.run()
