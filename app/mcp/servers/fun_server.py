from mcp.server.fastmcp import FastMCP

from app.tools import fun_utils
from app.tools.calculator import format_calculation

mcp = FastMCP("fun")


@mcp.tool()
def calculator(expression: str) -> str:
    """计算数学表达式，例如 (138*27+15)/3、sqrt(2)、15% of 200 请写成 200*0.15。"""
    return format_calculation(expression)


@mcp.tool()
def random_picker(options: list[str], count: int = 1) -> str:
    """从 2~20 个选项里随机选出 1~5 个，适合决定吃什么、谁先来。"""
    return fun_utils.random_pick(options, count)


@mcp.tool()
def roll_dice(sides: int = 6, count: int = 1) -> str:
    """掷骰子；sides 为面数（2~100），count 为次数（1~10）。"""
    return fun_utils.roll_dice(sides, count)


@mcp.tool()
def holiday(date: str | None = None) -> str:
    """查询某天是否是本地节日，或距离下一个节日还有几天；日期格式 YYYY-MM-DD。"""
    return fun_utils.holiday_info(date)


@mcp.tool()
def trivia(topic: str | None = None) -> str:
    """给出一条本地冷知识/脑筋急转弯，可按主题筛选。"""
    return fun_utils.trivia(topic)


if __name__ == "__main__":
    mcp.run()
