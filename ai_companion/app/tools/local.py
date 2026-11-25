from app.llm.base import ToolSpec
from app.tools.base import LocalTool
from app.tools.registry import ToolRegistry
from app.tools.time_utils import format_local_time


async def get_current_time(timezone: str = "Asia/Shanghai") -> str:
    return format_local_time(timezone)


def register_local_tools(registry: ToolRegistry) -> None:
    spec = ToolSpec(
        name="local__get_current_time",
        description="获取指定时区的当前日期、星期、时间和 UTC 偏移。",
        parameters={
            "type": "object",
            "properties": {
                "timezone": {
                    "type": "string",
                    "description": "IANA 时区，例如 Asia/Shanghai",
                }
            },
            "required": [],
        },
    )
    tool = LocalTool(spec, get_current_time)
    registry.register(
        spec,
        tool.execute,
        server="local",
        permission="read",
    )
