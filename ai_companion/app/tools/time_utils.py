from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

WEEKDAYS = (
    "星期一",
    "星期二",
    "星期三",
    "星期四",
    "星期五",
    "星期六",
    "星期日",
)


def format_local_time(timezone: str) -> str:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {timezone}") from exc

    now = datetime.now(zone)
    offset = now.strftime("%z")
    offset_text = f"{offset[:3]}:{offset[3:]}" if offset else ""

    return (
        f"{now:%Y-%m-%d} {WEEKDAYS[now.weekday()]} "
        f"{now:%H:%M:%S} {zone.key} (UTC{offset_text})"
    )
