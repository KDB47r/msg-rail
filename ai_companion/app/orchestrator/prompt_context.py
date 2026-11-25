"""Small dynamic system-prompt fragments shared by all chat entry points."""

from datetime import datetime
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Shanghai"
_WEEKDAYS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def current_date_line(timezone: str = DEFAULT_TIMEZONE) -> str:
    try:
        now = datetime.now(ZoneInfo(timezone))
    except Exception:
        now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    return (
        f"当前时间：{now:%Y-%m-%d %H:%M:%S}"
        f"（{_WEEKDAYS[now.weekday()]}，{timezone}）。"
    )
