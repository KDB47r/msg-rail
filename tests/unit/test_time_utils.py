from app.tools.time_utils import format_local_time


def test_format_local_time_includes_weekday_timezone_and_offset():
    text = format_local_time("Asia/Shanghai")

    assert "星期" in text
    assert "Asia/Shanghai" in text
    assert "UTC+08:00" in text
