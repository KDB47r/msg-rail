from app.tools.weather_utils import format_weather, weather_description


def test_weather_code_mapping():
    assert weather_description(0) == "晴"
    assert weather_description(61) == "小雨"
    assert "未知天气代码" in weather_description(999)


def test_format_weather_uses_place_and_current_values():
    place = {
        "name": "太原",
        "admin1": "山西",
        "country": "中国",
    }
    current = {
        "temperature_2m": 5.7,
        "relative_humidity_2m": 64,
        "wind_speed_10m": 2.8,
        "weather_code": 0,
        "time": "2026-11-22T17:00",
    }

    text = format_weather(place, current)

    assert "太原" in text
    assert "晴" in text
    assert "5.7" in text
    assert "64" in text
    assert "Open-Meteo" in text
