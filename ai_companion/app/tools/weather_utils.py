import httpx

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

WEATHER_CODES = {
    0: "晴",
    1: "大部晴朗",
    2: "多云",
    3: "阴",
    45: "雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "毛毛雨",
    55: "大毛毛雨",
    56: "冻毛毛雨",
    57: "强冻毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    66: "冻雨",
    67: "强冻雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    77: "雪粒",
    80: "小阵雨",
    81: "阵雨",
    82: "强阵雨",
    85: "小阵雪",
    86: "强阵雪",
    95: "雷暴",
    96: "雷暴伴小冰雹",
    99: "雷暴伴大冰雹",
}


def weather_description(code: int) -> str:
    return WEATHER_CODES.get(code, f"未知天气代码 {code}")


def format_weather(place: dict, current: dict) -> str:
    city = place.get("name") or "未知城市"
    admin1 = place.get("admin1")
    country = place.get("country")
    location = "，".join(part for part in (admin1, country) if part)
    location_text = f"（{location}）" if location else ""

    code = int(current.get("weather_code", -1))
    temperature = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")
    wind = current.get("wind_speed_10m")
    observed_at = current.get("time") or "未知"

    weather = weather_description(code)
    return (
        f"{city}{location_text}当前天气：{weather}，"
        f"{temperature}℃，湿度 {humidity}%，风速 {wind} km/h。"
        f"数据来源：Open-Meteo（真实数据），观测时间：{observed_at}。"
    )


async def fetch_weather(city: str, *, timeout: float = 10.0) -> str:
    """Resolve a city, fetch current weather, and return a model-friendly string."""

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "ai-companion/0.1"},
    ) as client:
        geocoding = await client.get(
            GEOCODING_URL,
            params={
                "name": city,
                "count": 1,
                "language": "zh",
                "format": "json",
            },
        )
        geocoding.raise_for_status()
        results = geocoding.json().get("results") or []
        if not results:
            return f"未找到城市：{city}"

        place = results[0]
        forecast = await client.get(
            FORECAST_URL,
            params={
                "latitude": place["latitude"],
                "longitude": place["longitude"],
                "current": (
                    "temperature_2m,relative_humidity_2m,"
                    "weather_code,wind_speed_10m"
                ),
                "timezone": "auto",
            },
        )
        forecast.raise_for_status()
        current = forecast.json().get("current") or {}

    return format_weather(place, current)
