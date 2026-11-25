"""Local, deterministic-ish fun tools for the A-B atmosphere bot."""

import json
import random
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

DATA_DIR = Path(__file__).resolve().parent.parent / "mcp" / "servers" / "data"
_RNG = random.SystemRandom()


class FunToolError(ValueError):
    pass


@lru_cache
def _load_json(name: str) -> dict:
    path = DATA_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


def random_pick(options: list[str], count: int = 1) -> str:
    cleaned = [str(item).strip() for item in options if str(item).strip()]
    if len(cleaned) < 2:
        raise FunToolError("至少需要两个选项")
    if len(cleaned) > 20:
        raise FunToolError("选项不能超过 20 个")
    if count < 1 or count > 5:
        raise FunToolError("count 需要在 1 到 5 之间")
    if count > len(cleaned):
        raise FunToolError("抽取数量不能超过选项数量")
    picked = _RNG.sample(cleaned, count)
    return "随机结果：" + "、".join(picked)


def roll_dice(sides: int = 6, count: int = 1) -> str:
    if sides < 2 or sides > 100:
        raise FunToolError("sides 需要在 2 到 100 之间")
    if count < 1 or count > 10:
        raise FunToolError("count 需要在 1 到 10 之间")
    rolls = [_RNG.randint(1, sides) for _ in range(count)]
    if count == 1:
        return f"掷出：{rolls[0]}"
    return f"掷出：{', '.join(map(str, rolls))}，总和 {sum(rolls)}"


def _today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def holiday_info(date_text: str | None = None) -> str:
    holidays = _load_json("cn_holidays.json").get("holidays", [])
    if date_text:
        try:
            target = date.fromisoformat(date_text)
        except ValueError as exc:
            raise FunToolError("日期格式应为 YYYY-MM-DD") from exc
    else:
        target = _today()

    entries = []
    for item in holidays:
        try:
            entries.append((date.fromisoformat(item["date"]), item["name"]))
        except (KeyError, ValueError):
            continue
    entries.sort()

    exact = [name for day, name in entries if day == target]
    if exact:
        return f"{target.isoformat()} 是" + "、".join(exact) + "。"
    upcoming = [(day, name) for day, name in entries if day > target]
    if not upcoming:
        return f"{target.isoformat()} 本地日历未收录，也没有可计算的后续节日。"
    day, name = upcoming[0]
    delta = (day - target).days
    return (
        f"{target.isoformat()} 本地日历未收录节日。"
        f"下一个是 {day.isoformat()} 的{name}，还有 {delta} 天。"
    )


def trivia(topic: str | None = None) -> str:
    items = _load_json("trivia.json").get("items", [])
    if topic:
        filtered = [
            item
            for item in items
            if topic.strip() in str(item.get("topic", ""))
        ]
    else:
        filtered = items
    if not filtered:
        raise FunToolError("没有找到这个主题的冷知识")
    item = _RNG.choice(filtered)
    return (
        f"题目：{item.get('question', '')}\n"
        f"答案：{item.get('answer', '')}\n"
        f"主题：{item.get('topic', '')}"
    )


def next_holiday_from_today() -> str:
    return holiday_info(_today().isoformat())


def days_until(target_date: date) -> int:
    return (target_date - _today()).days
