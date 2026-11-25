import pytest

from app.tools.fun_utils import (
    FunToolError,
    holiday_info,
    random_pick,
    roll_dice,
    trivia,
)


def test_random_picker_and_dice():
    assert "随机结果：" in random_pick(["火锅", "烧烤", "日料"], 1)
    assert "总和" in roll_dice(6, 2)


def test_random_picker_validates_bounds():
    with pytest.raises(FunToolError):
        random_pick(["只有一个"])
    with pytest.raises(FunToolError):
        roll_dice(1, 1)


def test_holiday_exact_and_next():
    assert "元旦" in holiday_info("2026-01-01")
    text = holiday_info("2026-11-24")
    assert "圣诞节" in text
    assert "31 天" in text


def test_trivia_has_question_and_answer():
    text = trivia("科学")
    assert "题目：" in text
    assert "答案：" in text
