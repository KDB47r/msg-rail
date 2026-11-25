import pytest

from app.tools.calculator import CalculatorError, calculate, format_calculation


def test_calculator_supports_basic_arithmetic():
    assert calculate("(138*27+15)/3") == "1247"
    assert calculate("2^10") == "1024"
    assert calculate("sqrt(2)") == "1.414213562"


def test_calculator_rejects_unsafe_syntax():
    for expression in ["__import__('os')", "open('/tmp/x')", "[1,2]", "1 and 2"]:
        with pytest.raises(CalculatorError):
            calculate(expression)


def test_calculator_reports_errors_as_text():
    assert format_calculation("1/0").startswith("ERROR[INVALID_EXPRESSION]")
    assert format_calculation("x + 1").startswith("ERROR[INVALID_EXPRESSION]")
