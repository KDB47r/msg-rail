"""Small, safe arithmetic evaluator exposed through MCP.

Never use eval/exec here. Only a fixed set of AST nodes and functions is allowed.
"""

import ast
import math
import operator

MAX_EXPRESSION_CHARS = 200
MAX_RESULT = 1e15


class CalculatorError(ValueError):
    pass


_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}

_FUNCS = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sqrt": math.sqrt,
}

_NAMES = {
    "pi": math.pi,
    "e": math.e,
}


def _eval_node(node: ast.AST) -> float:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(
            node.value, (int, float)
        ):
            raise CalculatorError("只支持数字")
        return float(node.value)

    if isinstance(node, ast.Name):
        if node.id not in _NAMES:
            raise CalculatorError(f"不支持的名称：{node.id}")
        return float(_NAMES[node.id])

    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
        return float(_UNARY_OPS[type(node.op)](_eval_node(node.operand)))

    if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
        left = _eval_node(node.left)
        right = _eval_node(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 1000:
            raise CalculatorError("指数过大")
        value = float(_BIN_OPS[type(node.op)](left, right))
        if abs(value) > MAX_RESULT:
            raise CalculatorError("结果过大")
        return value

    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCS:
            raise CalculatorError("不支持的函数")
        if node.keywords:
            raise CalculatorError("不支持关键字参数")
        args = [_eval_node(arg) for arg in node.args]
        if node.func.id == "sqrt" and (len(args) != 1 or args[0] < 0):
            raise CalculatorError("sqrt 参数不合法")
        return float(_FUNCS[node.func.id](*args))

    raise CalculatorError("表达式包含不支持的语法")


def calculate(expression: str) -> str:
    expression = expression.strip().replace("^", "**")
    if not expression:
        raise CalculatorError("表达式为空")
    if len(expression) > MAX_EXPRESSION_CHARS:
        raise CalculatorError("表达式过长")
    try:
        tree = ast.parse(expression, mode="eval")
        value = _eval_node(tree.body)
    except CalculatorError:
        raise
    except ZeroDivisionError as exc:
        raise CalculatorError("不能除以 0") from exc
    except (SyntaxError, ValueError, OverflowError) as exc:
        raise CalculatorError("表达式不合法") from exc

    if not math.isfinite(value):
        raise CalculatorError("结果不是有限数字")
    if value.is_integer():
        return str(int(value))
    return f"{value:.10g}"


def format_calculation(expression: str) -> str:
    try:
        return f"{expression.strip()} = {calculate(expression)}"
    except CalculatorError as exc:
        return f"ERROR[INVALID_EXPRESSION]: {exc}"
