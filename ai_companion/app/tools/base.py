import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from app.llm.base import ToolSpec


@dataclass(slots=True)
class ToolResult:
    call_id: str
    name: str
    server: str | None
    content: str
    status: str
    latency_ms: int
    error_code: str | None = None


class ToolExecutor(Protocol):
    def __call__(self, arguments: dict[str, object]) -> Awaitable[str]:
        ...


class LocalTool:
    """A Python function exposed as a tool."""

    def __init__(
        self,
        spec: ToolSpec,
        func: Callable[..., object],
    ) -> None:
        self.spec = spec
        self._func = func

    async def execute(self, arguments: dict[str, object]) -> str:
        result = self._func(**arguments)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, str) else str(result)
