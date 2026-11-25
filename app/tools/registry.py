import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.config import Settings
from app.llm.base import ToolCall, ToolSpec
from app.orchestrator.token_counter import estimate_tool_specs, truncate_to_tokens
from app.tools.base import ToolResult

logger = logging.getLogger(__name__)

_TOOL_NAME_RE = re.compile(r"[^A-Za-z0-9_-]")


def sanitize_name(name: str) -> str:
    return _TOOL_NAME_RE.sub("_", name)[:64] or "tool"


def qualified_name(server: str, tool: str) -> str:
    return f"{sanitize_name(server)}__{sanitize_name(tool)}"[:64]


@dataclass(slots=True)
class RegisteredTool:
    spec: ToolSpec
    executor: Callable[[dict[str, object]], Awaitable[str]]
    server: str | None
    permission: str


class ToolRegistry:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._tools: dict[str, RegisteredTool] = {}
        self._semaphore = asyncio.Semaphore(
            max(1, settings.tool_parallel_limit)
        )

    def register(
        self,
        spec: ToolSpec,
        executor: Callable[[dict[str, object]], Awaitable[str]],
        *,
        server: str | None = None,
        permission: str = "read",
    ) -> None:
        self._tools[spec.name] = RegisteredTool(
            spec=spec,
            executor=executor,
            server=server,
            permission=permission,
        )

    @staticmethod
    def _allowed(name: str, allowed: set[str] | None) -> bool:
        if allowed is None:
            return True
        if name in allowed:
            return True
        server = name.split("__", 1)[0]
        return f"{server}__*" in allowed

    def list_specs(
        self,
        allowed: set[str] | None = None,
    ) -> list[ToolSpec]:
        if self._settings.tool_permission_mode == "readonly_auto":
            specs = [
                tool.spec
                for tool in self._tools.values()
                if tool.permission == "read"
            ]
        else:
            specs = [tool.spec for tool in self._tools.values()]
        return [spec for spec in specs if self._allowed(spec.name, allowed)]

    def spec_token_estimate(
        self,
        allowed: set[str] | None = None,
    ) -> int:
        return estimate_tool_specs(self.list_specs(allowed))

    def server_for(self, name: str) -> str | None:
        tool = self._tools.get(name)
        return tool.server if tool is not None else None

    async def execute(
        self,
        call: ToolCall,
        allowed: set[str] | None = None,
    ) -> ToolResult:
        started = time.perf_counter()
        if not self._allowed(call.name, allowed):
            return self._result(
                call,
                None,
                "denied",
                "TOOL_DENIED",
                f"ERROR[TOOL_DENIED]: 工具 {call.name} 未被允许",
                started,
            )

        tool = self._tools.get(call.name)

        if tool is None:
            return self._result(
                call,
                None,
                "error",
                "TOOL_NOT_FOUND",
                f"ERROR[TOOL_NOT_FOUND]: 未知工具 {call.name}",
                started,
            )

        if (
            self._settings.tool_permission_mode == "readonly_auto"
            and tool.permission != "read"
        ):
            return self._result(
                call,
                tool.server,
                "denied",
                "TOOL_DENIED",
                f"ERROR[TOOL_DENIED]: 工具 {call.name} 未被允许",
                started,
            )

        if call.parse_error:
            return self._result(
                call,
                tool.server,
                "error",
                "INVALID_ARGUMENTS",
                "ERROR[INVALID_ARGUMENTS]: 工具参数不是合法 JSON 对象",
                started,
            )

        try:
            result = await asyncio.wait_for(
                tool.executor(call.arguments),
                timeout=self._settings.tool_timeout_seconds,
            )
        except asyncio.TimeoutError:
            return self._result(
                call,
                tool.server,
                "timeout",
                "TOOL_TIMEOUT",
                f"ERROR[TOOL_TIMEOUT]: 工具 {call.name} 执行超时",
                started,
            )
        except Exception as exc:  # noqa: BLE001 - convert tool failures to data
            logger.exception("tool execution failed name=%s", call.name)
            return self._result(
                call,
                tool.server,
                "error",
                "TOOL_ERROR",
                f"ERROR[TOOL_ERROR]: {exc}",
                started,
            )

        content = truncate_to_tokens(
            str(result),
            self._settings.max_tool_result_tokens,
        )
        return self._result(
            call,
            tool.server,
            "success",
            None,
            content,
            started,
        )

    async def execute_many(
        self,
        calls: list[ToolCall],
        allowed: set[str] | None = None,
    ) -> list[ToolResult]:
        async def run(call: ToolCall) -> ToolResult:
            async with self._semaphore:
                return await self.execute(call, allowed=allowed)

        return list(await asyncio.gather(*(run(call) for call in calls)))

    @staticmethod
    def _result(
        call: ToolCall,
        server: str | None,
        status: str,
        error_code: str | None,
        content: str,
        started: float,
    ) -> ToolResult:
        return ToolResult(
            call_id=call.id,
            name=call.name,
            server=server,
            content=content,
            status=status,
            latency_ms=int((time.perf_counter() - started) * 1000),
            error_code=error_code,
        )
