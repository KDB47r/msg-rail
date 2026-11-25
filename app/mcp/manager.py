import contextlib
import json
import logging
import os
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path

from app.config import Settings
from app.llm.base import ToolSpec
from app.tools.registry import ToolRegistry, qualified_name

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@dataclass(slots=True)
class MCPServerConfig:
    name: str
    transport: str
    command: str | None = None
    args: list[str] | None = None
    url: str | None = None


class MCPClientManager:
    """MCP Host-side client manager.

    P3 supports stdio. Streamable HTTP is the next step and is intentionally
    left out of this first implementation.
    """

    def __init__(self, settings: Settings, registry: ToolRegistry) -> None:
        self._settings = settings
        self._registry = registry
        self._sessions: dict[str, object] = {}
        self._stacks: dict[str, AsyncExitStack] = {}

    def _load_config(self) -> dict[str, MCPServerConfig]:
        path = Path(self._settings.mcp_config_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if not path.exists():
            return {}

        data = json.loads(path.read_text(encoding="utf-8"))
        servers = data.get("servers", {})
        return {
            name: MCPServerConfig(name=name, **config)
            for name, config in servers.items()
        }

    async def start(self) -> None:
        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ImportError:
            logger.warning(
                "mcp package is not installed; MCP tools are disabled"
            )
            return

        for config in self._load_config().values():
            if config.transport != "stdio":
                logger.warning(
                    "MCP transport %s is not implemented yet (server=%s)",
                    config.transport,
                    config.name,
                )
                continue

            try:
                await self._start_stdio(
                    config,
                    ClientSession,
                    StdioServerParameters,
                    stdio_client,
                )
            except Exception:
                logger.exception("failed to start MCP server %s", config.name)

    async def _start_stdio(
        self,
        config: MCPServerConfig,
        client_session_class,
        server_parameters_class,
        stdio_client,
    ) -> None:
        command = config.command or sys.executable
        if command == "{python}":
            command = sys.executable

        env = dict(os.environ)
        env.setdefault("PYTHONPATH", str(PROJECT_ROOT))

        params = server_parameters_class(
            command=command,
            args=config.args or [],
            env=env,
        )

        stack = AsyncExitStack()
        try:
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(
                client_session_class(read, write)
            )
            await session.initialize()

            result = await session.list_tools()
            tools = getattr(result, "tools", []) or []

            for tool in tools:
                schema = (
                    getattr(tool, "inputSchema", None)
                    or getattr(tool, "input_schema", None)
                    or {"type": "object", "properties": {}}
                )
                spec = ToolSpec(
                    name=qualified_name(config.name, tool.name),
                    description=tool.description or f"MCP tool {tool.name}",
                    parameters=schema,
                )

                async def executor(
                    arguments: dict[str, object],
                    *,
                    server: str = config.name,
                    remote_name: str = tool.name,
                ) -> str:
                    return await self.call_tool(server, remote_name, arguments)

                self._registry.register(
                    spec,
                    executor,
                    server=config.name,
                    permission="read",
                )

            self._sessions[config.name] = session
            self._stacks[config.name] = stack

            logger.info(
                "MCP server connected name=%s tools=%d",
                config.name,
                len(tools),
            )
        except Exception:
            # Close partially-entered stdio/session contexts in the same task
            # that entered them; otherwise anyio raises cancel-scope errors.
            with contextlib.suppress(Exception):
                await stack.aclose()
            raise

    async def call_tool(
        self,
        server: str,
        tool_name: str,
        arguments: dict[str, object],
    ) -> str:
        session = self._sessions.get(server)
        if session is None:
            raise RuntimeError(f"MCP server not connected: {server}")

        result = await session.call_tool(tool_name, arguments)

        texts: list[str] = []
        for block in getattr(result, "content", []) or []:
            text = getattr(block, "text", None)
            if text:
                texts.append(text)

        content = "\n".join(texts) or "(empty MCP result)"
        is_error = getattr(result, "isError", None)
        if is_error is None:
            is_error = getattr(result, "is_error", False)
        if is_error:
            raise RuntimeError(content)

        return content

    async def close(self) -> None:
        for name in reversed(list(self._stacks)):
            stack = self._stacks.pop(name)
            try:
                await stack.aclose()
            except Exception:
                logger.exception("failed to close MCP server %s", name)
        self._sessions.clear()
