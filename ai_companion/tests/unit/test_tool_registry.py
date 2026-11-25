import asyncio

import pytest

from app.config import Settings
from app.llm.base import ToolCall, ToolSpec
from app.tools.registry import ToolRegistry


def _settings(**overrides) -> Settings:
    values = {
        "tools_enabled": True,
        "tool_timeout_seconds": 0.1,
        "max_tool_result_tokens": 10,
        "tool_parallel_limit": 2,
    }
    values.update(overrides)
    return Settings(**values)


async def _echo(arguments: dict[str, object]) -> str:
    return str(arguments.get("text", "")) * 100


async def _slow(arguments: dict[str, object]) -> str:
    await asyncio.sleep(0.2)
    return "late"


@pytest.mark.asyncio
async def test_registry_executes_and_truncates():
    registry = ToolRegistry(_settings())
    spec = ToolSpec(
        name="test__echo",
        description="echo",
        parameters={"type": "object"},
    )
    registry.register(spec, _echo, server="test", permission="read")

    result = await registry.execute(
        ToolCall(id="c1", name="test__echo", arguments={"text": "你好"})
    )

    assert result.status == "success"
    assert "truncated" in result.content
    assert result.server == "test"


@pytest.mark.asyncio
async def test_registry_denies_write_tools_and_reports_unknown():
    registry = ToolRegistry(_settings())
    spec = ToolSpec(
        name="test__write",
        description="write",
        parameters={"type": "object"},
    )
    registry.register(spec, _echo, server="test", permission="write")

    assert all(item.name != "test__write" for item in registry.list_specs())

    denied = await registry.execute(
        ToolCall(id="c1", name="test__write", arguments={})
    )
    assert denied.status == "denied"
    assert denied.error_code == "TOOL_DENIED"

    unknown = await registry.execute(
        ToolCall(id="c2", name="missing", arguments={})
    )
    assert unknown.status == "error"
    assert unknown.error_code == "TOOL_NOT_FOUND"


@pytest.mark.asyncio
async def test_registry_profile_filters_specs_and_execution():
    registry = ToolRegistry(_settings())
    echo = ToolSpec(
        name="test__echo",
        description="echo",
        parameters={"type": "object"},
    )
    private = ToolSpec(
        name="test__private",
        description="private",
        parameters={"type": "object"},
    )
    registry.register(echo, _echo, server="test", permission="read")
    registry.register(private, _echo, server="test", permission="read")

    specs = registry.list_specs(allowed={"test__echo"})
    assert [spec.name for spec in specs] == ["test__echo"]

    denied = await registry.execute(
        ToolCall(id="c1", name="test__private", arguments={"text": "x"}),
        allowed={"test__echo"},
    )
    assert denied.status == "denied"
    assert denied.error_code == "TOOL_DENIED"


@pytest.mark.asyncio
async def test_registry_timeout():
    registry = ToolRegistry(_settings(tool_timeout_seconds=0.01))
    spec = ToolSpec(
        name="test__slow",
        description="slow",
        parameters={"type": "object"},
    )
    registry.register(spec, _slow, server="test", permission="read")

    result = await registry.execute(
        ToolCall(id="c1", name="test__slow", arguments={})
    )

    assert result.status == "timeout"
    assert result.error_code == "TOOL_TIMEOUT"
