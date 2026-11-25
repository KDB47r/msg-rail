import inspect
import json
from collections.abc import AsyncIterator

import openai
from openai import AsyncOpenAI

from app.config import Settings
from app.llm.base import Message, StreamEvent, ToolCall, ToolSpec, Usage


class OpenAICompatProvider:
    """Provider for OpenAI-compatible chat completion APIs.

    DeepSeek, Qwen (DashScope compatible mode), GLM and others expose the same
    chat/completions shape, so a single adapter covers them.
    """

    def __init__(self, settings: Settings) -> None:
        if not settings.llm_api_key:
            raise ValueError("LLM_API_KEY is required when LLM_PROVIDER=openai_compat")

        self._client = AsyncOpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            timeout=settings.llm_timeout_seconds,
        )
        self._model = settings.llm_model
        self._max_tokens = settings.llm_max_tokens
        self._temperature = settings.llm_temperature
        self._include_usage = settings.llm_include_usage

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        payload: dict[str, object] = {
            "model": self._model,
            "messages": [_message_payload(message) for message in messages],
            "stream": True,
            "max_tokens": self._max_tokens,
            "temperature": self._temperature,
        }
        if tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    },
                }
                for tool in tools
            ]
        if self._include_usage:
            payload["stream_options"] = {"include_usage": True}

        stream = None
        try:
            try:
                stream = await self._client.chat.completions.create(**payload)
            except openai.APIStatusError as exc:
                # Some OpenAI-compatible providers reject stream_options with 400.
                if "stream_options" not in payload or exc.status_code != 400:
                    raise
                payload.pop("stream_options")
                stream = await self._client.chat.completions.create(**payload)

            finish_reason: str | None = None
            usage: Usage | None = None
            tool_call_accumulator = _ToolCallAccumulator()

            async for chunk in stream:
                chunk_usage = getattr(chunk, "usage", None)
                if chunk_usage is not None:
                    usage = _to_usage(chunk_usage)

                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue

                choice = choices[0]
                delta = getattr(choice, "delta", None)
                if delta is not None:
                    reasoning = getattr(delta, "reasoning_content", None)
                    if reasoning:
                        yield StreamEvent(type="reasoning_delta", delta=reasoning)

                    content = getattr(delta, "content", None)
                    if content:
                        yield StreamEvent(type="text_delta", delta=content)

                    for tool_delta in getattr(delta, "tool_calls", None) or []:
                        tool_call_accumulator.add(tool_delta)

                if getattr(choice, "finish_reason", None):
                    finish_reason = choice.finish_reason

            tool_calls = tool_call_accumulator.build()
            if tool_calls:
                yield StreamEvent(type="tool_calls", tool_calls=tool_calls)

            if usage is not None:
                yield StreamEvent(type="usage", usage=usage)
            yield StreamEvent(type="end", finish_reason=finish_reason or "stop")
        finally:
            await _close_stream(stream)


class _ToolCallAccumulator:
    def __init__(self) -> None:
        self._parts: dict[int, dict[str, str]] = {}

    def add(self, tool_delta: object) -> None:
        index = getattr(tool_delta, "index", None)
        if index is None:
            index = len(self._parts)

        parts = self._parts.setdefault(
            index,
            {"id": "", "name": "", "arguments": ""},
        )

        tool_id = getattr(tool_delta, "id", None)
        if tool_id:
            parts["id"] = tool_id

        function = getattr(tool_delta, "function", None)
        if function is not None:
            name = getattr(function, "name", None)
            if name:
                parts["name"] = name
            arguments = getattr(function, "arguments", None)
            if arguments:
                parts["arguments"] += arguments

    def build(self) -> list[ToolCall]:
        return _build_tool_calls(self._parts)


def _message_payload(message: Message) -> dict[str, object]:
    payload: dict[str, object] = {
        "role": message.role,
        "content": message.content,
    }

    if message.tool_calls:
        payload["tool_calls"] = [
            {
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": call.raw_arguments
                    or json.dumps(call.arguments, ensure_ascii=False),
                },
            }
            for call in message.tool_calls
        ]

    if message.tool_call_id:
        payload["tool_call_id"] = message.tool_call_id
    if message.name:
        payload["name"] = message.name

    return payload


def _build_tool_calls(parts: dict[int, dict[str, str]]) -> list[ToolCall]:
    calls: list[ToolCall] = []
    for index in sorted(parts):
        part = parts[index]
        raw = part["arguments"] or "{}"
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                arguments = parsed
                parse_error = False
            else:
                arguments = {}
                parse_error = True
        except json.JSONDecodeError:
            arguments = {}
            parse_error = True

        calls.append(
            ToolCall(
                id=part["id"] or f"call_{index}",
                name=part["name"],
                arguments=arguments,
                raw_arguments=raw,
                parse_error=parse_error,
            )
        )
    return calls


def _to_usage(raw: object) -> Usage:
    prompt_tokens = getattr(raw, "prompt_tokens", 0) or 0
    completion_tokens = getattr(raw, "completion_tokens", 0) or 0
    details = getattr(raw, "prompt_tokens_details", None)
    cached_tokens = getattr(details, "cached_tokens", 0) if details is not None else 0
    return Usage(
        input_tokens=int(prompt_tokens),
        output_tokens=int(completion_tokens),
        cached_tokens=int(cached_tokens or 0),
    )


async def _close_stream(stream: object | None) -> None:
    if stream is None:
        return
    for name in ("close", "aclose"):
        close = getattr(stream, name, None)
        if close is None:
            continue
        result = close()
        if inspect.isawaitable(result):
            await result
        return
