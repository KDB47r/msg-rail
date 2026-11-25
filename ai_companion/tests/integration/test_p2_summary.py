import pytest

from app.orchestrator.summary_service import SummaryService
from app.storage.repositories import (
    ConversationRepository,
    MessageRepository,
    SummaryRepository,
    UserRepository,
)
from tests.helpers import stream_chat
from tests.providers import FailingProvider, RecordingProvider


@pytest.mark.asyncio
async def test_rolling_summary_is_stored_and_injected(client, app, settings):
    settings.context_window_tokens = 1000
    settings.llm_max_tokens = 50
    settings.summary_trigger_ratio = 0.05
    settings.summary_keep_recent_messages = 2
    settings.summary_sync = True
    settings.summary_max_tokens = 64

    provider = RecordingProvider("收到。")
    app.state.provider = provider

    conversation_id = None
    for index, text in enumerate(["第一轮", "第二轮", "第三轮"]):
        events = await stream_chat(
            client,
            settings,
            text,
            conversation_id=conversation_id,
            client_message_id=f"p2-{index}",
        )
        conversation_id = events[0]["data"]["conversation_id"]

    async with app.state.session_factory() as session:
        summary = await SummaryRepository(session).latest(conversation_id)

    assert summary is not None
    assert summary.content

    await stream_chat(
        client,
        settings,
        "第四轮",
        conversation_id=conversation_id,
        client_message_id="p2-3",
    )

    last_call = provider.calls[-1]
    assert summary.content in last_call[0].content
    assert all("第一轮" not in message.content for message in last_call)
    assert any("第三轮" in message.content for message in last_call)


@pytest.mark.asyncio
async def test_summary_failure_is_swallowed(app, settings):
    settings.summary_keep_recent_messages = 2

    async with app.state.session_factory() as session:
        user = await UserRepository(session).get_or_create(
            settings.dev_user_external_id
        )
        conversation = await ConversationRepository(session).create(
            user_id=user.id,
            title="test",
        )
        for index in range(6):
            await MessageRepository(session).create(
                conversation_id=conversation.id,
                role="user" if index % 2 == 0 else "assistant",
                content=f"m{index}",
            )
        await session.commit()
        conversation_id = conversation.id

    service = SummaryService(
        app.state.session_factory,
        FailingProvider("x"),
        settings,
    )
    await service.maybe_summarize(conversation_id)

    async with app.state.session_factory() as session:
        assert await SummaryRepository(session).latest(conversation_id) is None
