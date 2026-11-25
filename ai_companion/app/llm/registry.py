from app.config import Settings
from app.llm.base import LLMProvider
from app.llm.fake import FakeProvider
from app.llm.openai_compat import OpenAICompatProvider


def build_provider(settings: Settings) -> LLMProvider:
    provider = settings.llm_provider.strip().lower()

    if provider == "fake":
        return FakeProvider(
            text=settings.fake_response,
            delay=settings.fake_delay_seconds,
        )

    if provider in {"openai", "openai_compat"}:
        return OpenAICompatProvider(settings)

    raise ValueError(
        f"Unsupported LLM_PROVIDER={settings.llm_provider!r}; "
        "expected 'fake' or 'openai_compat'"
    )
