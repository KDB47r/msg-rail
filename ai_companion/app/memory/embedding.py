import logging

from openai import AsyncOpenAI

from app.config import Settings

logger = logging.getLogger(__name__)


class EmbeddingProvider:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None
        if settings.llm_api_key:
            self._client = AsyncOpenAI(
                api_key=settings.llm_api_key,
                base_url=settings.llm_base_url,
                timeout=settings.embedding_timeout_seconds,
            )

    async def embed(self, text: str) -> list[float]:
        if not text.strip():
            return []
        if self._client is None:
            raise RuntimeError("LLM_API_KEY is required for embeddings")

        response = await self._client.embeddings.create(
            model=self._settings.embedding_model,
            input=[text],
        )
        vector = response.data[0].embedding
        expected = self._settings.embedding_dimensions
        if len(vector) != expected:
            raise RuntimeError(
                f"embedding dimension mismatch: got {len(vector)}, expected {expected}"
            )
        return vector

    async def embed_many(self, texts: list[str]) -> list[list[float]]:
        return [await self.embed(text) for text in texts]

    async def embed_batch(
        self,
        texts: list[str],
        *,
        batch_size: int = 10,
    ) -> list[list[float]]:
        if not texts:
            return []
        if self._client is None:
            raise RuntimeError("LLM_API_KEY is required for embeddings")

        expected = self._settings.embedding_dimensions
        vectors: list[list[float]] = []

        for start in range(0, len(texts), max(1, batch_size)):
            batch = texts[start : start + batch_size]
            response = await self._client.embeddings.create(
                model=self._settings.embedding_model,
                input=batch,
            )
            data = sorted(
                response.data,
                key=lambda item: getattr(item, "index", 0),
            )
            for item in data:
                vector = item.embedding
                if len(vector) != expected:
                    raise RuntimeError(
                        "embedding dimension mismatch: "
                        f"got {len(vector)}, expected {expected}"
                    )
                vectors.append(vector)

        return vectors
