"""AI Worker: consume ai_trigger, call AI Companion, stream through Logic.

Architecture:
    Logic --Kafka ai_trigger--> this worker
    this worker --HTTP--> Logic /internal/ai/context
    this worker --HTTP SSE--> AI Companion /internal/v1/agent/stream
    this worker --HTTP--> Logic /internal/ai/delta | finalize | fail
    Logic --Kafka push_to_comet--> Job -> Comet -> browser

Run:
    python -m app.worker.ai_worker

Kafka support requires the optional dependency:
    pip install "aiokafka>=0.10"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TriggerEvent:
    job_id: str
    mode: str
    session_id: str
    bot_user_id: int
    trigger_message_id: str
    trigger_sender_id: int
    trigger_sender_name: str
    trigger_content: str
    target_user_ids: list[int]
    activated_seq: int = 0

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> TriggerEvent:
        return cls(
            job_id=str(data["job_id"]),
            mode=str(data["mode"]),
            session_id=str(data["session_id"]),
            bot_user_id=int(data["bot_user_id"]),
            trigger_message_id=str(data["trigger_message_id"]),
            trigger_sender_id=int(data.get("trigger_sender_id", 0)),
            trigger_sender_name=str(data.get("trigger_sender_name", "")),
            trigger_content=str(data.get("trigger_content", "")),
            target_user_ids=[
                int(item) for item in data.get("target_user_ids", [])
            ],
            activated_seq=int(data.get("activated_seq", 0)),
        )


class AiWorker:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(180.0, connect=10.0)
        )
        self._tasks: dict[str, asyncio.Task] = {}
        self._stopping: set[str] = set()

    async def close(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        await self._client.aclose()

    async def _logic_post(self, path: str, payload: dict[str, Any]) -> httpx.Response:
        return await self._client.post(
            self._settings.logic_base_url.rstrip("/") + path,
            headers={
                "Authorization": f"Bearer {self._settings.logic_internal_token}"
            },
            json=payload,
        )

    async def handle_trigger(self, trigger: TriggerEvent) -> None:
        if trigger.job_id in self._stopping:
            self._stopping.discard(trigger.job_id)
            return
        try:
            await self._run_trigger(trigger)
        except asyncio.CancelledError:
            logger.info("AI job cancelled job_id=%s", trigger.job_id)
            raise
        except Exception as exc:
            logger.exception("AI job failed job_id=%s", trigger.job_id)
            await self._fail(trigger.job_id, "WORKER_ERROR", str(exc))

    async def _run_trigger(self, trigger: TriggerEvent) -> None:
        context_resp = await self._logic_post(
            "/internal/ai/context", {"job_id": trigger.job_id}
        )
        if context_resp.status_code != 200:
            logger.warning(
                "context rejected job_id=%s status=%s body=%s",
                trigger.job_id,
                context_resp.status_code,
                context_resp.text[:300],
            )
            if context_resp.status_code != 409:
                await self._fail(
                    trigger.job_id,
                    "CONTEXT_FAILED",
                    f"Logic context HTTP {context_resp.status_code}",
                )
            return
        context = context_resp.json().get("data") or {}
        messages = context.get("messages") or []

        payload = {
            "request_id": trigger.job_id,
            "mode": trigger.mode,
            "user_id": f"im:{trigger.trigger_sender_id}",
            "session_id": trigger.session_id,
            "bot_user_id": trigger.bot_user_id,
            "trigger_message_id": trigger.trigger_message_id,
            "trigger_sender_id": trigger.trigger_sender_id,
            "trigger_sender_name": trigger.trigger_sender_name,
            "context_messages": messages,
            "persona_id": "xiaoheng",
            "idempotency_key": trigger.trigger_message_id,
        }

        buffer: list[str] = []
        content_parts: list[str] = []
        last_flush = time.monotonic()
        error: tuple[str, str] | None = None
        finished = False

        async def flush(force: bool = False) -> None:
            nonlocal last_flush
            if not buffer:
                return
            elapsed_ms = (time.monotonic() - last_flush) * 1000
            if (
                not force
                and len("".join(buffer)) < self._settings.ai_worker_batch_chars
                and elapsed_ms < self._settings.ai_worker_batch_ms
            ):
                return
            delta = "".join(buffer)
            buffer.clear()
            last_flush = time.monotonic()
            resp = await self._logic_post(
                "/internal/ai/delta",
                {"job_id": trigger.job_id, "delta": delta},
            )
            if resp.status_code != 200:
                logger.warning(
                    "delta rejected job_id=%s status=%s body=%s",
                    trigger.job_id,
                    resp.status_code,
                    resp.text[:200],
                )

        async with self._client.stream(
            "POST",
            self._settings.ai_self_base_url.rstrip("/")
            + "/internal/v1/agent/stream",
            headers={
                "Authorization": f"Bearer {self._settings.internal_token}"
            },
            json=payload,
        ) as response:
            if response.status_code != 200:
                body = (await response.aread()).decode("utf-8", "replace")
                raise RuntimeError(
                    f"AI Companion HTTP {response.status_code}: {body[:300]}"
                )

            event_name: str | None = None
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    if event_name:
                        raw = "\n".join(data_lines)
                        try:
                            data = json.loads(raw) if raw else {}
                        except json.JSONDecodeError:
                            data = {}
                        if event_name == "content_delta":
                            delta = data.get("delta")
                            if isinstance(delta, str) and delta:
                                buffer.append(delta)
                                content_parts.append(delta)
                                await flush()
                        elif event_name == "error":
                            error = (
                                str(data.get("code") or "AI_ERROR"),
                                str(data.get("message") or "AI 回复失败"),
                            )
                        elif event_name == "message_end":
                            finished = True
                    event_name = None
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event_name = line[len("event:") :].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[len("data:") :].strip())

        await flush(force=True)

        if error is not None:
            await self._fail(trigger.job_id, error[0], error[1])
            return
        if not finished:
            await self._fail(
                trigger.job_id,
                "STREAM_INCOMPLETE",
                "AI 流式响应未正常结束",
            )
            return

        content = "".join(content_parts).strip()
        if not content:
            await self._fail(trigger.job_id, "EMPTY_REPLY", "AI 返回了空内容")
            return

        resp = await self._logic_post(
            "/internal/ai/finalize",
            {"job_id": trigger.job_id, "content": content},
        )
        if resp.status_code != 200:
            logger.warning(
                "finalize rejected job_id=%s status=%s body=%s",
                trigger.job_id,
                resp.status_code,
                resp.text[:300],
            )

    async def _fail(self, job_id: str, code: str, message: str) -> None:
        try:
            await self._logic_post(
                "/internal/ai/fail",
                {"job_id": job_id, "code": code, "message": message},
            )
        except Exception:
            logger.exception("failed to report AI failure job_id=%s", job_id)

    def stop(self, job_id: str) -> None:
        self._stopping.add(job_id)
        task = self._tasks.get(job_id)
        if task is not None:
            task.cancel()

    async def run(self, trigger: TriggerEvent) -> None:
        task = asyncio.create_task(
            self.handle_trigger(trigger),
            name=f"ai-{trigger.job_id}",
        )
        self._tasks[trigger.job_id] = task
        try:
            await task
        finally:
            self._tasks.pop(trigger.job_id, None)

    async def consume_kafka(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer
        except ModuleNotFoundError as exc:
            raise SystemExit(
                "缺少 aiokafka。请先运行：\n"
                '  .venv/bin/pip install "aiokafka>=0.10"'
            ) from exc

        consumer = AIOKafkaConsumer(
            self._settings.kafka_ai_trigger_topic,
            bootstrap_servers=self._settings.kafka_brokers,
            group_id=self._settings.kafka_ai_consumer_group,
            enable_auto_commit=False,
            value_deserializer=None,
        )
        await consumer.start()
        logger.info(
            "AI worker consuming topic=%s group=%s",
            self._settings.kafka_ai_trigger_topic,
            self._settings.kafka_ai_consumer_group,
        )
        try:
            async for record in consumer:
                try:
                    data = json.loads(record.value.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    logger.warning("invalid ai_trigger payload")
                    await consumer.commit()
                    continue

                event_type = data.get("type")
                if event_type == "ai_stop":
                    job_id = str(data.get("job_id", ""))
                    if job_id:
                        self.stop(job_id)
                elif event_type == "ai_trigger":
                    trigger = TriggerEvent.from_json(data)
                    existing = self._tasks.get(trigger.job_id)
                    if existing is None or existing.done():
                        task = asyncio.create_task(
                            self.handle_trigger(trigger),
                            name=f"ai-{trigger.job_id}",
                        )
                        self._tasks[trigger.job_id] = task
                        task.add_done_callback(
                            lambda _t, jid=trigger.job_id: self._tasks.pop(
                                jid, None
                            )
                        )
                await consumer.commit()
        finally:
            await consumer.stop()


async def _amain(args: argparse.Namespace) -> None:
    settings = Settings()
    worker = AiWorker(settings)
    try:
        if args.trigger_json:
            data = json.loads(args.trigger_json)
            trigger = TriggerEvent.from_json(data)
            await worker.run(trigger)
        else:
            await worker.consume_kafka()
    finally:
        await worker.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="MsgRail AI Worker")
    parser.add_argument(
        "--trigger-json",
        help="Run one trigger JSON without Kafka (for local debugging)",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
