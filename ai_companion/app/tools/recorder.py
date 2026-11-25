import logging

from app.llm.base import ToolCall
from app.storage.models import ToolCallRecord
from app.tools.base import ToolResult

logger = logging.getLogger(__name__)


class ToolCallRecorder:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def record(
        self,
        *,
        run_id: str,
        step: int,
        call: ToolCall,
        result: ToolResult,
    ) -> None:
        try:
            async with self._session_factory() as session:
                session.add(
                    ToolCallRecord(
                        run_id=run_id,
                        call_id=call.id,
                        tool_name=call.name,
                        server_name=result.server,
                        step=step,
                        arguments_json=call.arguments,
                        result_json={
                            "content": result.content,
                            "status": result.status,
                            "error_code": result.error_code,
                        },
                        status=result.status,
                        latency_ms=result.latency_ms,
                        error_code=result.error_code,
                    )
                )
                await session.commit()
        except Exception:
            logger.exception(
                "failed to record tool call run_id=%s call_id=%s",
                run_id,
                call.id,
            )
