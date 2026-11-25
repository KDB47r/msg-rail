import openai


def map_provider_error(exc: Exception) -> tuple[str, bool, str]:
    """Map provider exceptions to stable SSE error codes.

    Returns (code, retryable, user_message).
    """

    if isinstance(exc, openai.APITimeoutError):
        return ("UPSTREAM_TIMEOUT", True, "模型响应超时")
    if isinstance(exc, openai.RateLimitError):
        return ("UPSTREAM_RATE_LIMITED", True, "模型服务限流，请稍后重试")
    if isinstance(exc, openai.APIConnectionError):
        return ("UPSTREAM_UNAVAILABLE", True, "无法连接模型服务")
    if isinstance(exc, openai.APIStatusError):
        return ("UPSTREAM_ERROR", exc.status_code >= 500, "模型服务返回错误")
    return ("INTERNAL_ERROR", False, "服务内部错误")
