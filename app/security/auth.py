import secrets

from fastapi import HTTPException, status


def check_bearer_token(authorization: str | None, expected_token: str) -> None:
    """P1 auth: verify the shared development bearer token."""

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )

    token = authorization.removeprefix("Bearer ").strip()
    if not secrets.compare_digest(
        token.encode("utf-8"),
        expected_token.encode("utf-8"),
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        )
