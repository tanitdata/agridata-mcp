"""API key authentication for tanitdata remote deployment."""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Contextvar set by auth middleware, read by logging middleware and tool wrapper
current_key_alias: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_key_alias", default=None
)

# ---------------------------------------------------------------------------
# API key store — Secrets Manager-backed with in-memory cache
# ---------------------------------------------------------------------------

_CACHE_TTL = 300  # 5 minutes


class ApiKeyStore:
    """Loads hashed API keys from a single Secrets Manager JSON secret.

    Secret format: {"alias": "sha256_hex_hash", ...}
    The server caches the secret in-memory and refreshes every 5 minutes.
    If API_KEYS_SECRET env var is unset, auth is disabled (local dev).
    """

    def __init__(self) -> None:
        self._secret_name = os.environ.get("API_KEYS_SECRET", "")
        # hash -> alias (inverted from the secret's alias -> hash format)
        self._keys: dict[str, str] = {}
        self._last_refresh: float = -_CACHE_TTL

    @property
    def enabled(self) -> bool:
        return bool(self._secret_name)

    def _refresh(self) -> None:
        """Fetch the secret from Secrets Manager and rebuild the lookup cache."""
        if not self._secret_name:
            return
        try:
            import boto3

            client = boto3.client("secretsmanager")
            resp = client.get_secret_value(SecretId=self._secret_name)
            secret: dict[str, str] = json.loads(resp["SecretString"])
            # Invert: secret stores {alias: hash}, we need {hash: alias}
            self._keys = {h: alias for alias, h in secret.items()}
            self._last_refresh = time.monotonic()
            logger.info("auth: loaded %d API keys from Secrets Manager", len(self._keys))
        except Exception as exc:
            logger.warning("auth: Secrets Manager refresh failed: %s", exc)
            # Keep serving with stale cache rather than locking everyone out

    def _maybe_refresh(self) -> None:
        if time.monotonic() - self._last_refresh > _CACHE_TTL:
            self._refresh()

    def verify(self, raw_token: str) -> str | None:
        """Return the alias if the token is valid, else None."""
        self._maybe_refresh()
        key_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        return self._keys.get(key_hash)


# ---------------------------------------------------------------------------
# Starlette middleware
# ---------------------------------------------------------------------------

_OPEN_PATHS = {"/health"}


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Validates Bearer tokens against the ApiKeyStore."""

    def __init__(self, app, key_store: ApiKeyStore) -> None:  # type: ignore[override]
        super().__init__(app)
        self.key_store = key_store

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        # Skip auth for health checks
        if request.url.path in _OPEN_PATHS:
            return await call_next(request)

        # If auth is disabled (no secret configured), pass through
        if not self.key_store.enabled:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            logger.warning("auth: missing or malformed Authorization header")
            return JSONResponse(
                {"error": "unauthorized", "message": "Bearer token required"},
                status_code=401,
            )

        token = auth_header[7:]  # strip "Bearer "
        alias = self.key_store.verify(token)
        if alias is None:
            logger.warning("auth: invalid API key attempted")
            return JSONResponse(
                {"error": "unauthorized", "message": "Invalid API key"},
                status_code=401,
            )

        # Store alias for downstream logging
        tok = current_key_alias.set(alias)
        try:
            response = await call_next(request)
        finally:
            current_key_alias.reset(tok)
        return response
