"""Request logging and tool-call instrumentation for tanitdata."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Sequence

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from tanitdata.auth import current_key_alias

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

# Dedicated logger for structured usage events (tool calls, HTTP requests).
# Separating from the main logger lets CloudWatch filter on logger name.
_usage_logger = logging.getLogger("tanitdata.usage")


# ---------------------------------------------------------------------------
# Structured JSON log helper
# ---------------------------------------------------------------------------


def _log_json(event: str, **fields: Any) -> None:
    """Emit a structured JSON log line to the tanitdata.usage logger."""
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event": event,
        **fields,
    }
    _usage_logger.info(json.dumps(record, default=str))


# ---------------------------------------------------------------------------
# HTTP request logging middleware
# ---------------------------------------------------------------------------


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Logs every HTTP request with timing and auth context."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000)

        # Only log MCP endpoint requests, not health checks
        if request.url.path != "/health":
            _log_json(
                "http_request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                key=current_key_alias.get(),
                ms=elapsed_ms,
            )
        return response


# ---------------------------------------------------------------------------
# Tool call instrumentation
# ---------------------------------------------------------------------------


def wrap_tool_calls(mcp: FastMCP) -> None:
    """Monkey-patch the tool manager's call_tool to log every tool invocation."""
    original = mcp._tool_manager.call_tool

    async def logged_call_tool(
        name: str, arguments: dict[str, Any], **kwargs: Any
    ) -> Sequence[Any] | dict[str, Any]:
        alias = current_key_alias.get()
        start = time.monotonic()
        ok = True
        try:
            result = await original(name, arguments, **kwargs)
            return result
        except Exception:
            ok = False
            raise
        finally:
            elapsed_ms = round((time.monotonic() - start) * 1000)
            _log_json(
                "tool_call",
                key=alias,
                tool=name,
                params=_safe_params(arguments),
                ms=elapsed_ms,
                ok=ok,
            )

    mcp._tool_manager.call_tool = logged_call_tool  # type: ignore[method-assign]


def _safe_params(args: dict[str, Any]) -> dict[str, Any]:
    """Truncate long parameter values to keep log lines reasonable."""
    out: dict[str, Any] = {}
    for k, v in args.items():
        if isinstance(v, str) and len(v) > 200:
            out[k] = v[:200] + "..."
        else:
            out[k] = v
    return out
