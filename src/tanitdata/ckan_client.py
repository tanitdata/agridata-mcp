"""Async CKAN API client for catalog.agridata.tn."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx


class CKANClient:
    """Async HTTP client for the CKAN DataStore API."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (
            base_url
            or os.environ.get("CKAN_BASE_URL")
            or "https://catalog.agridata.tn"
        )
        self._api_base = f"{self.base_url}/api/3/action"
        self._client: httpx.AsyncClient | None = None
        self._last_request_time: float = 0.0
        self._min_interval: float = 0.3  # seconds between requests

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={"User-Agent": "tanitdata/0.1"},
            )
        return self._client

    async def _rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def api_call(self, action: str, params: dict[str, Any]) -> dict | None:
        """Call a CKAN API action and return the result, or None on failure."""
        client = await self._ensure_client()
        await self._rate_limit()
        url = f"{self._api_base}/{action}"
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data["result"]
        return None

    async def datastore_sql(self, sql: str) -> dict | None:
        """Execute a SQL query against the DataStore."""
        return await self.api_call("datastore_search_sql", {"sql": sql})

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict | None:
        """Search a DataStore resource with optional filters."""
        params: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
            # CKAN expects filters as a JSON string
            import json

            params["filters"] = json.dumps(filters, ensure_ascii=False)
        return await self.api_call("datastore_search", params)

    async def package_search(
        self,
        query: str = "",
        fq: str = "",
        rows: int = 10,
        start: int = 0,
        facet_fields: list[str] | None = None,
    ) -> dict | None:
        """Search datasets with optional faceted search."""
        params: dict[str, Any] = {
            "q": query,
            "rows": rows,
            "start": start,
        }
        if fq:
            params["fq"] = fq
        if facet_fields:
            import json

            params["facet.field"] = json.dumps(facet_fields, ensure_ascii=False)
        return await self.api_call("package_search", params)

    async def package_show(self, dataset_id: str) -> dict | None:
        """Get full metadata for a single dataset."""
        return await self.api_call("package_show", {"id": dataset_id})
