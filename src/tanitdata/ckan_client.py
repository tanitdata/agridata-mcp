"""CKAN API client layer.

Two variants:

- `LiveClient` — async HTTP against catalog.agridata.tn. The historical
  implementation. Use when the portal is reachable.
- `SnapshotClient` — offline reads from the local snapshot (DuckDB over
  Parquet files + audit_full.json). Use when the portal is down or when
  running on a self-contained container image.

`make_client()` returns one or the other based on the `DATA_SOURCE`
environment variable (default `snapshot`).

`CKANClient` is kept as an alias of `LiveClient` so existing imports
(`from tanitdata.ckan_client import CKANClient`) and the old unit tests
continue to work unchanged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import httpx

from tanitdata import __version__

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------


class BaseClient:
    """Common interface implemented by LiveClient and SnapshotClient.

    Tool code depends only on these six async methods; the two variants
    are interchangeable.
    """

    base_url: str

    async def close(self) -> None:  # pragma: no cover - trivial default
        return None

    async def datastore_sql(self, sql: str) -> dict | None:
        raise NotImplementedError

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict | None:
        raise NotImplementedError

    async def package_search(
        self,
        query: str = "",
        fq: str = "",
        rows: int = 10,
        start: int = 0,
        facet_fields: list[str] | None = None,
    ) -> dict | None:
        raise NotImplementedError

    async def package_show(self, dataset_id: str) -> dict | None:
        raise NotImplementedError

    async def resource_show(self, resource_id: str) -> dict | None:
        raise NotImplementedError

    async def download_file(
        self,
        url: str,
        max_bytes: int = 5_242_880,
        resource_id: str | None = None,
    ) -> bytes | None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Live: HTTP client against catalog.agridata.tn
# ---------------------------------------------------------------------------


class LiveClient(BaseClient):
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
                headers={"User-Agent": f"tanitdata/{__version__}"},
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
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "CKAN API %s returned %s: %s",
                action, exc.response.status_code, exc,
            )
            return None
        except httpx.HTTPError as exc:
            logger.warning("CKAN API %s request failed: %s", action, exc)
            return None
        data = response.json()
        if data.get("success"):
            return data["result"]
        return None

    async def datastore_sql(self, sql: str) -> dict | None:
        return await self.api_call("datastore_search_sql", {"sql": sql})

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict | None:
        params: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
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
        params: dict[str, Any] = {"q": query, "rows": rows, "start": start}
        if fq:
            params["fq"] = fq
        if facet_fields:
            params["facet.field"] = json.dumps(facet_fields, ensure_ascii=False)
        return await self.api_call("package_search", params)

    async def package_show(self, dataset_id: str) -> dict | None:
        return await self.api_call("package_show", {"id": dataset_id})

    async def resource_show(self, resource_id: str) -> dict | None:
        return await self.api_call("resource_show", {"id": resource_id})

    async def download_file(
        self,
        url: str,
        max_bytes: int = 5_242_880,
        resource_id: str | None = None,  # ignored in live mode; present for interface parity
    ) -> bytes | None:
        client = await self._ensure_client()
        await self._rate_limit()
        try:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                length = resp.headers.get("content-length")
                if length and int(length) > max_bytes:
                    return None
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        return None
                    chunks.append(chunk)
            return b"".join(chunks)
        except httpx.HTTPStatusError as exc:
            logger.warning("Download %s returned %s", url, exc.response.status_code)
            return None
        except httpx.HTTPError as exc:
            logger.warning("Download %s failed: %s", url, exc)
            return None


# ---------------------------------------------------------------------------
# Snapshot: offline DuckDB + audit JSON
# ---------------------------------------------------------------------------


_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_TILDE_RE = re.compile(
    r'(?P<ident>'
    r'(?:(?:\w+|"[^"]+")\.)?'  # optional qualifier prefix: `alias.` or `"schema".`
    r'(?:\w+|"[^"]+")'         # identifier: bare word or double-quoted
    r')\s*~\s*(?P<lit>§§STR_\d+§§)'
)
_STR_RESTORE_RE = re.compile(r"§§STR_(\d+)§§")


def translate_postgres_tilde(sql: str) -> str:
    """Rewrite Postgres `<ident> ~ '<pattern>'` to `regexp_matches(<ident>, '<pattern>')`.

    DuckDB does not implement the `~` POSIX-regex operator. Tool code and
    LLM-authored SQL both use `~` as documented in schemas.json and the tool
    docstrings, so we translate at the executor boundary rather than changing
    the callers.

    Only occurrences outside of string literals are rewritten. `~` inside a
    `'single-quoted'` SQL literal is left alone. Identifiers may be bare,
    double-quoted, or qualified (`a."col"`).
    """
    # Stash string literals so the `~` regex can't match inside them
    literals: list[str] = []

    def _stash(m: re.Match[str]) -> str:
        literals.append(m.group(0))
        return f"§§STR_{len(literals) - 1}§§"

    sanitized = _STRING_LITERAL_RE.sub(_stash, sql)

    # Rewrite `<ident> ~ <string-literal-placeholder>` → regexp_matches(ident, literal)
    sanitized = _TILDE_RE.sub(
        lambda m: f"regexp_matches({m.group('ident')}, {m.group('lit')})",
        sanitized,
    )

    # Restore string literals
    return _STR_RESTORE_RE.sub(lambda m: literals[int(m.group(1))], sanitized)


class SnapshotClient(BaseClient):
    """Offline client backed by a DuckDB connection over snapshot Parquet files.

    Phase 1 implements `datastore_sql` and `datastore_search` (the DataStore
    read-path used by every tool that issues queries). The metadata-facing
    methods (`package_*`, `resource_show`, `download_file`) will be filled in
    as part of Phase 3/4 and currently raise `NotImplementedError` with a
    message directing operators at `DATA_SOURCE=live`.
    """

    # Sentinel "resource ID" used by the schema registry to enumerate all
    # DataStore-active resources. Live CKAN supports this natively; we
    # synthesise a compatible response from the audit JSON.
    _TABLE_METADATA = "_table_metadata"

    def __init__(
        self,
        parquet_dir: str | os.PathLike | None = None,
        audit_path: str | os.PathLike | None = None,
    ) -> None:
        self.base_url = "snapshot://local"
        self.parquet_dir = Path(
            parquet_dir
            or os.environ.get("SNAPSHOT_PARQUET_DIR")
            or Path(__file__).resolve().parent.parent.parent / "snapshot" / "parquet"
        )
        self.audit_path = Path(
            audit_path
            or os.environ.get("SNAPSHOT_AUDIT_PATH")
            or Path(__file__).resolve().parent.parent.parent / "audit_full.json"
        )

        # Lazy init — first call builds views and loads audit
        self._duck = None  # duckdb.DuckDBPyConnection
        self._lock = asyncio.Lock()
        self._views: set[str] = set()
        self._audit_cache: dict | None = None

    # ---- lazy init -------------------------------------------------------

    async def _ensure_ready(self) -> None:
        if self._duck is not None:
            return
        async with self._lock:
            if self._duck is not None:
                return
            self._duck = await asyncio.to_thread(self._build_connection)

    def _build_connection(self):
        """Open DuckDB and register one view per parquet file in the snapshot."""
        import duckdb

        con = duckdb.connect(":memory:")

        if not self.parquet_dir.exists():
            logger.warning(
                "SnapshotClient: parquet dir does not exist: %s — "
                "no DataStore tables will be available",
                self.parquet_dir,
            )
            return con

        registered = 0
        for pq in self.parquet_dir.glob("*.parquet"):
            rid = pq.stem  # "<uuid>"
            # Escape single quotes in the path for SQL string literal safety
            safe_path = str(pq).replace("'", "''").replace("\\", "/")
            try:
                con.execute(
                    f'CREATE OR REPLACE VIEW "{rid}" AS '
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                self._views.add(rid)
                registered += 1
            except Exception as exc:  # pragma: no cover - per-file failure
                logger.warning(
                    "SnapshotClient: failed to register view for %s: %s", rid, exc
                )

        logger.info(
            "SnapshotClient: %d Parquet views registered from %s",
            registered, self.parquet_dir,
        )
        return con

    def _load_audit(self) -> dict:
        """Lazy-load audit_full.json (cached for the process lifetime)."""
        if self._audit_cache is not None:
            return self._audit_cache
        if not self.audit_path.exists():
            logger.warning("SnapshotClient: audit not found at %s", self.audit_path)
            self._audit_cache = {
                "datasets": [],
                "resources": [],
                "organizations": [],
                "groups": [],
                "datastore_schemas": {},
            }
            return self._audit_cache
        with open(self.audit_path, encoding="utf-8") as f:
            self._audit_cache = json.load(f)
        return self._audit_cache

    # ---- query path ------------------------------------------------------

    @staticmethod
    def _rows_to_ckan_shape(cursor) -> dict:
        """Format a DuckDB cursor result into CKAN's datastore_search response shape."""
        desc = cursor.description or []
        col_names = [d[0] for d in desc]
        rows = cursor.fetchall()
        records = [dict(zip(col_names, row)) for row in rows]
        # CKAN reports all DataStore column types as "text" (our Parquet is
        # VARCHAR-only to match that invariant).
        fields = [{"id": name, "type": "text"} for name in col_names]
        return {"records": records, "fields": fields, "total": len(records)}

    async def datastore_sql(self, sql: str) -> dict | None:
        await self._ensure_ready()
        translated = translate_postgres_tilde(sql)

        def _run() -> dict | None:
            try:
                cur = self._duck.execute(translated)
            except Exception as exc:
                logger.warning("SnapshotClient SQL failed: %s\nSQL: %s", exc, translated)
                return None
            return self._rows_to_ckan_shape(cur)

        return await asyncio.to_thread(_run)

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict | None:
        await self._ensure_ready()

        # Special case — schema registry uses this to enumerate all resources
        if resource_id == self._TABLE_METADATA:
            return self._table_metadata_response(limit=limit, offset=offset)

        if resource_id not in self._views:
            # Same failure class as live CKAN when a resource isn't DataStore-active
            logger.info(
                "SnapshotClient: no view for resource %s (not in snapshot)",
                resource_id,
            )
            return None

        where = self._build_where(filters) if filters else ""

        # Build base SQL for data read. `limit=0` means "schema only, no rows"
        # — CKAN returns empty records + fields. Preserve that contract.
        def _run() -> dict | None:
            try:
                # Total count (respecting filters) for parity with CKAN's `total`
                total_sql = f'SELECT COUNT(*) FROM "{resource_id}"{where}'
                total = self._duck.execute(total_sql).fetchone()[0]

                limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else " LIMIT 0"
                offset_clause = f" OFFSET {int(offset)}" if offset else ""
                cur = self._duck.execute(
                    f'SELECT * FROM "{resource_id}"{where}{limit_clause}{offset_clause}'
                )
                out = self._rows_to_ckan_shape(cur)
                out["total"] = int(total)
                return out
            except Exception as exc:
                logger.warning(
                    "SnapshotClient datastore_search failed for %s: %s",
                    resource_id, exc,
                )
                return None

        return await asyncio.to_thread(_run)

    @staticmethod
    def _build_where(filters: dict[str, Any]) -> str:
        """Translate CKAN filter dict into a SQL WHERE clause.

        CKAN accepts `{col: value}` (equality) or `{col: [values]}` (IN).
        """
        parts: list[str] = []
        for col, val in filters.items():
            col_sql = f'"{col}"'
            if isinstance(val, list):
                escaped = ", ".join(
                    f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in val
                )
                parts.append(f"{col_sql} IN ({escaped})")
            else:
                esc = str(val).replace("'", "''")
                parts.append(f"{col_sql} = '{esc}'")
        if not parts:
            return ""
        return " WHERE " + " AND ".join(parts)

    def _table_metadata_response(self, limit: int, offset: int) -> dict:
        """Synthesize a `_table_metadata`-style response from audit JSON.

        Live CKAN returns rows like `{name, alias_of, oid}`. The schema
        registry only cares about `name` (the resource UUID) and the
        `alias_of` flag for skipping aliases. We emit both; `alias_of` is
        always None because the audit records only real tables.
        """
        audit = self._load_audit()
        rids = [r["id"] for r in audit["resources"] if r.get("datastore_active")]
        # Preserve the view availability: only include resources that have a
        # registered DuckDB view (i.e. a corresponding parquet file).
        rids = [rid for rid in rids if rid in self._views]

        sliced = rids[offset : offset + limit] if limit else rids[offset:]
        records = [
            {"name": rid, "alias_of": None, "oid": None} for rid in sliced
        ]
        return {
            "records": records,
            "fields": [
                {"id": "name", "type": "text"},
                {"id": "alias_of", "type": "text"},
                {"id": "oid", "type": "text"},
            ],
            "total": len(rids),
        }

    # ---- metadata path: package_search / package_show / resource_show --

    # Parse one `key:value` pair from a CKAN `fq` string. CKAN's Solr backend
    # supports more complex syntax, but the tanitdata tools only emit the
    # simple space-separated form, which is what we model here.
    _FQ_TOKEN_RE = re.compile(r'(\w+):("[^"]+"|\S+)')

    @staticmethod
    def _parse_fq(fq: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        if not fq:
            return parsed
        for m in SnapshotClient._FQ_TOKEN_RE.finditer(fq):
            key = m.group(1).lower()
            val = m.group(2).strip('"')
            parsed[key] = val
        return parsed

    def _build_search_indices(self) -> None:
        """Build the per-process search indices over the audit.

        Produces four parallel arrays in `_search_index`:
          - datasets:          the raw dataset dicts from the audit
          - title_norm:        accent-folded lowercase titles
          - notes_norm:        accent-folded lowercase notes
          - tag_norm_sets:     set of accent-folded tag tokens per dataset
        Plus sidecar maps:
          - _datasets_by_id / _datasets_by_name
          - _resources_by_dataset_id: dataset_id → list of resource dicts
          - _org_title_by_slug
          - _res_formats_by_dataset_id: dataset_id → set of uppercase formats
        """
        if hasattr(self, "_search_index"):
            return
        # Late import to avoid pulling bibliography on module load
        from tanitdata.tools.bibliography import _normalize

        audit = self._load_audit()
        datasets = audit.get("datasets", [])

        title_norm: list[str] = []
        notes_norm: list[str] = []
        tag_norm_sets: list[set[str]] = []
        for ds in datasets:
            title_norm.append(_normalize(str(ds.get("title") or "")))
            notes_norm.append(_normalize(str(ds.get("notes") or "")))
            tags = ds.get("tags") or []
            tag_tokens = set()
            for t in tags:
                # Tag list entries can be bare strings or {name, display_name}
                # objects in some CKAN versions; handle both.
                if isinstance(t, dict):
                    t = t.get("name") or t.get("display_name") or ""
                tag_tokens.update(_normalize(str(t)).split())
            tag_norm_sets.append(tag_tokens)

        self._search_index = {
            "datasets": datasets,
            "title_norm": title_norm,
            "notes_norm": notes_norm,
            "tag_norm_sets": tag_norm_sets,
        }
        self._datasets_by_id = {d["id"]: d for d in datasets}
        self._datasets_by_name = {d["name"]: d for d in datasets}
        self._org_title_by_slug = {
            o.get("name", ""): o.get("title", "")
            for o in audit.get("organizations", [])
        }
        # Group entries from audit carry a mojibake-decoded display name
        # when possible; fall back to the slug.
        self._group_title_by_slug = {
            g.get("name", ""): g.get("display_name") or g.get("title") or ""
            for g in audit.get("groups", [])
        }

        self._resources_by_dataset_id = {}
        self._res_formats_by_dataset_id = {}
        self._resources_by_id: dict[str, dict] = {}
        for r in audit.get("resources", []):
            ds_id = r.get("dataset_id", "")
            self._resources_by_dataset_id.setdefault(ds_id, []).append(r)
            self._resources_by_id[r["id"]] = r
            fmt = (r.get("format") or "").upper()
            if fmt:
                self._res_formats_by_dataset_id.setdefault(ds_id, set()).add(fmt)

    def _hydrate_dataset(self, ds: dict) -> dict:
        """Return a copy of `ds` with embedded `organization`, `groups`, `resources`.

        Mirrors CKAN's `package_show` / `package_search` response shape so
        downstream consumers (tools/utilities that read `ds.organization.title`,
        `ds.groups[].display_name`, `ds.resources[].id`, …) work unchanged.
        """
        out = dict(ds)
        org_slug = ds.get("organization") or ""
        out["organization"] = {
            "name": org_slug,
            "title": self._org_title_by_slug.get(org_slug, org_slug),
        }
        group_slugs = ds.get("groups") or []
        out["groups"] = [
            {
                "name": slug,
                "display_name": self._group_title_by_slug.get(slug, slug),
            }
            for slug in group_slugs
        ]
        out["resources"] = list(self._resources_by_dataset_id.get(ds["id"], []))
        out["num_resources"] = len(out["resources"])
        return out

    async def package_search(
        self,
        query: str = "",
        fq: str = "",
        rows: int = 10,
        start: int = 0,
        facet_fields: list[str] | None = None,
    ) -> dict | None:
        await self._ensure_ready()
        self._build_search_indices()

        # Late import — `_normalize` lives in bibliography to avoid duplication
        from tanitdata.tools.bibliography import _normalize

        # Parse hard filters from fq
        fq_parsed = self._parse_fq(fq)
        org_filter = fq_parsed.get("organization")
        group_filter = fq_parsed.get("groups")
        format_filter = fq_parsed.get("res_format")
        if format_filter:
            format_filter = format_filter.upper()

        # Tokenise query (accent-folded, length ≥ 2). For each keyword we
        # also compute a 5-char prefix used as a lightweight French-stemming
        # fallback so `exportation`/`exportations`/`exportés` all reach the
        # same documents regardless of which form the query used.
        keywords = [k for k in _normalize(query).split() if len(k) >= 2]
        prefixes = [k[:5] if len(k) >= 6 else None for k in keywords]

        def _score(i: int, ds: dict) -> int:
            """Score a dataset against the parsed keyword list.

            Per-keyword: title=3, tags=2, notes=1. Each keyword first tries
            an exact substring match; if that fails, it falls back to its
            5-char prefix as a lightweight French-stemming proxy (so
            `exportation` can reach titles carrying `exportes`). Prefix
            matches receive the same weight as exact matches — the prefix
            is deliberately conservative (≥ 5 chars, keyword ≥ 6 chars)
            to keep noise low.

            On top of the per-field scoring, we add a coverage bonus
            (+2 per distinct keyword matched) so datasets that hit BOTH
            `exportation` and `poisson` outrank ones that only match the
            common keyword across many fields. This is the same signal
            Solr's coordination factor captures.

            Missing keywords do NOT disqualify — a dataset matching a
            single keyword can still rank, just below more-covered ones.
            """
            idx = self._search_index
            title = idx["title_norm"][i]
            notes = idx["notes_norm"][i]
            tag_set = idx["tag_norm_sets"][i]
            total = 0
            covered = 0
            for j, kw in enumerate(keywords):
                matched = False
                pref = prefixes[j]
                # Title (weight 3) — prefer exact substring, fall back to prefix
                if kw in title:
                    total += 3
                    matched = True
                elif pref and pref in title:
                    total += 3
                    matched = True
                # Tags (weight 2)
                if any(kw in t for t in tag_set):
                    total += 2
                    matched = True
                elif pref and any(pref in t for t in tag_set):
                    total += 2
                    matched = True
                # Notes (weight 1) — notes are noisy, keep prefix fallback off
                if kw in notes:
                    total += 1
                    matched = True
                if matched:
                    covered += 1
            if len(keywords) > 1:
                total += 2 * covered
            return total

        datasets = self._search_index["datasets"]
        matched: list[tuple[int, dict]] = []
        for i, ds in enumerate(datasets):
            # Hard filters first (cheaper than scoring)
            if org_filter and (ds.get("organization") or "") != org_filter:
                continue
            if group_filter and group_filter not in (ds.get("groups") or []):
                continue
            if format_filter:
                formats = self._res_formats_by_dataset_id.get(ds["id"], set())
                if format_filter not in formats:
                    continue

            if keywords:
                score = _score(i, ds)
                if score <= 0:
                    # No keyword matched any field — drop the dataset.
                    continue
            else:
                score = 0

            matched.append((score, ds))

        # Sort by score desc; stable secondary sort by title for determinism
        matched.sort(key=lambda sd: (-sd[0], (sd[1].get("title") or "")))

        count = len(matched)
        page = matched[start : start + rows]
        results = [self._hydrate_dataset(ds) for _, ds in page]

        # Facet aggregation — counted over the full filtered result set,
        # not the page slice (matches CKAN's behaviour).
        search_facets: dict[str, Any] = {}
        if facet_fields:
            all_datasets = [ds for _, ds in matched]
            if "organization" in facet_fields:
                counter: dict[str, int] = {}
                for ds in all_datasets:
                    slug = ds.get("organization") or ""
                    if slug:
                        counter[slug] = counter.get(slug, 0) + 1
                items = [
                    {
                        "name": slug,
                        "display_name": self._org_title_by_slug.get(slug, slug),
                        "count": n,
                    }
                    for slug, n in counter.items()
                ]
                items.sort(key=lambda x: -x["count"])
                search_facets["organization"] = {
                    "title": "organization",
                    "items": items,
                }
            if "groups" in facet_fields:
                counter = {}
                for ds in all_datasets:
                    for slug in ds.get("groups") or []:
                        counter[slug] = counter.get(slug, 0) + 1
                items = [
                    {
                        "name": slug,
                        "display_name": self._group_title_by_slug.get(slug, slug),
                        "count": n,
                    }
                    for slug, n in counter.items()
                ]
                items.sort(key=lambda x: -x["count"])
                search_facets["groups"] = {"title": "groups", "items": items}
            if "res_format" in facet_fields:
                counter = {}
                for ds in all_datasets:
                    for fmt in self._res_formats_by_dataset_id.get(ds["id"], set()):
                        counter[fmt] = counter.get(fmt, 0) + 1
                items = [
                    {"name": fmt, "display_name": fmt, "count": n}
                    for fmt, n in counter.items()
                ]
                items.sort(key=lambda x: -x["count"])
                search_facets["res_format"] = {
                    "title": "res_format",
                    "items": items,
                }

        return {
            "count": count,
            "results": results,
            "search_facets": search_facets,
        }

    async def package_show(self, dataset_id: str) -> dict | None:
        await self._ensure_ready()
        self._build_search_indices()

        ds = (
            self._datasets_by_id.get(dataset_id)
            or self._datasets_by_name.get(dataset_id)
        )
        if ds is None:
            return None
        return self._hydrate_dataset(ds)

    async def resource_show(self, resource_id: str) -> dict | None:
        await self._ensure_ready()
        self._build_search_indices()
        r = self._resources_by_id.get(resource_id)
        if r is None:
            return None
        # Attach parent dataset metadata (matches CKAN's response shape: the
        # full resource dict plus `package_id` == dataset_id).
        return dict(r)

    # ---- offline file read ---------------------------------------------

    # CKAN resource download URLs follow a fixed pattern:
    #   https://<host>/dataset/<dataset-uuid>/resource/<resource-uuid>/download/<filename>
    # Extracting the resource UUID lets us resolve callers who only pass the URL.
    _URL_RESOURCE_RE = re.compile(
        r"/resource/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})/",
        re.IGNORECASE,
    )

    def _load_scrape_index(self) -> dict:
        """Lazy-load snapshot/scrape_index.json (sidecar written by build_snapshot.py)."""
        if hasattr(self, "_scrape_index_cache"):
            return self._scrape_index_cache

        # Search order: explicit env var → sibling of audit → sibling of parquet_dir
        candidates = []
        env_path = os.environ.get("SNAPSHOT_SCRAPE_INDEX")
        if env_path:
            candidates.append(Path(env_path))
        candidates.append(self.audit_path.parent / "snapshot" / "scrape_index.json")
        candidates.append(self.parquet_dir.parent / "scrape_index.json")

        for p in candidates:
            if p.exists():
                with open(p, encoding="utf-8") as f:
                    self._scrape_index_cache = json.load(f)
                # Keep a Path reference to resolve relative file paths
                self._scrape_index_root = Path(
                    self._scrape_index_cache.get("meta", {}).get("scrape_root", "")
                ) or p.parent.parent
                return self._scrape_index_cache

        logger.warning(
            "SnapshotClient: scrape_index.json not found; "
            "download_file will fail for non-DataStore resources"
        )
        self._scrape_index_cache = {"meta": {}, "resources": {}}
        self._scrape_index_root = Path(".")
        return self._scrape_index_cache

    def _resolve_scrape_path(self, resource_id: str) -> Path | None:
        """Map a resource UUID → absolute path to its cached scrape file."""
        idx = self._load_scrape_index()
        rel = (idx.get("resources") or {}).get(resource_id)
        if not rel:
            return None
        # The sidecar stores paths relative to the original scrape root. The
        # scrape folder may have moved between build time and runtime, so
        # also accept an alternate root via env var.
        env_root = os.environ.get("SNAPSHOT_SCRAPE_ROOT")
        if env_root:
            return Path(env_root) / rel
        return self._scrape_index_root / rel

    async def download_file(
        self,
        url: str,
        max_bytes: int = 5_242_880,
        resource_id: str | None = None,
    ) -> bytes | None:
        """Return the bytes of a cached scrape file.

        `resource_id` takes precedence when provided. Falls back to parsing
        the CKAN resource UUID out of the `url`. If the resource isn't in
        the local scrape, returns None — matching LiveClient's failure
        semantics so `read_resource_tool`'s existing error-path formatting
        kicks in (including the portal URL for when live access resumes).

        Size cap applies locally too: files larger than `max_bytes` return
        None (same contract as LiveClient).
        """
        await self._ensure_ready()

        rid = resource_id
        if rid is None:
            m = self._URL_RESOURCE_RE.search(url or "")
            if m:
                rid = m.group(1).lower()
        if rid is None:
            logger.info(
                "SnapshotClient.download_file: could not extract resource_id from URL %r",
                url,
            )
            return None

        path = self._resolve_scrape_path(rid)
        if path is None or not path.exists():
            logger.info(
                "SnapshotClient.download_file: resource %s not cached locally", rid
            )
            return None

        size = path.stat().st_size
        if size > max_bytes:
            logger.info(
                "SnapshotClient.download_file: %s exceeds cap (%d > %d)",
                rid, size, max_bytes,
            )
            return None

        def _read_bytes() -> bytes:
            with open(path, "rb") as f:
                return f.read()

        return await asyncio.to_thread(_read_bytes)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_client(source: str | None = None) -> BaseClient:
    """Return the appropriate client for the current deployment.

    `source` is read from the `DATA_SOURCE` environment variable when not
    given. Accepted values: `snapshot` (default) or `live`. Any other value
    raises `ValueError` so misconfiguration is loud rather than silent.
    """
    chosen = (source or os.environ.get("DATA_SOURCE") or "snapshot").lower()
    if chosen == "live":
        logger.info("CKAN client: LiveClient (DATA_SOURCE=live)")
        return LiveClient()
    if chosen == "snapshot":
        logger.info("CKAN client: SnapshotClient (DATA_SOURCE=snapshot)")
        return SnapshotClient()
    raise ValueError(
        f"Unknown DATA_SOURCE={chosen!r}. Expected 'snapshot' or 'live'."
    )


# ---------------------------------------------------------------------------
# Back-compat
# ---------------------------------------------------------------------------


# Keep the name `CKANClient` importable so existing tools/tests that do
# `from tanitdata.ckan_client import CKANClient` get the live implementation
# (preserving historical behaviour).
CKANClient = LiveClient


__all__ = [
    "BaseClient",
    "LiveClient",
    "SnapshotClient",
    "CKANClient",
    "make_client",
    "translate_postgres_tilde",
]
