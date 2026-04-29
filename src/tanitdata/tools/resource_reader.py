"""Resource reader: download and parse non-DataStore files (CSV, XLSX)."""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

from tanitdata.ckan_client import BaseClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.formatting import SKIP_COLS, format_datastore_result, format_source_footer

logger = logging.getLogger(__name__)

# Process-scoped cache: resource_id -> (fields, rows)
_cache: dict[str, tuple[list[str], list[dict[str, str]]]] = {}


def _sniff_format(data: bytes) -> str:
    """Return 'xlsx', 'xls_ole', or 'csv' based on the first bytes.

    The portal has ~150 files whose declared format lies about their real
    format (the inventory confirmed this: 10 .csv files are XLSX and
    143 .xls files are actually XLSX). Sniffing magic bytes before
    parsing avoids cryptic openpyxl / csv errors on those files and
    lets the reader recover gracefully.
    """
    head = data[:8]
    if head[:4] == b"PK\x03\x04":
        return "xlsx"
    if head[:4] == b"\xd0\xcf\x11\xe0":
        return "xls_ole"
    return "csv"


def _parse_csv(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    """Parse CSV bytes into (field_names, rows)."""
    # Try UTF-8 first, fall back to latin-1
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = data.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    fields = list(reader.fieldnames or [])
    rows = [{k: (v or "") for k, v in row.items()} for row in reader]
    return fields, rows


def _parse_xlsx(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    """Parse XLSX bytes into (field_names, rows)."""
    from openpyxl import load_workbook

    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active
    if ws is None:
        return [], []

    rows_iter = ws.iter_rows(values_only=True)
    # First row = headers
    header_row = next(rows_iter, None)
    if not header_row:
        wb.close()
        return [], []

    fields = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(header_row)]
    rows: list[dict[str, str]] = []
    for row in rows_iter:
        record = {}
        for i, val in enumerate(row):
            if i < len(fields):
                record[fields[i]] = str(val) if val is not None else ""
        rows.append(record)

    wb.close()
    return fields, rows


def _parse_xls_ole(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    """Parse legacy binary XLS (OLE2) bytes into (field_names, rows).

    Uses xlrd, which is the only maintained library that still supports
    the 1997-2003 binary format. xlrd is an optional runtime dependency —
    a clear ImportError here is better than a silent empty result.
    """
    try:
        import xlrd
    except ImportError as exc:
        raise RuntimeError(
            "Legacy XLS (binary OLE2) parsing requires xlrd. "
            "Install with `pip install xlrd`."
        ) from exc

    wb = xlrd.open_workbook(file_contents=data)
    ws = wb.sheet_by_index(0)
    if ws.nrows == 0:
        return [], []

    fields = [
        str(ws.cell_value(0, i)) if ws.cell_value(0, i) is not None else f"col_{i}"
        for i in range(ws.ncols)
    ]
    rows: list[dict[str, str]] = []
    for r in range(1, ws.nrows):
        record = {}
        for c in range(ws.ncols):
            v = ws.cell_value(r, c)
            record[fields[c]] = "" if v is None else str(v)
        rows.append(record)
    return fields, rows


async def read_resource(
    client: BaseClient,
    registry: SchemaRegistry,
    resource_id: str,
    limit: int = 100,
) -> str:
    """Download and parse a non-DataStore resource file.

    Supports CSV and XLSX formats. Caches parsed results in memory.
    """
    # Check cache first
    if resource_id in _cache:
        fields, all_rows = _cache[resource_id]
        rows = all_rows[:limit]
        return _format_output(fields, rows, len(all_rows), resource_id, registry, cached=True)

    # Get resource metadata
    try:
        meta = await client.resource_show(resource_id)
    except Exception:
        meta = None
    if not meta:
        return f"Resource `{resource_id}` not found on the portal."

    # Check if DataStore-active — redirect user
    if meta.get("datastore_active"):
        return (
            f"Resource `{resource_id}` is DataStore-active. "
            f"Use `query_datastore` instead for SQL access."
        )

    url = meta.get("url", "")
    fmt = (meta.get("format") or "").upper()
    name = meta.get("name", "")

    if not url:
        return f"Resource `{resource_id}` has no download URL."

    if fmt not in ("CSV", "XLSX", "XLS"):
        return (
            f"Resource `{resource_id}` ({name}) is in **{fmt}** format. "
            f"Only CSV and XLSX files can be read. "
            f"Download manually: {url}"
        )

    # Download. Pass resource_id through so SnapshotClient can resolve the
    # local scrape path without parsing the URL; LiveClient ignores it.
    data = await client.download_file(url, resource_id=resource_id)
    if data is None:
        # Either the file exceeds the 5 MB cap, or (snapshot mode) it
        # isn't cached locally. Either way, the best fallback is to point
        # the user at the portal URL for manual download when live access
        # resumes.
        return (
            f"Resource `{resource_id}` ({name}) is not available locally "
            f"(either missing from the offline snapshot or over the 5 MB cap). "
            f"Download manually: {url}"
        )

    # Parse — dispatch by magic bytes, not by declared format, because the
    # portal has ~150 files whose extension lies (.csv that are XLSX,
    # .xls that are XLSX). The `_sniff_format` helper is a no-op on correctly
    # labelled files.
    real_fmt = _sniff_format(data)
    try:
        if real_fmt == "xlsx":
            fields, all_rows = _parse_xlsx(data)
        elif real_fmt == "xls_ole":
            fields, all_rows = _parse_xls_ole(data)
        else:
            fields, all_rows = _parse_csv(data)
    except Exception as e:
        logger.warning("Failed to parse resource %s: %s", resource_id, e)
        return (
            f"Failed to parse resource `{resource_id}` ({name}): {e}\n"
            f"Download manually: {url}"
        )

    if not fields:
        return f"Resource `{resource_id}` ({name}) has no readable data."

    # Cache
    _cache[resource_id] = (fields, all_rows)

    rows = all_rows[:limit]
    return _format_output(fields, rows, len(all_rows), resource_id, registry, cached=False)


def _format_output(
    fields: list[str],
    rows: list[dict[str, str]],
    total: int,
    resource_id: str,
    registry: SchemaRegistry,
    cached: bool,
) -> str:
    """Format parsed file data in the same style as query_datastore."""
    clean_fields = [f for f in fields if f not in SKIP_COLS]

    lines: list[str] = []

    # Reuse the standard formatter
    lines.append(
        format_datastore_result(
            records=rows,
            fields=clean_fields,
            total=total,
            resource_id=resource_id,
        )
    )

    if cached:
        lines.append("\n*(cached — previously downloaded)*")

    # Source attribution
    source = registry.get_source_attribution(resource_id)
    if source:
        lines.append("")
        lines.append(format_source_footer([source]))

    return "\n".join(lines)
