"""Preview tool: full schema + sample rows for any DataStore resource."""

from __future__ import annotations

import re
from typing import Any

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.formatting import format_source_footer

_SKIP_COLS = {"_id", "_full_text"}

# Column-name patterns for unit hints
_UNIT_PATTERNS: list[tuple[str, str]] = [
    (r"_quintaux$|_qx$", "quintaux"),
    (r"_tonnes?$|_t$", "tonnes"),
    (r"_ha$|hectare", "hectares"),
    (r"_kg$|_kilos?$", "kg"),
    (r"_litres?$|_l$", "litres"),
    (r"_mm$|precipitations_mm", "mm"),
    (r"_m3$|million_m3", "m3"),
    (r"_dn$|_dinars?$", "dinars"),
    (r"_pourcent$|_percent$|pourcentage", "%"),
]

# Patterns for inferring likely types from sample values
_NUMERIC_RE = re.compile(r"^-?\d+([.,]\d+)?$")
_DATE_RE = re.compile(
    r"^\d{4}[-/]\d{2}[-/]\d{2}"  # 2024-01-15 or 2024/01/15
    r"|^\d{2}[-/]\d{2}[-/]\d{4}"  # 15-01-2024 or 15/01/2024
)


def _unit_hints(fields: list[str]) -> list[str]:
    """Extract unit hints from column names."""
    hints: list[str] = []
    seen: set[str] = set()
    for f in fields:
        fl = f.lower()
        for pat, unit in _UNIT_PATTERNS:
            if re.search(pat, fl) and unit not in seen:
                hints.append(f"`{f}` -> {unit}")
                seen.add(unit)
                break
    return hints


def _infer_type(values: list[str]) -> str:
    """Infer a likely type from sample values."""
    non_empty = [v for v in values if v and v.strip()]
    if not non_empty:
        return "text"
    numeric_count = sum(1 for v in non_empty if _NUMERIC_RE.match(v.strip()))
    if numeric_count >= len(non_empty) * 0.8:
        return "likely numeric"
    date_count = sum(1 for v in non_empty if _DATE_RE.match(v.strip()))
    if date_count >= len(non_empty) * 0.8:
        return "likely date"
    return "text"


async def get_resource_preview(
    client: CKANClient,
    registry: SchemaRegistry,
    resource_id: str,
) -> str:
    """Return the full schema plus 3 sample rows for any DataStore resource.

    Includes field names with inferred types, sample records, record count,
    unit hints, source attribution, and data availability.
    """
    # Fetch schema + 3 rows in one call
    try:
        result = await client.datastore_search(resource_id=resource_id, limit=3)
    except Exception:
        result = None
    if not result:
        return (
            f"Could not fetch data from resource `{resource_id}`. "
            f"It may not be DataStore-active or the ID may be incorrect."
        )

    raw_fields = result.get("fields", [])
    records = result.get("records", [])
    total = result.get("total", 0)

    # Filter internal columns
    fields = [f for f in raw_fields if f.get("id") not in _SKIP_COLS]
    field_names = [f["id"] for f in fields]

    lines: list[str] = []
    lines.append(f"## Resource preview: `{resource_id}`")
    lines.append(f"**Total records:** {total:,}")
    lines.append("")

    # Schema with inferred types
    lines.append("### Schema")
    for f in fields:
        fname = f["id"]
        # Collect sample values for this field from the 3 rows
        samples = [str(rec.get(fname, "")) for rec in records]
        inferred = _infer_type(samples)
        stored_type = f.get("type", "text")
        type_label = inferred if inferred != "text" else stored_type
        lines.append(f"- `{fname}` ({type_label})")
    lines.append("")

    # Unit hints
    hints = _unit_hints(field_names)
    if hints:
        lines.append("**Unit hints:** " + "; ".join(hints))
        lines.append("")

    # Sample rows as table
    if records:
        lines.append(f"### Sample data ({len(records)} row{'s' if len(records) != 1 else ''})")

        # Filter to visible columns
        cols = [f["id"] for f in fields]

        if len(cols) <= 8:
            # Markdown table
            lines.append("| " + " | ".join(cols) + " |")
            lines.append("| " + " | ".join("---" for _ in cols) + " |")
            for rec in records:
                vals = []
                for c in cols:
                    v = str(rec.get(c, ""))
                    # Truncate long values for readability
                    if len(v) > 40:
                        v = v[:37] + "..."
                    # Escape pipe characters in values
                    v = v.replace("|", "\\|")
                    vals.append(v)
                lines.append("| " + " | ".join(vals) + " |")
        else:
            # List format for wide schemas
            for j, rec in enumerate(records, 1):
                lines.append(f"**Row {j}:**")
                for c in cols:
                    v = str(rec.get(c, ""))
                    if len(v) > 80:
                        v = v[:77] + "..."
                    lines.append(f"  - {c}: {v}")
                lines.append("")
    else:
        lines.append("*No records in this resource.*")

    lines.append("")

    # Registry note
    lines.append("*All fields are stored as text. Use `::numeric` or `::timestamp` casts in SQL for math/date operations.*")

    # Data availability context
    ctx = registry.get_resource_context(resource_id)
    if ctx:
        domains = ctx.get("domains", [])
        gov = ctx.get("gouvernorat")
        for d in domains:
            avail = registry.get_data_availability(d, gouvernorat=gov)
            if avail:
                lines.append("")
                lines.append(f"**Data availability ({d}):** {avail}")

    # Source attribution
    source = registry.get_source_attribution(resource_id)
    if source:
        lines.append("")
        lines.append(format_source_footer([source]))

    return "\n".join(lines)
