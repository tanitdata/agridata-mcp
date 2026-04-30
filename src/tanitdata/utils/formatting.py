"""Result formatting utilities for MCP tool responses."""

from __future__ import annotations

from typing import Any

PORTAL_BASE = "https://catalog.agridata.tn/dataset"

# Internal DataStore columns to skip in all result presentation
SKIP_COLS = {"_id", "_full_text"}


def format_dataset_list(datasets: list[dict[str, Any]]) -> str:
    """Format a list of datasets as readable text for MCP responses."""
    if not datasets:
        return "No datasets found."

    lines: list[str] = []
    for i, ds in enumerate(datasets, 1):
        title = ds.get("title", ds.get("name", "Untitled"))
        org = ds.get("organization", {})
        org_title = org.get("title", "Unknown") if isinstance(org, dict) else "Unknown"
        num_resources = ds.get("num_resources", 0)
        groups = [g.get("display_name", g.get("name", "")) for g in ds.get("groups", [])]
        group_str = ", ".join(groups) if groups else "None"

        ds_slug = ds.get("name", ds.get("id", "N/A"))

        lines.append(f"### {i}. {title}")
        lines.append(f"- **Dataset slug:** `{ds_slug}`")
        lines.append(f"- **Organization:** {org_title}")
        lines.append(f"- **Groups:** {group_str}")
        lines.append(f"- **Resources:** {num_resources}")
        lines.append(f"- **Portal:** {PORTAL_BASE}/{ds_slug}")
        if ds.get("notes"):
            notes = ds["notes"][:200]
            if len(ds["notes"]) > 200:
                notes += "..."
            lines.append(f"- **Description:** {notes}")
        lines.append("")

    return "\n".join(lines)


def format_datastore_result(
    records: list[dict[str, Any]],
    fields: list[dict[str, str]] | list[str],
    total: int | None = None,
    resource_id: str = "",
) -> str:
    """Format DataStore query results as readable text with schema info."""
    lines: list[str] = []

    # Schema section
    if fields:
        lines.append("**Schema:**")
        for f in fields:
            if isinstance(f, dict):
                name = f.get("id", "?")
                ftype = f.get("type", "text")
                if name == "_id":
                    continue
                lines.append(f"- `{name}` ({ftype})")
            else:
                lines.append(f"- `{f}`")
        lines.append("")

    # Summary
    if total is not None:
        lines.append(f"**Total records:** {total:,}")
    lines.append(f"**Showing:** {len(records)} record(s)")
    if resource_id:
        lines.append(f"**Resource ID:** `{resource_id}`")
    lines.append("")

    # Records as a compact table or list
    if records:
        # Get column names from first record, skip internal columns
        cols = [k for k in records[0].keys() if k not in SKIP_COLS]

        if len(cols) <= 6:
            # Markdown table for narrow results
            lines.append("| " + " | ".join(cols) + " |")
            lines.append("| " + " | ".join("---" for _ in cols) + " |")
            for rec in records:
                vals = [str(rec.get(c, "")) for c in cols]
                lines.append("| " + " | ".join(vals) + " |")
        else:
            # List format for wide results
            for j, rec in enumerate(records, 1):
                lines.append(f"**Record {j}:**")
                for c in cols:
                    lines.append(f"  - {c}: {rec.get(c, '')}")
                lines.append("")

    return "\n".join(lines)


def format_source_footer(sources: list[dict[str, str]]) -> str:
    """Format a Sources section from source attribution dicts.

    Each source dict should have: resource_id, resource_name, dataset_name,
    dataset_title, organization_title, portal_url.
    """
    if not sources:
        return ""

    lines: list[str] = ["---", "**Sources:**"]

    for src in sources:
        title = src.get("dataset_title") or src.get("dataset_name", "Unknown dataset")
        org = src.get("organization_title") or src.get("organization", "")
        rid = src.get("resource_id", "")
        url = src.get("portal_url", "")

        if len(sources) == 1:
            lines.append(f"- **Dataset:** {title}")
            if org:
                lines.append(f"- **Organization:** {org}")
            lines.append(f"- **Resource ID:** `{rid}`")
            if url:
                lines.append(f"- **Portal:** {url}")
        else:
            entry = f"- {title}"
            if org:
                entry += f" ({org})"
            lines.append(entry)
            parts: list[str] = [f"`{rid}`"]
            if url:
                parts.append(url)
            lines.append(f"  {' — '.join(parts)}")

    return "\n".join(lines)
