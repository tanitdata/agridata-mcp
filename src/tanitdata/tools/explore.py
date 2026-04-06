"""Explore tool: browse any domain's resources without executing DataStore queries."""

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


def _sample_fields(fields: list[str], n: int = 3) -> list[str]:
    """Return up to n sample field names, skipping internal columns."""
    clean = [f for f in fields if f not in _SKIP_COLS]
    return clean[:n]


async def explore_domain(
    client: CKANClient,
    registry: SchemaRegistry,
    domain: str,
    gouvernorat: str | None = None,
    keyword: str | None = None,
) -> str:
    """Explore a domain's resources without executing DataStore queries.

    Returns resource metadata, field lists, unit hints, coverage summary,
    and source attribution for each matching resource.
    """
    valid_domains = registry.domains
    if domain not in valid_domains:
        return (
            f"Unknown domain `{domain}`.\n\n"
            f"Available domains: {', '.join(f'`{d}`' for d in sorted(valid_domains))}"
        )

    resources = registry.get_domain_resources(domain, gouvernorat=gouvernorat)

    if not resources:
        avail = registry.get_data_availability(domain, gouvernorat=gouvernorat)
        return f"No resources found.\n\n**Availability:** {avail}"

    # Keyword filter: match against resource name, dataset name, or field names
    if keyword:
        kw = keyword.lower()
        filtered = []
        for res in resources:
            name_match = kw in res.get("name", "").lower()
            dataset_match = kw in res.get("dataset", "").lower()
            field_match = any(kw in f.lower() for f in res.get("fields", []))
            if name_match or dataset_match or field_match:
                filtered.append(res)
        resources = filtered

    if not resources:
        extra = f" matching keyword '{keyword}'" if keyword else ""
        gov_note = f" in {gouvernorat}" if gouvernorat else ""
        return f"No resources found{gov_note}{extra} in domain `{domain}`."

    # Exclude Excel overflow resources
    resources = [r for r in resources if r.get("records", 0) < 1_048_575]

    lines: list[str] = []
    gov_label = f" -- {gouvernorat}" if gouvernorat else ""
    kw_label = f" (keyword: '{keyword}')" if keyword else ""
    lines.append(f"## {domain}{gov_label}{kw_label}")
    lines.append(f"**{len(resources)} resource(s)**\n")

    # Resource listing
    sources: list[dict[str, str]] = []
    gov_resources: dict[str, int] = {}

    for res in resources:
        rid = res["id"]
        name = res.get("name", "")
        dataset = res.get("dataset", "")
        fields = [f for f in res.get("fields", []) if f not in _SKIP_COLS]
        records = res.get("records", 0)

        # Get governorate for this resource
        ctx = registry.get_resource_context(rid)
        gov = ctx.get("gouvernorat", "unknown") if ctx else "unknown"
        gov_resources[gov] = gov_resources.get(gov, 0) + 1

        lines.append(f"### {name or dataset}")
        lines.append(f"- **Resource ID:** `{rid}`")
        lines.append(f"- **Dataset:** {dataset}")
        lines.append(f"- **Governorate:** {gov}")
        lines.append(f"- **Records:** {records:,}")
        lines.append(f"- **Fields ({len(fields)}):** {', '.join(f'`{f}`' for f in fields)}")

        # Sample columns
        sample = _sample_fields(fields)
        if sample:
            lines.append(f"- **Sample columns:** {', '.join(f'`{s}`' for s in sample)}")

        # Unit hints
        hints = _unit_hints(fields)
        if hints:
            lines.append(f"- **Unit hints:** {'; '.join(hints)}")

        lines.append("")

        # Collect source attribution
        src = registry.get_source_attribution(rid)
        if src:
            sources.append(src)

    # Coverage summary
    lines.append("---")
    lines.append("## Coverage summary")
    coverage = registry.get_coverage_summary(domain)
    if coverage:
        for gov, count in sorted(coverage.items()):
            marker = " *" if gouvernorat and gov.lower() == gouvernorat.lower() else ""
            lines.append(f"- **{gov}:** {count} resource(s){marker}")
    lines.append("")

    # Data availability
    avail = registry.get_data_availability(domain, gouvernorat=gouvernorat)
    lines.append(f"**Data availability:** {avail}")

    # Source footer (compact -- deduplicate by dataset)
    seen_datasets: set[str] = set()
    unique_sources: list[dict[str, str]] = []
    for src in sources:
        ds = src.get("dataset_name", "")
        if ds not in seen_datasets:
            seen_datasets.add(ds)
            unique_sources.append(src)

    if unique_sources:
        lines.append("")
        lines.append(format_source_footer(unique_sources))

    return "\n".join(lines)
