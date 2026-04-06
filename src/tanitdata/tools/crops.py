"""Crop production tool: query_crop_production.

Queries 239 crop_production resources across 23+ governorates.
176 unique schema signatures handled via column-name-driven detection:
  - detect production/area/year/crop columns by name pattern
  - infer production units from column name (tonnes, quintaux, 1000 quintaux)
  - normalize production values to tonnes in the response
  - handle wide-format (year-as-column) and crop-as-column formats
  - skip Excel overflow resources (>=1,048,575 rows)

SQL strategy: always SELECT * (avoids CKAN DataStore SQL restrictions on
CASE WHEN and keeps URL short). Normalization happens in Python display.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.formatting import format_source_footer

logger = logging.getLogger(__name__)

_SKIP_COLS = {"_id", "_full_text"}
_OVERFLOW = 1_048_575
_MAX_RESOURCES = 15
_MAX_PER_GOV = 5

# ---------------------------------------------------------------------------
# Crop type resolution
# ---------------------------------------------------------------------------

# User input -> keywords for matching resource/dataset names and field names
_CROP_ALIASES: dict[str, list[str]] = {
    # Cereals
    "cereales": ["cereale", "ble", "orge"],
    "céréales": ["cereale", "ble", "orge"],
    "cereals": ["cereale", "ble", "orge"],
    "ble": ["ble"], "blé": ["ble"], "wheat": ["ble"],
    "blé dur": ["ble dur"], "ble dur": ["ble dur"],
    "orge": ["orge"], "barley": ["orge"],
    # Olives
    "olives": ["olive"], "olive": ["olive"],
    # Fruit trees
    "arboriculture": ["arboriculture", "fruitier", "fruitiere"],
    "fruit trees": ["arboriculture", "fruitier", "fruitiere"],
    "fruits": ["arboriculture", "fruitier", "fruitiere"],
    # Vegetables
    "maraîchères": ["maraichere", "maraicher"],
    "maraicheres": ["maraichere", "maraicher"],
    "vegetables": ["maraichere", "maraicher"],
    "légumes": ["maraichere", "maraicher"],
    "tomate": ["tomate"], "tomato": ["tomate"],
    "pomme de terre": ["pomme de terre"], "potato": ["pomme de terre"],
    # Fodder
    "fourragères": ["fourragere", "fourrage"],
    "fourrageres": ["fourragere", "fourrage"],
    "fodder": ["fourragere", "fourrage"],
}

# Keywords -> SQL ILIKE patterns for filtering general crop-type column values
_CROP_ILIKE: dict[str, list[str]] = {
    "cereale": ["%bl%dur%", "%bl%tendre%", "%orge%", "%triticale%", "%avoine%"],
    "ble": ["%bl%"],
    "ble dur": ["%bl%dur%"],
    "orge": ["%orge%"],
    "olive": ["%olive%"],
    "arboriculture": ["%arbo%"],
    "fruitier": ["%arbo%", "%fruitier%"],
    "fruitiere": ["%arbo%", "%fruitiere%"],
    "maraichere": ["%tomate%", "%piment%", "%oignon%", "%pomme%terre%"],
    "maraicher": ["%tomate%", "%piment%", "%oignon%", "%pomme%terre%"],
    "tomate": ["%tomate%"],
    "pomme de terre": ["%pomme%terre%"],
    "fourragere": ["%fourrag%"],
    "fourrage": ["%fourrag%"],
}

# General-purpose crop columns whose VALUES can be filtered with ILIKE.
# Variety-specific columns (olives, type_arbo) are excluded — their values
# are sub-types (e.g. "Chetoui") that don't match category keywords.
_GENERAL_CROP_COLS = {"type_de_culture", "culture", "cultures", "culture_ hiver"}

_MULTI_RE = re.compile(r"\s+vs\.?\s+|\s+et\s+|\s*,\s*", re.IGNORECASE)


def _resolve_crop(crop_type: str) -> tuple[list[str], list[str]]:
    """Resolve crop_type input -> (resource_keywords, sql_ilike_patterns)."""
    key = crop_type.lower().strip()
    keywords = _CROP_ALIASES.get(key, [key])
    seen: set[str] = set()
    patterns: list[str] = []
    for kw in keywords:
        for p in _CROP_ILIKE.get(kw, [f"%{kw}%"]):
            if p not in seen:
                seen.add(p)
                patterns.append(p)
    return keywords, patterns


# ---------------------------------------------------------------------------
# Column classification
# ---------------------------------------------------------------------------


def _prod_unit(col: str) -> tuple[str, float]:
    """Detect production unit from column name -> (unit_label, multiplier_to_tonnes)."""
    cl = col.lower()
    if "1000quintaux" in cl or "1000_quintaux" in cl:
        return "1000 quintaux", 100.0
    if "_qx" in cl and "qx/ha" not in cl:
        return "quintaux", 0.1
    if "quintaux" in cl:
        return "quintaux", 0.1
    if "_tonnes" in cl or "_tonne" in cl:
        return "tonnes", 1.0
    if cl.endswith("_t") and len(cl) >= 4:
        return "tonnes", 1.0
    return "tonnes (assumed)", 1.0


def _classify(fields: list[str]) -> dict[str, Any]:
    """Classify resource fields by role: production, area, year, crop, gov, etc."""
    prod: list[tuple[str, str, float]] = []
    area: list[tuple[str, str]] = []
    year_col: str | None = None
    crop_cols: list[str] = []
    gov_col: str | None = None
    deleg_col: str | None = None
    wide_years: list[str] = []
    has_conduite = False

    for f in fields:
        if f in _SKIP_COLS or f == "None":
            continue
        fl = f.lower()

        # Wide-format: year as column name
        if re.match(r"^(19|20)\d{2}$", f):
            wide_years.append(f)
            continue

        # Area columns -- check BEFORE production to avoid
        # 'superficie_recoltee' being misclassified as production
        if (
            any(k in fl for k in ("superficie", "surface"))
            or fl.endswith("_ha")
            or "hectare" in fl
        ):
            area.append((f, "hectares"))
            continue

        # Production columns (pattern 1: starts with production/estimation)
        if fl.startswith("production") or fl.startswith("estimation_production"):
            unit, mult = _prod_unit(f)
            prod.append((f, unit, mult))
            continue

        # Production columns (pattern 2: ends with unit suffix)
        if (
            fl.endswith("_qx")
            or fl.endswith("_quintaux")
            or fl.endswith("_tonnes")
            or fl.endswith("_tonne")
            or (fl.endswith("_t") and len(fl) >= 4)
        ):
            unit, mult = _prod_unit(f)
            prod.append((f, unit, mult))
            continue

        # Year / campaign column
        if fl in ("annee", "année", "annees") and not year_col:
            year_col = f
            continue
        if "campagne" in fl and "pluviometrie" not in fl and not year_col:
            year_col = f
            continue

        # Crop type identifier columns
        if fl in (
            "type_de_culture", "type_arbo", "culture", "cultures",
            "culture_ hiver", "olives", "espece", "variete", "arboriculture",
        ):
            crop_cols.append(f)
            continue

        # Governorate / delegation
        if "gouvernorat" in fl:
            gov_col = f
            continue
        if "delegation" in fl:
            deleg_col = f
            continue

        # Notable metadata
        if fl == "conduite_culturale":
            has_conduite = True

    # Catch ANNEE (all-caps) as year column
    if not year_col:
        for f in fields:
            if f == "ANNEE":
                year_col = f
                break

    has_numeric = bool(prod) or bool(area)

    return {
        "production": prod,
        "area": area,
        "year_col": year_col,
        "crop_cols": crop_cols,
        "gov_col": gov_col,
        "deleg_col": deleg_col,
        "wide_years": sorted(wide_years),
        "has_conduite": has_conduite,
        "is_wide": len(wide_years) >= 3,
        "has_numeric": has_numeric,
    }


# ---------------------------------------------------------------------------
# Resource filtering
# ---------------------------------------------------------------------------


def _strip_accents(s: str) -> str:
    """Minimal accent removal for keyword matching."""
    for a, b in [
        ("é", "e"), ("è", "e"), ("ê", "e"), ("ë", "e"),
        ("à", "a"), ("â", "a"), ("î", "i"), ("ï", "i"),
        ("ô", "o"), ("û", "u"), ("ù", "u"), ("ç", "c"),
    ]:
        s = s.replace(a, b)
    return s


def _name_match(
    name: str, dataset: str, keywords: list[str], fields: list[str] | None = None
) -> bool:
    """Check if resource/dataset name or field names match any crop keyword."""
    text = _strip_accents((name + " " + dataset.replace("-", " ")).lower())
    if any(_strip_accents(kw) in text for kw in keywords):
        return True
    # Also check field names (catches crop-as-column resources)
    if fields:
        fields_text = _strip_accents(" ".join(fields).lower())
        if any(_strip_accents(kw) in fields_text for kw in keywords):
            return True
    return False


def _filter_resources(
    resources: list[dict], crop_kw: list[str] | None
) -> list[dict]:
    """Filter by crop keywords, skip overflow, sort by record count desc."""
    out = [
        r
        for r in resources
        if r.get("records", 0) < _OVERFLOW
        and (
            not crop_kw
            or _name_match(
                r.get("name", ""),
                r.get("dataset", ""),
                crop_kw,
                r.get("fields"),
            )
        )
    ]
    out.sort(key=lambda r: r.get("records", 0), reverse=True)
    return out


# ---------------------------------------------------------------------------
# SQL building (always SELECT * to avoid CKAN SQL restrictions)
# ---------------------------------------------------------------------------


def _build_sql(
    rid: str,
    schema: dict,
    crop_sql: list[str] | None,
    year_from: int | None,
    year_to: int | None,
    limit: int = 200,
) -> str:
    """Build SQL for a crop_production resource.

    Always uses SELECT * to keep queries short and avoid CKAN DataStore SQL
    restrictions (CASE WHEN is not supported). Normalization in Python.
    """
    where: list[str] = []

    # Regex guard on primary production column (filter out non-numeric rows)
    if schema["production"]:
        col = schema["production"][0][0]
        where.append(f'"{col}" ~ \'^-?[0-9.]+$\'')

    # Crop type SQL filter -- only on general crop columns, and only when
    # the resource has numeric columns (otherwise crop-named columns might
    # be VALUE columns, not type identifiers)
    if crop_sql and schema["crop_cols"] and schema["has_numeric"]:
        general = [c for c in schema["crop_cols"] if c.lower() in _GENERAL_CROP_COLS]
        if general:
            cc = general[0]
            parts = [f'"{cc}" ILIKE \'{p}\'' for p in crop_sql]
            where.append("(" + " OR ".join(parts) + ")")

    # Year filter (LEFT handles both "2020" and "2020/2021" formats)
    if schema["year_col"] and (year_from or year_to):
        yc = schema["year_col"]
        if year_from:
            where.append(f'LEFT("{yc}", 4) >= \'{year_from}\'')
        if year_to:
            where.append(f'LEFT("{yc}", 4) <= \'{year_to}\'')

    order = f'"{schema["year_col"]}" DESC' if schema["year_col"] else "1"
    w = " AND ".join(where) if where else "1=1"
    return f'SELECT * FROM "{rid}" WHERE {w} ORDER BY {order} LIMIT {limit}'


# ---------------------------------------------------------------------------
# Formatting (with production normalization)
# ---------------------------------------------------------------------------

_NUMERIC_RE = re.compile(r"^-?[0-9.]+$")


def _records_table(
    records: list[dict],
    schema: dict | None = None,
    max_rows: int = 50,
) -> list[str]:
    """Format records as a markdown table, normalizing production values."""
    if not records:
        return ["*No records.*"]
    cols = [k for k in records[0] if k not in _SKIP_COLS and k != "None"]
    if not cols:
        return ["*No data columns.*"]

    # Build production multiplier map for normalization
    prod_mult: dict[str, float] = {}
    if schema:
        for col, _unit, mult in schema.get("production", []):
            if mult != 1.0:
                prod_mult[col] = mult

    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for rec in records[:max_rows]:
        vals = []
        for c in cols:
            v = rec.get(c, "")
            # Normalize production columns
            if c in prod_mult and isinstance(v, str) and _NUMERIC_RE.match(v):
                v = f"{float(v) * prod_mult[c]:,.2f}"
            elif isinstance(v, float):
                v = f"{v:,.2f}"
            elif isinstance(v, int):
                v = f"{v:,}"
            vals.append(str(v).replace("|", "/"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(records) > max_rows:
        lines.append(f"\n*... {len(records) - max_rows} more rows not shown.*")
    return lines


def _unit_notes(schema: dict) -> str:
    """Generate unit conversion annotation for production columns."""
    notes = [
        f"`{col}`: {unit} (x{mult} -> tonnes)"
        for col, unit, mult in schema["production"]
        if mult != 1.0
    ]
    return "Unit conversions: " + "; ".join(notes) if notes else ""


# ---------------------------------------------------------------------------
# Inventory mode (no API calls -- registry data only)
# ---------------------------------------------------------------------------


def _build_inventory(resources: list[dict], registry: SchemaRegistry) -> str:
    """Summarize available crop production data by governorate and category."""
    by_gov: dict[str, list[dict]] = {}
    overflow = 0
    wide = 0

    for r in resources:
        if r.get("records", 0) >= _OVERFLOW:
            overflow += 1
            continue
        gov = registry._resource_gov.get(r["id"], "national")
        if _classify(r.get("fields", [])).get("is_wide"):
            wide += 1
        by_gov.setdefault(gov, []).append(r)

    total = sum(len(v) for v in by_gov.values())
    gov_count = len([g for g in by_gov if g != "national"])

    lines = [
        "# Crop Production Data Inventory",
        "",
        f"**{total} resources** across **{gov_count} governorates** + national datasets.",
        "",
        "| Governorate | Resources | Records |",
        "| --- | ---: | ---: |",
    ]
    for gov in sorted(by_gov):
        rl = by_gov[gov]
        recs = sum(r.get("records", 0) for r in rl)
        lines.append(f"| {gov} | {len(rl)} | {recs:,} |")
    lines.append("")

    cats = {
        "Cereals (cereales)": ["cereale"],
        "Olives": ["olive"],
        "Fruit trees (arboriculture)": ["arboriculture", "fruitier", "fruitiere"],
        "Vegetables (maraicheres)": ["maraicher", "maraichere"],
        "Fodder (fourrageres)": ["fourragere", "fourrage"],
    }
    lines.append("**Crop categories detected:**")
    for label, kws in cats.items():
        n = sum(
            1
            for r in resources
            if _name_match(r.get("name", ""), r.get("dataset", ""), kws, r.get("fields"))
        )
        if n:
            lines.append(f"- **{label}**: {n} resources")
    lines.append("")

    if overflow:
        lines.append(
            f"*{overflow} resource(s) excluded (Excel overflow: >={_OVERFLOW:,} rows).*"
        )
    if wide:
        lines.append(
            f"*{wide} wide-format resource(s) (year-as-column from DGEDA).*"
        )
    lines.append("")
    lines.append(
        "Filter with `crop_type`, `gouvernorat`, `year_from`/`year_to`, `metric`."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


async def _run_queries(
    client: CKANClient,
    registry: SchemaRegistry,
    resources: list[dict],
    crop_sql: list[str] | None,
    year_from: int | None,
    year_to: int | None,
    metric: str,
    cap: int = _MAX_RESOURCES,
) -> tuple[list[tuple[dict, dict, list[dict]]], int, int]:
    """Query resources and return (results, empty_count, skipped_count).

    Each result is (resource_dict, schema_dict, records_list).
    """
    results: list[tuple[dict, dict, list[dict]]] = []
    empty = 0
    queried = resources[:cap]
    skipped = max(0, len(resources) - cap)

    for r in queried:
        schema = _classify(r.get("fields", []))

        # For yield metric, require both production and area columns
        if metric == "yield" and (not schema["production"] or not schema["area"]):
            continue

        sql = _build_sql(r["id"], schema, crop_sql, year_from, year_to)

        try:
            result = await client.datastore_sql(sql)
        except Exception as exc:
            logger.debug("Crop query failed for %s: %s", r["id"], exc)
            empty += 1
            continue

        records = result.get("records", []) if result else []
        if not records:
            empty += 1
            continue

        results.append((r, schema, records))

    return results, empty, skipped


def _format_results(
    title: str,
    results: list[tuple[dict, dict, list[dict]]],
    registry: SchemaRegistry,
    metric: str,
    empty: int,
    skipped: int,
) -> str:
    """Format query results as markdown."""
    lines = [f"# {title}", ""]
    if metric == "yield":
        lines.append("*Yield = production (tonnes) / area (hectares)*\n")

    sources: list[dict] = []

    for r, schema, records in results:
        lines.append(f"## {r.get('name', 'Unknown')}")
        notes = _unit_notes(schema)
        if notes:
            lines.append(f"*{notes}*")
        if schema["is_wide"]:
            lines.append("*Wide format: year values as column names.*")
        if not schema["has_numeric"]:
            lines.append("*Raw data (unconventional schema -- values not normalized).*")
        lines.append("")
        lines.extend(_records_table(records, schema))
        lines.append("")

        src = registry.get_source_attribution(r["id"])
        if src:
            sources.append(src)

    if empty:
        lines.append(
            f"*{empty} resource(s) matched but returned no data for the given filters.*"
        )
    if skipped:
        lines.append(
            f"*{skipped} more matching resource(s) not queried "
            f"-- narrow your filters for more specific results.*"
        )
    lines.append("")

    if sources:
        lines.append(format_source_footer(sources))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Multi-governorate comparison
# ---------------------------------------------------------------------------


async def _compare_govs(
    client: CKANClient,
    registry: SchemaRegistry,
    gov_list: list[str],
    crop_type: str | None,
    crop_kw: list[str] | None,
    crop_sql: list[str] | None,
    year_from: int | None,
    year_to: int | None,
    metric: str,
) -> str:
    """Compare crop data across multiple governorates."""
    label = crop_type.title() if crop_type else "Crop Production"
    lines = [f"# {label} -- {', '.join(gov_list)}", ""]
    sources: list[dict] = []

    for gov in gov_list:
        resources = registry.get_domain_resources("crop_production", gouvernorat=gov)
        filtered = _filter_resources(resources, crop_kw)
        lines.append(f"## {gov}")

        if not filtered:
            lines.append("No matching resources.\n")
            continue

        results, empty, _ = await _run_queries(
            client, registry, filtered, crop_sql,
            year_from, year_to, metric, cap=_MAX_PER_GOV,
        )

        if not results:
            lines.append("No data for the given filters.\n")
            continue

        for r, schema, records in results:
            lines.append(f"**{r.get('name', '?')}**")
            notes = _unit_notes(schema)
            if notes:
                lines.append(f"*{notes}*")
            lines.append("")
            lines.extend(_records_table(records, schema, max_rows=20))
            lines.append("")

            src = registry.get_source_attribution(r["id"])
            if src:
                sources.append(src)

        if empty:
            lines.append(f"*{empty} resource(s) with no data.*")
        lines.append("")

    if sources:
        lines.append(format_source_footer(sources))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def query_crop_production(
    client: CKANClient,
    registry: SchemaRegistry,
    crop_type: str | None = None,
    gouvernorat: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    metric: str = "production",
) -> str:
    """Query agricultural production data from Tunisia's crop production domain.

    Modes:
    - No arguments: data inventory by governorate and crop category
    - With filters: query matching resources, normalize production to tonnes
    - Multi-governorate (comma or 'vs' separated): side-by-side comparison

    metric: 'production' (default), 'superficie'/'area', or 'yield' (tonnes/ha).
    """
    if metric not in ("production", "superficie", "area", "yield"):
        return (
            f"Invalid metric '{metric}'. "
            f"Use 'production', 'superficie', 'area', or 'yield'."
        )
    if metric == "area":
        metric = "superficie"

    crop_kw = crop_sql = None
    if crop_type:
        crop_kw, crop_sql = _resolve_crop(crop_type)

    # Multi-governorate comparison
    if gouvernorat:
        parts = [p.strip() for p in _MULTI_RE.split(gouvernorat) if p.strip()]
        if len(parts) > 1:
            return await _compare_govs(
                client, registry, parts, crop_type, crop_kw, crop_sql,
                year_from, year_to, metric,
            )

    # Get resources
    all_res = registry.get_domain_resources("crop_production", gouvernorat=gouvernorat)
    if not all_res:
        if gouvernorat:
            avail = registry.get_data_availability(
                "crop_production", gouvernorat=gouvernorat
            )
            return f"No crop production data for **{gouvernorat}**.\n\n{avail}"
        return "No crop production resources in the registry."

    # Inventory mode
    if not crop_type and not gouvernorat and not year_from and not year_to:
        return _build_inventory(all_res, registry)

    # Filter + query
    filtered = _filter_resources(all_res, crop_kw)
    if not filtered:
        return (
            "No matching resources"
            + (f" for **{crop_type}**" if crop_type else "")
            + (f" in **{gouvernorat}**" if gouvernorat else "")
            + f".\nTotal in selection: {len(all_res)}."
        )

    results, empty, skipped = await _run_queries(
        client, registry, filtered, crop_sql, year_from, year_to, metric,
    )

    if not results:
        yr = ""
        if year_from or year_to:
            parts = []
            if year_from:
                parts.append(f"from {year_from}")
            if year_to:
                parts.append(f"to {year_to}")
            yr = f" ({' '.join(parts)})"
        return (
            f"No data returned from {min(len(filtered), _MAX_RESOURCES)} resource(s)"
            + (f" for **{crop_type}**" if crop_type else "")
            + (f" in **{gouvernorat}**" if gouvernorat else "")
            + yr
            + "."
        )

    # Build title
    title = " -- ".join(
        filter(
            None,
            [
                crop_type.title() if crop_type else "Crop Production",
                gouvernorat,
                {"superficie": "(Area)", "yield": "(Yield)"}.get(metric),
            ],
        )
    )
    return _format_results(title, results, registry, metric, empty, skipped)
