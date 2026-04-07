"""Bibliography search tool: ILIKE search across ONAGRI bibliographic resources."""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.formatting import format_source_footer

logger = logging.getLogger(__name__)

# --- Resource tiers -----------------------------------------------------------

# Tier 1: rich resources with Titre + Resume (queried via SQL ILIKE)
_TIER1 = [
    {
        "id": "777225ea-5e56-4ddb-b170-c245100fbf80",
        "name": "Base ONAGRI",
        "has_resume": True,
        "titre_col": "Titre",
        "resume_col": "Resume",
        "year_col": "Annee",
        "lang_col": "Langue",
        "author_col": "Auteur_affil",
        "source_col": "Source",
        "pdf_col": None,
    },
    {
        "id": "3f83d71e-f4a0-43a4-9b96-67853493cb10",
        "name": "Fonds ONAGRI",
        "has_resume": True,
        "titre_col": "Titre",
        "resume_col": "Resume",
        "year_col": "Annee",
        "lang_col": "Langue",
        "author_col": "Auteur_affil",
        "source_col": "source",  # lowercase in Fonds
        "pdf_col": "source",     # contains full PDF URL
    },
]

# Tier 2: thematic libraries (SQL blocked — 409; fetched via datastore_search + Python filter)
_TIER2 = [
    {
        "id": "4167ce10-7bd5-4b4b-b8d2-d92b38fb61bd",
        "name": "Agriculture",
        "theme": "agriculture",
        "titre_col": "Titre",
        "year_col": "Annee",
        "author_col": "Auteur",  # not Auteur_affil
        "pdf_col": "Nom_fichier",
    },
    {
        "id": "19b0bb80-bebc-41a0-acaa-d6658e317154",
        "name": "Water",
        "theme": "water",
        "titre_col": "Titre",
        "year_col": "Annee",
        "author_col": "Auteur",
        "pdf_col": "Nom_fichier",
    },
    {
        "id": "fb3d6e21-21fc-496b-816b-ca9f04d69fbe",
        "name": "Forestry",
        "theme": "forestry",
        "titre_col": "Titre",
        "year_col": "Annee",
        "author_col": "Auteur",
        "pdf_col": "Nom_fichier",
    },
    {
        "id": "353338ca-7e7e-43eb-9167-f84b6cb6dab8",
        "name": "Fisheries",
        "theme": "fisheries",
        "titre_col": "Titre",
        "year_col": "Annee",
        "author_col": "Auteur",
        "pdf_col": "Nom_fichier",
    },
]

_THEME_RESOURCES = {r["theme"]: r for r in _TIER2}

_ONAGRI_PDF_BASE = "https://www.onagri.nat.tn/uploads/docagri"

_YEAR_RE = re.compile(r"^\d{4}$")

# Process-scoped cache for Tier 2 resources (small — ~900 records total)
_tier2_cache: dict[str, list[dict]] = {}


# --- Helpers ------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Strip accents and lowercase for matching."""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _split_keywords(query: str) -> list[str]:
    """Split query into keywords, stripping accents."""
    return [kw for kw in _normalize(query).split() if len(kw) >= 2]


def _escape_sql(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def _build_ilike_clause(
    keywords: list[str],
    columns: list[str],
) -> str:
    """Build a WHERE clause: ALL keywords must match in ANY of the columns."""
    parts: list[str] = []
    for kw in keywords:
        escaped = _escape_sql(kw)
        col_clauses = [f'"{col}" ILIKE \'%{escaped}%\'' for col in columns]
        parts.append(f"({' OR '.join(col_clauses)})")
    return " AND ".join(parts)


def _build_tier1_query(
    resource: dict,
    keywords: list[str],
    year_from: int | None,
    year_to: int | None,
    language: str | None,
    limit: int,
) -> str:
    """Build SQL query for a Tier 1 bibliography resource."""
    rid = resource["id"]
    titre_col = resource["titre_col"]
    resume_col = resource.get("resume_col")

    search_cols = [titre_col]
    if resume_col:
        search_cols.append(resume_col)

    # SELECT columns
    select_cols = [f'"{titre_col}"']
    if resource.get("author_col"):
        select_cols.append(f'"{resource["author_col"]}"')
    if resource.get("year_col"):
        select_cols.append(f'"{resource["year_col"]}"')
    if resume_col:
        select_cols.append(f'"{resume_col}"')
    if resource.get("lang_col"):
        select_cols.append(f'"{resource["lang_col"]}"')
    if resource.get("source_col"):
        select_cols.append(f'"{resource["source_col"]}"')
    if resource.get("pdf_col") and resource["pdf_col"] != resource.get("source_col"):
        select_cols.append(f'"{resource["pdf_col"]}"')

    sql = f'SELECT {", ".join(select_cols)} FROM "{rid}"'

    # WHERE
    where_parts: list[str] = []
    if keywords:
        where_parts.append(_build_ilike_clause(keywords, search_cols))
    if year_from and resource.get("year_col"):
        yc = resource["year_col"]
        where_parts.append(f'"{yc}" ~ \'^\\d{{4}}$\' AND "{yc}" >= \'{year_from}\'')
    if year_to and resource.get("year_col"):
        yc = resource["year_col"]
        where_parts.append(f'"{yc}" ~ \'^\\d{{4}}$\' AND "{yc}" <= \'{year_to}\'')
    if language and resource.get("lang_col"):
        where_parts.append(
            f'"{resource["lang_col"]}" ILIKE \'%{_escape_sql(language.upper())}%\''
        )

    if where_parts:
        sql += f' WHERE {" AND ".join(where_parts)}'

    sql += f" LIMIT {limit}"
    return sql


def _match_record_python(
    record: dict,
    keywords: list[str],
    titre_col: str,
    year_from: int | None,
    year_to: int | None,
) -> bool:
    """Check if a Tier 2 record matches the search criteria (Python-side filtering)."""
    # Keyword filter: all keywords must appear in titre
    if keywords:
        titre = _normalize(str(record.get(titre_col, "")))
        if not all(kw in titre for kw in keywords):
            return False

    # Year filter
    year_str = str(record.get("Annee", "")).strip()
    if (year_from or year_to) and _YEAR_RE.match(year_str):
        year_int = int(year_str)
        if year_from and year_int < year_from:
            return False
        if year_to and year_int > year_to:
            return False
    elif year_from or year_to:
        # Non-numeric year — skip when year filter is active
        return False

    return True


def _score_record(
    record: dict,
    keywords: list[str],
    titre_col: str,
    resume_col: str | None,
) -> int:
    """Score a record: Titre match = 2 points, Resume match = 1 point per keyword."""
    if not keywords:
        return 0
    score = 0
    titre = _normalize(str(record.get(titre_col, "")))
    resume = _normalize(str(record.get(resume_col, ""))) if resume_col else ""
    for kw in keywords:
        if kw in titre:
            score += 2
        if resume and kw in resume:
            score += 1
    return score


def _pdf_url(resource: dict, record: dict) -> str | None:
    """Extract or construct PDF URL from a record."""
    pdf_col = resource.get("pdf_col")
    if not pdf_col:
        return None

    raw = str(record.get(pdf_col, "")).strip()
    if not raw or raw == "None":
        return None

    # Fonds: source column contains full URL
    if raw.startswith("http"):
        return raw.replace("www.onagri.tn", "www.onagri.nat.tn").replace(
            "http://www.onagri.nat.tn", "https://www.onagri.nat.tn"
        )

    # Thematic libraries: Nom_fichier -> construct URL
    filename = raw if raw.lower().endswith(".pdf") else f"{raw}.pdf"
    return f"{_ONAGRI_PDF_BASE}/{filename}"


def _format_results(
    scored_records: list[tuple[int, dict, dict]],
    keywords: list[str],
    query: str,
    total_searched: int,
    limit: int,
) -> str:
    """Format scored bibliography results as markdown."""
    lines: list[str] = []

    if not scored_records:
        lines.append(f"No bibliographic records found for **{query}**.")
        lines.append(f"Searched {total_searched:,} records across ONAGRI catalogs.")
        lines.append("")
        lines.append("*Try broader keywords or remove year/language filters.*")
        return "\n".join(lines)

    shown = scored_records[:limit]
    count = len(scored_records)
    showing = f" (showing top {len(shown)})" if count > len(shown) else ""

    lines.append(
        f"**Found {count} bibliographic record(s)** "
        f"for **{query}**{showing} (searched {total_searched:,} records).\n"
    )

    for i, (score, record, res_cfg) in enumerate(shown, 1):
        titre = record.get(res_cfg["titre_col"], "Untitled")
        lines.append(f"### {i}. {titre}")

        author = record.get(res_cfg.get("author_col", ""), "")
        year = record.get(res_cfg.get("year_col", ""), "")
        lang = record.get(res_cfg.get("lang_col", ""), "")

        meta_parts: list[str] = []
        if author:
            meta_parts.append(f"**Author:** {author}")
        if year:
            meta_parts.append(f"**Year:** {year}")
        if lang:
            meta_parts.append(f"**Language:** {lang}")
        meta_parts.append(f"**Source:** {res_cfg['name']}")
        lines.append(" | ".join(meta_parts))

        # Resume (truncated)
        resume_col = res_cfg.get("resume_col")
        if resume_col:
            resume = str(record.get(resume_col, "")).strip()
            if resume:
                if len(resume) > 300:
                    resume = resume[:300] + "..."
                lines.append(f"  {resume}")

        # PDF link
        url = _pdf_url(res_cfg, record)
        if url:
            lines.append(f"  **PDF:** {url}")

        lines.append("")

    # Source attribution
    sources_seen: set[str] = set()
    source_list: list[dict[str, str]] = []
    for _, _, res_cfg in shown:
        rid = res_cfg["id"]
        if rid not in sources_seen:
            sources_seen.add(rid)
            source_list.append({
                "resource_id": rid,
                "resource_name": res_cfg["name"],
                "dataset_title": f"ONAGRI - {res_cfg['name']}",
                "organization_title": "ONAGRI",
                "portal_url": "https://catalog.agridata.tn/dataset/base-de-documentation-de-l-onagri",
            })

    if source_list:
        lines.append(format_source_footer(source_list))

    lines.append("")
    lines.append(
        "*PDF availability: ONAGRI documents are hosted on onagri.nat.tn. "
        "Some older links may be unavailable.*"
    )

    return "\n".join(lines)


# --- Tier 2 fetching ---------------------------------------------------------


async def _fetch_tier2(client: CKANClient, resource: dict) -> list[dict]:
    """Fetch all records from a Tier 2 resource, with process-scoped caching.

    Tier 2 resources block SQL (409 CONFLICT), so we use datastore_search
    and cache the results. Resources are small (~100-665 records each).
    """
    rid = resource["id"]
    if rid in _tier2_cache:
        return _tier2_cache[rid]

    try:
        result = await client.datastore_search(rid, limit=1000)
    except Exception as exc:
        logger.warning("Failed to fetch Tier 2 resource %s: %s", resource["name"], exc)
        return []

    if not result:
        return []

    records = result.get("records", [])
    _tier2_cache[rid] = records
    logger.info("Cached %d records from Tier 2 resource %s", len(records), resource["name"])
    return records


# --- Main entry point ---------------------------------------------------------


async def search_bibliography(
    client: CKANClient,
    registry: SchemaRegistry,
    query: str,
    year_from: int | None = None,
    year_to: int | None = None,
    language: str | None = None,
    theme: str | None = None,
    limit: int = 20,
) -> str:
    """Search ONAGRI bibliographic catalogs by keyword, year, language, and theme.

    Uses tiered execution: queries Tier 1 (Base + Fonds, 25,047 records) first.
    Only queries Tier 2 (4 thematic libraries, ~900 records) if fewer than `limit`
    results found, or if a specific theme is requested.

    Tier 1 uses SQL ILIKE (searches Titre + Resume).
    Tier 2 uses datastore_search + Python filtering (SQL blocked on these resources).
    """
    keywords = _split_keywords(query)
    if not keywords and not year_from and not year_to and not language:
        return (
            "Please provide a search query (keywords in French, Arabic, or English) "
            "or at least a year range or language filter."
        )

    all_records: list[tuple[int, dict, dict]] = []  # (score, record, resource_config)
    total_searched = 0

    # If a specific theme is requested, search only that thematic library (Tier 2)
    if theme:
        theme_lower = _normalize(theme)
        res_cfg = _THEME_RESOURCES.get(theme_lower)
        if not res_cfg:
            available = ", ".join(_THEME_RESOURCES.keys())
            return f"Unknown theme `{theme}`. Available themes: {available}"

        records = await _fetch_tier2(client, res_cfg)
        total_searched = len(records)
        for rec in records:
            if _match_record_python(rec, keywords, res_cfg["titre_col"], year_from, year_to):
                score = _score_record(rec, keywords, res_cfg["titre_col"], None)
                all_records.append((score, rec, res_cfg))

        all_records.sort(key=lambda x: x[0], reverse=True)
        return _format_results(all_records, keywords, query, total_searched, limit)

    # --- Tier 1: Base + Fonds (SQL ILIKE on Titre + Resume) ---
    for res_cfg in _TIER1:
        sql = _build_tier1_query(res_cfg, keywords, year_from, year_to, language, limit)
        try:
            result = await client.datastore_sql(sql)
        except Exception as exc:
            logger.warning("Bibliography query failed for %s: %s", res_cfg["name"], exc)
            continue

        if result:
            records = result.get("records", [])
            for rec in records:
                score = _score_record(
                    rec, keywords, res_cfg["titre_col"], res_cfg.get("resume_col")
                )
                all_records.append((score, rec, res_cfg))

    total_searched = 25_047  # Base: 22,782 + Fonds: 2,265

    # --- Tier 2: thematic libraries (datastore_search + Python filter) ---
    if len(all_records) < limit:
        for res_cfg in _TIER2:
            records = await _fetch_tier2(client, res_cfg)
            total_searched += len(records)
            for rec in records:
                if _match_record_python(rec, keywords, res_cfg["titre_col"], year_from, year_to):
                    score = _score_record(rec, keywords, res_cfg["titre_col"], None)
                    all_records.append((score, rec, res_cfg))

    # Sort by score descending
    all_records.sort(key=lambda x: x[0], reverse=True)

    return _format_results(all_records, keywords, query, total_searched, limit)
