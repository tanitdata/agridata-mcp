"""Explore tool: browse any domain's resources without executing DataStore queries."""

from __future__ import annotations

import re
import unicodedata
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


def _filter_by_keyword(
    resources: list[dict[str, Any]], keyword: str,
) -> list[dict[str, Any]]:
    """Filter resources by keyword against name, dataset, or field names.

    Uses word-boundary matching with accent/plural normalization so that
    'cereales' matches 'céréales' and 'ble' doesn't match 'cultivable'.
    """
    kw = _normalize(keyword.lower())
    pattern = re.compile(r"\b" + re.escape(kw) + r"\b")

    def _matches(text: str) -> bool:
        # Normalize each word, rejoin for word-boundary matching
        words = re.split(r"[_\s()\-/,.'\"]+", text.lower())
        normalized = " ".join(_normalize(w) for w in words if w)
        return bool(pattern.search(normalized))

    return [
        res for res in resources
        if _matches(res.get("name", ""))
        or _matches(res.get("dataset", "").replace("-", " "))
        or any(_matches(f) for f in res.get("fields", []))
    ]


# ---------------------------------------------------------------------------
# Year-range extraction
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")


def _extract_year_range(*texts: str) -> tuple[int, int] | None:
    """Extract min-max year range from text strings."""
    years: set[int] = set()
    for text in texts:
        if text:
            years.update(int(m) for m in _YEAR_RE.findall(text))
    if years:
        return min(years), max(years)
    return None


# ---------------------------------------------------------------------------
# Topic-relevance filtering for national resources
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Token normalization (accent stripping + basic French plural)
# ---------------------------------------------------------------------------


def _normalize(word: str) -> str:
    """Strip accents and trailing plural markers for consistent matching.

    'céréales' -> 'cereale', 'grenadiers' -> 'grenadier', 'maraîchères' -> 'maraichere'
    """
    # Strip accents: é -> e, è -> e, ê -> e, etc.
    nfkd = unicodedata.normalize("NFKD", word)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Basic French plural: strip trailing 's' or 'x', but only for words > 4 chars
    # to avoid mangling short words like "dans", "mais", "pois"
    if len(stripped) > 4 and stripped[-1] in ("s", "x"):
        stripped = stripped[:-1]
    return stripped


# Tokens too common to discriminate between crop types / domains.
# All entries are in _normalize()-d form (no accents, plurals stripped).
_NOISE_TOKENS: set[str] = {
    # French articles / prepositions
    "les", "des", "par", "une", "sur", "aux", "dans", "pour", "avec",
    "sans", "entre", "selon", "tout", "plus",
    # Temporal
    "annee", "date", "campagne", "agricole", "periode",
    "observation", "nbre", "mois", "annuelle", "mensuelle",
    # Geographic / structural
    "delegation", "gouvernorat", "total", "nombre", "capacite",
    "latitude", "longitude", "nom", "zone", "region",
    # Units
    "tonne", "hectare", "quintau",
    "dinar", "unite", "kilo", "litre", "1000",
    # Generic agricultural (appears in almost every resource)
    "production", "superficie", "surface", "exploitation",
    "culture", "vegetale",
    "agricole", "terre",
    # Common descriptor words in French dataset names
    "evolution", "repartition", "situation", "suivi",
    "donnee", "indicateur", "statistique", "liste",
    "globale", "certifiee",
    "moyen", "moyenne",
}

# Umbrella crop terms -> specific crop names they encompass.
# When a gov-specific resource has "Cereales" as a field, we also want to
# match national resources about "blé", "orge", etc.
# Keys and values are in _normalize()-d form.
_CROP_EXPANSIONS: dict[str, set[str]] = {
    "cereale": {"ble", "orge", "triticale", "avoine", "sorgho"},
    "arboriculture": {
        "pommier", "poirier", "grenadier", "figuier", "vigne",
        "amandier", "pistachier", "agrume", "noyer",
    },
    "olivier": {"olive", "huile"},
    "maraichere": {
        "tomate", "pomme", "oignon", "piment",
        "artichaut", "courgette", "melon", "pasteque",
    },
    "legumineuse": {"pois", "feve", "lentille", "haricot"},
    "fourrage": {"fourragere", "luzerne", "bersim"},
    "agrume": {"citru", "orange", "mandarine", "citron", "clementine"},
}


def _tokenize(text: str) -> set[str]:
    """Split text into normalized lowercase tokens, filtering short and noise words."""
    return {
        _normalize(w) for w in re.split(r"[_\s()\-/,.'\"]+", text.lower())
        if len(w) > 2 and _normalize(w) not in _NOISE_TOKENS and not w.isdigit()
    }


def _extract_topic_tokens(resources: list[dict[str, Any]]) -> set[str]:
    """Extract discriminative topic tokens from gov-specific resources.

    Uses field names, resource names, and dataset slugs.
    Expands umbrella crop terms (e.g. "cereales" -> "ble", "orge", ...).
    """
    tokens: set[str] = set()
    for res in resources:
        for f in res.get("fields", []):
            if f not in _SKIP_COLS:
                tokens.update(_tokenize(f))
        tokens.update(_tokenize(res.get("name", "")))
        tokens.update(_tokenize(res.get("dataset", "").replace("-", " ")))
    # Expand umbrella terms
    expanded: set[str] = set()
    for token in tokens:
        if token in _CROP_EXPANSIONS:
            expanded.update(_CROP_EXPANSIONS[token])
    tokens.update(expanded)
    return tokens


def _resource_matches_tokens(res: dict[str, Any], tokens: set[str]) -> bool:
    """Check if a resource's metadata contains any of the given tokens."""
    res_tokens: set[str] = set()
    for f in res.get("fields", []):
        if f not in _SKIP_COLS:
            res_tokens.update(_tokenize(f))
    res_tokens.update(_tokenize(res.get("name", "")))
    res_tokens.update(_tokenize(res.get("dataset", "").replace("-", " ")))
    return bool(res_tokens & tokens)


def _format_resource_block(
    res: dict[str, Any],
    registry: SchemaRegistry,
    note: str | None = None,
) -> tuple[list[str], dict[str, str] | None]:
    """Format a single resource as markdown lines. Returns (lines, source_dict)."""
    rid = res["id"]
    name = res.get("name", "")
    dataset = res.get("dataset", "")
    fields = [f for f in res.get("fields", []) if f not in _SKIP_COLS]
    records = res.get("records", 0)

    ctx = registry.get_resource_context(rid)
    gov = ctx.get("gouvernorat", "unknown") if ctx else "unknown"

    # Year range from resource/dataset names
    yr = _extract_year_range(name, dataset.replace("-", " "))
    if yr:
        year_label = f" ({yr[0]})" if yr[0] == yr[1] else f" ({yr[0]}-{yr[1]})"
    else:
        year_label = ""

    lines: list[str] = []
    lines.append(f"### {name or dataset}{year_label}")
    lines.append(f"- **Resource ID:** `{rid}`")
    lines.append(f"- **Dataset:** {dataset}")
    lines.append(f"- **Governorate:** {gov}")
    lines.append(f"- **Records:** {records:,}")
    lines.append(f"- **Fields ({len(fields)}):** {', '.join(f'`{f}`' for f in fields)}")

    sample = _sample_fields(fields)
    if sample:
        lines.append(f"- **Sample columns:** {', '.join(f'`{s}`' for s in sample)}")

    hints = _unit_hints(fields)
    if hints:
        lines.append(f"- **Unit hints:** {'; '.join(hints)}")

    if note:
        lines.append(f"- {note}")

    lines.append("")

    src = registry.get_source_attribution(rid)
    return lines, src


def _exclude_overflow(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Exclude Excel overflow resources (>= 1,048,575 rows)."""
    return [r for r in resources if r.get("records", 0) < 1_048_575]


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

    When gouvernorat is specified, returns two sections:
    1. Governorate-specific resources
    2. National resources that may contain governorate-level data
    """
    valid_domains = registry.domains
    if domain not in valid_domains:
        return (
            f"Unknown domain `{domain}`.\n\n"
            f"Available domains: {', '.join(f'`{d}`' for d in sorted(valid_domains))}"
        )

    # Fetch governorate-specific resources
    gov_resources = registry.get_domain_resources(domain, gouvernorat=gouvernorat)

    # Fetch national resources when a governorate is specified
    national_resources: list[dict[str, Any]] = []
    if gouvernorat:
        national_resources = registry.get_domain_resources(domain, gouvernorat="national")

    # Apply keyword filter to both sets
    if keyword:
        gov_resources = _filter_by_keyword(gov_resources, keyword)
        national_resources = _filter_by_keyword(national_resources, keyword)

    # Exclude overflow from both sets
    gov_resources = _exclude_overflow(gov_resources)
    national_resources = _exclude_overflow(national_resources)

    # Filter national resources by topic relevance when no explicit keyword
    if national_resources and not keyword:
        topic_tokens = _extract_topic_tokens(gov_resources)
        if topic_tokens:
            filtered = [r for r in national_resources if _resource_matches_tokens(r, topic_tokens)]
            if filtered:
                national_resources = filtered
            # If filtering removes everything, keep all (inference too aggressive)

    if not gov_resources and not national_resources:
        avail = registry.get_data_availability(domain, gouvernorat=gouvernorat)
        extra = f" matching keyword '{keyword}'" if keyword else ""
        gov_note = f" in {gouvernorat}" if gouvernorat else ""
        return f"No resources found{gov_note}{extra} in domain `{domain}`.\n\n**Availability:** {avail}"

    lines: list[str] = []
    sources: list[dict[str, str]] = []
    kw_label = f" (keyword: '{keyword}')" if keyword else ""

    if gouvernorat:
        # --- Section 1: Governorate-specific ---
        lines.append(f"## {gouvernorat} -- {domain} resources ({len(gov_resources)} governorate-specific){kw_label}")
        lines.append("")

        if gov_resources:
            for res in gov_resources:
                block, src = _format_resource_block(res, registry)
                lines.extend(block)
                if src:
                    sources.append(src)
        else:
            lines.append(f"*No governorate-specific resources for {gouvernorat}.*")
            lines.append("")

        # --- Section 2: National resources ---
        lines.append("---")
        lines.append(f"## National resources that may contain {gouvernorat} data ({len(national_resources)} resources)")
        lines.append("")

        if national_resources:
            note = (
                f"National dataset -- may contain data broken down by governorate column. "
                f"Use `get_resource_preview` to check if a Gouvernorat/Delegation column exists, "
                f"then filter with `WHERE \"Gouvernorat\" ILIKE '%{gouvernorat}%'`."
            )
            for res in national_resources:
                block, src = _format_resource_block(res, registry, note=note)
                lines.extend(block)
                if src:
                    sources.append(src)
        else:
            lines.append("*No national resources in this domain.*")
            lines.append("")

    else:
        # No governorate filter -- single flat listing (original behavior)
        lines.append(f"## {domain}{kw_label}")
        lines.append(f"**{len(gov_resources)} resource(s)**\n")

        for res in gov_resources:
            block, src = _format_resource_block(res, registry)
            lines.extend(block)
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
