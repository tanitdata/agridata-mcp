"""Build a value catalog from vocabulary_raw.json for Phase 3 semantic enrichment.

Local processing only (no API calls). Reads vocabulary_raw.json + schemas.json and
produces value_catalog.json containing:
- Abbreviation dictionary (field name suffixes → units/meanings)
- Semantic concepts (column name variants → deduplicated canonical values)
- Per-domain vocabulary summaries

Output feeds Step 3 (concept annotations) of Phase 3.

Usage:
    python scripts/build_value_catalog.py
"""

from __future__ import annotations

import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Abbreviation dictionary — field name suffix → meaning
# ---------------------------------------------------------------------------
# Derived from manual inspection of field name patterns in vocabulary_raw.json.
# Only includes suffixes that appear >= 5 times and represent units or quantities.

ABBREVIATIONS: dict[str, dict] = {
    "_ha": {
        "expansion": "hectares",
        "symbol": "ha",
        "category": "area",
    },
    "_qx": {
        "expansion": "quintaux",
        "symbol": "qx",
        "category": "weight",
    },
    "_qt": {
        "expansion": "quintaux",
        "symbol": "Qt",
        "category": "weight",
    },
    "_q": {
        "expansion": "quintaux",
        "symbol": "q",
        "category": "weight",
    },
    "_t": {
        "expansion": "tonnes",
        "symbol": "t",
        "category": "weight",
    },
    "_kg": {
        "expansion": "kilogrammes",
        "symbol": "kg",
        "category": "weight",
    },
    "_mm": {
        "expansion": "millimètres",
        "symbol": "mm",
        "category": "length",
    },
    "_m": {
        "expansion": "mètres",
        "symbol": "m",
        "category": "length",
    },
    "_tete": {
        "expansion": "nombre de têtes",
        "symbol": "têtes",
        "category": "count",
    },
    "_jour": {
        "expansion": "par jour",
        "symbol": "/jour",
        "category": "rate",
    },
    "_md": {
        "expansion": "millions de dinars",
        "symbol": "MD",
        "category": "currency",
    },
    "_dt": {
        "expansion": "dinars tunisiens",
        "symbol": "DT",
        "category": "currency",
    },
    "_cube": {
        "expansion": "mètre cube (mille m³)",
        "symbol": "m³",
        "category": "volume",
    },
    "_adi": {
        "expansion": "autorisations de déclaration d'investissement",
        "symbol": "ADI",
        "category": "count",
    },
}

# Language suffixes — not units, but important for column variant grouping
LANGUAGE_SUFFIXES = {"_ar", "_fr", "_arabe"}

# ---------------------------------------------------------------------------
# Semantic concept definitions — maps column name variants to concept IDs
# ---------------------------------------------------------------------------
# Each concept groups column names that represent the same semantic entity.
# Matching is case-insensitive. Variants listed explicitly to avoid false
# positives (e.g., "Type" is ambiguous without domain context).

CONCEPT_COLUMNS: dict[str, list[str]] = {
    # --- Geographic ---
    "delegation": [
        "Delegation", "delegation", "DELEGATION",
        "Delegation_fr", "delegation_fr", "Delegation-fr",
        "Delegation_ar", "delegation_ar", "Delegation-ar", "delegation-ar",
        "Delegation_arabe", "Delegations",
    ],
    "gouvernorat": [
        "Gouvernorat", "gouvernorat", "GOUVERNORAT",
        "gouvernorat_fr", "gouvernorat_ar",
        "gouvernorat-lwly@", "gouvernorat_lwly@", "gouvernorat_wly@",
        "Region", "region",
    ],
    "secteur": [
        "Secteur", "secteur", "SECTEUR_ACTIVITE",
    ],

    # --- Agricultural entities ---
    "crop_type": [
        "Culture", "culture", "Nom_Culture",
        "type_de_culture", "type_de_culture_FR", "type_de_culture_AR",
        "Variete", "variete",
        "conduite_culturale", "Conduite_culturale",
        "Type_arbo",
    ],
    "fish_product": [
        "nature_du_produit", "categorie",
        "Espece", "Espece_animale",
        "Produit", "Type_produit",
        "Filiere",
    ],
    "fishing_activity": [
        "type_peche", "Type_de_peche", "type_de_peche",
        "Type_activite_peche", "type_de_navire", "Type_de_flotte",
        "type_barque", "Type_unite_peche", "Type_unite",
        "type_de_permis", "type_de_certification",
    ],
    "water_body": [
        "Nom_nappe", "nom_nappe", "Nom_nappe_fr", "Nom_nappe_ar",
        "Nom_nappe_phreatique",
        "Nom_puit", "Nom_GDA",
        "Nom_lac_ou_barrage", "Nom_barrage", "Nom_ouvrage", "Nature_ouvrage",
        "Type_nappe", "Type_d'usage",
        "type_analyse_eau", "type_analyse_sol",
    ],
    "olive_entity": [
        "Nom_huilerie", "nom_huilerie_ar",
    ],
    "livestock_type": [
        "Type_etablissement",
    ],

    # --- Climate/Measurement ---
    "climate_parameter": [
        "nom_fr", "nom_ar", "Nom_fr", "Nom_ar",
        "sensor_name", "parameter",
    ],
    "unit": [
        "unite", "Unite", "unit",
    ],

    # --- Economic ---
    "company": [
        "societe", "Societe", "Nom_societe", "nom_societe",
        "nom_etablissement_fr",
        "Nom_entrepot_fr",
        "Nom_de_SMSA", "nom_smbsa_ar",
        "Nom_cellule_vulgarisation",
    ],
    "trade_destination": [
        "Pays",
    ],

    # --- Administrative ---
    "campaign": [
        "Campagne", "campagne",
    ],
    "intervention": [
        "Nature_intervention", "Theme",
        "type_d'infraction", "nature_du_delit_forestier_ar",
        "Type_de _rapprochement",
    ],
    "port": [
        "Port", "nom_port_ar", "nom_port_fr",
    ],
    "location_type": [
        "Mode_location", "Type_location", "Mode",
        "Etat", "etat",
    ],
    "irrigation_entity": [
        "nom_perimetre_ar", "nom_groupement_ar", "nom_groupement",
        "nom_centre_ar",
        "nature_du_stockage_ar", "nature_du_stockage_fr",
    ],

    # --- Bibliography ---
    "language": [
        "Langue",
    ],
    "bibliography_file": [
        "Nom_fichier",
    ],
}

# Reverse lookup: column_name → concept_id
_COL_TO_CONCEPT: dict[str, str] = {}
for _concept, _cols in CONCEPT_COLUMNS.items():
    for _col in _cols:
        _COL_TO_CONCEPT[_col] = _concept


# ---------------------------------------------------------------------------
# Text normalization helpers
# ---------------------------------------------------------------------------

def strip_accents(s: str) -> str:
    """Remove diacritical marks for deduplication comparison."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.category(c).startswith("M"))


def normalize_value(s: str) -> str:
    """Normalize a categorical value for deduplication."""
    s = s.strip()
    # Strip leading dashes/bullet artifacts (common in Kef data)
    s = re.sub(r"^[-\u2013\u2014*]+\s*", "", s)
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s


def is_noise(s: str) -> bool:
    """Return True if the value is clearly not a real categorical label."""
    if not s:
        return True
    # Pure numbers (data entry errors in categorical columns)
    if re.match(r"^-?\d+\.?\d*$", s):
        return True
    # Very long strings (>120 chars) — notes accidentally in categorical columns
    if len(s) > 120:
        return True
    # Strings that start with a digit and contain non-name characters
    # (e.g., "40قلعة سنان", "12نبـر + الطويرف")
    if re.match(r"^\d+[^\d]", s) and len(s) > 6:
        return True
    return False


def dedup_values(values: list[str]) -> list[str]:
    """Deduplicate values, keeping the most complete/accented variant.

    When two values differ only by accent or case (e.g., 'Béja' vs 'Beja',
    'GabÃ¨s' vs 'Gabès'), keep the one that looks most correct:
    - Prefer the one with proper Unicode accents over mojibake (Ã)
    - Prefer the accented form over the unaccented form
    - For exact duplicates after normalization, keep the first seen
    """
    # Group by accent-stripped lowercase key
    groups: dict[str, list[str]] = defaultdict(list)
    for v in values:
        norm = normalize_value(v)
        if is_noise(norm):
            continue
        key = strip_accents(norm).lower()
        groups[key].append(norm)

    result = []
    for key, variants in sorted(groups.items()):
        # Pick the best variant
        # Penalize mojibake (contains Ã)
        def score(v: str) -> tuple:
            has_mojibake = "Ã" in v
            has_accents = v != strip_accents(v)
            # Prefer: no mojibake > has accents > length (more complete)
            return (not has_mojibake, has_accents, len(v))

        best = max(variants, key=score)
        result.append(best)

    return sorted(result)


# ---------------------------------------------------------------------------
# Abbreviation scanning
# ---------------------------------------------------------------------------

def scan_abbreviations(
    resource_schemas: dict,
    domain_resource_ids: dict[str, set[str]],
) -> dict[str, dict]:
    """Scan field names for known abbreviation suffixes.

    Returns the ABBREVIATIONS dict enriched with field_count and
    example_fields per suffix.
    """
    result = {}
    for suffix, info in ABBREVIATIONS.items():
        matching_fields: list[str] = []
        domain_counts: Counter = Counter()

        for rid, schema in resource_schemas.items():
            for field in schema["fields"]:
                if field.lower().endswith(suffix):
                    matching_fields.append(field)
                    # Count which domain this resource belongs to
                    for domain, rids in domain_resource_ids.items():
                        if rid in rids:
                            domain_counts[domain] += 1

        # Deduplicate field name examples
        unique_fields = sorted(set(matching_fields))
        # Pick up to 5 representative examples
        examples = unique_fields[:5]

        result[suffix] = {
            **info,
            "field_count": len(matching_fields),
            "unique_field_names": len(unique_fields),
            "example_fields": examples,
            "domains": dict(domain_counts.most_common()),
        }

    return result


# ---------------------------------------------------------------------------
# Concept extraction
# ---------------------------------------------------------------------------

def build_concepts(
    categorical_values: dict,
    domain_resource_ids: dict[str, set[str]],
) -> dict[str, dict]:
    """Group categorical values by semantic concept.

    Returns a dict of concept_id → {column_variants, domains, values, stats}.
    """
    concepts: dict[str, dict] = {}

    for concept_id, col_names in CONCEPT_COLUMNS.items():
        # Collect all values across all resources for this concept
        all_values: list[str] = []
        column_hits: Counter = Counter()  # which column variants actually appear
        domain_resource_count: Counter = Counter()  # resources per domain
        resource_count = 0

        for rid, cols in categorical_values.items():
            matched_in_resource = False
            for col_name in col_names:
                if col_name in cols:
                    vals = cols[col_name]
                    all_values.extend(vals)
                    column_hits[col_name] += 1
                    if not matched_in_resource:
                        matched_in_resource = True
                        resource_count += 1
                        for domain, rids in domain_resource_ids.items():
                            if rid in rids:
                                domain_resource_count[domain] += 1

        if not all_values:
            continue

        # Deduplicate
        canonical = dedup_values(all_values)

        concepts[concept_id] = {
            "column_variants": [
                col for col in col_names if column_hits[col] > 0
            ],
            "column_variant_counts": {
                col: count for col, count in column_hits.most_common()
            },
            "resource_count": resource_count,
            "domains": dict(domain_resource_count.most_common()),
            "raw_value_count": len(all_values),
            "canonical_value_count": len(canonical),
            "values": canonical,
        }

    return concepts


# ---------------------------------------------------------------------------
# Domain vocabulary summaries
# ---------------------------------------------------------------------------

def build_domain_summaries(
    concepts: dict[str, dict],
    abbreviation_scan: dict[str, dict],
    resource_schemas: dict,
    domain_resource_ids: dict[str, set[str]],
) -> dict[str, dict]:
    """Build per-domain vocabulary summaries."""
    summaries = {}

    for domain, rids in domain_resource_ids.items():
        # Which concepts appear in this domain?
        domain_concepts = []
        for concept_id, concept in concepts.items():
            if domain in concept["domains"]:
                domain_concepts.append({
                    "concept": concept_id,
                    "resource_count": concept["domains"][domain],
                    "value_count": concept["canonical_value_count"],
                })

        # Which abbreviations appear in this domain?
        domain_abbrevs = []
        for suffix, info in abbreviation_scan.items():
            if domain in info["domains"]:
                domain_abbrevs.append({
                    "suffix": suffix,
                    "expansion": info["expansion"],
                    "symbol": info["symbol"],
                    "field_count": info["domains"][domain],
                })

        # Collect sample field names from this domain's resources
        sample_fields: Counter = Counter()
        for rid in rids:
            if rid in resource_schemas:
                for field in resource_schemas[rid]["fields"]:
                    sample_fields[field] += 1

        # Top field names that contain abbreviations (most useful for the LLM)
        abbrev_fields = []
        for field, count in sample_fields.most_common():
            for suffix in ABBREVIATIONS:
                if field.lower().endswith(suffix):
                    abbrev_fields.append(field)
                    break
            if len(abbrev_fields) >= 15:
                break

        summaries[domain] = {
            "concepts": sorted(
                domain_concepts,
                key=lambda x: -x["resource_count"],
            ),
            "abbreviations": sorted(
                domain_abbrevs,
                key=lambda x: -x["field_count"],
            ),
            "top_abbreviated_fields": abbrev_fields,
            "total_resources_with_vocab": sum(
                1 for rid in rids if rid in resource_schemas
            ),
        }

    return summaries


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    vocab_path = ROOT / "vocabulary_raw.json"
    schemas_path = ROOT / "schemas.json"
    output_path = ROOT / "value_catalog.json"

    if not vocab_path.exists():
        print(f"ERROR: {vocab_path} not found. Run extract_vocabulary.py first.")
        sys.exit(1)
    if not schemas_path.exists():
        print(f"ERROR: {schemas_path} not found.")
        sys.exit(1)

    print(f"Loading {vocab_path.name} ...")
    with open(vocab_path, "r", encoding="utf-8") as f:
        vocab = json.load(f)

    print(f"Loading {schemas_path.name} ...")
    with open(schemas_path, "r", encoding="utf-8") as f:
        schemas = json.load(f)

    categorical_values = vocab["categorical_values"]
    resource_schemas = vocab["resource_schemas"]

    # Build domain → resource_id set mapping from schemas.json
    domain_resource_ids: dict[str, set[str]] = {}
    for domain, info in schemas["domain_resource_index"].items():
        domain_resource_ids[domain] = {r["id"] for r in info["resources"]}

    # --- Step 1: Scan abbreviations ---
    print("Scanning field name abbreviations ...")
    abbreviation_scan = scan_abbreviations(resource_schemas, domain_resource_ids)
    total_abbrev_fields = sum(a["field_count"] for a in abbreviation_scan.values())
    print(f"  {len(abbreviation_scan)} suffix patterns, {total_abbrev_fields} field matches")

    # --- Step 2: Build concepts ---
    print("Building semantic concepts ...")
    concepts = build_concepts(categorical_values, domain_resource_ids)
    total_canonical = sum(c["canonical_value_count"] for c in concepts.values())
    print(f"  {len(concepts)} concepts, {total_canonical} canonical values")

    # Report unmapped columns
    mapped_cols = set(_COL_TO_CONCEPT.keys())
    actual_cols: set[str] = set()
    for rid, cols in categorical_values.items():
        actual_cols.update(cols.keys())
    unmapped = actual_cols - mapped_cols
    if unmapped:
        print(f"  {len(unmapped)} unmapped columns (not assigned to any concept):")
        for col in sorted(unmapped):
            count = sum(1 for rid, cols in categorical_values.items() if col in cols)
            print(f"    {col} ({count} resources)")

    # --- Step 3: Domain summaries ---
    print("Building domain vocabulary summaries ...")
    domain_summaries = build_domain_summaries(
        concepts, abbreviation_scan, resource_schemas, domain_resource_ids,
    )

    # --- Assemble output ---
    output = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_files": ["vocabulary_raw.json", "schemas.json"],
            "total_concepts": len(concepts),
            "total_canonical_values": total_canonical,
            "total_abbreviation_patterns": len(abbreviation_scan),
            "total_abbreviation_field_matches": total_abbrev_fields,
        },
        "abbreviations": abbreviation_scan,
        "concepts": concepts,
        "domain_vocabulary": domain_summaries,
    }

    print(f"\nWriting {output_path.name} ...")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size_kb = output_path.stat().st_size / 1024
    print(f"  {size_kb:.0f} KB written")

    # --- Summary ---
    print("\n=== Value Catalog Summary ===")
    print(f"Concepts: {len(concepts)}")
    for cid, c in sorted(concepts.items(), key=lambda x: -x[1]["resource_count"]):
        print(
            f"  {cid}: {c['resource_count']} resources, "
            f"{c['canonical_value_count']} values, "
            f"columns: {c['column_variants']}"
        )

    print(f"\nAbbreviations: {len(abbreviation_scan)}")
    for suffix, a in sorted(
        abbreviation_scan.items(), key=lambda x: -x[1]["field_count"]
    ):
        print(f"  {suffix} = {a['expansion']} ({a['field_count']} fields)")

    print(f"\nDomain summaries: {len(domain_summaries)}")
    for domain, s in domain_summaries.items():
        print(
            f"  {domain}: {len(s['concepts'])} concepts, "
            f"{len(s['abbreviations'])} abbreviation patterns, "
            f"{len(s['top_abbreviated_fields'])} sample abbrev fields"
        )


if __name__ == "__main__":
    main()
