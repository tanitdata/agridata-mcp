"""Extract vocabulary from agridata.tn portal for Phase 3 semantic enrichment.

Mines the CKAN API to produce vocabulary_raw.json containing:
- Dataset metadata (titles, descriptions, tags)
- Group names and descriptions
- Field schemas for all DataStore resources
- Categorical column DISTINCT values

~1,200 API calls, ~10-15 min runtime (0.3s rate limit). No new dependencies.
Output feeds Step 2 (value catalog) of Phase 3.

Usage:
    python scripts/extract_vocabulary.py
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from pathlib import Path

# Add src/ to path so we can import tanitdata
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from tanitdata.ckan_client import CKANClient

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Column classification heuristics
# ---------------------------------------------------------------------------

# Columns to always skip (internal, numeric, date, coordinate)
SKIP_EXACT: set[str] = {"_id", "_full_text", "None"}

SKIP_PATTERNS: list[re.Pattern] = [
    re.compile(r"^\d{4}$"),          # year-as-column-names (2007, 2008, ...)
    re.compile(r"^_"),               # internal CKAN columns
    re.compile(r"^date$", re.I),
    re.compile(r"^ann[ée]e$", re.I),
    re.compile(r"^valeur$", re.I),
    re.compile(r"^value$", re.I),
    re.compile(r"^prix", re.I),      # prix, prixmin, prixmax
    re.compile(r"^latitude$", re.I),
    re.compile(r"^longitude$", re.I),
    re.compile(r"^elevation$", re.I),
]

# Column names (lowercase) that are known categorical — always sample
CATEGORICAL_NAMES: set[str] = {
    # Geographic
    "delegation", "délégation", "delegation_ar",
    "gouvernorat", "gouvernorat_ar",
    "region", "région", "milieu",
    "pays",
    # Biological / product
    "culture", "espece", "espèce", "variete", "variété",
    "type", "nature", "produit",
    "nature_du_produit", "type_de_produit",
    # EAV parameter columns
    "nom_fr", "nom_ar", "sensor_name", "parameter",
    # Units
    "unite", "unité", "unit",
    # Classification
    "conduite_culturale", "conduite",
    "saison", "campagne",
    "secteur", "categorie", "catégorie",
    "filiere", "filière",
    "mode", "etat", "état",
    "groupe", "classe",
    "theme", "thème", "langue",
    # Fisheries
    "societe", "société", "port",
}

# Prefixes that suggest categorical (lowercase matching)
CATEGORICAL_PREFIXES: tuple[str, ...] = (
    "delegation", "gouvernorat", "type_", "nature_",
    "espece_", "variete_", "nom_", "conduite_",
    "categorie_", "mode_", "secteur_", "filiere_",
)

# Cap per column to avoid huge output — columns with more values are skipped
MAX_DISTINCT_VALUES = 500


def _should_skip(col_name: str) -> bool:
    """Return True if the column should be skipped (not categorical)."""
    if col_name in SKIP_EXACT:
        return True
    return any(pat.match(col_name) for pat in SKIP_PATTERNS)


def _is_categorical(col_name: str) -> bool:
    """Return True if the column name matches categorical heuristics."""
    lower = col_name.lower().strip()
    if lower in CATEGORICAL_NAMES:
        return True
    return lower.startswith(CATEGORICAL_PREFIXES)


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------


async def extract_datasets(
    client: CKANClient,
) -> tuple[list[dict], dict[str, dict]]:
    """Fetch all dataset metadata and build resource → dataset mapping.

    Returns:
        datasets: list of dataset dicts (title, notes, tags, org, groups)
        resource_map: {resource_id: {name, dataset, dataset_title, org, org_title}}
    """
    datasets: list[dict] = []
    resource_map: dict[str, dict] = {}
    offset = 0
    page_size = 100
    total: int | None = None

    while True:
        result = await client.package_search(query="", rows=page_size, start=offset)
        if not result:
            break

        if total is None:
            total = result.get("count", 0)
            print(f"  Fetching {total} datasets...", flush=True)

        for ds in result.get("results", []):
            org = ds.get("organization") or {}
            if isinstance(org, dict):
                org_slug = org.get("name", "")
                org_title = org.get("title", "")
            else:
                org_slug = org_title = ""

            datasets.append({
                "name": ds.get("name", ""),
                "title": ds.get("title", ""),
                "notes": (ds.get("notes") or "")[:500],
                "tags": [t.get("display_name", "") for t in ds.get("tags", [])],
                "organization": org_slug,
                "organization_title": org_title,
                "groups": [g.get("display_name", "") for g in ds.get("groups", [])],
                "num_resources": ds.get("num_resources", 0),
            })

            # Build resource → dataset mapping from resource list
            for res in ds.get("resources", []):
                rid = res.get("id", "")
                if rid:
                    resource_map[rid] = {
                        "name": res.get("name", ""),
                        "dataset": ds.get("name", ""),
                        "dataset_title": ds.get("title", ""),
                        "organization": org_slug,
                        "organization_title": org_title,
                        "format": res.get("format", ""),
                        "datastore_active": res.get("datastore_active", False),
                    }

        offset += page_size
        if offset >= total:
            break
        print(f"    ...{offset}/{total}", flush=True)

    print(f"  Done: {len(datasets)} datasets, {len(resource_map)} resources mapped", flush=True)
    return datasets, resource_map


async def extract_groups(client: CKANClient) -> list[dict]:
    """Fetch all thematic groups with descriptions."""
    result = await client.api_call("group_list", {"all_fields": "true"})
    if not result:
        print("  Warning: group_list returned no data", flush=True)
        return []

    groups = []
    for g in result:
        groups.append({
            "name": g.get("name", ""),
            "display_name": g.get("display_name", ""),
            "description": (g.get("description") or "")[:500],
            "package_count": g.get("package_count", 0),
        })

    print(f"  Done: {len(groups)} groups", flush=True)
    return groups


async def extract_resource_schemas(
    client: CKANClient,
    resource_map: dict[str, dict],
) -> dict[str, dict]:
    """Fetch field schemas for all DataStore resources.

    Uses schemas.json as a baseline (saves ~789 API calls), fetches from
    the live API only for resources not already covered.

    Returns: {resource_id: {name, dataset, fields: [str], records: int}}
    """
    # --- Load known field lists from schemas.json ---
    known_fields: dict[str, list[str]] = {}
    known_meta: dict[str, dict] = {}

    schemas_path = ROOT / "schemas.json"
    if schemas_path.exists():
        with open(schemas_path, encoding="utf-8") as f:
            schemas = json.load(f)

        # From domain_resource_index
        for domain_data in schemas.get("domain_resource_index", {}).values():
            for res in domain_data.get("resources", []):
                rid = res["id"]
                fields = [
                    f for f in res.get("fields", [])
                    if f not in ("_id", "_full_text")
                ]
                if fields and rid not in known_fields:
                    known_fields[rid] = fields
                    known_meta[rid] = {
                        "name": res.get("name", ""),
                        "dataset": res.get("dataset", ""),
                        "records": res.get("records", 0),
                    }

        # From clusters
        for cluster in schemas.get("clusters", []):
            fields = [
                f for f in cluster.get("fields", [])
                if f not in ("_id", "_full_text")
            ]
            for res in cluster.get("resources", []):
                rid = res["id"]
                if rid not in known_fields and fields:
                    known_fields[rid] = fields
                    known_meta[rid] = {
                        "name": res.get("name", ""),
                        "dataset": res.get("dataset", ""),
                        "records": res.get("records", 0),
                    }

        print(f"  Loaded {len(known_fields)} field lists from schemas.json", flush=True)

    # --- Fetch live resource IDs from _table_metadata ---
    result = await client.datastore_search(resource_id="_table_metadata", limit=5000)
    if not result:
        print("  Warning: _table_metadata returned no data", flush=True)
        return {}

    live_ids: set[str] = set()
    for rec in result.get("records", []):
        rid = rec.get("name") or rec.get("_id")
        if rid and rid != "_table_metadata" and not rec.get("alias_of"):
            live_ids.add(str(rid))

    print(f"  {len(live_ids)} live DataStore resources", flush=True)

    # --- Build output: use known schemas + resource_map for names ---
    resources: dict[str, dict] = {}

    for rid in live_ids:
        rm = resource_map.get(rid, {})
        if rid in known_fields:
            km = known_meta.get(rid, {})
            resources[rid] = {
                "name": rm.get("name") or km.get("name", ""),
                "dataset": rm.get("dataset") or km.get("dataset", ""),
                "fields": known_fields[rid],
                "records": km.get("records", 0),
            }
        # else: will fetch from API below

    # --- Fetch schemas for resources not in schemas.json ---
    need_fetch = [rid for rid in live_ids if rid not in known_fields]
    print(f"  Fetching schemas for {len(need_fetch)} resources not in schemas.json...", flush=True)

    fetched = 0
    failed = 0
    for rid in need_fetch:
        try:
            sr = await client.datastore_search(resource_id=rid, limit=0)
            if sr:
                fields = [
                    f["id"] for f in sr.get("fields", [])
                    if f.get("id") not in ("_id", "_full_text")
                ]
                rm = resource_map.get(rid, {})
                resources[rid] = {
                    "name": rm.get("name", ""),
                    "dataset": rm.get("dataset", ""),
                    "fields": fields,
                    "records": sr.get("total", 0),
                }
                fetched += 1
        except Exception:
            failed += 1

        done = fetched + failed
        if done % 50 == 0 and done > 0:
            print(f"    ...{done}/{len(need_fetch)}", flush=True)

    print(
        f"  Done: {len(resources)} resource schemas "
        f"({len(known_fields)} from schemas.json, {fetched} fetched, {failed} failed)",
        flush=True,
    )
    return resources


async def extract_categorical_values(
    client: CKANClient,
    resource_schemas: dict[str, dict],
) -> dict[str, dict[str, list[str]]]:
    """For each resource, fetch DISTINCT values for categorical columns.

    Returns: {resource_id: {column_name: [distinct_values]}}
    """
    # Identify all (resource_id, column) pairs to sample
    pairs: list[tuple[str, str]] = []
    for rid, info in resource_schemas.items():
        for col in info["fields"]:
            if _should_skip(col):
                continue
            if _is_categorical(col):
                pairs.append((rid, col))

    print(f"  {len(pairs)} categorical columns to sample", flush=True)

    results: dict[str, dict[str, list[str]]] = {}
    sampled = 0
    fallback_count = 0
    too_many = 0
    errors = 0

    for rid, col in pairs:
        values = await _fetch_distinct_sql(client, rid, col)

        if values is None:
            # SQL blocked or failed — try datastore_search fallback
            values = await _fetch_distinct_fallback(client, rid, col)
            if values is not None:
                fallback_count += 1

        if values is None:
            errors += 1
        elif not values:
            too_many += 1  # exceeded MAX_DISTINCT_VALUES
        else:
            results.setdefault(rid, {})[col] = values
            sampled += 1

        done = sampled + fallback_count + too_many + errors
        if done % 100 == 0 and done > 0:
            print(f"    ...{done}/{len(pairs)} ({sampled} sampled)", flush=True)

    total_values = sum(len(v) for cols in results.values() for v in cols.values())
    print(
        f"  Done: {sampled} columns sampled ({total_values} distinct values), "
        f"{fallback_count} via fallback, {too_many} skipped (too many values), "
        f"{errors} errors",
        flush=True,
    )
    return results


async def _fetch_distinct_sql(
    client: CKANClient,
    resource_id: str,
    column: str,
) -> list[str] | None:
    """Fetch DISTINCT values via SQL. Returns list, empty list (too many), or None (failed)."""
    sql = (
        f'SELECT DISTINCT "{column}" FROM "{resource_id}" '
        f"WHERE \"{column}\" IS NOT NULL AND \"{column}\" != '' "
        f'ORDER BY "{column}" LIMIT {MAX_DISTINCT_VALUES + 1}'
    )
    result = await client.datastore_sql(sql)
    if result is None:
        return None

    records = result.get("records", [])
    values = [str(r.get(column, "")).strip() for r in records if r.get(column)]
    # Deduplicate after stripping (some values differ only by whitespace)
    values = sorted(set(values))

    if len(values) > MAX_DISTINCT_VALUES:
        return []  # too many — not useful as vocabulary
    return values


async def _fetch_distinct_fallback(
    client: CKANClient,
    resource_id: str,
    column: str,
) -> list[str] | None:
    """Fallback: fetch records via datastore_search and extract unique values in Python."""
    # Request only the column we need via the fields parameter
    result = await client.api_call("datastore_search", {
        "resource_id": resource_id,
        "fields": column,
        "limit": 5000,
    })
    if not result:
        return None

    seen: set[str] = set()
    for rec in result.get("records", []):
        val = rec.get(column)
        if val and str(val).strip():
            seen.add(str(val).strip())

    if len(seen) > MAX_DISTINCT_VALUES:
        return []
    return sorted(seen)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    print("=== Phase 3 Step 1: Vocabulary Extraction ===\n", flush=True)
    start_time = time.monotonic()

    client = CKANClient()

    try:
        # 1. Dataset metadata (also builds resource → dataset mapping)
        print("[1/4] Extracting dataset metadata...", flush=True)
        datasets, resource_map = await extract_datasets(client)

        # 2. Group metadata
        print("\n[2/4] Extracting groups...", flush=True)
        groups = await extract_groups(client)

        # 3. Resource field schemas
        print("\n[3/4] Extracting resource field schemas...", flush=True)
        resource_schemas = await extract_resource_schemas(client, resource_map)

        # 4. Categorical column values
        print("\n[4/4] Sampling categorical column values...", flush=True)
        categorical_values = await extract_categorical_values(client, resource_schemas)

        # --- Assemble output ---
        total_cat_cols = sum(len(v) for v in categorical_values.values())
        total_values = sum(
            len(vals) for cols in categorical_values.values()
            for vals in cols.values()
        )

        output = {
            "meta": {
                "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "portal_url": client.base_url,
                "total_datasets": len(datasets),
                "total_groups": len(groups),
                "total_resources": len(resource_schemas),
                "total_categorical_columns": total_cat_cols,
                "total_distinct_values": total_values,
            },
            "datasets": datasets,
            "groups": groups,
            "resource_schemas": {
                rid: {
                    "name": info.get("name", ""),
                    "dataset": info.get("dataset", ""),
                    "fields": info["fields"],
                    "records": info.get("records", 0),
                }
                for rid, info in resource_schemas.items()
            },
            "categorical_values": categorical_values,
        }

        out_path = ROOT / "vocabulary_raw.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        elapsed = time.monotonic() - start_time
        size_kb = out_path.stat().st_size / 1024

        print(f"\n=== Done in {elapsed:.0f}s ===", flush=True)
        print(f"Output: {out_path.name} ({size_kb:.0f} KB)", flush=True)
        print(f"  {len(datasets)} datasets", flush=True)
        print(f"  {len(groups)} groups", flush=True)
        print(f"  {len(resource_schemas)} resource schemas", flush=True)
        print(f"  {total_cat_cols} categorical columns", flush=True)
        print(f"  {total_values} distinct values", flush=True)

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
