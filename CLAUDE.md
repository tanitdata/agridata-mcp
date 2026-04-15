# CLAUDE.md — Tanitdata MCP Server

## Project Identity

**tanitdata** is an MCP (Model Context Protocol) server that provides AI-mediated access to Tunisia's agricultural open data portal (agridata.tn), covering structured numerical data (DataStore tables) and bibliographic catalogs (25,944 records with PDF links) so that researchers can query both simultaneously through natural language.

See RESEARCH.md for academic research context and evaluation methodology.

## Architecture

```
┌─────────────────────────────────────────────┐
│              MCP Client                      │
│  (Claude Desktop / Cursor / Windsurf)        │
└──────────────┬──────────────────────────────┘
               │ MCP Protocol (stdio local / streamable-http remote)
┌──────────────▼──────────────────────────────┐
│           tanitdata MCP Server               │
│                                              │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │ Data Tools   │  │ Knowledge Tools      │  │
│  │              │  │                      │  │
│  │ climate  ✅  │  │ bibliography ✅      │  │
│  │ generic_sql✅│  │                      │  │
│  │ read_file ✅ │  │                      │  │
│  │ search   ✅  │  │                      │  │
│  └──────┬──────┘  └──────────┬───────────┘  │
│         │                    │               │
│  ┌──────▼────────────────────▼───────────┐  │
│  │ CKAN Client + Schema Registry         │  │
│  └──────┬────────────────────────────────┘  │
└─────────┼───────────────────────────────────┘
          │ HTTPS
┌─────────▼───────────────────────────────────┐
│  catalog.agridata.tn (CKAN 2.9+)            │
│  1,248 DataStore resources · 1,108 datasets   │
└─────────────────────────────────────────────┘
```

## Tech Stack

- **Language:** Python 3.11+
- **MCP SDK:** `mcp[cli]` (official Anthropic Python SDK)
- **HTTP client:** `httpx` (async, lazy-initialized, rate-limited)
- **XLSX parser:** `openpyxl` (read-only mode for non-DataStore files)
- **Data validation:** `pydantic` (installed, not yet heavily used)
- **Env config:** `python-dotenv` (supports `CKAN_BASE_URL` override)
- **Transport:** stdio (local dev), streamable-http (remote deployment)
- **Test framework:** `pytest` + `pytest-asyncio` (asyncio_mode = "strict")

## Dependencies (actual — from pyproject.toml)

```toml
dependencies = ["mcp[cli]", "httpx", "pydantic", "python-dotenv", "openpyxl", "boto3"]

[dependency-groups]
dev = ["pytest", "pytest-asyncio"]
```

## Portal Facts

- **Portal URL:** https://catalog.agridata.tn
- **API base:** https://catalog.agridata.tn/api/3/action
- **Datasets:** 1,108
- **Resources:** ~1,571 (1,248 DataStore-active)
- **Organizations:** 55 (but `organization_list` only returns 25 — use faceted search via `package_search` instead)
- **Groups:** 21 thematic groups
- **License:** All data under Licence Nationale de Données Publiques Ouvertes
- **Language:** French-dominant, some Arabic metadata and field names

## CKAN API Patterns (Confirmed Working)

### Basic API Call (with rate limiting)
```python
# CKANClient enforces 0.3s minimum interval between requests
# Override portal URL: set CKAN_BASE_URL env var

async def api_call(self, action: str, params: dict) -> dict | None:
    url = f"https://catalog.agridata.tn/api/3/action/{action}"
    response = await self.client.get(url, params=params, timeout=30)
    data = response.json()
    if data.get("success"):
        return data["result"]
    return None
```

### DataStore SQL (confirmed working — all capabilities)
```python
# Basic query
result = await client.datastore_sql(f'SELECT * FROM "{resource_id}" LIMIT 100')

# Numeric casting (CRITICAL: all fields are stored as text)
'SELECT "nom_fr", AVG("valeur"::numeric) as avg FROM "{rid}" WHERE "valeur" ~ \'^-?[0-9.]+$\' GROUP BY "nom_fr"'

# Date casting
'SELECT "Date"::timestamp, "nom_fr", "valeur" FROM "{rid}" WHERE "Date" > \'2026-01-01\' LIMIT 50'

# Numeric WHERE with regex guard
'SELECT * FROM "{rid}" WHERE "valeur"::numeric > 30 AND "nom_fr" = \'Air temperature\' LIMIT 20'

# DATE_TRUNC for time series aggregation (confirmed working)
'SELECT DATE_TRUNC(\'day\', "Date"::timestamp) as day, AVG("valeur"::numeric) as avg_val FROM "{rid}" WHERE ... GROUP BY day ORDER BY day LIMIT 200'
'SELECT DATE_TRUNC(\'month\', "Date"::timestamp) as month, SUM("valeur"::numeric) as total FROM "{rid}" WHERE ... GROUP BY month ORDER BY month LIMIT 200'

# ILIKE for case-insensitive parameter matching (confirmed working)
'"nom_fr" ILIKE \'%temperature%\''
'"nom_fr" ILIKE \'%itesse%vent%\''   # matches "Vitesse du vent"

# DISTINCT ON for latest-per-group (confirmed working — PostgreSQL extension)
'SELECT DISTINCT ON ("nom_fr") "nom_fr" as sensor, "Date" as reading_time, "valeur"::numeric as value, "unite" as unit FROM "{rid}" WHERE "valeur" ~ \'^-?[0-9.]+$\' ORDER BY "nom_fr", "Date" DESC'

# Cross-resource JOIN (confirmed working)
'SELECT a.*, b.* FROM "{rid_a}" a, "{rid_b}" b WHERE a."Delegation" = b."delegation" LIMIT 50'

# Live DataStore inventory (used by schema registry live refresh)
result = await client.datastore_search(resource_id="_table_metadata", limit=5000)
# Returns: name (= resource_id), alias_of (non-null means it's an alias, skip it)
```

### Confirmed SQL Capabilities
- SELECT, COUNT, WHERE, ORDER BY, GROUP BY, DISTINCT, LIKE, ILIKE, LIMIT/OFFSET, aliases: **all work**
- `::numeric`, `::timestamp` type casting: **works**
- `DATE_TRUNC('day'|'month'|'year', col::timestamp)`: **works**
- `DISTINCT ON (col)`: **works** (PostgreSQL-specific)
- Cross-resource JOINs: **works**
- Regex filtering (`~` operator): **works**
- `information_schema` access: **BLOCKED** (use schema registry instead)
- Result limit: 32,000 rows per query
- Baseline latency: ~0.45s per query (network-dominated)
- Rate limit: 0.3s min interval enforced by CKANClient

### Dataset Search (workaround for organization gap)
```python
# organization_list only returns 25 of 55 orgs — use package_search instead
result = await client.package_search(
    query=query,
    fq=f"organization:{org_name}",
    rows=rows,
    start=offset,
    facet_fields=["organization", "groups", "res_format"],
)
```

### Schema Discovery (per resource)
```python
result = await client.datastore_search(resource_id=rid, limit=0)
fields = result["fields"]  # [{"id": "Date", "type": "text"}, ...]
```

## Schema Registry — Two-Layer Design

### Overview

`SchemaRegistry` in `schema_registry.py` has two layers:

- **Static layer** — loaded once from `schemas.json` at startup via `registry.load()`. Never mutated. Contains: `domain_resource_index`, `clusters`, `arabic_field_decoding`.
- **Live layer** — seeded from the static layer at startup, then refreshed every 6 hours in the background. Contains: up-to-date resource list, field schemas, record counts, dataset→org mapping.

### Startup sequence
```
server lifespan starts
  → registry.load()          # loads schemas.json (~28ms), seeds live layer
  → asyncio.create_task(_background_refresh())   # starts live refresh without blocking tools
tools are immediately callable (static data available)
~10s later: live refresh completes (1,248+ resources, 1,108 dataset→org mappings)
```

### maybe_refresh pattern
Every tool calls `await registry.maybe_refresh(client)` before doing anything. This:
- Checks if `refresh_interval` (6h) has elapsed
- If so, acquires a lock and calls `_refresh()` (double-checked locking)
- Most calls return instantly (~0 µs)

### Key query methods

```python
# Returns resources for a domain, enriched with live field lists and record counts
resources = registry.get_domain_resources("climate_stations")
resources = registry.get_domain_resources("crop_production", gouvernorat="Béja")

# Human-readable availability summary (used in query_datastore responses)
avail = registry.get_data_availability("dams", gouvernorat="Kairouan")
# → "Kairouan — 2 DataStore resource(s), 271 total records, 2020–2023"

# Source attribution for a resource (used in all tool response footers)
source = registry.get_source_attribution("ec7daec9-da4b-47a4-9ea9-f6b5ca820955")
# → {resource_id, resource_name, dataset_name, dataset_title, organization,
#    organization_title, portal_url}

# Domain + governorate context for a resource (used in query_datastore)
ctx = registry.get_resource_context("some-resource-id")
# → {"domains": ["crop_production"], "gouvernorat": "Béja"}

# Governorate → resource_count map for a domain
coverage = registry.get_coverage_summary("crop_production")
# → {"Béja": 3, "Bizerte": 2, "national": 5, ...}

# Schema lookup (live first, then cluster fallback, then domain index)
fields = registry.get_resource_schema("some-resource-id")
```

### Governorate extraction (5-tier strategy)

`extract_governorate(org_slug, dataset_slug, resource_name)` tries in order:
1. **Organization slug** → `CRDA_SLUG_MAP` (e.g. `crda-beja` → `Béja`); `NATIONAL_ORGS` → `"national"`
2. **Dataset slug** → regex `gouvernorat-de-{name}` / `gouvernorat-{name}`
3. **Resource display name** → regex `Gouvernorat de {Name}`
4. **Bare governorate name** in any text (word-boundary match, long keys first to avoid false positives)
5. **Locality/delegation name** → `LOCALITY_MAP` (e.g. `"bir ben kemla"` → `"Mahdia"`, `"ghezala"` → `"Bizerte"`)

Supporting maps (all in `schema_registry.py`):
- `GOVERNORATE_MAP`: 31 entries, lower-case key → canonical French name
- `CRDA_SLUG_MAP`: 24 CRDA org slugs → governorate
- `NATIONAL_ORGS`: 24 national-level org slugs (DGGREE, DGACTA, ONAGRI, etc.)
- `LOCALITY_MAP`: 60 delegation/station-site names → parent governorate

### schemas.json structure

- `meta`: totals (789 resources, 583 unique patterns, 513 singletons, 70 clusters)
- `clusters[]`: 70 shared schema patterns covering 276 resources
- `domain_resource_index`: 11 domains with curated resource lists, field schemas, record counts
- `arabic_field_decoding`: mojibake → Arabic/French mapping for 19 Bizerte price datasets

### Key Domain Resource Counts (from schemas.json — verified 2026-04)

| Domain | Resources | Records | Key Fields |
|---|---|---|---|
| climate_stations | 24 | ~213,700 | varies by EAV variant (see below) |
| rainfall | 48 | ~213,900 | delegation/station, mois, precipitations_mm |
| dams | 23 | 986 | Nom du barrage, Capacite, Quantite_stockee | *(removed from portal as of 2026-04)* |
| crop_production | 239 | ~1,062,300 | 176 unique schemas — see crop section below |
| olive_harvest | 49 | 2,983 | Delegation, Production_Huile_en_tonne, Nb_oliviers |
| prices | 42 | 6,550 | Variete, prix, prixmin, prixmax (+ mojibake variants) |
| fisheries | 64 | 10,496 | nature_du_produit, quantite_kilos, societe |
| investments | 59 | 5,167 | GOUVERNORAT, INV_DECLARE, NBR_EMPLOI |
| livestock | 99 | ~1,053,970 | (warning: Excel overflow resources exist at 1,048,575 rows) |
| water_resources | 108 | 5,212 | Nom de la nappe, Delegation, Ressources_million_m3 |
| trade_exports | 28 | 1,002 | produit, valeur_exportation, quantite, pays |
| documentation | 7 | 25,948 | Titre, Auteur, Annee, Nom_fichier, Resume |

### Climate Station EAV Schemas (3 variants)

The live inventory has 24 climate resources: 23 sensor stations + 1 metadata-only (Informations générales).

| Variant | Org | Date col | Param col | Value col | Unit col |
|---|---|---|---|---|---|
| `standard` | DGGREE (most) | `Date` | `nom_fr` | `valeur` | `unite` |
| `english` | DGGREE (older) | `date` | `sensor_name` | `value` | `unit` |
| `dgacta` | DGACTA | `date` | `parameter` | `value` | `unite` |

Metadata-only resources are detected by the presence of `{nom, Longitude, Latitude}` fields — no sensor data, skip for queries.

Sensor stations by governorate (live, as of 2026-04): Bizerte (4), Jendouba (2), Mahdia (3), Nabeul (3), Zaghouan (4), Kasserine (2), Siliana (2), Béja (1), Kairouan (1), Le Kef (1) = **10 sensor governorates**.

Kébili is the **11th governorate** in the domain but holds only 1 metadata-only resource ("Informations générales", fields: nom, Longitude, Latitude, elevation) — no sensor data. The inventory output reports 10 because it counts sensor stations only (`by_gov` is built from `sensor_stations`, not `metadata_stations`).

## Climate Tool — Parameter Mapping

### Natural language → canonical group → ILIKE patterns

```python
# _PARAM_ALIASES (user input → canonical)
"température" | "temperature" | "temp" | "air temperature" | "hc air temperature" → "temperature"
"vent" | "vitesse du vent" | "wind speed" | "wind" | "u-sonic wind speed"          → "wind"
"direction du vent" | "wind direction"                                               → "wind_direction"
"pluie" | "rain" | "precipitations" | "précipitations" | "precipitation"            → "rain"
"humidité" | "humidity" | "humidite" | "relative humidity"                          → "humidity"
"rayonnement" | "solar" | "radiation" | "solar radiation"                           → "solar"
"leaf wetness" | "mouillage foliaire"                                                → "leaf_wetness"
"soil moisture" | "humidité du sol"                                                  → "soil_moisture"
"deltat"                                                                             → "deltat"

# _PARAM_ILIKE (canonical → SQL ILIKE patterns applied to param column)
"temperature"    → ["%temperature%"]
"wind"           → ["%itesse%vent%", "%wind%speed%"]
"wind_direction" → ["%direction%vent%", "%wind%direction%", "%wind%dir%"]
"rain"           → ["%recipit%", "%pluie%"]
"humidity"       → ["%humidit%", "%humidity%"]
"solar"          → ["%radia%", "%solar%"]
"leaf_wetness"   → ["%leaf%wetness%", "%mouillage%"]
"soil_moisture"  → ["%soil%moisture%", "%humidit%sol%"]
"deltat"         → ["%deltat%"]
```

Precipitation (`rain` canonical) uses `SUM` aggregation; all other sensors use `AVG`.
Unrecognised parameter input: falls back to raw `ILIKE '%user_input%'` on the param column.

## Tool Inventory

### P0: search_datasets ✅ Implemented
Search portal datasets by keyword, organization, thematic group, format.
- Input: `query: str`, `organization: str?`, `group: str?`, `format: str?`, `limit: int = 10`
- Output: list of datasets with id, title, description, organization, groups, num_resources, url
- API: `package_search` with `facet_fields=["organization", "groups", "res_format"]`

### P0: get_dataset_details ✅ Implemented
Get full metadata and resource list for a specific dataset.
- Input: `dataset_id: str` (slug or UUID)
- Output: dataset metadata + list of resources with id, name, format, datastore_active
- API: `package_show`

### P0: query_datastore ✅ Implemented
Execute SQL against any DataStore resource. Returns schema + records + source attribution.
- Input: `resource_id: str?`, `dataset_id: str?`, `sql: str?`, `limit: int = 100`
- Accepts either a resource UUID or a dataset slug — auto-resolves slugs to the first DataStore-active resource via `package_show` lookup
- If SQL references the original slug, it is rewritten with the resolved UUID
- Output: Arabic field decoding (if applicable), schema, records table, value hints for categorical columns, registry schema note, data availability context, source attribution footer
- If no SQL: uses `datastore_search` (returns schema + first `limit` rows)
- If SQL: uses `datastore_search_sql`
- Automatically annotates Arabic/mojibake fields using the registry mapping
- Appends `get_data_availability()` context if the resource is in the domain index

### P0: query_climate_stations ✅ Implemented
Query climate station data from Tunisia's weather monitoring network (24 resources across 11 governorates; 23 sensor stations in 10 governorates + 1 metadata-only in Kébili).
- Input: `station: str?`, `parameter: str?`, `date_from: str?`, `date_to: str?`, `aggregation: str = "raw"` ("raw"|"daily"|"monthly"), `latest: bool = False`
- Modes:
  - **Inventory** (no args): full station list with live sensor names, record counts, latest readings, grouped by governorate. First call ~7s (fetches sensor lists); cached, subsequent calls ~0s.
  - **Station details** (station only): metadata and available sensors with latest reading dates
  - **Data query** (station + parameter, or parameter only): time series; precipitation uses SUM, others AVG; partial date ranges with station-aware diagnosis when no data found
  - **Multi-station comparison** (comma or "vs" in station, e.g. "Bizerte vs Mahdia"): side-by-side data
  - **Latest readings** (latest=True): DISTINCT ON query returning most recent reading per sensor
- Station matching: partial match on name, governorate, or dataset slug; canonical aliases via GOVERNORATE_MAP
- Handles 3 EAV schema variants: auto-detected from field names
- Implementation: `src/tanitdata/tools/climate.py`

### P0: list_organizations ✅ Implemented
List all data-producing organizations with dataset counts.
- Input: `query: str = ""`
- Output: sorted-by-count list of org names and dataset counts
- Implementation: `package_search` with `rows=0`, reads `search_facets.organization.items`

### P0: read_resource ✅ Implemented
Download and parse non-DataStore resource files (CSV, XLSX) on demand.
- Input: `resource_id: str`, `limit: int = 100`
- Output: schema + data rows in the same format as query_datastore, source attribution
- Downloads via `resource_show` URL, parses with stdlib `csv` or `openpyxl`
- On-demand download with process-scoped in-memory cache (instant on repeat calls)
- Redirects to query_datastore if resource is DataStore-active
- Graceful handling: invalid IDs, unsupported formats (PDF etc.), files >5MB
- Implementation: `src/tanitdata/tools/resource_reader.py`
- Benchmark: ~0.8s first call (download + parse), ~0s cached

### P0: search_bibliography ✅ Implemented
Search ONAGRI's bibliographic catalogs (25,944 records across 6 resources) by keyword, year, language, theme.
- Input: `query: str`, `year_from: int?`, `year_to: int?`, `language: str?` ("FR"|"AR"|"EN"), `theme: str?` ("agriculture"|"water"|"forestry"|"fisheries"), `limit: int = 20`
- Output: ranked results with title, author, year, language, abstract (if available), PDF links, source attribution
- **Tiered execution:**
  - **Tier 1** (Base ONAGRI 22,782 + Fonds ONAGRI 2,265 = 25,047 records): SQL ILIKE on `Titre` + `Resume`. Queried first.
  - **Tier 2** (4 thematic libraries: Agriculture 665, Water 113, Forestry 79, Fisheries 40 = 897 records): SQL blocked (409 CONFLICT) — fetched via `datastore_search` + Python-side keyword/year filtering. Process-scoped cache (~0s on repeat).
  - Tier 2 only queried if Tier 1 returns fewer than `limit` results, or if a `theme` is specified.
- **Scoring:** Titre match = 2 points, Resume match = 1 point per keyword. Results sorted by score descending.
- **PDF URLs:** Fonds records contain full URLs in `source` field (resolved to `onagri.nat.tn`). Thematic library records use `Nom_fichier` → `https://www.onagri.nat.tn/uploads/docagri/{Nom_fichier}.pdf`.
- **Year filter:** String comparison with `^\d{4}$` regex guard (handles `'ND'`, `'SD'`, `None` values in Annee).
- **Language filter:** ILIKE `'%FR%'` (handles `(FR)`, `(Fr)`, `FR`, compound values like `(EN, FR, ES, AR)`). Tier 1 only (Tier 2 has no Langue column).
- Implementation: `src/tanitdata/tools/bibliography.py`
- Benchmark: ~0.6s (Tier 1 only), ~1.8s (Tier 1 + Tier 2), ~0.01s (Tier 2 cached theme query)
### P0: get_dashboard_link ✅ Implemented
Map a query topic to the relevant interactive dashboard URL(s).
- Input: `topic: str`
- Output: dashboard title, URL, description (single match) or ranked list (multiple matches)
- 18 dashboards indexed with bilingual keyword matching (French + English)
- Accent-normalized matching: "céréales" and "cereales" both work
- Multi-match support: broad topics (e.g. "dattes") return all relevant dashboards
- No-match fallback: returns the full list of 18 dashboards
- Implementation: `src/tanitdata/tools/dashboards.py`
- Benchmark: instant (pure local lookup, no API calls)

## Data Quality Issues

1. **All fields are text** — always cast with `::numeric` or `::timestamp` for math/date. Guard numeric casts: `WHERE "valeur" ~ '^-?[0-9.]+$'`.
2. **Arabic mojibake fields** — 39 resources. The 19 Bizerte price datasets are decoded via `schemas.json` `arabic_field_decoding.field_mapping`. `query_datastore` applies this automatically. For others, present raw field names.
3. **`None` field names** — 39 resources have fields literally named `"None"`. Skip in schema presentation.
4. **Excel overflow** — 2 resources with 1,048,575 rows (Kébili livestock, fruit tree evolution). Flag as corrupted and exclude from queries.
5. **Year-as-column names** — 29 resources use "2007", "2008"... as column names (wide format). Note this in schema presentation so the LLM can unpivot if needed.
6. **Content-type mismatches** — 86 resources have CSV↔XLSX format mislabeling. Trust `datastore_active` flag, not `format` field.
7. **Organization gap** — `organization_list` returns 25 of 55 orgs. Always use faceted `package_search` instead.
8. **`_id` and `_full_text` columns** — every DataStore resource has these internal columns. Always skip them in schema/result presentation. `SKIP_COLS = {"_id", "_full_text"}` in `utils/formatting.py`, imported by climate and other tools.
9. **EAV format** — climate_stations, and possibly other sensor domains, use Entity-Attribute-Value layout. Filter by `nom_fr`/`sensor_name`/`parameter` and cast `valeur`/`value` to numeric. Never treat these as wide-format tables.
10. **Windows console encoding** — station names with non-ASCII characters (e.g. "Données climatiques") may appear as `Donn?es` in Windows terminals. This is a terminal display issue, not a data bug; the stored data is correctly UTF-8.
11. **Source attribution** — every tool response should end with `format_source_footer(sources)` from `utils/formatting.py`. Format: single source → labelled fields; multiple sources → compact list with resource_id + portal URL.

## Project Structure

```
tanitdata/
├── CLAUDE.md                   # This file
├── README.md
├── pyproject.toml
├── Dockerfile                  # Multi-stage Docker build for AWS deployment
├── .dockerignore
├── schemas.json                        # Pre-computed schema registry (static layer)
├── value_hints.json                    # Per-resource categorical values (runtime, 236 KB)
├── claude_desktop_config.example.json  # Example Claude Desktop config
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD: test → build → push ECR → deploy ECS
├── src/
│   └── tanitdata/
│       ├── __init__.py         # __version__ = "2.0.0"
│       ├── server.py           # MCP server: FastMCP, lifespan, tool registrations, /health
│       ├── auth.py             # Optional API key auth (Secrets Manager-backed, disabled by default)
│       ├── middleware.py       # Request logging + tool-call instrumentation (structured JSON)
│       ├── ckan_client.py      # Async CKAN API client (rate-limited, lazy-init)
│       ├── schema_registry.py  # Two-layer registry (static + live, 6h refresh)
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── search.py       # ✅ search_datasets, get_dataset_details, list_organizations
│       │   ├── datastore.py    # ✅ query_datastore (SQL + Arabic decode + value hints + availability)
│       │   ├── climate.py          # ✅ query_climate_stations (3 EAV variants, caching, comparison)
│       │   ├── resource_reader.py # ✅ read_resource (CSV/XLSX download+parse, on-demand cache)
│       │   ├── dashboards.py      # ✅ get_dashboard_link (18 dashboards, keyword matching)
│       │   ├── bibliography.py    # ✅ search_bibliography (6 resources, tiered, cached)
│       └── utils/
│           ├── __init__.py
│           ├── formatting.py   # format_dataset_list, format_datastore_result, format_source_footer
│           └── arabic.py       # decode_arabic_fields, annotate_fields_with_arabic
├── scripts/
│   ├── regenerate_domains.py   # Rebuild domain_resource_index in schemas.json
│   ├── extract_vocabulary.py   # Phase 3 Step 1: mine portal vocabulary → vocabulary_raw.json
│   ├── build_value_catalog.py  # Phase 3 Step 2: local processing → value_catalog.json
│   └── extract_value_hints.py  # Phase 3 Step 4b: vocabulary_raw.json → value_hints.json
├── tests/
│   ├── __init__.py
│   ├── test_ckan_client.py     # Unit tests: base URL, API base (no network)
│   ├── test_schema_registry.py # Unit tests: static load, domain lookup, cluster lookup
│   └── test_tools/
│       └── __init__.py         # placeholder — no tool tests yet
```

## Development Workflow

1. `pip install -e .` to install in editable mode (or use `uv`)
2. Run unit tests: `pytest tests/`
4. MCP Inspector for tool testing: `mcp dev src/tanitdata/server.py`
5. Connect to Claude Desktop for integration testing
6. Run against live portal API — no mock needed (all data is public, read-only)

### Docker (remote deployment)
```bash
docker build -t tanitdata .
docker run -p 8000:8000 tanitdata    # streamable-http on :8000/mcp, health on :8000/health
```
Override defaults with env vars: `MCP_TRANSPORT`, `FASTMCP_HOST`, `FASTMCP_PORT`, `SCHEMAS_PATH`.

## Known Issues and Limitations

### Portal-side
- **Jendouba GDA BOUHERTMA** station only has: Battery, Precipitation, Solar Panel, Solar radiation, wind speed/direction. No temperature, humidity, or DeltaT sensor — queries for those return empty.
- **Sousse governorate** has no climate station DataStore resources (no match for "Sousse" in query_climate_stations).
- **DGACTA stations** have very sparse data (27–162 records vs. 21,000–38,000 for DGGREE). Data updates appear periodic rather than continuous.
- **Bizerte multi-sensor station** (`5ae01acd`) data stops at 2024-12-15. A separate multi-sensor Jendouba resource (`91f1c4b8`) also stops at 2024-12. Both show as "Données climatiques de la station" in the portal (truncated display names).
- **Climate domain count mismatch**: static schemas.json lists 21 climate_stations resources; live inventory shows 24 (3 added since audit). `get_domain_resources()` enriches static with live data.

### Code-side
- `query_datastore` MCP tool does not expose a `filters` dict parameter (the underlying Python function accepts it but it was intentionally omitted from the server registration).
- Sensor list caching (`_sensor_cache` dict) in `climate.py` is process-scoped — it resets on server restart but survives the lifetime of a session, making repeated inventory calls fast (~0s after the first).
- `tests/test_tools/` has only an `__init__.py`. No automated tests for tool outputs yet — development has relied on live benchmark scripts at the project root.
- **CKAN DataStore SQL restriction**: `CASE WHEN` is not in the DataStore SQL whitelist (returns 409 CONFLICT). Some resources also return 403 FORBIDDEN for SQL queries. The 4 ONAGRI thematic library resources (Agriculture, Water, Forestry, Fisheries) block all SQL queries (409); `search_bibliography` works around this via `datastore_search` + Python filtering.

## Tool Strategy (v1.4+)

**Domain-specific tools** are only built where there's a bounded, single-schema pattern that benefits from specialized logic:
- `query_climate_stations` — 3 EAV schema variants, parameter aliasing, aggregation (SUM/AVG), sensor caching
- `search_bibliography` — 6 bibliographic resources (2 Tier 1 SQL + 4 Tier 2 cached), tiered execution, keyword scoring, PDF URL construction

**Everything else** goes through the generic workflow:
1. `search_datasets_tool` — find datasets by keyword/org/group
2. `get_dataset_details_tool` — see resources, check DataStore active status
3. If DataStore active → `query_datastore_tool` (SQL access)
4. If not DataStore active → `read_resource_tool` (download + parse CSV/XLSX)

**Shortcut:** The LLM can skip step 2 and pass a dataset slug directly to `query_datastore_tool(dataset_id=slug)`. The tool auto-resolves the slug to the first DataStore-active resource via `package_show`. This eliminates the most common LLM error (passing a dataset slug where a resource UUID is expected).

This keeps the server lean (8 tools) and lets the LLM handle schema variability naturally.

## Phase 2 — RAG (Deferred, Spun Off)

A knowledge layer (search_documents tool with vector store over PDF corpus) was prototyped and
abandoned in April 2026. OCR assessment found 85% of the 2,423 downloadable PDFs are poorly
scanned 1970s fiches techniques, and most remaining untextable PDFs are Arabic documents
incompatible with available OCR. The 357 text-extractable PDFs are mostly regulatory/registry
content — insufficient analytical narrative to justify a RAG pipeline. search_bibliography
covers document discovery adequately with 25,944 keyword-searchable records and PDF download links.

Spun off to `Z:/Tanitdata_KnowledgeRAG/` as an independent project. Full knowledge transfer
document generated 2026-04-14 covering corpus assessment, chunking, embedding model, and
integration plan. See that project's CLAUDE.md for details.

## Phase 3 — Semantic Layer Enrichment (Complete)

Strengthening schemas.json with comprehensive vocabulary and concept mappings to improve
query accuracy. The current schemas.json has resource→domain mappings but lacks the actual
column values and concept relationships that tools need to construct correct SQL.

### Problem Statement

The LLM handles multilingual translation natively (user says "wheat" → LLM knows "blé").
But it fails downstream: constructs `WHERE "culture" ILIKE '%blé%'` when the actual column
value is `Ble_dur_qx`. The fix is making portal-side vocabulary visible to the LLM — what
exact strings exist in which columns — not expanding user-facing vocabulary.

### Agreed Plan (4 Steps)

**Step 1 — Vocabulary Extraction** (COMPLETE — 2026-04-14):
Script `scripts/extract_vocabulary.py` mines the portal via CKAN API.
Output: `vocabulary_raw.json` (1,792 KB, gitignored). Runtime: ~11 min (~1,200 API calls).
Results: 1,108 datasets, 21 groups, 1,248 resource schemas, 1,106 categorical columns
sampled (14,044 distinct values across 750 resources). Top columns by frequency:
Delegation (307), unite (112), nom_ar/nom_fr (103/100), Gouvernorat (63).
Uses schemas.json as baseline for known field lists (saves ~734 API calls), fetches
remaining schemas live. Fallback to `datastore_search` for SQL-blocked resources (409).

**Step 2 — Value Catalog** (COMPLETE — 2026-04-14):
Script `scripts/build_value_catalog.py` — output: `value_catalog.json` (145 KB, gitignored).
Pure local processing of vocabulary_raw.json + schemas.json (no API calls, instant runtime).
Results: 20 semantic concepts, 3,642 canonical values (deduplicated, noise-filtered),
14 abbreviation patterns (562 field name matches), 12 domain vocabulary summaries.
Top concepts by resource count: delegation (372 resources, 802 values), unit (120, 63),
gouvernorat (117, 189), climate_parameter (113, 492), crop_type (57, 279), secteur (48, 401).
Key abbreviations: `_ha` (hectares, 348 fields), `_t` (tonnes, 73), `_qx` (quintaux, 18),
`_mm` (millimètres, 17), `_tete` (nombre de têtes, 13), `_md` (millions de dinars, 10).
Value deduplication handles accent variants (Béja/Beja), mojibake (BÃ©ja), and data noise
(pure numbers, long note strings accidentally in categorical columns).

**Step 3 — Concept Annotations** (SKIPPED — folded into Step 4):
Step 2's value catalog already covers concept→value mappings, domain associations, and the
abbreviation dictionary. The remaining useful pieces (temporal notes, unit context, geographic
relevance) are small and descriptive — they belong in tool descriptions where the LLM reads
them upfront, not in a separate JSON layer that requires discovery at query time.
Decision agreed with research collaborator 2026-04-14.

**Step 4 — Context Delivery Optimization** (COMPLETE — 2026-04-14):
Two-point intervention delivering semantic layer data at the right moment in the LLM's
decision cycle. Informed by research collaborator's insight: "the semantic layer is not a
vocabulary expansion project — it's a context delivery optimization."

**Point A — Tool description enrichment** (before any query):
Enriched `query_datastore_tool` description in `server.py` with targeted additions.
Changes (query_datastore_tool only, +7 lines):
- **Abbreviation dictionary** (1 line): field suffix → unit mapping (_ha=hectares, _t=tonnes,
  _qx=quintaux, _mm=millimètres, _tete=têtes, _md=millions de dinars, _dt=dinars tunisiens,
  _cube=mille m³, _kg=kilogrammes). Not inferable from column names alone.
- **Wide-format warning** (2 lines): many crop resources encode the crop name IN the column name
  (Ble_dur_ha, Orge_qx, Fourrages_t) — SELECT directly, do NOT use WHERE/ILIKE. This targets
  the #1 LLM SQL error pattern.
- **Crop name examples** (1 line): Ble_dur (durum wheat), Ble_tendre (soft wheat), Orge (barley),
  Triticale, Fourrages (fodder), Legumineuses (legumes), Olivier (olive), Palmier (palm).
- **Query strategy hint** (2 lines): "If unsure about exact column names or values, call with
  no SQL first to preview the schema and sample data." Universal recovery path.
Other tool descriptions unchanged — their responses already surface the relevant vocabulary.

**Point B — Response value hints** (after query runs):
New runtime artifact `value_hints.json` (236 KB, committed) contains per-resource categorical
values extracted from vocabulary_raw.json: 735 resources, 1,077 columns, 11,721 values.
Script: `scripts/extract_value_hints.py`. Loaded at startup by SchemaRegistry alongside
schemas.json. New method `registry.get_column_hints(resource_id, column_names)` returns
exact categorical values for columns in the query result. `query_datastore` appends these
as "Value hints for follow-up queries" after the records table. Zero API calls — local
dictionary lookup only. Example output:
```
**Value hints for follow-up queries:**
- `Delegation`: 9 values — Beja Nord, Beja Sud, Nefza, Testour, Amdoun, ...
- `nom_fr`: 9 values — Direction du vent, Humidité relative, Température, ...
```
The LLM now sees exact strings for WHERE clauses before writing follow-up SQL.
Coverage: 735 of 1,248 DataStore resources (59%). Resources without hints are those
with no categorical columns or with only single-value columns.

### Benchmarking (Complete — 2026-04-14)

10-query A/B benchmark run by research collaborator using Claude Sonnet 4.6.
L2 (baseline `87fd3a4`): 4/10 COMPLETE. L3 (semantic layer): 6/10 COMPLETE.
Significant improvement on 2/10 (dataset discovery), marginal efficiency on 3/10,
no difference on 5/10. Total tool calls: L2 104, L3 98.
See RESEARCH.md Section 4 for full statistics and per-query analysis.

### Constraints

- schemas.json must remain a static file loadable at startup — no database, no API calls during concept lookup
- File size: current 641 KB, target under 1 MB after enrichment
- Every concept mapping must be traceable to actual portal content — no hallucinated terms
- Existing 12 domain structure stays. Concepts are an additional layer on top, not a replacement
- All existing registry methods (get_domain_resources, get_coverage_summary, get_data_availability) must continue to work unchanged

## Deployment (AWS)

### Transport
- `MCP_TRANSPORT=stdio` (default, local) or `MCP_TRANSPORT=streamable-http` (remote)
- Remote mode: FastMCP spins up Uvicorn/Starlette on `FASTMCP_HOST:FASTMCP_PORT` (default `127.0.0.1:8000`; container overrides to `0.0.0.0`)
- `FASTMCP_STATELESS_HTTP=true` — each POST to `/mcp` is independent (no session state in transport layer)
- `/health` endpoint returns `{"status": "ok", "version": "2.1.0"}` for ALB and Docker health checks

### Static data in containers
- `SchemaRegistry(schemas_path=...)` accepts `SCHEMAS_PATH` env var (default: relative to source tree)
- Container sets `SCHEMAS_PATH=/app/schemas.json` — baked into the Docker image
- `value_hints.json` is copied alongside schemas.json (same directory); loaded automatically by the registry
- The 6-hour live refresh updates the live layer from the CKAN portal; the static layer from schemas.json never mutates at runtime

### AWS architecture
- Hosted at `https://mcp.tanitdata.org/mcp` — unauthenticated public access
- ECS Fargate (ARM64), ALB with ACM TLS, CloudWatch structured logging
- CI/CD: GitHub Actions → ECR → ECS rolling deployment (triggered by `v*` tags)

### Client configuration (remote)

Claude Desktop (via `mcp-remote`):
```json
{
  "mcpServers": {
    "tanitdata": {
      "command": "npx",
      "args": ["mcp-remote", "https://mcp.tanitdata.org/mcp"]
    }
  }
}
```

Clients with native remote MCP support (Gemini Code Assist, etc.):
```json
{
  "mcpServers": {
    "tanitdata": {
      "url": "https://mcp.tanitdata.org/mcp"
    }
  }
}
```

## Key Decisions

- **Python-only** — no TypeScript, no forking existing CKAN MCP servers
- **Async throughout** — httpx async client, async tool handlers, rate-limited (0.3s)
- **Schema registry has two layers** — static (schemas.json, instant) seeded at startup, enriched by live refresh every 6h in a background task
- **Tools return structured markdown** — headers, markdown tables, source footer; compatible with any MCP client
- **Lean tool set** — 8 tools covering structured data + bibliography; RAG deferred (see Phase 2 section)
- **Dual transport** — stdio for local dev (default), streamable-http for remote deployment via `MCP_TRANSPORT` env var
- **Containerized deployment** — multi-stage Dockerfile, GitHub Actions CI/CD
- **No mock tests for tools** — live portal API is public and fast; real integration tests are more valuable than mocked unit tests for this use case
- **Auto-resolve dataset slugs** — `query_datastore_tool` accepts both resource UUIDs and dataset slugs; non-UUID identifiers are resolved via `package_show` to the first DataStore-active resource. Turns the most common LLM error into a valid workflow.
