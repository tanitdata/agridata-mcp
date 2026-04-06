# CLAUDE.md — Tanitdata MCP Server

## Project Identity

**tanitdata** is an MCP (Model Context Protocol) server that provides AI-mediated access to Tunisia's agricultural open data portal (agridata.tn). It bridges structured numerical data (DataStore tables) with unstructured knowledge (PDF reports and bibliographic records) so that researchers can query both simultaneously through natural language.

## Architecture

```
┌─────────────────────────────────────────────┐
│              MCP Client                      │
│  (Claude Desktop / Cursor / Windsurf)        │
└──────────────┬──────────────────────────────┘
               │ MCP Protocol (stdio)
┌──────────────▼──────────────────────────────┐
│           tanitdata MCP Server               │
│                                              │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │ Data Tools   │  │ Knowledge Tools      │  │
│  │              │  │                      │  │
│  │ climate  ✅  │  │ search_documents     │  │
│  │ generic_sql✅│  │ search_bibliography  │  │
│  │ read_file ✅ │  │                      │  │
│  │ search   ✅  │  │                      │  │
│  └──────┬──────┘  └──────────┬───────────┘  │
│         │                    │               │
│  ┌──────▼──────┐  ┌─────────▼────────────┐  │
│  │ CKAN Client  │  │ Vector Store         │  │
│  │ + Schema     │  │ (Phase 2)            │  │
│  │   Registry   │  │                      │  │
│  └──────┬──────┘  └──────────────────────┘  │
└─────────┼───────────────────────────────────┘
          │ HTTPS
┌─────────▼───────────────────────────────────┐
│  catalog.agridata.tn (CKAN 2.9+)            │
│  789 DataStore resources · 1,102 datasets    │
└─────────────────────────────────────────────┘
```

## Tech Stack

- **Language:** Python 3.11+
- **MCP SDK:** `mcp[cli]` (official Anthropic Python SDK)
- **HTTP client:** `httpx` (async, lazy-initialized, rate-limited)
- **XLSX parser:** `openpyxl` (read-only mode for non-DataStore files)
- **Data validation:** `pydantic` (installed, not yet heavily used)
- **Env config:** `python-dotenv` (supports `CKAN_BASE_URL` override)
- **Transport:** stdio (local dev), SSE (future remote deployment)
- **Test framework:** `pytest` + `pytest-asyncio` (asyncio_mode = "strict")

Phase 2 only (not installed): ChromaDB/Qdrant, PyPDF2, langdetect, sentence-transformers.

## Dependencies (actual — from pyproject.toml)

```toml
dependencies = ["mcp[cli]", "httpx", "pydantic", "python-dotenv", "openpyxl"]

[dependency-groups]
dev = ["pytest", "pytest-asyncio"]
```

## Portal Facts

- **Portal URL:** https://catalog.agridata.tn
- **API base:** https://catalog.agridata.tn/api/3/action
- **Datasets:** 1,102
- **Resources:** 1,614 (789 DataStore-active, 48.9%)
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
~10s later: live refresh completes (789+ resources, 1102 dataset→org mappings)
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
- `GOVERNORATE_MAP`: 27 entries, lower-case key → canonical French name
- `CRDA_SLUG_MAP`: 24 CRDA org slugs → governorate
- `NATIONAL_ORGS`: ~20 national-level org slugs (DGGREE, DGACTA, ONAGRI, etc.)
- `LOCALITY_MAP`: ~50 delegation/station-site names → parent governorate

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

## Tool Inventory (Phase 1 — DataStore)

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
- Input: `resource_id: str`, `sql: str?`, `limit: int = 100`
- Output: Arabic field decoding (if applicable), schema, records table, registry schema note, data availability context, source attribution footer
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

### P0: search_bibliography — not yet built
Search ONAGRI's bibliographic catalog (22,782 records) by title, author, year, keyword.
- Input: `query: str`, `year_from: int?`, `year_to: int?`, `language: str?` ("FR"|"AR"|"EN"), `limit: int = 20`
- Output: title, author, year, language, abstract, source
- Implementation: SQL ILIKE against the ONAGRI base DataStore resource
- Resource dataset: `base-de-documentation-de-l-onagri` (22,782 records)
- Fields: Titre, Auteur_affil, Annee, Langue, Resume, Source, M_titre_orig
- **Phase 1 scope:** search `Titre` AND `Resume` (abstract) via ILIKE — both are text fields and work today without vectors. `Resume` is the primary content field; many records have rich French/Arabic abstracts that contain keywords not present in the title.
- **Phase 2 bridge:** `Resume` is the target for semantic embedding. Phase 1 SQL ILIKE over `Resume` is the baseline; Phase 2 replaces it with vector similarity search over the same field. Design the tool so the retrieval backend is swappable without changing the tool interface.

### P0: get_dashboard_link — not yet built
Map a query topic to the relevant interactive dashboard URL.
- Input: `topic: str`
- Output: dashboard title, URL, description
- Implementation: static dict lookup from CLAUDE.md Dashboard URL Map

### Dashboard URL Map
```python
DASHBOARDS = {
    "climate": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-changement-climatique/",
    "cereals_annual": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-annuels-des-cereales/",
    "cereals_monthly": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-mensuels-des-cereales/",
    "cereal_prices": "https://dashboards.agridata.tn/fr/detail_dashboard/evolution-des-prix-fob-du-ble-dur-selon-lorigine/",
    "olive_oil": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-de-lhuile-dolive/",
    "fisheries": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-de-la-peche-et-de-laquaculture/",
    "dates": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-de-la-production-des-dattes/",
    "vegetables": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-annuels-des-cultures-maraicheres/",
    "dams": "https://dashboards.agridata.tn/fr/detail_dashboard/situation-hydraulique-de-la-societe-dexploitation-du-canal-et-des-adductions-des-eaux-du-nord/",
    "investments": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-indicateurs-de-linvestissement-dans-le-secteur-de-lagriculture-et-de-la-peche/",
    "citrus": "https://dashboards.agridata.tn/fr/detail_dashboard/indicateurs-de-performance-de-la-filiere-agrumicole/",
    "citrus_exports": "https://dashboards.agridata.tn/fr/detail_dashboard/exportations-des-agrumes-tunisiens-indicateurs-cles-en-quantite-et-en-valeur/",
    "forest_fires": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-incendies-de-foret/",
    "rainfall": "https://dashboards.agridata.tn/fr/detail_dashboard/tableau-de-bord-des-quantites-journalieres-de-pluie-enregistrees/",
    "citrus_production": "https://dashboards.agridata.tn/fr/detail_dashboard/analyse-spatiale-et-temporelle-de-la-production-dagrumes-en-tonnes/",
}
```

## Data Quality Issues

1. **All fields are text** — always cast with `::numeric` or `::timestamp` for math/date. Guard numeric casts: `WHERE "valeur" ~ '^-?[0-9.]+$'`.
2. **Arabic mojibake fields** — 39 resources. The 19 Bizerte price datasets are decoded via `schemas.json` `arabic_field_decoding.field_mapping`. `query_datastore` applies this automatically. For others, present raw field names.
3. **`None` field names** — 39 resources have fields literally named `"None"`. Skip in schema presentation.
4. **Excel overflow** — 2 resources with 1,048,575 rows (Kébili livestock, fruit tree evolution). Flag as corrupted and exclude from queries.
5. **Year-as-column names** — 29 resources use "2007", "2008"... as column names (wide format). Note this in schema presentation so the LLM can unpivot if needed.
6. **Content-type mismatches** — 86 resources have CSV↔XLSX format mislabeling. Trust `datastore_active` flag, not `format` field.
7. **Organization gap** — `organization_list` returns 25 of 55 orgs. Always use faceted `package_search` instead.
8. **`_id` and `_full_text` columns** — every DataStore resource has these internal columns. Always skip them in schema/result presentation. `_SKIP_COLS = {"_id", "_full_text"}` in formatting and climate tools.
9. **EAV format** — climate_stations, and possibly other sensor domains, use Entity-Attribute-Value layout. Filter by `nom_fr`/`sensor_name`/`parameter` and cast `valeur`/`value` to numeric. Never treat these as wide-format tables.
10. **Windows console encoding** — station names with non-ASCII characters (e.g. "Données climatiques") may appear as `Donn?es` in Windows terminals. This is a terminal display issue, not a data bug; the stored data is correctly UTF-8.
11. **Source attribution** — every tool response should end with `format_source_footer(sources)` from `utils/formatting.py`. Format: single source → labelled fields; multiple sources → compact list with resource_id + portal URL.

## Project Structure

```
tanitdata/
├── CLAUDE.md                   # This file
├── README.md
├── pyproject.toml
├── schemas.json                # Pre-computed schema registry (static layer)
├── audit_full.json             # Raw portal audit data (source for schemas.json)
├── claude_desktop_config.json  # Example Claude Desktop config
├── test_climate.py             # Live integration benchmark (3 scenarios)
├── test_stress.py              # Live stress test (9 scenarios, performance summary)
├── src/
│   └── tanitdata/
│       ├── __init__.py         # __version__ = "0.1.0"
│       ├── server.py           # MCP server: FastMCP, lifespan, tool registrations
│       ├── ckan_client.py      # Async CKAN API client (rate-limited, lazy-init)
│       ├── schema_registry.py  # Two-layer registry (static + live, 6h refresh)
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── search.py       # ✅ search_datasets, get_dataset_details, list_organizations
│       │   ├── datastore.py    # ✅ query_datastore (SQL + Arabic decode + availability context)
│       │   ├── climate.py          # ✅ query_climate_stations (3 EAV variants, caching, comparison)
│       │   ├── resource_reader.py # ✅ read_resource (CSV/XLSX download+parse, on-demand cache)
│       │   ├── bibliography.py    # planned — does not exist yet
│       │   └── dashboards.py      # planned — does not exist yet
│       └── utils/
│           ├── __init__.py
│           ├── formatting.py   # format_dataset_list, format_datastore_result, format_source_footer
│           └── arabic.py       # decode_arabic_fields, annotate_fields_with_arabic
├── tests/
│   ├── __init__.py
│   ├── test_ckan_client.py     # Unit tests: base URL, API base (no network)
│   ├── test_schema_registry.py # Unit tests: static load, domain lookup, cluster lookup
│   └── test_tools/
│       └── __init__.py         # placeholder — no tool tests yet
```

## Development Workflow

1. `pip install -e .` to install in editable mode (or use `uv`)
2. Test with live portal: `python test_climate.py`, `python test_stress.py`, or `python test_foundation.py`
3. Run unit tests: `pytest tests/`
4. MCP Inspector for tool testing: `mcp dev src/tanitdata/server.py`
5. Connect to Claude Desktop for integration testing
6. Run against live portal API — no mock needed (all data is public, read-only)

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
- **CKAN DataStore SQL restriction**: `CASE WHEN` is not in the DataStore SQL whitelist (returns 409 CONFLICT). Some resources also return 403 FORBIDDEN for SQL queries.

## Tool Strategy (v1.3+)

**Domain-specific tools** are only built where there's a bounded, single-schema pattern that benefits from specialized logic:
- `query_climate_stations` — 3 EAV schema variants, parameter aliasing, aggregation (SUM/AVG), sensor caching
- `search_bibliography` (planned) — single known resource, ILIKE over Titre+Resume, Phase 2 vector bridge

**Everything else** goes through the generic workflow:
1. `search_datasets_tool` — find datasets by keyword/org/group
2. `get_dataset_details_tool` — see resources, check DataStore active status
3. If DataStore active → `query_datastore_tool` (SQL access)
4. If not DataStore active → `read_resource_tool` (download + parse CSV/XLSX)

This keeps the server lean (6 tools) and lets the LLM handle schema variability naturally.

## Build Sequence for Remaining Phase 1 Tools

1. **`search_bibliography`** — single known resource, ILIKE over Titre+Resume, no domain routing. Validates the bibliography search pattern before Phase 2 vector enhancement.

2. **`get_dashboard_link`** — trivial static dict lookup, ~10 minutes.

## Phase 2 Scope (Knowledge Layer — not for Phase 1)

- `search_documents` tool: RAG over 994 downloadable PDFs (97 direct + 897 from ONAGRI thematic libraries)
- Vector store integration (ChromaDB/Qdrant, local mode)
- PDF download pipeline (batch via Nom_fichier URL pattern)
- Multilingual chunking and embedding (French + Arabic, sentence-transformers multilingual-e5-large)
- Document-DataStore cross-referencing by governorate, theme, and year
- Computed climate indices (ET₀ via Penman-Monteith, SPI for drought assessment)

## Key Decisions

- **Python-only** — no TypeScript, no forking existing CKAN MCP servers
- **Async throughout** — httpx async client, async tool handlers, rate-limited (0.3s)
- **Schema registry has two layers** — static (schemas.json, instant) seeded at startup, enriched by live refresh every 6h in a background task
- **Tools return structured markdown** — headers, markdown tables, source footer; compatible with any MCP client
- **Phase 1 = DataStore tools only** — knowledge/RAG layer is Phase 2
- **Local development** — stdio transport, Claude Desktop for testing
- **No mock tests for tools** — live portal API is public and fast; real integration tests are more valuable than mocked unit tests for this use case
