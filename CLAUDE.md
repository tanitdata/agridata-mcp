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
│  │ climate      │  │ search_documents     │  │
│  │ crops        │  │ search_bibliography  │  │
│  │ dams         │  │                      │  │
│  │ fisheries    │  │                      │  │
│  │ prices       │  │                      │  │
│  │ generic_sql  │  │                      │  │
│  │ search       │  │                      │  │
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
- **MCP SDK:** `mcp` (official Anthropic Python SDK)
- **HTTP client:** `httpx` (async)
- **Data validation:** `pydantic`
- **Vector store (Phase 2):** ChromaDB or Qdrant (local mode)
- **PDF processing (Phase 2):** PyPDF2, langdetect
- **Embeddings (Phase 2):** sentence-transformers (multilingual-e5-large)
- **Transport:** stdio (local dev), SSE (future remote deployment)

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

### Basic API Call
```python
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
result = await self.api_call("datastore_search_sql", {
    "sql": f'SELECT * FROM "{resource_id}" LIMIT 100'
})

# Type casting (CRITICAL: all fields are stored as text)
# Numeric casting:
'SELECT "nom_fr", AVG("valeur"::numeric) as avg_val FROM "{rid}" WHERE "valeur" ~ \'^-?[0-9.]+$\' GROUP BY "nom_fr"'

# Date casting:
'SELECT "Date"::timestamp, "nom_fr", "valeur" FROM "{rid}" WHERE "Date" > \'2026-01-01\' LIMIT 50'

# Numeric WHERE:
'SELECT * FROM "{rid}" WHERE "valeur"::numeric > 30 AND "nom_fr" = \'Air temperature\' LIMIT 20'

# Cross-resource JOIN (confirmed working):
'SELECT a.*, b.* FROM "{rid_a}" a, "{rid_b}" b WHERE a."Delegation" = b."delegation" LIMIT 50'
```

### Confirmed SQL Capabilities
- SELECT, COUNT, WHERE, ORDER BY, GROUP BY, DISTINCT, LIKE, LIMIT/OFFSET, aliases: **all work**
- `::numeric`, `::timestamp` type casting: **works**
- Cross-resource JOINs: **works**
- Regex filtering (`~` operator): **works**
- `information_schema` access: **BLOCKED** (use schema registry instead)
- Result limit: 32,000 rows per query
- Baseline latency: ~0.45s per query (network-dominated)

### Dataset Search (workaround for organization gap)
```python
# organization_list only returns 25 of 55 orgs — use package_search instead
result = await self.api_call("package_search", {
    "q": query,
    "fq": f"organization:{org_name}" if org_name else "",
    "rows": rows,
    "start": offset,
    "facet.field": '["organization", "groups", "res_format"]',
})
```

### Schema Discovery (per resource)
```python
result = await self.api_call("datastore_search", {
    "resource_id": rid,
    "limit": 0  # returns fields metadata without records
})
fields = result["fields"]  # [{"id": "Date", "type": "text"}, ...]
```

## Schema Registry

The file `schemas.json` contains the pre-computed schema registry built from the portal audit. Structure:

- `meta`: totals (789 resources, 583 unique patterns, 513 singletons)
- `clusters[]`: shared schema patterns (70 clusters covering 276 resources)
- `domain_resource_index`: resources grouped by thematic domain with field lists
- `arabic_field_decoding`: mapping table for mojibake field names in Bizerte price datasets

### Key Domain Resource Counts
| Domain | Resources | Records | Key Fields |
|---|---|---|---|
| climate_stations | 21 | 211,311 | Date, nom_ar, nom_fr, unite, valeur |
| rainfall | 25 | 2,970 | delegation/station, mois, precipitations_mm |
| dams | 11 | 271 | Nom du barrage, Capacite, Quantite_stockee |
| crop_production | 56 | 632 | type_de_culture, superficie_hectares, production_tonnes/quintaux |
| olive_harvest | 31 | 1,152 | Delegation, Production_Huile_en_tonne, Nb_oliviers |
| prices | 21 | 1,165 | Variete, prix, prixmin, prixmax (+ mojibake variants) |
| fisheries | 52 | 2,476 | nature_du_produit, quantite_kilos, societe |
| investments | 52 | 4,947 | GOUVERNORAT, INV_DECLARE, NBR_EMPLOI |
| livestock | 23 | 1,049,494 | (warning: 2 resources with Excel overflow at 1,048,575 rows) |
| water_resources | 51 | 2,643 | Nom de la nappe, Delegation, Ressources_million_m3 |
| documentation | 7 | 25,992 | Titre, Auteur, Annee, Nom_fichier, Resume |

### Climate Station Schema (EAV format — largest clean cluster)
21 resources share this exact schema:
- `Date` (text, but castable to timestamp): "2026-03-15 10:00:00"
- `nom_ar` (text): Arabic parameter name
- `nom_fr` (text): French parameter name — one of: "Air temperature", "Relative humidity", "Vitesse du vent", "Direction du vent", "Solar radiation", "Precipitations", "DeltaT", "Leaf wetness", "Soil moisture"
- `unite` (text): unit — "°C", "%", "m/s", "°", "W/m2", "mm", etc.
- `valeur` (text, castable to numeric): the measurement value

This is Entity-Attribute-Value format. To get a time series for one parameter, filter by `nom_fr` and cast `valeur` to numeric.

## Tool Inventory (Phase 1 — DataStore)

### P0: search_datasets
Search portal datasets by keyword, organization, thematic group, format.
- Input: `query: str`, `organization: str?`, `group: str?`, `format: str?`, `limit: int = 10`
- Output: list of datasets with id, title, description, organization, groups, num_resources, url
- API: `package_search`

### P0: get_dataset_details
Get full metadata and resource list for a specific dataset.
- Input: `dataset_id_or_name: str`
- Output: dataset metadata + list of resources with id, name, format, datastore_active, url
- API: `package_show`

### P0: query_datastore
Execute a SQL query against any DataStore resource. Include schema in response.
- Input: `resource_id: str`, `sql: str?`, `filters: dict?`, `limit: int = 100`
- Output: schema (field names + inferred types), records, total count
- API: `datastore_search_sql` (if sql provided) or `datastore_search` (if filters)

### P0: query_climate_stations
Query climate station data by station, parameter, and date range.
- Input: `station_name: str?`, `parameter: str?` (e.g. "Air temperature"), `date_from: str?`, `date_to: str?`, `aggregation: str?` ("hourly"|"daily"|"monthly")
- Output: time series data with numeric values, station metadata
- Implementation: find matching resources from schema registry, build SQL with `valeur::numeric` cast, pivot EAV to columnar if multiple parameters requested

### P0: query_crop_production
Query agricultural production data by crop type, governorate, and campaign year.
- Input: `crop_type: str?` ("cereales"|"olives"|"arboriculture"|"fourrageres"|"maraicheres"), `gouvernorat: str?`, `year_range: str?`
- Output: production data (tonnes/quintaux), cultivated area (hectares), yield
- Implementation: search schema registry by domain, handle 3 different schema families (cereals, olives, fruit trees)

### P0: query_dam_levels
Query dam/reservoir storage levels and compute fill rates.
- Input: `barrage_name: str?`, `gouvernorat: str?`
- Output: dam name, capacity, current storage, fill rate (%), delegation
- Implementation: query + compute `(quantite_stockee / capacite) * 100`

### P0: query_fisheries
Query fisheries production and export data.
- Input: `product_type: str?`, `gouvernorat: str?`, `year: str?`
- Output: species, quantities, exporting companies
- Implementation: search registry for fisheries domain resources

### P0: search_bibliography
Search ONAGRI's bibliographic catalog (22,782 records) by title, author, year, keyword.
- Input: `query: str`, `year_from: int?`, `year_to: int?`, `language: str?` ("FR"|"AR"|"EN"), `limit: int = 20`
- Output: title, author, year, language, abstract, source
- Implementation: SQL query with ILIKE against the ONAGRI base DataStore resource
- Resource dataset: `base-de-documentation-de-l-onagri` (22,782 records)
- Fields: Titre, Auteur_affil, Annee, Langue, Resume, Source, M_titre_orig

### P0: list_organizations
List all data-producing organizations with dataset counts.
- Input: none (or `query: str?` to filter)
- Output: list of orgs with name, title, dataset count
- Implementation: use `package_search` with `facet.field=["organization"]` and `rows=0` to get org counts from facets

### P0: get_dashboard_link
Map a query topic to the relevant interactive dashboard URL.
- Input: `topic: str`
- Output: dashboard title, URL, description
- Implementation: static mapping from keywords to dashboard URLs at dashboards.agridata.tn

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

## Data Quality Issues to Handle

1. **All fields are text** — always cast with `::numeric` or `::timestamp` when doing math/date operations. Guard with regex: `WHERE "valeur" ~ '^-?[0-9.]+$'` before casting.
2. **Arabic mojibake fields** — 39 resources. Use the decoding table in `schemas.json` for the 19 Bizerte price datasets. For others, present the raw field names and let the LLM work with them.
3. **`None` field names** — 39 resources have fields literally named "None". Skip these fields in schema presentation.
4. **Excel overflow** — 2 resources with 1,048,575 rows (Kébili livestock, fruit tree evolution). Flag these as corrupted and exclude from queries.
5. **Year-as-column names** — 29 resources use "2007", "2008"... as column names (wide format). Note this in schema presentation so the LLM can unpivot if needed.
6. **Content-type mismatches** — 86 resources have CSV↔XLSX format mislabeling. Trust `datastore_active` flag, not `format` field.
7. **Organization gap** — `organization_list` returns 25 of 55 orgs. Always use faceted search via `package_search` instead.

## Project Structure
```
tanitdata/
├── CLAUDE.md                   # This file
├── README.md
├── pyproject.toml
├── schemas.json                # Pre-computed schema registry
├── src/
│   └── tanitdata/
│       ├── __init__.py
│       ├── server.py           # MCP server entry point
│       ├── ckan_client.py      # Async CKAN API client
│       ├── schema_registry.py  # Schema lookup and domain routing
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── search.py       # search_datasets, get_dataset_details, list_organizations
│       │   ├── datastore.py    # query_datastore (generic SQL)
│       │   ├── climate.py      # query_climate_stations
│       │   ├── crops.py        # query_crop_production
│       │   ├── dams.py         # query_dam_levels
│       │   ├── fisheries.py    # query_fisheries
│       │   ├── bibliography.py # search_bibliography
│       │   └── dashboards.py   # get_dashboard_link
│       └── utils/
│           ├── __init__.py
│           ├── formatting.py   # Result formatting for MCP responses
│           └── arabic.py       # Arabic field decoding
├── tests/
│   ├── test_ckan_client.py
│   ├── test_schema_registry.py
│   └── test_tools/
└── claude_desktop_config.json  # Example Claude Desktop config
```

## Development Workflow

1. `uv` for dependency management (fast, modern)
2. Test with MCP Inspector during development: `mcp dev src/tanitdata/server.py`
3. Connect to Claude Desktop for integration testing
4. Run against live portal API — no mock needed (all data is public, read-only)

## Phase 2 Scope (Knowledge Layer — not for initial build)

For context only. Do NOT implement these in Phase 1:

- `search_documents` tool: RAG over 994 downloadable PDFs (97 direct + 897 from ONAGRI thematic libraries)
- Vector store integration (ChromaDB/Qdrant)
- PDF download pipeline (batch download via Nom_fichier URL pattern)
- Multilingual chunking and embedding (French + Arabic)
- Document-DataStore cross-referencing by governorate, theme, and year
- Computed climate indices (ET₀, SPI)

## Key Decisions

- **Python-only** — no TypeScript, no forking existing CKAN MCP servers
- **Async throughout** — httpx async client, async tool handlers
- **Schema registry is static** — loaded from `schemas.json` at startup, not queried live
- **Tools return structured text** — JSON blocks within markdown, compatible with any MCP client
- **Phase 1 = DataStore tools only** — knowledge/RAG layer is Phase 2
- **Local development** — stdio transport, Claude Desktop for testing
