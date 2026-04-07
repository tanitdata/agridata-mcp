# MEMORY.md — Development Log

## Current State
- **Server version:** v1.5 (bibliography search tool)
- **Tools registered (8):**
  1. `search_datasets_tool` — keyword/org/group search across 1,102 datasets
  2. `get_dataset_details_tool` — dataset metadata + resource list
  3. `query_datastore_tool` — SQL against any DataStore resource (789 resources); accepts `resource_id` (UUID) or `dataset_id` (slug) with auto-resolution
  4. `read_resource_tool` — download + parse non-DataStore CSV/XLSX (~800 resources)
  5. `list_organizations_tool` — 55 orgs with dataset counts
  6. `query_climate_stations_tool` — 24 stations, 3 EAV variants, caching, comparison
  7. `get_dashboard_link_tool` — 18 interactive dashboards, bilingual keyword matching
  8. `search_bibliography_tool` — 25,944 records across 6 ONAGRI resources, tiered execution, keyword scoring
- **Git tags:** `v1.0-foundation`, `v1.1-climate`, `v1.2-crops`, `v1.3-foundation-pivot`, `v1.4-dashboards-autoresolve`, `v1.5-bibliography`

## Recent Changes (v1.4 → v1.5)
- **search_bibliography tool** — searches 6 ONAGRI bibliographic resources (25,944 records total). Tiered execution: Tier 1 (Base 22,782 + Fonds 2,265) via SQL ILIKE on Titre+Resume; Tier 2 (4 thematic libraries, 897 records) via datastore_search + Python filtering (SQL blocked on these resources). Keyword scoring (Titre=2pts, Resume=1pt). PDF URL construction via onagri.nat.tn. Year filter with regex guard. Language ILIKE filter. Process-scoped Tier 2 cache. 10/10 test scenarios pass.

## Previous Changes (v1.3 → v1.4)
- **get_dashboard_link tool** — 18 dashboards indexed, accent-normalized bilingual keyword matching, top-score-only filtering. 19/19 tests pass.
- **Dataset slug auto-resolution** — `query_datastore_tool` now accepts `dataset_id` (slug) alongside `resource_id` (UUID). Non-UUID identifiers are resolved via `package_show` to the first DataStore-active resource. SQL references to the slug are rewritten with the resolved UUID. Eliminates the most common LLM error.
- **Label disambiguation** — `formatting.py` changed `**ID:**` to `**Dataset slug:**`; `search.py` changed `ID:` to `Resource ID:` for resource entries. Reduces LLM confusion between dataset slugs and resource UUIDs.

## Key Decisions
- Removed `explore_domain` and `get_resource_preview` (v1.3) — explore_domain produced 5,000+ token responses that slowed inference worse than 3 sequential lightweight calls. resource_preview was redundant with `query_datastore(limit=3)`.
- Removed `query_crop_production` (v1.2 → v1.3 pivot) — 176 unique schemas across 239 resources, brittle unit/metric rules, P0 bugs after 713 lines of code. Generic tools handle it better.
- Removed `query_dam_levels` from build plan — dam data removed from portal as of 2026-04.
- Foundation pivot: LLM handles domain routing via generic tools. Domain-specific tools only for bounded single-schema patterns (climate stations: 3 EAV variants; bibliography: single resource).
- Tool responses must stay lightweight and chainable — never build "dump everything" tools.
- Auto-resolve dataset slugs rather than throwing validation errors — turns LLM mistakes into valid workflows.

## What Works
- **search_datasets:** keyword, org, group, format filtering with faceted search. Workaround for organization_list 25/55 gap.
- **query_datastore:** SQL with Arabic field decoding (19 Bizerte price datasets), data availability context, source attribution footer. Handles `::numeric`/`::timestamp` casts, `ILIKE`, `DISTINCT ON`, cross-resource JOINs. Auto-resolves dataset slugs to resource UUIDs.
- **query_climate_stations:** 3 EAV schema variants auto-detected. Modes: inventory, station details, data query, multi-station comparison (`vs`), latest readings. Precipitation uses SUM, all others AVG. Sensor list caching (first call ~7s, cached ~0s). 5-tier governorate extraction covers 10 sensor governorates + 1 metadata-only (Kebili).
- **read_resource:** CSV (multi-encoding) and XLSX (openpyxl read-only) parsing. Process-scoped cache. Redirects DataStore-active resources to query_datastore. 5MB size limit. ~0.8s first call, ~0s cached.
- **get_dashboard_link:** 18 dashboards indexed. Accent-normalized bilingual keyword matching (FR+EN). Single match -> direct link; multi-match -> ranked list (top-score only); no match -> full list. Instant (pure local, no API calls). 19/19 test scenarios pass.
- **search_bibliography:** 6 ONAGRI resources (25,944 records). Tiered execution: Tier 1 SQL ILIKE (Titre+Resume), Tier 2 datastore_search+Python filter (SQL blocked). Keyword scoring, year regex guard, language ILIKE, PDF URL construction (onagri.nat.tn). Process-scoped Tier 2 cache. 10/10 test scenarios pass.
- **Schema registry:** Static layer from schemas.json (12 domains, 70 clusters, 789 resources). Live layer refreshes every 6h in background. Governorate extraction (CRDA slug map, national orgs, locality map). Coverage summaries, data availability, source attribution all work.

## What Was Built and Removed (with reasons)
| Tool/Feature | Commits | Lines | Reason Removed |
|---|---|---|---|
| `query_crop_production` | `d25dbdc` | 713 | 176 unique schemas, unit normalization too brittle, P0 bugs on basic queries |
| `explore_domain` | `ea2a92e`, `cbd86dd` | 388 | 5,000-token responses drowned the LLM; not fixable by trimming (239 crop resources inherently heavy) |
| `get_resource_preview` | `ea2a92e` | 172 | Near-total overlap with `query_datastore(limit=3)`; type inference was nice but the LLM infers same from sample values |

## Known Issues

### Portal-side
- **409 CONFLICT on some resources:** National cereal resources (ble tendre `ac604ad4`, triticale `7f4505dd`) return 409 when queried via SQL. `CASE WHEN` also blocked (not in DataStore SQL whitelist).
- **403 FORBIDDEN on some resources:** Some resources reject SQL queries entirely.
- **Olive data gap:** No cross-governorate olive production data after 2016 campaign (national resource `efe84210` covers 1994-2016 only).
- **Jendouba GDA BOUHERTMA:** No temperature, humidity, or DeltaT sensors — only battery, precipitation, solar panel, solar radiation, wind speed/direction.
- **Sousse:** No climate station DataStore resources at all.
- **DGACTA stations:** Very sparse data (27-162 records vs 21,000-38,000 for DGGREE).
- **Bizerte multi-sensor** (`5ae01acd`) and **Jendouba multi-sensor** (`91f1c4b8`): Data stops at 2024-12.
- **Dam data:** Removed from portal as of 2026-04. Dashboard still exists but underlying data gone.

### Code-side
- `tests/test_tools/` has only `__init__.py`. No automated tests for tool outputs — development relies on live benchmark scripts.
- `query_datastore` MCP tool does not expose `filters` dict parameter (intentionally omitted).
- `VIRTUAL_ENV=c:\Users\HP` set in dev environment (not persistent, but causes `uv` warnings).
- Windows `python3` command requires symlink to work (`mklink C:\Python314\python3.exe C:\Python314\python.exe`).
- Climate domain count mismatch: static schemas.json lists 21; live inventory shows 24.
- `test_foundation.py` at project root tests removed tools (explore_domain, resource_preview) and is no longer runnable.
- **ONAGRI thematic libraries block SQL** — 4 Tier 2 bibliography resources (Agriculture, Water, Forestry, Fisheries) return 409 CONFLICT on all SQL queries. Worked around via `datastore_search` + Python filtering.
- **Bibliography Annee field** — contains non-numeric values (`'ND'`, `'SD'`, `'N.D.'`, `None`). Year filter uses string comparison with `^\d{4}$` regex guard.
- **Bibliography Langue field** — inconsistent format: `(FR)`, `(Fr)`, `FR`, compound `(EN, FR, ES, AR)`. Filter uses ILIKE `'%FR%'` instead of exact match.

## Completed
- [x] Foundation tools: search_datasets, get_dataset_details, query_datastore, list_organizations (`d600148`)
- [x] Schema registry with two-layer design, 12 domains, 70 clusters, coverage diagnostics (`d600148`)
- [x] Climate stations tool: 3 EAV variants, parameter aliasing, SUM/AVG, sensor caching (`a38a3a0`)
- [x] Governorate extraction fix: 1 -> 10 sensor governorates + 1 metadata-only (`a38a3a0`)
- [x] CLAUDE.md synced with actual schemas.json: corrected domain counts, 176 crop schemas documented (`a4c448a`)
- [x] Crop production tool built and tested (`d25dbdc`) — later removed
- [x] explore_domain + resource_preview built (`ea2a92e`, `cbd86dd`) — later removed
- [x] Foundation pivot: slim to generic workflow + read_resource (`d216a6e`)
- [x] read_resource tool: CSV/XLSX download+parse, cache, 5MB limit (`d216a6e`)
- [x] get_dashboard_link tool: 18 dashboards, bilingual keyword matching, 19/19 tests pass
- [x] Dataset slug auto-resolution in query_datastore_tool (UUID regex check, package_show fallback, SQL rewrite)
- [x] Label disambiguation: "Dataset slug" vs "Resource ID" in search output
- [x] search_bibliography tool: 6 ONAGRI resources, tiered execution, keyword scoring, PDF URLs, 10/10 tests pass

## Next Steps
- [ ] Rerun full benchmark with final Phase 1 toolset (8 tools)
- [ ] Phase 2: RAG pipeline over 994 documents

## Failed Approaches (don't retry)
1. **Domain-specific query tools for multi-schema domains** (crop_production, fisheries, livestock) — 176 schemas in crop alone, brittle unit/metric detection rules, P0 bugs after 713 lines. The LLM handles schema variability better than deterministic code.
2. **explore_domain with full field lists** — 5,000-token responses drowned the LLM context. Trimming didn't help because the underlying data (239 resources across 23 governorates) is inherently heavy.
3. **get_resource_preview as a separate tool** — added a tool call and ~0.3s latency without enough value over `query_datastore(resource_id, limit=3)`.
4. **Topic-filtered national resources** — complex token extraction, crop expansion dicts, noise filtering — all built into explore_domain which was itself removed.
5. **Strict resource_id validation** — throwing errors when LLM passes dataset slugs instead of UUIDs. Auto-resolution is strictly better.

## Benchmark Scenarios (from test_stress.py, 9 queries)
| # | Query | Tests |
|---|---|---|
| Q1 | Station inventory (full, no args) | Map data, first-call latency (~7s) |
| Q2 | 3-station comparison (no param) | Multi-station details without parameter filter |
| Q3a | Bizerte wind direction (6mo monthly) | Time series with monthly aggregation |
| Q3b | Mahdia wind direction | P0 fix: matches "U-sonic wind dir" ILIKE pattern |
| Q4 | Solar radiation 3 stations (12mo) | Multi-station comparison with date range |
| Q5 | Precipitation all stations | P0 fix: SUM aggregation for rain (not AVG) |
| Q6 | Latest readings — Bizerte | DISTINCT ON query, latest mode |
| Q7 | Latest temperature — Mahdia | Latest + parameter filter combined |
| Q8 | Station inventory (repeat) | Cache hit: should be ~0s after Q1 |
| Q9 | Nonexistent station (Sousse) | Graceful error handling |

## Bibliography Test Results (10/10 pass)
| # | Scenario | Results | Time |
|---|---|---|---|
| Q1 | Broad keyword: cereales | 10 found (5 shown) | 0.63s |
| Q2 | Multi-keyword: olive Sfax | 6 found | 0.63s |
| Q3 | Year filter: irrigation 2015-2020 | 9 found (Tier 1+2) | 1.84s |
| Q4 | Language filter: cereales AR | 13 found (Tier 1+2) | 0.63s |
| Q5 | Theme filter: thon fisheries | 0 (40 records, no match) | 0.00s |
| Q6 | Tiered: foret incendie | 9 found | 0.62s |
| Q7 | No results: xyznonexistent | 0 | 0.62s |
| Q8 | Year range only: 2023-2024 | 52 found | 0.60s |
| Q9 | Theme agriculture: climat | 27 found | 0.01s |
| Q10 | Cache hit: eau agriculture | 34 found | 0.01s |

## Dashboard Test Results (19/19 pass)
All keyword matching scenarios validated after two fix rounds:
- Single-topic matches (cereales, olive, climat, etc.)
- Multi-match filtering (dattes -> 3 dashboards, agrumes -> 3 dashboards)
- Broad topics with top-score-only filtering ("cereal production and prices" -> 3 cereal dashboards, no citrus noise)
- No-match fallback returns full 18-dashboard list
- Accent normalization (cereales = cereales)

## Audit Results (from AUDIT_REPORT.md, 2026-04-06)
Tested explore_domain + query_datastore workflow on 3 queries (before v1.3 pivot):

| Query | Result | Issues |
|---|---|---|
| Compare cereal production: Beja, Jendouba, Le Kef | Data retrieved, sparse but valid | 409 CONFLICT on suggested national resources |
| Highest olive production 2022/2023 | Cannot answer | Portal data gap: olive data stops at 2016 |
| Fruit tree area evolution in Nabeul | 48 records, 2022-2024 | None — full success |

## Commit History
| Hash | Tag | Message |
|---|---|---|
| `d600148` | `v1.0-foundation` | Phase 1 foundation: MCP server with search, datastore, schema registry |
| `a38a3a0` | `v1.1-climate` | V1.1: climate tool + CLAUDE.md updated |
| `d25dbdc` | `v1.2-crops` | V1.2: query_crop_production tool (later removed) |
| `ea2a92e` | — | Replace crop_production with explore_domain + resource_preview |
| `cbd86dd` | — | explore_domain: topic-filtered national resources |
| `d216a6e` | `v1.3-foundation-pivot` | V1.3: replace explore_domain + resource_preview with read_resource |
| `213f68d` | `v1.4-dashboards-autoresolve` | V1.4: dashboard tool + dataset slug auto-resolution |
