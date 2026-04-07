# Tool Audit Report — 2026-04-06

## Overview

Three test queries were run against the tanitdata MCP server after deploying **Component 1: gap detection in `query_datastore`** and the **two-section layout in `explore_domain`**. This report documents tool behavior, data findings, and issues discovered.

---

## Query 1: Compare cereal production trends across Béja, Jendouba, and Kef over the last 5 years

### Tool chain used
1. `explore_domain_tool(domain="crop_production", gouvernorat=X, keyword="céréales")` × 3
2. `query_datastore_tool` with SQL aggregation × 3

### explore_domain behavior

| Governorate | Gov-specific resources | National resources shown | Bridge note |
|---|---|---|---|
| Béja | 4 (cereals area, veg production, land use, silos) | 5 (spanning 2007–2019) | ">>> 5 national datasets below (132 records spanning 2007-2019) contain Béja data..." |
| Jendouba | 3 (cereal storage, veg production, land use) | 5 (same national set) | Same bridge note pattern |
| Le Kef | 2 (cereal storage, land exploitation) | 5 (same national set) | Same bridge note pattern |

**Verdict: Two-section layout works correctly.** National resources are clearly separated, bridge note is prominent with year range and record count. Section 2 header includes blockquote guidance.

### query_datastore gap detection

| Resource queried | Records | Years covered | Gap detection fired? | Alternatives suggested |
|---|---|---|---|---|
| Béja: Production végétale (412b16a0) | 7 aggregated | 2017/2018 – 2023/2024 | **YES** | Blé tendre (700 rec), Triticale (307 rec), Irrigated surfaces (24 rec) |
| Jendouba: Production Végétale (1c58946a) | 3 aggregated | 2021/2022 – 2023/2024 | **YES** | Blé tendre (700 rec), Triticale (307 rec), Irrigated surfaces (24 rec) |
| Le Kef: Production Végétale (e459eaa7) | 2 aggregated | 2023 – 2024 | **YES** | Blé tendre (700 rec), Blé dur (24 rec), Triticale (307 rec) |

**Verdict: Gap detection works correctly.** All three gov-specific resources (7, 3, and 2 aggregated records respectively, all < 200 threshold) triggered the "Data gap" section with ready-to-use SQL including the governorate ILIKE filter.

### Actual cereal data retrieved

| Governorate | Campaign | Total cereals (tonnes) |
|---|---|---|
| **Béja** | 2017/2018 | 690,000 |
| | 2018/2019 | 820,000 |
| | 2019/2020 | 580,000 |
| | 2020/2021 | 740,000 |
| | 2021/2022 | 800,000 |
| | 2022/2023 | 220,000 |
| | 2023/2024 | 5,200,000 |
| **Jendouba** | 2021/2022 | 3,800,000 |
| | 2022/2023 | 1,240,000 |
| | 2023/2024 | 288,105 |
| **Le Kef** | 2023 | 147,552 |
| | 2024 | 1,182,864 |

Béja has 7 years of coverage; Jendouba has 3; Le Kef has only 2. The gap detection correctly identified this sparsity.

### Issue discovered: national alternatives return 409 CONFLICT

Both suggested national resources (`ac604ad4` blé tendre, `7f4505dd` triticale) **return 409 CONFLICT** when queried via SQL. This is a known DataStore limitation (some resources block SQL queries). The gap detection correctly identifies the right resources but the agent **cannot actually execute the suggested SQL**.

**Impact:** High. The gap detection suggests resources the agent can't use. An agent following the suggestion would hit a 409 error and need to fall back to browse mode (datastore_search without SQL), which doesn't support WHERE filtering.

**Recommendation:** Add a pre-check in `_find_national_alternatives` that tests SQL support for candidate resources (e.g., `SELECT 1 FROM "rid" LIMIT 1`), or maintain a blocklist of 409-returning resources in the schema registry.

---

## Query 2: Which governorate had the highest olive production in the 2022/2023 campaign?

### Tool chain used
1. `explore_domain_tool(domain="olive_harvest", keyword="production")` — browsed all olive resources
2. `query_datastore_tool` on national olive production resource (efe84210) with date filter
3. `search_datasets_tool` for recent olive production datasets

### Findings

**The query cannot be answered with available data.** The national olive production resource (`efe84210`) covers 1994–2016 only. Querying for 2022/2023 returned 0 records.

Available olive_harvest resources by governorate:

| Source | Governorates | Years | Data type |
|---|---|---|---|
| National (DGEDA) | All 22 | 1994–2016 | Production by gov (tonnes) |
| CRDA Médenine | Médenine only | 2016–2022 | Olive harvest by delegation |
| CRDA Sfax | Sfax only | 2017–2018 | Bio olive oil, forest composition |
| CRDA Tunis | Tunis only | Multiple campaigns | Production per delegation |
| CRDA Sousse | Sousse only | Unknown | Production evolution |

**No cross-governorate olive production dataset exists for 2022/2023.** The crop_production domain's governorate-specific resources (Béja, Jendouba, Kef etc.) include an `Oliviers` column, but each only covers its own governorate — there is no unified view.

**Verdict:** This is a **portal data gap**, not a tool issue. The tools correctly reported what's available and what isn't.

### explore_domain behavior
- National-first listing with 5 national + 15 gov-specific resources worked correctly
- Coverage summary showed 18/24 governorates have olive_harvest data
- Source attribution footer present on all outputs

---

## Query 3: Show me the evolution of fruit tree cultivation area in Nabeul

### Tool chain used
1. `explore_domain_tool(domain="crop_production", gouvernorat="Nabeul", keyword="arboriculture")`
2. `query_datastore_tool` with SQL on Nabeul superficie resource (9915dce4)

### explore_domain behavior

Found 2 gov-specific resources (2022–2024) + 3 national resources (2017–2019). Bridge note present.

### query_datastore results

Full delegation-level data retrieved successfully (48 records):

| Year | Total arboriculture (ha) | Top 3 delegations |
|---|---|---|
| 2022 | 62,627 | Bouargoub (8,132), Hammamet (7,513), Grombalia (7,486) |
| 2023 | 63,028 | Bouargoub (8,254), Hammamet (7,660), Grombalia (7,478) |
| 2024 | 62,450 | Bouargoub (8,354), Hammamet (7,827), Grombalia (7,501) |

**Gap detection fired correctly** (48 records < 200 threshold), suggesting national resources:
- Pommier production (240 records) — apple trees
- Grenadiers production (240 records) — pomegranate trees
- Poirier production (240 records) — pear trees

All with ready-to-use SQL: `WHERE "Gouvernorat" ILIKE '%Nabeul%'`

**Verdict:** Full success. Data retrieved, gap detection suggests relevant national fruit tree resources for longer historical coverage.

---

## Summary: Component 1 (Gap Detection) Audit

### What works

| Feature | Status | Notes |
|---|---|---|
| Two-section layout in explore_domain | **PASS** | National resources clearly separated with bridge note |
| Year range in section headers | **PASS** | "(4 governorate-specific, 2022-2024)" |
| Bridge note between sections | **PASS** | Record count + year range + call to action |
| Gap detection threshold (< 200 records) | **PASS** | Fires correctly for all sparse resources |
| Topic-aware matching (SQL column extraction) | **PASS** | "Cereales" → matches blé, triticale national resources |
| Crop expansion (_CROP_EXPANSIONS) | **PASS** | "cereale" expands to {ble, orge, triticale, avoine, sorgho} |
| Ready-to-use SQL in suggestions | **PASS** | Includes resource ID, gov column, ILIKE filter |
| No false positives for national resources | **PASS** | National resources (> 200 records) don't trigger gap detection |
| Source attribution footers | **PASS** | Present on all tool outputs |

### Issues found

| Issue | Severity | Description |
|---|---|---|
| **409 CONFLICT on suggested resources** | **HIGH** | National cereal resources (blé tendre `ac604ad4`, triticale `7f4505dd`) return 409 when queried via SQL. Gap detection correctly identifies them but the agent can't execute the suggested queries. |
| Olive data gap (2017–present) | Medium | No cross-governorate olive production data after 2016. Portal-side issue, not a tool bug. |
| National cereal "collecte" resource has no gov column | Low | `01439c45` (Collecte des céréales) has `annee`, `type-produit`, `quantite` — no Gouvernorat column. Correctly excluded from gap detection suggestions. |

### Recommendations

1. **409 pre-check** (HIGH priority): Before suggesting a national alternative, verify it supports SQL queries. Either:
   - Test each candidate with a lightweight `SELECT 1 FROM "rid" LIMIT 1` query
   - Or maintain a registry-level `sql_blocked` flag populated during live refresh

2. **Fallback to browse mode** (MEDIUM): When a suggested resource returns 409, the gap detection should note: "This resource does not support SQL queries. Use browse mode (no sql parameter) to see sample data."

3. **Cross-domain olive production** (LOW): Consider adding a "search across crop_production governorate resources for olive columns" capability for queries that span the olive_harvest domain's temporal gap.
