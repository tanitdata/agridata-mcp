"""Climate stations tool: query_climate_stations.

Queries sensor data from Tunisia's weather monitoring network.
Two organizations operate stations:
  - DGGREE (FieldClimate, real-time hourly data)
  - DGACTA (environmental sensors, periodic updates)

Three EAV schema variants exist:
  - standard: Date, nom_ar, nom_fr, unite, valeur  (most DGGREE stations)
  - english:  date, sensor_name, aggregation_type, unit, value  (older DGGREE)
  - dgacta:   date, parameter, value, unite  (DGACTA stations)
"""

from __future__ import annotations

import re
from typing import Any

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import GOVERNORATE_MAP, SchemaRegistry
from tanitdata.utils.formatting import format_source_footer

# ---------------------------------------------------------------------------
# Schema variant detection
# ---------------------------------------------------------------------------

_VARIANTS: dict[str, dict[str, str]] = {
    "standard": {"date": "Date", "param": "nom_fr", "value": "valeur", "unit": "unite"},
    "english": {"date": "date", "param": "sensor_name", "value": "value", "unit": "unit"},
    "dgacta": {"date": "date", "param": "parameter", "value": "value", "unit": "unite"},
}

_METADATA_FIELDS = {"nom", "Longitude", "Latitude"}


def _detect_variant(fields: list[str]) -> str | None:
    """Detect the EAV schema variant from a resource's field list.

    Returns None for metadata-only resources (not sensor data).
    """
    fset = set(fields)
    if _METADATA_FIELDS.issubset(fset):
        return None
    if "nom_fr" in fset and "valeur" in fset:
        return "standard"
    if "sensor_name" in fset and "value" in fset:
        return "english"
    if "parameter" in fset and "value" in fset:
        return "dgacta"
    return None


# ---------------------------------------------------------------------------
# Parameter normalization
# ---------------------------------------------------------------------------

# Natural language → canonical group
_PARAM_ALIASES: dict[str, str] = {
    # Temperature
    "température": "temperature",
    "temperature": "temperature",
    "temp": "temperature",
    "air temperature": "temperature",
    "hc air temperature": "temperature",
    # Wind speed
    "vent": "wind",
    "vitesse du vent": "wind",
    "wind speed": "wind",
    "wind": "wind",
    "u-sonic wind speed": "wind",
    # Wind direction
    "direction du vent": "wind_direction",
    "wind direction": "wind_direction",
    # Precipitation
    "pluie": "rain",
    "rain": "rain",
    "precipitations": "rain",
    "précipitations": "rain",
    "precipitation": "rain",
    # Humidity
    "humidité": "humidity",
    "humidity": "humidity",
    "humidite": "humidity",
    "relative humidity": "humidity",
    # Solar
    "rayonnement": "solar",
    "solar": "solar",
    "radiation": "solar",
    "solar radiation": "solar",
    # Others
    "leaf wetness": "leaf_wetness",
    "mouillage foliaire": "leaf_wetness",
    "soil moisture": "soil_moisture",
    "humidité du sol": "soil_moisture",
    "deltat": "deltat",
    # Rainfall (additional aliases for rainfall domain integration)
    "pluviométrie": "rain",
    "pluviometrie": "rain",
    "rainfall": "rain",
    "drought": "rain",
    "sécheresse": "rain",
    "secheresse": "rain",
}

# Canonical group → ILIKE patterns to match against the param column
_PARAM_ILIKE: dict[str, list[str]] = {
    "temperature": ["%temperature%"],
    "wind": ["%itesse%vent%", "%wind%speed%"],
    "wind_direction": ["%direction%vent%", "%wind%direction%", "%wind%dir%"],
    "rain": ["%recipit%", "%pluie%"],
    "humidity": ["%humidit%", "%humidity%"],
    "solar": ["%radia%", "%solar%"],
    "leaf_wetness": ["%leaf%wetness%", "%mouillage%"],
    "soil_moisture": ["%soil%moisture%", "%humidit%sol%"],
    "deltat": ["%deltat%"],
}


def _param_where(parameter: str, param_col: str) -> str:
    """Build a SQL WHERE fragment for parameter filtering."""
    canonical = _PARAM_ALIASES.get(parameter.lower().strip())
    if canonical and canonical in _PARAM_ILIKE:
        parts = [f'"{param_col}" ILIKE \'{p}\'' for p in _PARAM_ILIKE[canonical]]
        return "(" + " OR ".join(parts) + ")"
    # Fall back to raw ILIKE on user input
    safe = parameter.replace("'", "''")
    return f'"{param_col}" ILIKE \'%{safe}%\''


# ---------------------------------------------------------------------------
# Station filtering
# ---------------------------------------------------------------------------

_MULTI_RE = re.compile(r"\s+vs\.?\s+|\s+et\s+|\s*,\s*", re.IGNORECASE)


def _split_multi(query: str) -> list[str]:
    """Split multi-station query: 'Bizerte vs Mahdia' or 'Bizerte, Mahdia'."""
    parts = [p.strip() for p in _MULTI_RE.split(query) if p.strip()]
    return parts if len(parts) > 1 else [query]


def _filter_stations(stations: list[dict], query: str) -> list[dict]:
    """Filter sensor stations by governorate, name, or locality."""
    q = query.lower().strip()
    sensor = [s for s in stations if s.get("variant")]

    # Exact governorate match
    matches = [s for s in sensor if s["governorate"].lower() == q]
    if matches:
        return matches

    # Canonical governorate lookup
    canonical = GOVERNORATE_MAP.get(q)
    if canonical:
        matches = [s for s in sensor if s["governorate"] == canonical]
        if matches:
            return matches

    # Name / dataset slug fuzzy match
    matches = []
    for s in sensor:
        name_l = s.get("name", "").lower()
        ds_l = s.get("dataset", "").replace("-", " ").lower()
        if q in name_l or q in ds_l:
            matches.append(s)
    return matches


def _available_stations_msg(stations: list[dict], query: str) -> str:
    """List all available stations when no match is found."""
    sensor = [s for s in stations if s.get("variant")]
    by_gov: dict[str, list[dict]] = {}
    for s in sensor:
        by_gov.setdefault(s["governorate"], []).append(s)

    lines = [f"No station matching **{query}**. Available climate stations:\n"]
    for gov in sorted(by_gov.keys()):
        entries = by_gov[gov]
        names = ", ".join(
            f"{s.get('name', '?')} ({s.get('org_title', '')})" for s in entries
        )
        lines.append(f"- **{gov}**: {names}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------


def _is_cumulative_param(parameter: str) -> bool:
    """Return True if the parameter should use SUM instead of AVG."""
    canonical = _PARAM_ALIASES.get(parameter.lower().strip())
    return canonical == "rain"


def _build_data_sql(
    rid: str,
    variant: str,
    parameter: str,
    date_from: str | None,
    date_to: str | None,
    aggregation: str,
    limit: int = 200,
) -> str:
    """Build a SQL query for climate sensor data."""
    cols = _VARIANTS[variant]
    d, v, p, u = cols["date"], cols["value"], cols["param"], cols["unit"]

    where = [
        f'"{v}" ~ \'^-?[0-9.]+$\'',
        _param_where(parameter, p),
    ]
    if date_from:
        where.append(f'"{d}" >= \'{date_from.replace(chr(39), "")}\'')
    if date_to:
        where.append(f'"{d}" <= \'{date_to.replace(chr(39), "")}\'')
    w = " AND ".join(where)

    # Precipitation uses SUM (cumulative); other sensors use AVG
    cumulative = _is_cumulative_param(parameter)
    if cumulative:
        agg_expr = (
            f'SUM("{v}"::numeric) as total, '
            f'MAX("{v}"::numeric) as max_val, '
            f"COUNT(*) as readings"
        )
    else:
        agg_expr = (
            f'AVG("{v}"::numeric) as avg_val, '
            f'MIN("{v}"::numeric) as min_val, '
            f'MAX("{v}"::numeric) as max_val, '
            f"COUNT(*) as readings"
        )

    if aggregation == "daily":
        return (
            f"SELECT DATE_TRUNC('day', \"{d}\"::timestamp) as day, "
            f"{agg_expr} "
            f'FROM "{rid}" WHERE {w} '
            f"GROUP BY day ORDER BY day LIMIT {limit}"
        )
    if aggregation == "monthly":
        return (
            f"SELECT DATE_TRUNC('month', \"{d}\"::timestamp) as month, "
            f"{agg_expr} "
            f'FROM "{rid}" WHERE {w} '
            f"GROUP BY month ORDER BY month LIMIT {limit}"
        )
    # raw
    return (
        f'SELECT "{d}", "{p}", "{v}"::numeric as value, "{u}" '
        f'FROM "{rid}" WHERE {w} '
        f'ORDER BY "{d}" DESC LIMIT {limit}'
    )


# ---------------------------------------------------------------------------
# Sensor discovery (used in inventory and station details)
# ---------------------------------------------------------------------------


# Sensor list cache: resource_id → list of {name, latest}
_sensor_cache: dict[str, list[dict[str, str]]] = {}


async def _fetch_sensors(
    client: CKANClient, rid: str, variant: str
) -> list[dict[str, str]]:
    """Fetch distinct sensor names and their latest reading date.

    Results are cached for the lifetime of the process (cleared on server restart).
    """
    if rid in _sensor_cache:
        return _sensor_cache[rid]

    cols = _VARIANTS[variant]
    sql = (
        f'SELECT "{cols["param"]}" as param, MAX("{cols["date"]}") as latest '
        f'FROM "{rid}" '
        f'GROUP BY "{cols["param"]}" '
        f'ORDER BY "{cols["param"]}"'
    )
    result = await client.datastore_sql(sql)
    if not result:
        return []
    sensors = [
        {"name": r.get("param", ""), "latest": r.get("latest", "")}
        for r in result.get("records", [])
    ]
    _sensor_cache[rid] = sensors
    return sensors


async def _get_cached_sensors(
    client: CKANClient, station: dict
) -> list[dict[str, str]]:
    """Get sensor list for a station, using cache when available."""
    variant = station.get("variant")
    if not variant:
        return []
    return await _fetch_sensors(client, station["id"], variant)


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------

_SKIP_COLS = {"_id", "_full_text"}


def _sensor_matches_parameter(sensor_names: list[str], parameter: str) -> bool:
    """Check if any sensor name matches a parameter query."""
    canonical = _PARAM_ALIASES.get(parameter.lower().strip())
    if canonical and canonical in _PARAM_ILIKE:
        for name in sensor_names:
            name_l = name.lower()
            for pat in _PARAM_ILIKE[canonical]:
                # Convert SQL ILIKE pattern to regex: % → .*
                regex = pat.replace("%", ".*")
                if re.search(regex, name_l):
                    return True
        return False
    # Direct substring match
    p = parameter.lower()
    return any(p in s.lower() for s in sensor_names)


def _extract_date_range(records: list[dict], aggregation: str) -> str:
    """Extract the date range spanned by query results."""
    # The time column name depends on the aggregation mode
    date_key = {"daily": "day", "monthly": "month"}.get(aggregation)
    if not date_key:
        # raw mode — look for common date column names
        for k in ("Date", "date"):
            if k in records[0]:
                date_key = k
                break
    if not date_key or date_key not in records[0]:
        return ""
    dates = [str(r.get(date_key, "")) for r in records if r.get(date_key)]
    if not dates:
        return ""
    # Truncate timestamps to date portion for display
    first = dates[0][:10]
    last = dates[-1][:10]
    if first == last:
        return first
    # Records may be in DESC order (raw) or ASC order (aggregated)
    if first > last:
        first, last = last, first
    return f"{first} to {last}"


def _records_table(records: list[dict], max_rows: int = 50) -> list[str]:
    """Format records as a markdown table."""
    if not records:
        return ["*No records.*"]
    cols = [k for k in records[0] if k not in _SKIP_COLS]
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for rec in records[:max_rows]:
        vals = []
        for c in cols:
            v = rec.get(c, "")
            if isinstance(v, float):
                v = f"{v:.2f}"
            vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    if len(records) > max_rows:
        lines.append(f"\n*… {len(records) - max_rows} more rows not shown.*")
    return lines


# ---------------------------------------------------------------------------
# Mode: Inventory (no station, no parameter)
# ---------------------------------------------------------------------------


async def _build_inventory(
    client: CKANClient,
    registry: SchemaRegistry,
    stations: list[dict],
) -> str:
    """Full station inventory with live sensor lists."""
    sensor_stations = [s for s in stations if s.get("variant")]
    metadata_stations = [s for s in stations if not s.get("variant") and s.get("fields")]

    # Fetch sensor lists (cached after first call)
    for s in sensor_stations:
        sensors = await _get_cached_sensors(client, s)
        s["sensors"] = sensors
        s["latest"] = max(
            (sn["latest"] for sn in sensors if sn["latest"]), default=""
        )

    # Group by governorate
    by_gov: dict[str, list[dict]] = {}
    for s in sensor_stations:
        by_gov.setdefault(s["governorate"], []).append(s)

    gov_count = len([g for g in by_gov if g != "national"])

    lines = [
        "# Climate Station Inventory",
        "",
        f"**{len(sensor_stations)} sensor stations** across **{gov_count} governorates**.",
        "Sources: DGGREE (FieldClimate, real-time), DGACTA (environmental, periodic).",
        "",
    ]

    for gov in sorted(by_gov.keys()):
        gov_stations = by_gov[gov]
        plural = "s" if len(gov_stations) > 1 else ""
        lines.append(
            f"## {gov} ({len(gov_stations)} station{plural})"
        )
        lines.append("")

        for s in gov_stations:
            name = s.get("name", "Unnamed")
            org = s.get("org_title", "")
            records = s.get("records", 0)
            sensors = s.get("sensors", [])
            latest = s.get("latest", "")
            sensor_names = ", ".join(sn["name"] for sn in sensors if sn["name"]) or "—"

            lines.append(f"**{name}**")
            lines.append(f"- Organization: {org}")
            lines.append(f"- Records: {records:,}")
            lines.append(f"- Sensors: {sensor_names}")
            if latest:
                lines.append(f"- Latest reading: {latest}")
            lines.append(f"- Resource ID: `{s['id']}`")
            lines.append("")

    if metadata_stations:
        lines.append("## Metadata-Only Resources")
        lines.append("")
        for s in metadata_stations:
            lines.append(
                f"- **{s.get('name', 'Unnamed')}** (`{s['id']}`): "
                f"{', '.join(s.get('fields', []))}"
            )
        lines.append("")

    # Source attribution (deduplicated, capped at 5)
    seen_ds: set[str] = set()
    unique_sources: list[dict] = []
    for s in stations:
        src = s.get("_source")
        if src:
            ds = src.get("dataset_name", "")
            if ds and ds not in seen_ds:
                seen_ds.add(ds)
                unique_sources.append(src)
    if unique_sources:
        lines.append(format_source_footer(unique_sources[:5]))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Mode: Station details (station specified, no parameter)
# ---------------------------------------------------------------------------


async def _station_details(
    client: CKANClient,
    registry: SchemaRegistry,
    matched: list[dict],
) -> str:
    """Show station metadata and available sensors for matched stations."""
    lines: list[str] = []
    sources: list[dict] = []

    for s in matched:
        variant = s.get("variant")
        if not variant:
            continue

        sensors = await _get_cached_sensors(client, s)
        name = s.get("name", "Unnamed")

        lines.append(f"## {name}")
        lines.append(f"- **Governorate:** {s['governorate']}")
        lines.append(f"- **Organization:** {s.get('org_title', '')}")
        lines.append(f"- **Records:** {s.get('records', 0):,}")
        lines.append(f"- **Schema variant:** {variant}")
        lines.append(f"- **Resource ID:** `{s['id']}`")
        lines.append("")

        if sensors:
            lines.append("**Available sensors:**")
            lines.append("")
            lines.append("| Sensor | Latest Reading |")
            lines.append("| --- | --- |")
            for sn in sensors:
                lines.append(f"| {sn['name']} | {sn['latest']} |")
            lines.append("")

        if s.get("_source"):
            sources.append(s["_source"])

    if not lines:
        return "No sensor data stations found in the selection."

    if sources:
        lines.append(format_source_footer(sources))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Mode: Data query (parameter specified)
# ---------------------------------------------------------------------------


async def _query_parameter(
    client: CKANClient,
    registry: SchemaRegistry,
    stations: list[dict],
    parameter: str,
    date_from: str | None,
    date_to: str | None,
    aggregation: str,
) -> str:
    """Query a sensor parameter across matched stations.

    Tries all matched stations; returns data from those that have the
    requested parameter. If none do, reports available sensors instead.
    """
    lines: list[str] = []
    sources: list[dict] = []
    no_data: list[dict] = []

    # Sort by record count descending — prefer richer stations
    ordered = sorted(
        [s for s in stations if s.get("variant")],
        key=lambda s: s.get("records", 0),
        reverse=True,
    )

    for s in ordered:
        sql = _build_data_sql(
            s["id"], s["variant"], parameter,
            date_from, date_to, aggregation,
        )
        result = await client.datastore_sql(sql)
        records = result.get("records", []) if result else []

        if not records:
            no_data.append(s)
            continue

        name = s.get("name", "Unnamed")
        gov = s["governorate"]
        org = s.get("org_title", "")

        # Extract date range from results
        date_range = _extract_date_range(records, aggregation)

        lines.append(f"## {name} ({gov})")
        header = f"Organization: {org} | Records in result: {len(records)}"
        if date_range:
            header += f" | Data range: {date_range}"
        lines.append(header)
        lines.append("")
        lines.extend(_records_table(records))
        lines.append("")

        if s.get("_source"):
            sources.append(s["_source"])

    if not lines:
        # No station returned data — diagnose why for each station
        msg = [f"No **{parameter}** data found at any matched station.\n"]
        has_param_stations: list[dict] = []
        no_param_stations: list[dict] = []

        for s in no_data:
            variant = s.get("variant")
            if not variant:
                continue
            sensors = await _get_cached_sensors(client, s)
            s["_sensors"] = sensors
            sensor_names = [sn["name"] for sn in sensors]
            if _sensor_matches_parameter(sensor_names, parameter):
                has_param_stations.append(s)
            else:
                no_param_stations.append(s)

        if has_param_stations:
            msg.append(
                "**These stations have the sensor but no data in the "
                "requested date range:**\n"
            )
            for s in has_param_stations:
                latest = max(
                    (sn["latest"] for sn in s["_sensors"] if sn["latest"]),
                    default="unknown",
                )
                msg.append(
                    f"- **{s.get('name', '?')}** ({s['governorate']}): "
                    f"latest reading is **{latest}** — try widening or "
                    f"adjusting the date range"
                )
            msg.append("")

        if no_param_stations:
            msg.append("**These stations do not have this sensor:**\n")
            for s in no_param_stations:
                sensor_list = ", ".join(
                    sn["name"] for sn in s.get("_sensors", [])
                )
                msg.append(
                    f"- **{s.get('name', '?')}** ({s['governorate']}): "
                    f"{sensor_list}"
                )
        return "\n".join(msg)

    # Note stations that lacked the parameter
    if no_data:
        lines.append(
            f"*Note: {len(no_data)} other station(s) in the selection "
            f"had no {parameter} data:*"
        )
        for s in no_data:
            lines.append(f"  - {s.get('name', '?')} ({s['governorate']})")
        lines.append("")

    if sources:
        lines.append(format_source_footer(sources))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Mode: Multi-station comparison
# ---------------------------------------------------------------------------


async def _multi_station_compare(
    client: CKANClient,
    registry: SchemaRegistry,
    all_stations: list[dict],
    queries: list[str],
    parameter: str | None,
    date_from: str | None,
    date_to: str | None,
    aggregation: str,
) -> str:
    """Compare data across multiple station groups."""
    if not parameter:
        # Without a parameter, show station details with sensor lists
        lines = ["# Station Comparison\n"]
        sources: list[dict] = []
        for q in queries:
            matched = _filter_stations(all_stations, q)
            if not matched:
                lines.append(f"## {q.title()}\nNo stations found.\n")
                continue
            lines.append(f"## {q.title()}")
            for s in matched:
                variant = s.get("variant")
                if not variant:
                    continue
                sensors = await _get_cached_sensors(client, s)
                sensor_names = ", ".join(
                    sn["name"] for sn in sensors if sn["name"]
                ) or "—"
                latest = max(
                    (sn["latest"] for sn in sensors if sn["latest"]),
                    default="—",
                )
                lines.append(
                    f"**{s.get('name', '?')}** ({s.get('org_title', '')})"
                )
                lines.append(f"- Governorate: {s['governorate']}")
                lines.append(f"- Records: {s.get('records', 0):,}")
                lines.append(f"- Sensors: {sensor_names}")
                lines.append(f"- Latest reading: {latest}")
                lines.append(f"- Resource ID: `{s['id']}`")
                lines.append("")
                if s.get("_source"):
                    sources.append(s["_source"])
        if sources:
            lines.append(format_source_footer(sources))
        return "\n".join(lines)

    # Compare a parameter across station groups
    agg = aggregation if aggregation != "raw" else "daily"
    lines = [f"# Comparing **{parameter}** across stations\n"]
    sources: list[dict] = []

    for q in queries:
        matched = _filter_stations(all_stations, q)
        if not matched:
            lines.append(f"## {q.title()}\nNo stations found.\n")
            continue

        lines.append(f"## {q.title()}")

        for s in matched:
            variant = s.get("variant")
            if not variant:
                continue

            sql = _build_data_sql(
                s["id"], variant, parameter,
                date_from, date_to, agg,
            )
            result = await client.datastore_sql(sql)
            records = result.get("records", []) if result else []

            if not records:
                lines.append(f"**{s.get('name', '?')}**: No {parameter} data.\n")
                continue

            date_range = _extract_date_range(records, agg)
            hdr = f"**{s.get('name', '?')}** ({s.get('org_title', '')})"
            if date_range:
                hdr += f" — {date_range}"
            lines.append(hdr)
            lines.extend(_records_table(records, max_rows=30))
            lines.append("")

            if s.get("_source"):
                sources.append(s["_source"])

    if sources:
        lines.append(format_source_footer(sources))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Mode: Latest readings
# ---------------------------------------------------------------------------


async def _latest_readings(
    client: CKANClient,
    registry: SchemaRegistry,
    stations: list[dict],
    parameter: str | None,
) -> str:
    """Fetch the most recent reading per sensor for each matched station."""
    lines: list[str] = []
    sources: list[dict] = []

    for s in stations:
        variant = s.get("variant")
        if not variant:
            continue
        cols = _VARIANTS[variant]
        d, v, p, u = cols["date"], cols["value"], cols["param"], cols["unit"]

        where = [f'"{v}" ~ \'^-?[0-9.]+$\'']
        if parameter:
            where.append(_param_where(parameter, p))
        w = " AND ".join(where)

        # Use DISTINCT ON to get the latest reading per sensor
        # CKAN DataStore runs PostgreSQL, so DISTINCT ON is supported
        sql = (
            f'SELECT DISTINCT ON ("{p}") "{p}" as sensor, '
            f'"{d}" as reading_time, "{v}"::numeric as value, "{u}" as unit '
            f'FROM "{s["id"]}" WHERE {w} '
            f'ORDER BY "{p}", "{d}" DESC'
        )
        result = await client.datastore_sql(sql)
        records = result.get("records", []) if result else []

        if not records:
            continue

        name = s.get("name", "Unnamed")
        lines.append(f"## {name} ({s['governorate']})")
        lines.append(
            f"Organization: {s.get('org_title', '')} | "
            f"Resource ID: `{s['id']}`"
        )
        lines.append("")
        lines.extend(_records_table(records))
        lines.append("")

        if s.get("_source"):
            sources.append(s["_source"])

    if not lines:
        return "No recent readings found at the matched stations."

    if sources:
        lines.append(format_source_footer(sources))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rainfall domain integration
# ---------------------------------------------------------------------------

_MONTH_COLS = {
    "septembre", "octobre", "novembre", "decembre",
    "janvier", "fevrier", "mars", "avril", "mai", "juin", "juillet", "aout",
}

_PRECIP_MARKERS = {"pluviom", "precipit", "pluie", "cumul_mm", "rainfall"}


def _has_precip_field(fields: list[str]) -> bool:
    """Check if any field name contains a precipitation-related term."""
    for f in fields:
        fl = f.lower()
        if any(m in fl for m in _PRECIP_MARKERS):
            return True
    return False


def _is_rain_parameter(parameter: str) -> bool:
    """Check if the parameter should trigger rainfall domain search."""
    canonical = _PARAM_ALIASES.get(parameter.lower().strip())
    return canonical == "rain"


def _detect_rainfall_schema(fields: list[str]) -> str | None:
    """Detect rainfall resource schema: 'eav', 'monthly', 'pivoted', 'annual', or None."""
    fset = {f.lower() for f in fields}
    # EAV format (same as climate stations — skip, already queried)
    if ("nom_fr" in fset and "valeur" in fset) or \
       ("sensor_name" in fset and "value" in fset) or \
       ("parameter" in fset and "value" in fset):
        return "eav"
    # Monthly aggregate: has a mois column
    if "mois" in fset:
        return "monthly"
    # Pivoted: has month-name columns (>= 6 months present)
    if len(fset & _MONTH_COLS) >= 6:
        return "pivoted"
    # Annual: has Annee + precipitation-related field (campaign/annual summaries)
    if "annee" in fset and _has_precip_field(fields):
        return "annual"
    return None


def _find_field(fields: list[str], *candidates: str) -> str | None:
    """Find actual field name matching candidates (case-insensitive)."""
    field_map = {f.lower(): f for f in fields}
    for c in candidates:
        if c.lower() in field_map:
            return field_map[c.lower()]
    return None


def _build_rainfall_sql(
    rid: str,
    fields: list[str],
    schema_type: str,
    date_from: str | None,
    date_to: str | None,
    limit: int = 200,
) -> str | None:
    """Build SQL for a rainfall domain resource."""
    skip = {"_id", "_full_text"}
    select_cols = [f'"{f}"' for f in fields if f not in skip]

    if schema_type == "monthly":
        mois = _find_field(fields, "Mois", "mois")
        annee = _find_field(fields, "Annee", "annee")
        if not mois:
            return None
        where = [f'"{mois}" IS NOT NULL']
        if annee and date_from:
            y = date_from[:4]
            where.append(f'"{annee}" ~ \'^\\d{{4}}$\' AND "{annee}" >= \'{y}\'')
        if annee and date_to:
            y = date_to[:4]
            where.append(f'"{annee}" <= \'{y}\'')
        order = f'"{annee}", "{mois}"' if annee else f'"{mois}"'
        return (
            f"SELECT {', '.join(select_cols)} "
            f'FROM "{rid}" '
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY {order} LIMIT {limit}"
        )

    if schema_type in ("pivoted", "annual"):
        annee = _find_field(fields, "Annee", "annee")
        where_parts = []
        if annee and date_from:
            y = date_from[:4]
            where_parts.append(
                f'"{annee}" ~ \'^\\d{{4}}$\' AND "{annee}" >= \'{y}\''
            )
        if annee and date_to:
            y = date_to[:4]
            where_parts.append(f'"{annee}" <= \'{y}\'')
        where_clause = f"WHERE {' AND '.join(where_parts)} " if where_parts else ""
        order = f'ORDER BY "{annee}" ' if annee else ""
        return (
            f"SELECT {', '.join(select_cols)} "
            f'FROM "{rid}" '
            f"{where_clause}"
            f"{order}LIMIT {limit}"
        )

    return None


async def _query_rainfall_domain(
    client: CKANClient,
    registry: SchemaRegistry,
    station: str | None,
    date_from: str | None,
    date_to: str | None,
    exclude_rids: set[str],
) -> str:
    """Query historical pluviometry from the rainfall domain.

    Returns formatted markdown, or empty string if no data found.
    """
    gov_filter: str | None = None
    if station:
        q = station.lower().strip()
        gov_filter = GOVERNORATE_MAP.get(q)
        if not gov_filter:
            # Check if input is already a canonical name
            for v in GOVERNORATE_MAP.values():
                if v.lower() == q:
                    gov_filter = v
                    break

    resources = registry.get_domain_resources("rainfall", gouvernorat=gov_filter)
    if not resources:
        return ""

    # Exclude resources already queried via climate_stations domain
    resources = [r for r in resources if r["id"] not in exclude_rids]
    if not resources:
        return ""

    lines = ["# Historical Pluviometry Records", ""]
    sources: list[dict] = []
    found_data = False

    for res in resources:
        rid = res["id"]
        fields = res.get("fields", [])
        schema_type = _detect_rainfall_schema(fields)

        # Skip EAV resources (already handled by climate_stations domain)
        if not schema_type or schema_type == "eav":
            continue

        sql = _build_rainfall_sql(rid, fields, schema_type, date_from, date_to)
        if not sql:
            continue

        try:
            result = await client.datastore_sql(sql)
        except Exception:
            continue

        if not result:
            continue
        records = result.get("records", [])
        if not records:
            continue

        found_data = True
        gov = registry._resource_gov.get(rid, "")
        name = res.get("name", "Unnamed")

        lines.append(f"## {name}")
        header = f"Governorate: {gov} | Records: {len(records)}"
        if schema_type == "pivoted":
            header += " | Format: yearly summary with monthly columns"
        lines.append(header)
        lines.append("")
        lines.extend(_records_table(records, max_rows=40))
        lines.append("")

        source = registry.get_source_attribution(rid)
        if source:
            sources.append(source)

    if not found_data:
        return ""

    if sources:
        lines.append(format_source_footer(sources))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def query_climate_stations(
    client: CKANClient,
    registry: SchemaRegistry,
    station: str | None = None,
    parameter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    aggregation: str = "raw",
    latest: bool = False,
) -> str:
    """Query climate station data from Tunisia's weather monitoring network.

    Modes:
    - No arguments: full station inventory with sensor lists
    - Station only: station details and available sensors
    - Station + parameter: sensor data query (searches all matching stations)
    - Parameter only: search all stations for this parameter
    - Multi-station (comma / 'vs' separated): side-by-side comparison
    - latest=True: most recent reading per sensor at matched stations
    """
    if aggregation not in ("raw", "daily", "monthly"):
        return f"Invalid aggregation '{aggregation}'. Use 'raw', 'daily', or 'monthly'."

    all_resources = registry.get_domain_resources("climate_stations")
    if not all_resources:
        return "No climate station resources found in the registry."

    # Enrich each resource with variant, governorate, and org info
    stations: list[dict[str, Any]] = []
    for res in all_resources:
        variant = _detect_variant(res.get("fields", []))
        gov = registry._resource_gov.get(res["id"], "national")
        source = registry.get_source_attribution(res["id"])
        stations.append({
            **res,
            "variant": variant,
            "governorate": gov,
            "org_title": source.get("organization_title", "") if source else "",
            "_source": source,
        })

    # Check if this is a rain-related query that should also search rainfall domain
    rain_query = parameter is not None and _is_rain_parameter(parameter)
    climate_rids = {s["id"] for s in stations}

    # --- Inventory mode ---
    if not station and not parameter and not latest:
        return await _build_inventory(client, registry, stations)

    # --- Multi-station comparison ---
    if station:
        multi = _split_multi(station)
        if len(multi) > 1:
            if latest:
                # Latest mode across multiple station groups
                all_matched: list[dict] = []
                for q in multi:
                    all_matched.extend(_filter_stations(stations, q))
                return await _latest_readings(
                    client, registry, all_matched, parameter
                )
            result = await _multi_station_compare(
                client, registry, stations, multi,
                parameter, date_from, date_to, aggregation,
            )
            if rain_query:
                for q in multi:
                    rainfall = await _query_rainfall_domain(
                        client, registry, q, date_from, date_to, climate_rids,
                    )
                    if rainfall:
                        result += "\n\n---\n\n" + rainfall
            return result

        matched = _filter_stations(stations, station)
        if not matched:
            if rain_query:
                # No climate stations, but rainfall domain may have data
                rainfall = await _query_rainfall_domain(
                    client, registry, station, date_from, date_to, climate_rids,
                )
                if rainfall:
                    return (
                        f"No climate sensor stations found for **{station}**.\n\n"
                        + rainfall
                    )
                return (
                    f"No climate stations or historical rainfall data "
                    f"available for **{station}**."
                )
            return _available_stations_msg(stations, station)
    else:
        # No station filter — all sensor resources
        matched = [s for s in stations if s.get("variant")]

    # --- Latest readings mode ---
    if latest:
        return await _latest_readings(client, registry, matched, parameter)

    # --- Data query ---
    if parameter:
        result = await _query_parameter(
            client, registry, matched, parameter,
            date_from, date_to, aggregation,
        )
        if rain_query:
            rainfall = await _query_rainfall_domain(
                client, registry, station, date_from, date_to, climate_rids,
            )
            if rainfall:
                result += "\n\n---\n\n" + rainfall
        return result

    # --- Station details ---
    return await _station_details(client, registry, matched)
