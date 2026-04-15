"""tanitdata MCP server — entry point."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager

# Suppress DEBUG noise from third-party libraries before any imports
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("mcp").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)

from mcp.server.fastmcp import FastMCP

from tanitdata import __version__
from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.bibliography import search_bibliography
from tanitdata.tools.climate import query_climate_stations
from tanitdata.tools.dashboards import get_dashboard_link
from tanitdata.tools.datastore import query_datastore
from tanitdata.tools.resource_reader import read_resource
from tanitdata.tools.search import (
    get_dataset_details,
    list_organizations,
    search_datasets,
)

logger = logging.getLogger(__name__)

client = CKANClient()

_schemas_path = os.environ.get("SCHEMAS_PATH")
registry = SchemaRegistry(schemas_path=_schemas_path)


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Load the static registry synchronously, then kick off a background live refresh."""
    # Load schemas.json — fast (~28ms), must complete before tools are callable
    registry.load()
    logger.info("tanitdata: static registry ready, starting background live refresh")

    # Background refresh so the first tool call is never blocked by a network request
    task = asyncio.create_task(_background_refresh())

    yield

    # Cleanup
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    await client.close()


async def _background_refresh():
    """Perform the initial live refresh without blocking tool calls."""
    try:
        await registry._refresh(client)
    except Exception as exc:
        logger.warning("tanitdata: background live refresh failed: %s", exc)


mcp = FastMCP(
    "tanitdata",
    lifespan=lifespan,
    host=os.environ.get("FASTMCP_HOST", "127.0.0.1"),
    port=int(os.environ.get("FASTMCP_PORT", "8000")),
    stateless_http=os.environ.get("FASTMCP_STATELESS_HTTP", "").lower() == "true",
)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


async def _resolve_to_resource(identifier: str) -> tuple[str, str]:
    """Resolve an identifier to a DataStore resource UUID.

    Returns (resource_id, note). If identifier is already a UUID, note is empty.
    If it's a dataset slug, looks up the dataset and picks the first DataStore-active resource.
    """
    if _UUID_RE.match(identifier):
        return identifier, ""

    # Treat as dataset slug
    try:
        result = await client.package_show(identifier)
    except Exception:
        result = None
    if not result:
        return identifier, ""  # let downstream error handle it

    ds_title = result.get("title", identifier)
    resources = result.get("resources", [])

    # Prefer first DataStore-active resource
    for res in resources:
        if res.get("datastore_active"):
            rid = res["id"]
            name = res.get("name", "Unnamed")
            note = (
                f"**Auto-resolved** dataset slug `{identifier}` → "
                f"resource `{rid}` ({name}) from *{ds_title}*\n\n"
            )
            return rid, note

    # Fallback: first resource (may not be DataStore-active)
    if resources:
        rid = resources[0]["id"]
        name = resources[0].get("name", "Unnamed")
        note = (
            f"**Auto-resolved** dataset slug `{identifier}` → "
            f"resource `{rid}` ({name}) from *{ds_title}* "
            f"(warning: not DataStore-active — query may fail, try read_resource instead)\n\n"
        )
        return rid, note

    return identifier, ""  # no resources, let downstream error handle it


@mcp.tool()
async def search_datasets_tool(
    query: str,
    organization: str | None = None,
    group: str | None = None,
    format: str | None = None,
    limit: int = 10,
) -> str:
    """Search Tunisia's agricultural open data portal (catalog.agridata.tn) for datasets by keyword, organization, thematic group, or format.

    Returns dataset titles, IDs, organizations, groups, and descriptions.
    Keywords can be in French or Arabic. Examples: 'olive production', 'céréales', 'pluviométrie'.
    """
    await registry.maybe_refresh(client)
    return await search_datasets(
        client=client,
        query=query,
        organization=organization,
        group=group,
        format_filter=format,
        limit=min(limit, 100),
    )


@mcp.tool()
async def get_dataset_details_tool(dataset_id: str) -> str:
    """Get full metadata and resource list for a specific dataset on catalog.agridata.tn.

    Use after search_datasets to explore a dataset's resources.
    Provide the dataset ID or slug name (e.g. 'station-climatique-station-jendouba-gda-bouhertma-fieldclimate').
    """
    await registry.maybe_refresh(client)
    return await get_dataset_details(client=client, dataset_id=dataset_id)


@mcp.tool()
async def query_datastore_tool(
    resource_id: str | None = None,
    dataset_id: str | None = None,
    sql: str | None = None,
    limit: int = 100,
) -> str:
    """Query any DataStore resource on catalog.agridata.tn using SQL or simple browse.

    Accepts a resource_id (UUID like 'ec7daec9-...') or a dataset_id (slug like 'pluviometrie-gouvernorat-de-beja').
    If a dataset slug is provided, auto-resolves to the first DataStore-active resource in that dataset.
    Prefer resource_id when you have it — it's faster (skips the lookup).

    Returns the resource schema (column names and types) plus data records.
    All fields are stored as text — use ::numeric or ::timestamp casts for math/date operations.
    Supports SELECT, WHERE, GROUP BY, ORDER BY, JOIN, LIKE, regex (~), LIMIT.

    If no SQL is provided, returns the first `limit` rows with schema info.
    When writing SQL, use double quotes for column names and the resource_id as the table name.
    Example: SELECT "nom_fr", "valeur"::numeric FROM "<resource_id>" WHERE "nom_fr" = 'Air temperature' LIMIT 10

    Field name suffixes encode units: _ha=hectares, _t=tonnes, _qx=quintaux, _mm=millimètres,
    _tete=têtes, _md=millions de dinars, _dt=dinars tunisiens, _cube=mille m³, _kg=kilogrammes.

    Many crop/production resources use WIDE FORMAT: the crop name IS the column name (e.g. Ble_dur_ha,
    Orge_qx, Fourrages_t). SELECT these columns directly — do NOT use WHERE/ILIKE on them.
    Common crop prefixes: Ble_dur (durum wheat), Ble_tendre (soft wheat), Orge (barley),
    Triticale, Fourrages (fodder), Legumineuses (legumes), Olivier (olive), Palmier (palm).

    QUERY STRATEGY: If unsure about exact column names or values, call with no SQL first to preview
    the schema and sample data, then construct your filtered query using the exact values returned.

    When analyzing a specific governorate, ensure consistency across dimensions. If you retrieve cereal production data for Bizerte, also retrieve rainfall or climate data for Bizerte — not a different governorate. Cross-dimensional analysis should be geographically aligned.
    """
    identifier = resource_id or dataset_id
    if not identifier:
        return "Please provide a `resource_id` (UUID) or `dataset_id` (slug)."

    resolved_id, resolution_note = await _resolve_to_resource(identifier)

    # If SQL references the original slug, swap in the resolved UUID
    if sql and resolution_note and identifier in sql:
        sql = sql.replace(identifier, resolved_id)

    await registry.maybe_refresh(client)
    result = await query_datastore(
        client=client,
        registry=registry,
        resource_id=resolved_id,
        sql=sql,
        limit=limit,
    )

    if resolution_note:
        return resolution_note + result
    return result


@mcp.tool()
async def read_resource_tool(
    resource_id: str,
    limit: int = 100,
) -> str:
    """Read a non-DataStore resource file (CSV or XLSX) from catalog.agridata.tn.

    Use this for resources where DataStore active = No. Downloads the file on first call,
    parses it, and caches the result. Returns schema + data rows like query_datastore.

    For DataStore-active resources, use query_datastore instead (supports SQL).
    Max file size: 5 MB. Supported formats: CSV, XLSX.
    """
    await registry.maybe_refresh(client)
    return await read_resource(
        client=client,
        registry=registry,
        resource_id=resource_id,
        limit=limit,
    )


@mcp.tool()
async def list_organizations_tool(query: str = "") -> str:
    """List all data-producing organizations on catalog.agridata.tn with their dataset counts.

    Useful for discovering which ministries and agencies publish data.
    Optionally filter by keyword.
    """
    await registry.maybe_refresh(client)
    return await list_organizations(client=client, query=query)


@mcp.tool()
async def query_climate_stations_tool(
    station: str | None = None,
    parameter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    aggregation: str = "raw",
    latest: bool = False,
) -> str:
    """Queries climate station sensor data AND historical pluviometry records.

    For real-time weather data, uses the DGGREE/DGACTA sensor network (24 stations, 11 governorates).
    For historical monthly rainfall by delegation (useful for drought analysis), uses CRDA pluviometry
    datasets covering 10+ governorates. Rainfall-related parameters ('pluie', 'rainfall', 'précipitations',
    'pluviométrie', 'drought', 'sécheresse') automatically search both domains.

    Modes:
    - No arguments: full station inventory with sensor lists per station
    - station only: station details and available sensors
    - station + parameter: sensor data (searches all matching stations for the parameter)
    - parameter only: search all stations for this parameter
    - latest=True: most recent reading per sensor at matched stations

    station: partial match against station name or governorate (e.g. 'Jendouba', 'Bizerte', 'Ghezala').
             Use comma or 'vs' to compare multiple stations (e.g. 'Bizerte vs Mahdia').
    parameter: sensor type. Accepts French ('température', 'vent', 'pluie', 'humidité', 'rayonnement')
               or English ('temperature', 'wind', 'rain', 'humidity', 'solar').
               Also accepts 'rainfall', 'pluviométrie', 'drought', 'sécheresse' for historical rainfall.
    date_from / date_to: ISO date strings (e.g. '2025-01-01').
    aggregation: 'raw' (default), 'daily', or 'monthly'. Precipitation uses SUM; other sensors use AVG.
    latest: if True, return the single most recent reading per sensor instead of a time series.

    When analyzing a specific governorate, ensure consistency across dimensions. If you retrieve cereal production data for Bizerte, also retrieve rainfall or climate data for Bizerte — not a different governorate. Cross-dimensional analysis should be geographically aligned.
    """
    await registry.maybe_refresh(client)
    return await query_climate_stations(
        client=client,
        registry=registry,
        station=station,
        parameter=parameter,
        date_from=date_from,
        date_to=date_to,
        aggregation=aggregation,
        latest=latest,
    )


@mcp.tool()
async def get_dashboard_link_tool(topic: str) -> str:
    """Find interactive dashboards on dashboards.agridata.tn for a given topic.

    Maps a topic (in French or English) to the relevant dashboard URL(s).
    18 dashboards available: climate, cereals, olive oil, dates, citrus, fisheries,
    vegetables, dams, investments, forest fires, rainfall, cereal prices, and more.

    Returns one link for a clear match, or a ranked list when multiple dashboards are relevant.
    Examples: 'céréales', 'olive oil', 'dattes export', 'agrumes', 'climate change'.

    Dashboards are for interactive exploration, not a substitute for answering data questions. If the user asks about current dam levels or current rainfall, query the actual data first using query_datastore or query_climate_stations, then offer the dashboard link as a supplement for further exploration. Never respond with only a dashboard link when actual data can be queried.
    """
    return get_dashboard_link(topic=topic)


@mcp.tool()
async def search_bibliography_tool(
    query: str,
    year_from: int | None = None,
    year_to: int | None = None,
    language: str | None = None,
    theme: str | None = None,
    limit: int = 20,
) -> str:
    """Search ONAGRI's bibliographic catalogs (25,000+ records) for publications, reports, and studies.

    Covers agriculture, water resources, forestry, and fisheries in Tunisia.
    Keywords can be in French, Arabic, or English. Searches titles and abstracts.

    Examples: 'céréales production rendement', 'olive Sfax', 'irrigation Kairouan'.

    year_from / year_to: filter by publication year (e.g. 2010, 2023).
    language: 'FR', 'AR', or 'EN'.
    theme: restrict to a thematic library — 'agriculture', 'water', 'forestry', or 'fisheries'.
    """
    await registry.maybe_refresh(client)
    return await search_bibliography(
        client=client,
        registry=registry,
        query=query,
        year_from=year_from,
        year_to=year_to,
        language=language,
        theme=theme,
        limit=min(limit, 100),
    )


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    """Lightweight health probe for ALB / Docker HEALTHCHECK."""
    from starlette.responses import JSONResponse

    return JSONResponse({"status": "ok", "version": __version__})


def main():
    """Entry point for the tanitdata CLI."""
    transport = os.environ.get("MCP_TRANSPORT", "stdio")

    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        # HTTP: get Starlette app, add auth + logging middleware, run uvicorn.
        # FastMCP has no middleware injection API — streamable_http_app()
        # builds its middleware list internally (lines 969-985 in FastMCP).
        # This is the only way to add custom middleware.
        import anyio
        import uvicorn

        from tanitdata.auth import ApiKeyStore, BearerAuthMiddleware
        from tanitdata.middleware import RequestLoggingMiddleware, wrap_tool_calls

        app = mcp.streamable_http_app()
        key_store = ApiKeyStore()
        app.add_middleware(BearerAuthMiddleware, key_store=key_store)
        app.add_middleware(RequestLoggingMiddleware)
        wrap_tool_calls(mcp)

        config = uvicorn.Config(
            app,
            host=mcp.settings.host,
            port=mcp.settings.port,
            log_level=mcp.settings.log_level.lower(),
        )
        server = uvicorn.Server(config)
        anyio.run(server.serve)


if __name__ == "__main__":
    main()
