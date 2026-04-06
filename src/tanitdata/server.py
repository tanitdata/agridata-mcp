"""tanitdata MCP server — entry point."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

# Suppress DEBUG noise from third-party libraries before any imports
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("mcp").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)

from mcp.server.fastmcp import FastMCP

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.climate import query_climate_stations
from tanitdata.tools.datastore import query_datastore
from tanitdata.tools.explore import explore_domain
from tanitdata.tools.preview import get_resource_preview
from tanitdata.tools.search import (
    get_dataset_details,
    list_organizations,
    search_datasets,
)

logger = logging.getLogger(__name__)

client = CKANClient()
registry = SchemaRegistry()


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


mcp = FastMCP("tanitdata", lifespan=lifespan)


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
    resource_id: str,
    sql: str | None = None,
    limit: int = 100,
) -> str:
    """Query any DataStore resource on catalog.agridata.tn using SQL or simple browse.

    Returns the resource schema (column names and types) plus data records.
    All fields are stored as text — use ::numeric or ::timestamp casts for math/date operations.
    Supports SELECT, WHERE, GROUP BY, ORDER BY, JOIN, LIKE, regex (~), LIMIT.

    If no SQL is provided, returns the first `limit` rows with schema info.
    When writing SQL, use double quotes for column names and the resource_id as the table name.
    Example: SELECT "nom_fr", "valeur"::numeric FROM "<resource_id>" WHERE "nom_fr" = 'Air temperature' LIMIT 10
    """
    await registry.maybe_refresh(client)
    return await query_datastore(
        client=client,
        registry=registry,
        resource_id=resource_id,
        sql=sql,
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
async def explore_domain_tool(
    domain: str,
    gouvernorat: str | None = None,
    keyword: str | None = None,
) -> str:
    """Explore any domain's resources on catalog.agridata.tn without executing DataStore queries.

    Browse available resources by domain, governorate, and keyword. Returns resource metadata,
    field lists, unit hints, coverage summary, and source attribution.

    Use this to understand what data is available before querying with query_datastore_tool.

    domain: one of the 11 registry domains — climate_stations, rainfall, dams, crop_production,
            olive_harvest, prices, fisheries, investments, livestock, water_resources,
            trade_exports, documentation.
    gouvernorat: filter by governorate name (e.g. 'Béja', 'Jendouba', 'Sfax').
    keyword: filter resources by name, dataset, or field name (e.g. 'céréales', 'olives',
             'dattes', 'crevettes', 'blé').
    """
    await registry.maybe_refresh(client)
    return await explore_domain(
        client=client,
        registry=registry,
        domain=domain,
        gouvernorat=gouvernorat,
        keyword=keyword,
    )


@mcp.tool()
async def get_resource_preview_tool(resource_id: str) -> str:
    """Get full schema and 3 sample rows for any DataStore resource on catalog.agridata.tn.

    Returns field names with inferred types (text, likely numeric, likely date),
    sample records, record count, unit hints from column names, and source attribution.

    Use after explore_domain_tool to inspect a specific resource before writing SQL.
    """
    await registry.maybe_refresh(client)
    return await get_resource_preview(
        client=client,
        registry=registry,
        resource_id=resource_id,
    )


@mcp.tool()
async def query_climate_stations_tool(
    station: str | None = None,
    parameter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    aggregation: str = "raw",
    latest: bool = False,
) -> str:
    """Query climate station data from Tunisia's weather monitoring network.

    24 stations across 11 governorates. Sources: DGGREE (FieldClimate, real-time), DGACTA (environmental, periodic).

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
    date_from / date_to: ISO date strings (e.g. '2025-01-01').
    aggregation: 'raw' (default), 'daily', or 'monthly'. Precipitation uses SUM; other sensors use AVG.
    latest: if True, return the single most recent reading per sensor instead of a time series.
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


def main():
    """Entry point for the tanitdata CLI."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
