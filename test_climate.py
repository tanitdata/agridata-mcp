"""Benchmark tests for query_climate_stations against the live portal."""

import asyncio

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.climate import query_climate_stations


async def main():
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()

    print("=" * 80)
    print("BENCHMARK 1: Station Inventory")
    print("=" * 80)
    result = await query_climate_stations(client=client, registry=registry)
    print(result)

    print("\n" + "=" * 80)
    print("BENCHMARK 2: Average temperature in Jendouba (last 3 months)")
    print("=" * 80)
    result = await query_climate_stations(
        client=client,
        registry=registry,
        station="Jendouba",
        parameter="temperature",
        date_from="2026-01-01",
        date_to="2026-04-05",
        aggregation="monthly",
    )
    print(result)

    print("\n" + "=" * 80)
    print("BENCHMARK 3: Compare wind speed Bizerte vs Mahdia")
    print("=" * 80)
    result = await query_climate_stations(
        client=client,
        registry=registry,
        station="Bizerte vs Mahdia",
        parameter="wind",
        date_from="2025-01-01",
        date_to="2025-12-31",
        aggregation="monthly",
    )
    print(result)

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
