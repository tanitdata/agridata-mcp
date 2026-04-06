"""Stress test: execute the dashboard query components against query_climate_stations."""

import asyncio
import time

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.climate import query_climate_stations


async def timed(label, coro):
    t0 = time.perf_counter()
    try:
        result = await coro
        elapsed = time.perf_counter() - t0
        print(f"\n{'='*80}")
        print(f"[{label}] — {elapsed:.1f}s")
        print('='*80)
        # Print first 2000 chars to keep output manageable
        print(result[:2000])
        if len(result) > 2000:
            print(f"\n... ({len(result)} total chars, truncated)")
        return {"label": label, "elapsed": elapsed, "chars": len(result), "result": result}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"\n[{label}] — ERROR after {elapsed:.1f}s: {e}")
        import traceback; traceback.print_exc()
        return {"label": label, "elapsed": elapsed, "chars": 0, "error": str(e)}


async def main():
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()
    results = []

    # Q1: Full inventory (needed for "map of all stations")
    r = await timed("Q1: Station inventory (map data)",
        query_climate_stations(client=client, registry=registry))
    results.append(r)

    # Q2: Multi-station comparison WITHOUT parameter (P1 fix: should show details)
    r = await timed("Q2: 3-station comparison (no param, should show sensors)",
        query_climate_stations(client=client, registry=registry,
            station="Jendouba, Bizerte, Mahdia"))
    results.append(r)

    # Q3a: Wind direction — Bizerte last 6 months
    r = await timed("Q3a: Bizerte wind direction (6mo monthly)",
        query_climate_stations(client=client, registry=registry,
            station="Bizerte", parameter="wind direction",
            date_from="2025-10-01", date_to="2026-04-05",
            aggregation="monthly"))
    results.append(r)

    # Q3b: Wind direction — Mahdia (P0 fix: should now match "U-sonic wind dir")
    r = await timed("Q3b: Mahdia wind direction (P0 fix test)",
        query_climate_stations(client=client, registry=registry,
            station="Mahdia", parameter="wind direction",
            date_from="2025-10-01", date_to="2026-04-05",
            aggregation="monthly"))
    results.append(r)

    # Q4: Solar radiation seasonality — 12 months (date range in response: P1 fix)
    r = await timed("Q4: Solar radiation 3 stations (12mo, check date range)",
        query_climate_stations(client=client, registry=registry,
            station="Jendouba, Bizerte, Mahdia",
            parameter="solar",
            date_from="2025-04-01", date_to="2026-04-05",
            aggregation="monthly"))
    results.append(r)

    # Q5: Precipitation for agricultural season (P0 fix: should use SUM)
    r = await timed("Q5: Precipitation all stations (SUM fix test)",
        query_climate_stations(client=client, registry=registry,
            parameter="rain",
            date_from="2025-09-01", date_to="2026-04-05",
            aggregation="monthly"))
    results.append(r)

    # Q6: Latest readings mode (P2 fix)
    r = await timed("Q6: Latest readings — Bizerte (P2 fix test)",
        query_climate_stations(client=client, registry=registry,
            station="Bizerte", latest=True))
    results.append(r)

    # Q7: Latest readings with parameter filter
    r = await timed("Q7: Latest temperature — Mahdia (latest + param)",
        query_climate_stations(client=client, registry=registry,
            station="Mahdia", parameter="temperature", latest=True))
    results.append(r)

    # Q8: Inventory again (P2 cache fix: should be fast since sensors cached)
    r = await timed("Q8: Station inventory CACHED (P2 cache test)",
        query_climate_stations(client=client, registry=registry))
    results.append(r)

    # Q9: Edge cases
    r = await timed("Q9: Nonexistent station 'Sousse'",
        query_climate_stations(client=client, registry=registry,
            station="Sousse", parameter="temperature"))
    results.append(r)

    await client.close()

    # Summary
    print("\n\n" + "="*80)
    print("PERFORMANCE SUMMARY")
    print("="*80)
    for r in results:
        err = f" ERROR: {r.get('error','')}" if r.get('error') else ""
        print(f"  {r['label']:55s}  {r['elapsed']:5.1f}s  {r['chars']:6d} chars{err}")

    total = sum(r['elapsed'] for r in results)
    print(f"\n  {'TOTAL':55s}  {total:5.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
