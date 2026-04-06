"""Quick live test for explore_domain and get_resource_preview tools."""

import asyncio
import time

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.explore import explore_domain
from tanitdata.tools.preview import get_resource_preview


async def timed(label, coro):
    t0 = time.perf_counter()
    try:
        result = await coro
        elapsed = time.perf_counter() - t0
        print(f"\n{'='*80}")
        print(f"[{label}] -- {elapsed:.1f}s")
        print("=" * 80)
        print(result[:3000])
        if len(result) > 3000:
            print(f"\n... ({len(result)} total chars, truncated)")
        return {"label": label, "elapsed": elapsed, "chars": len(result)}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"\n[{label}] -- ERROR after {elapsed:.1f}s: {e}")
        import traceback; traceback.print_exc()
        return {"label": label, "elapsed": elapsed, "chars": 0, "error": str(e)}


async def main():
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()
    results = []

    # T1: Explore crop_production domain (no filters)
    r = await timed("T1: explore crop_production (inventory)",
        explore_domain(client=client, registry=registry, domain="crop_production"))
    results.append(r)

    # T2: Explore crop_production with gouvernorat filter
    r = await timed("T2: explore crop_production Beja",
        explore_domain(client=client, registry=registry, domain="crop_production", gouvernorat="Beja"))
    results.append(r)

    # T3: Explore with keyword
    r = await timed("T3: explore crop_production keyword=cereales",
        explore_domain(client=client, registry=registry, domain="crop_production", keyword="cereales"))
    results.append(r)

    # T4: Explore climate_stations
    r = await timed("T4: explore climate_stations",
        explore_domain(client=client, registry=registry, domain="climate_stations"))
    results.append(r)

    # T5: Explore fisheries
    r = await timed("T5: explore fisheries",
        explore_domain(client=client, registry=registry, domain="fisheries"))
    results.append(r)

    # T6: Invalid domain
    r = await timed("T6: explore invalid domain",
        explore_domain(client=client, registry=registry, domain="nonexistent"))
    results.append(r)

    # T7: Preview a known crop resource (first from Beja)
    beja_resources = registry.get_domain_resources("crop_production", gouvernorat="Beja")
    if beja_resources:
        rid = beja_resources[0]["id"]
        r = await timed(f"T7: preview crop resource {rid[:12]}...",
            get_resource_preview(client=client, registry=registry, resource_id=rid))
        results.append(r)

    # T8: Preview a climate resource
    climate_resources = registry.get_domain_resources("climate_stations")
    if climate_resources:
        rid = climate_resources[0]["id"]
        r = await timed(f"T8: preview climate resource {rid[:12]}...",
            get_resource_preview(client=client, registry=registry, resource_id=rid))
        results.append(r)

    # T9: Preview invalid resource
    r = await timed("T9: preview invalid resource",
        get_resource_preview(client=client, registry=registry, resource_id="nonexistent-id"))
    results.append(r)

    await client.close()

    # Summary
    print("\n\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    for r in results:
        err = f" ERROR: {r.get('error', '')}" if r.get("error") else ""
        print(f"  {r['label']:55s}  {r['elapsed']:5.1f}s  {r['chars']:6d} chars{err}")
    total = sum(r["elapsed"] for r in results)
    print(f"\n  {'TOTAL':55s}  {total:5.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
