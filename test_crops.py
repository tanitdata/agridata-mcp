"""Benchmark tests for query_crop_production against the live portal."""

import asyncio
import time

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.crops import query_crop_production


async def timed(label, coro):
    t0 = time.perf_counter()
    try:
        result = await coro
        elapsed = time.perf_counter() - t0
        print(f"\n{'='*80}")
        print(f"[{label}] — {elapsed:.1f}s")
        print("=" * 80)
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

    # Q1: Inventory (no args)
    r = await timed("Q1: Crop production inventory",
        query_crop_production(client=client, registry=registry))
    results.append(r)

    # Q2: Cereal production in Béja
    r = await timed("Q2: Cereals in Béja",
        query_crop_production(client=client, registry=registry,
            crop_type="cereales", gouvernorat="Béja"))
    results.append(r)

    # Q3: Olive production in Tunis governorate
    r = await timed("Q3: Olives in Tunis",
        query_crop_production(client=client, registry=registry,
            crop_type="olives", gouvernorat="Tunis"))
    results.append(r)

    # Q4: Compare cereal production Béja vs Jendouba vs Kef
    r = await timed("Q4: Cereals — Béja vs Jendouba vs Kef",
        query_crop_production(client=client, registry=registry,
            crop_type="cereales", gouvernorat="Béja, Jendouba, Kef"))
    results.append(r)

    # Q5: Fruit tree area in Nabeul
    r = await timed("Q5: Arboriculture area in Nabeul",
        query_crop_production(client=client, registry=registry,
            crop_type="arboriculture", gouvernorat="Nabeul",
            metric="superficie"))
    results.append(r)

    # Q6: Vegetable production across all governorates
    r = await timed("Q6: Vegetables — all governorates",
        query_crop_production(client=client, registry=registry,
            crop_type="vegetables"))
    results.append(r)

    # Q7: Cereal yield in Siliana
    r = await timed("Q7: Cereal yield in Siliana",
        query_crop_production(client=client, registry=registry,
            crop_type="cereales", gouvernorat="Siliana",
            metric="yield"))
    results.append(r)

    # Q8: DGEDA national cereal data (no governorate)
    r = await timed("Q8: National cereal production (DGEDA)",
        query_crop_production(client=client, registry=registry,
            crop_type="cereales"))
    results.append(r)

    # Q9: Fodder production in Kasserine
    r = await timed("Q9: Fodder in Kasserine",
        query_crop_production(client=client, registry=registry,
            crop_type="fodder", gouvernorat="Kasserine"))
    results.append(r)

    # Q10: Nonexistent governorate
    r = await timed("Q10: Nonexistent — Sousse olives",
        query_crop_production(client=client, registry=registry,
            crop_type="olives", gouvernorat="Sousse"))
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
