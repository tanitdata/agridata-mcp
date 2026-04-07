"""Live integration test for search_bibliography tool."""

import asyncio
import sys
import time

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.bibliography import search_bibliography


async def run_tests():
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()

    scenarios = [
        {
            "name": "Q1: Broad keyword - cereales",
            "kwargs": {"query": "cereales", "limit": 5},
        },
        {
            "name": "Q2: Multi-keyword - olive Sfax",
            "kwargs": {"query": "olive Sfax", "limit": 5},
        },
        {
            "name": "Q3: Year filter - irrigation 2015-2020",
            "kwargs": {"query": "irrigation", "year_from": 2015, "year_to": 2020, "limit": 5},
        },
        {
            "name": "Q4: Language filter - Arabic",
            "kwargs": {"query": "cereales", "language": "AR", "limit": 5},
        },
        {
            "name": "Q5: Theme filter - fisheries",
            "kwargs": {"query": "thon", "theme": "fisheries", "limit": 5},
        },
        {
            "name": "Q6: Tiered execution - foret incendie",
            "kwargs": {"query": "foret incendie", "limit": 30},
        },
        {
            "name": "Q7: No results",
            "kwargs": {"query": "xyznonexistent12345", "limit": 5},
        },
        {
            "name": "Q8: Year range only",
            "kwargs": {"query": "", "year_from": 2023, "year_to": 2024, "limit": 5},
        },
        {
            "name": "Q9: Theme agriculture - climat",
            "kwargs": {"query": "climat", "theme": "agriculture", "limit": 5},
        },
        {
            "name": "Q10: Tier 2 cache hit (repeat agriculture)",
            "kwargs": {"query": "eau", "theme": "agriculture", "limit": 5},
        },
    ]

    results = []
    for sc in scenarios:
        print(f"\n{'='*60}")
        print(f"  {sc['name']}")
        print(f"{'='*60}")
        t0 = time.perf_counter()
        try:
            output = await search_bibliography(client=client, registry=registry, **sc["kwargs"])
            elapsed = time.perf_counter() - t0
            # Count results (lines starting with ###)
            count = sum(1 for line in output.split("\n") if line.startswith("### "))
            print(f"  Results: {count} | Time: {elapsed:.2f}s")
            # Print first 500 chars (safe for Windows console)
            preview = output[:500]
            for line in preview.split("\n"):
                try:
                    print(f"  {line}")
                except UnicodeEncodeError:
                    print(f"  {line.encode('ascii', errors='replace').decode()}")
            if len(output) > 500:
                print(f"  ... ({len(output)} total chars)")
            results.append({"name": sc["name"], "count": count, "time": elapsed, "ok": True})
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            print(f"  ERROR: {exc}")
            results.append({"name": sc["name"], "count": 0, "time": elapsed, "ok": False})

    await client.close()

    # Summary
    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    for r in results:
        status = "PASS" if r["ok"] else "FAIL"
        print(f"  [{status}] {r['name']} -> {r['count']} results in {r['time']:.2f}s")

    failures = sum(1 for r in results if not r["ok"])
    print(f"\n  {len(results) - failures}/{len(results)} passed")
    return failures


if __name__ == "__main__":
    failures = asyncio.run(run_tests())
    sys.exit(failures)
