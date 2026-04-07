"""Test rainfall domain integration in query_climate_stations."""

import asyncio
import time
import sys

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.climate import query_climate_stations


async def run_test(name, **kwargs):
    """Run a single test and print results."""
    print(f"\n{'='*70}")
    print(f"TEST: {name}")
    print(f"Args: {kwargs}")
    print(f"{'='*70}")
    t0 = time.perf_counter()
    try:
        result = await query_climate_stations(client=client, registry=registry, **kwargs)
        elapsed = time.perf_counter() - t0
        # Truncate for display
        display = result[:3000] if len(result) > 3000 else result
        try:
            print(display)
        except UnicodeEncodeError:
            print(display.encode("ascii", errors="replace").decode())
        if len(result) > 3000:
            print(f"\n... [{len(result) - 3000} chars truncated]")
        print(f"\n[Time: {elapsed:.2f}s | Length: {len(result)} chars]")

        # Check key indicators
        has_pluviometry = "Pluviometry" in result or "pluviom" in result.lower() or "Historical" in result
        has_rainfall_data = "precipit" in result.lower() or "pluviometrie" in result.lower() or "cumul" in result.lower()
        has_source = "catalog.agridata.tn" in result
        print(f"[Has pluviometry section: {has_pluviometry}]")
        print(f"[Has rainfall data fields: {has_rainfall_data}]")
        print(f"[Has source URL: {has_source}]")
        return result
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"ERROR: {type(e).__name__}: {e}")
        print(f"[Time: {elapsed:.2f}s]")
        return None


async def main():
    global client, registry
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()

    # Seed live layer
    print("Refreshing registry...")
    # Force a refresh by resetting the timestamp
    registry._last_refresh = 0.0
    await registry.maybe_refresh(client)
    print("Registry ready.\n")

    # T1: Siliana + pluie — should return CRDA pluviometry data
    r1 = await run_test("T1: Siliana pluie", parameter="pluie", station="Siliana")

    # T2: Ben Arous + rainfall — should return Ben Arous monthly rainfall
    r2 = await run_test("T2: Ben Arous rainfall", parameter="rainfall", station="Ben Arous")

    # T3: Bizerte + wind speed — should NOT include rainfall resources
    r3 = await run_test("T3: Bizerte wind (no rainfall)", parameter="wind speed", station="Bizerte")

    # T4: Bizerte + précipitations — should return sensor precipitation data
    # (Bizerte has no separate CRDA pluviometry DataStore resources — only EAV sensor data)
    r4 = await run_test("T4: Bizerte precipitations (sensor)", parameter="précipitations", station="Bizerte")

    # T5: Sfax + pluie — should return clean "no data" message, not crash
    r5 = await run_test("T5: Sfax pluie (no data)", parameter="pluie", station="Sfax")

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    results = {
        "T1 Siliana pluie": r1 and ("Historical" in r1 or "pluviom" in r1.lower()),
        "T2 Ben Arous rainfall": r2 and ("Historical" in r2 or "pluviom" in r2.lower()),
        "T3 Bizerte wind (no rainfall)": r3 and "Historical" not in r3 and "Pluviometry" not in r3,
        "T4 Bizerte precip (sensor)": r4 and ("recipit" in r4.lower() or "pluie" in r4.lower()),
        "T5 Sfax pluie (no crash)": r5 is not None,
    }
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {status}: {name}")

    passed = sum(1 for v in results.values() if v)
    print(f"\n{passed}/{len(results)} tests passed")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
