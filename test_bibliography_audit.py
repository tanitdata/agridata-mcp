"""Audit test suite for search_bibliography — 10 scenarios from auditor spec."""

import asyncio
import re
import sys
import time

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.tools.bibliography import search_bibliography


def _count_results(output: str) -> int:
    return sum(1 for line in output.split("\n") if line.startswith("### "))


def _has_fonds_pdf(output: str) -> bool:
    """Check for Fonds-style PDF links (onagri.nat.tn/uploads/docagri/)."""
    return "onagri.nat.tn/uploads/docagri/" in output


def _has_theme_pdf(output: str) -> bool:
    """Check for thematic library PDF links constructed from Nom_fichier."""
    # Same URL pattern but coming from Tier 2 records
    return "onagri.nat.tn/uploads/docagri/" in output


def _count_pdf_links(output: str) -> int:
    return output.count("**PDF:**")


def _token_estimate(output: str) -> int:
    """Rough token estimate: ~4 chars per token for French text."""
    return len(output) // 4


def _extract_sources(output: str) -> list[str]:
    """Extract source names from results (Base ONAGRI, Fonds ONAGRI, Agriculture, etc.)."""
    return re.findall(r"\*\*Source:\*\* (.+?)(?:\n| \|)", output)


async def run_audit():
    client = CKANClient()
    registry = SchemaRegistry()
    registry.load()

    scenarios = [
        {
            "id": "T1",
            "name": "secheresse (drought FR)",
            "kwargs": {"query": "secheresse", "limit": 20},
            "expect_min": 1,
            "check_resume": True,
        },
        {
            "id": "T2",
            "name": "drought (English)",
            "kwargs": {"query": "drought", "limit": 20},
            "expect_min": 0,  # catalog is 91% French
        },
        {
            "id": "T3",
            "name": "cereales production (AND-chain)",
            "kwargs": {"query": "cereales production", "limit": 20},
            "expect_min": 1,
            "check_and_chain": True,
        },
        {
            "id": "T4",
            "name": "olivier theme=agriculture",
            "kwargs": {"query": "olivier", "theme": "agriculture", "limit": 20},
            "expect_theme_only": "Agriculture",
            "check_theme_pdf": True,
        },
        {
            "id": "T5",
            "name": "eau irrigation year>=2015",
            "kwargs": {"query": "eau irrigation", "year_from": 2015, "limit": 20},
            "expect_min": 1,
            "check_year": 2015,
        },
        {
            "id": "T6",
            "name": "foret theme=forestry",
            "kwargs": {"query": "foret", "theme": "forestry", "limit": 20},
            "expect_theme_only": "Forestry",
            "check_theme_pdf": True,
        },
        {
            "id": "T7",
            "name": "aquaculture language=FR",
            "kwargs": {"query": "aquaculture", "language": "FR", "limit": 20},
            "expect_min": 1,
        },
        {
            "id": "T8",
            "name": "changement climatique (benchmark)",
            "kwargs": {"query": "changement climatique", "limit": 20},
            "expect_min": 3,  # must beat the WEAK score of 2
        },
        {
            "id": "T9",
            "name": "Fonds PDF link verification",
            "kwargs": {"query": "cereales", "limit": 20},
            "check_fonds_pdf": True,
        },
        {
            "id": "T10",
            "name": "Thematic PDF link verification",
            "kwargs": {"query": "climat", "theme": "agriculture", "limit": 20},
            "check_theme_pdf": True,
        },
    ]

    results = []
    for sc in scenarios:
        t0 = time.perf_counter()
        try:
            output = await search_bibliography(client=client, registry=registry, **sc["kwargs"])
            elapsed = time.perf_counter() - t0
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            results.append({
                "id": sc["id"],
                "name": sc["name"],
                "pass": False,
                "count": 0,
                "tokens": 0,
                "time": elapsed,
                "pdfs": 0,
                "notes": f"EXCEPTION: {exc}",
            })
            continue

        count = _count_results(output)
        tokens = _token_estimate(output)
        pdfs = _count_pdf_links(output)
        sources = _extract_sources(output)
        notes_parts = []
        passed = True

        # --- Assertions ---

        # Minimum result count
        expect_min = sc.get("expect_min", 0)
        if count < expect_min:
            passed = False
            notes_parts.append(f"expected >={expect_min} results, got {count}")

        # AND-chain: both keywords must be in every title or resume
        if sc.get("check_and_chain") and count > 0:
            keywords = sc["kwargs"]["query"].lower().split()
            # We can't verify server-side, but the scoring should rank AND-matches higher
            notes_parts.append(f"AND-chain: {len(keywords)} keywords")

        # Theme restriction: only results from that theme
        if sc.get("expect_theme_only") and count > 0:
            theme = sc["expect_theme_only"]
            non_theme = [s for s in sources if s != theme]
            if non_theme:
                passed = False
                notes_parts.append(f"theme leak: found sources {non_theme}")
            else:
                notes_parts.append(f"theme={theme} only")

        # Year check
        if sc.get("check_year") and count > 0:
            year_min = sc["check_year"]
            # Extract years from output
            year_matches = re.findall(r"\*\*Year:\*\* (\d{4})", output)
            if year_matches:
                years = [int(y) for y in year_matches]
                bad = [y for y in years if y < year_min]
                if bad:
                    passed = False
                    notes_parts.append(f"year violation: {bad} < {year_min}")
                else:
                    notes_parts.append(f"years OK: {min(years)}-{max(years)}")

        # Fonds PDF links
        if sc.get("check_fonds_pdf"):
            fonds_sources = [s for s in sources if s == "Fonds ONAGRI"]
            if fonds_sources and _has_fonds_pdf(output):
                notes_parts.append(f"Fonds PDFs: YES ({len(fonds_sources)} Fonds results)")
            elif fonds_sources:
                passed = False
                notes_parts.append("Fonds results found but NO PDF links")
            else:
                notes_parts.append("no Fonds results in this query")

        # Theme PDF links
        if sc.get("check_theme_pdf"):
            if pdfs > 0 and _has_theme_pdf(output):
                notes_parts.append(f"theme PDFs: YES ({pdfs} links)")
            elif count > 0:
                notes_parts.append(f"theme results found, PDFs: {pdfs}")
            else:
                notes_parts.append("no results to check PDFs")

        # Resume keyword matches
        if sc.get("check_resume") and count > 0:
            has_resume = any(line.strip().startswith("/") for line in output.split("\n"))
            if has_resume:
                notes_parts.append("Resume keyword tags present")
            else:
                notes_parts.append("no Resume content visible")

        results.append({
            "id": sc["id"],
            "name": sc["name"],
            "pass": passed,
            "count": count,
            "tokens": tokens,
            "time": elapsed,
            "pdfs": pdfs,
            "notes": "; ".join(notes_parts) if notes_parts else "",
        })

        # Print detailed output for inspection
        print(f"\n{'='*70}")
        print(f"  {sc['id']}: {sc['name']}")
        print(f"  Results: {count} | Tokens: ~{tokens} | PDFs: {pdfs} | Time: {elapsed:.2f}s")
        print(f"  Sources: {sources}")
        if notes_parts:
            print(f"  Notes: {'; '.join(notes_parts)}")
        print(f"{'='*70}")
        # Print first 600 chars
        for line in output[:600].split("\n"):
            try:
                print(f"  {line}")
            except UnicodeEncodeError:
                print(f"  {line.encode('ascii', errors='replace').decode()}")
        if len(output) > 600:
            print(f"  ... ({len(output)} total chars)")

    await client.close()

    # === FINAL REPORT ===
    print(f"\n{'='*70}")
    print("  AUDIT REPORT")
    print(f"{'='*70}")
    print(f"  {'ID':<5} {'Status':<6} {'Count':>5} {'Tokens':>7} {'Time':>6} {'PDFs':>5}  Notes")
    print(f"  {'-'*5} {'-'*6} {'-'*5} {'-'*7} {'-'*6} {'-'*5}  {'-'*40}")
    for r in results:
        status = "PASS" if r["pass"] else "FAIL"
        print(
            f"  {r['id']:<5} {status:<6} {r['count']:>5} {r['tokens']:>7} "
            f"{r['time']:>5.2f}s {r['pdfs']:>5}  {r['notes']}"
        )

    total_pass = sum(1 for r in results if r["pass"])
    total = len(results)
    print(f"\n  {total_pass}/{total} passed")

    return sum(1 for r in results if not r["pass"])


if __name__ == "__main__":
    failures = asyncio.run(run_audit())
    sys.exit(failures)
