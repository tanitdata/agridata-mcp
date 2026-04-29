#!/usr/bin/env python3
"""Regenerate value_hints.json directly from the snapshot Parquet files.

v3.1.1. The original extractor (scripts/extract_value_hints.py) pulled hints
from `vocabulary_raw.json`, which was extracted from the live CKAN API in
April 2026. That source:

- Accent-strips column names ("Delegation") where openpyxl-built Parquet
  preserves diacritics ("Délégation") — handled at runtime today by an
  accent-folded fallback in SchemaRegistry.get_column_hints, but better
  to avoid the mismatch at the source.
- Can drift from the Parquet snapshot (different vintages of the same
  resource).
- Covers resources the snapshot doesn't, and vice versa.

This script reads every *.parquet under snapshot/parquet/ and emits hints
with the same shape and conventions as the original, keyed by the column
names that actually appear in query results.

Categorical rule — a column qualifies when it has ≥ 2 distinct non-empty
values, ≤ 50 distinct non-empty values, AND < 50 % of values parse as
numeric (so columns like "2020" in a wide-format resource, or misaligned
CSVs whose "Gouvernorat" column holds production numbers, don't pollute
the hints).

Usage:
    python scripts/extract_value_hints_from_parquet.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "snapshot" / "parquet"
OUTPUT = ROOT / "value_hints.json"

MAX_VALUES = 50
MIN_VALUES = 2
MAX_DISTINCT_FOR_CATEGORICAL = 50
NUMERIC_THRESHOLD = 0.5  # column disqualified when >= this fraction of values are numbers

# Columns that are never worth hinting on — internal, or known to be noise.
SKIP_COLS = {"_id", "_full_text"}

# Numeric regex — integer or decimal, optionally signed
_NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def numeric_fraction(values: list[str]) -> float:
    """Fraction of values that parse as plain numbers (for noise filtering)."""
    if not values:
        return 0.0
    hits = sum(1 for v in values if _NUM_RE.match(v))
    return hits / len(values)


def extract_hints_for_file(con, parquet_path: Path) -> dict[str, list[str]]:
    """Return {column_name: [sorted_values]} for categorical columns in one parquet."""
    rid = parquet_path.stem
    safe_path = str(parquet_path).replace("\\", "/").replace("'", "''")

    # Get column list + distinct counts in one pass
    try:
        cols_info = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{safe_path}')"
        ).fetchall()
    except Exception:
        return {}

    hints: dict[str, list[str]] = {}
    for row in cols_info:
        col_name = row[0]
        if col_name in SKIP_COLS:
            continue

        try:
            vals = con.execute(
                f"""
                SELECT DISTINCT "{col_name}"
                FROM read_parquet('{safe_path}')
                WHERE "{col_name}" IS NOT NULL
                  AND "{col_name}" != ''
                """
            ).fetchall()
        except Exception:
            continue

        distinct = [r[0] for r in vals if r[0] is not None]
        distinct = [str(v).strip() for v in distinct if str(v).strip()]

        n = len(distinct)
        if n < MIN_VALUES or n > MAX_DISTINCT_FOR_CATEGORICAL:
            continue
        if numeric_fraction(distinct) >= NUMERIC_THRESHOLD:
            continue

        hints[col_name] = sorted(distinct)[:MAX_VALUES]

    return hints


def main() -> int:
    if not PARQUET_DIR.exists():
        print(f"ERROR: {PARQUET_DIR} not found. Run scripts/build_snapshot.py first.")
        return 1

    parquet_files = sorted(PARQUET_DIR.glob("*.parquet"))
    print(f"Scanning {len(parquet_files)} parquet files...")

    con = duckdb.connect(":memory:")
    resource_values: dict[str, dict[str, list[str]]] = {}
    total_resources = 0
    total_columns = 0
    total_values = 0

    for i, pq in enumerate(parquet_files, 1):
        if i % 100 == 0:
            print(f"  {i}/{len(parquet_files)}")
        rid = pq.stem
        hints = extract_hints_for_file(con, pq)
        if hints:
            resource_values[rid] = hints
            total_resources += 1
            total_columns += len(hints)
            total_values += sum(len(v) for v in hints.values())

    output = {
        "meta": {
            "description": "Per-resource categorical values for query response enrichment",
            "source": "snapshot/parquet/ (v3.1.1+)",
            "total_resources": total_resources,
            "total_columns": total_columns,
            "total_values": total_values,
            "max_values_per_column": MAX_VALUES,
        },
        "resource_values": resource_values,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = OUTPUT.stat().st_size / 1024
    print()
    print(f"Written {OUTPUT.name}: {size_kb:.0f} KB")
    print(f"  {total_resources} resources, {total_columns} columns, {total_values} values")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
