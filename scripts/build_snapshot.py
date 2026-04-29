#!/usr/bin/env python3
"""Build the offline snapshot: convert scraped tabular files to Parquet.

Reads the scrape directory (agridata_TN_Scraped_26-11-2026/ by default) and
audit_full.json, and writes one <resource_uuid>.parquet per DataStore-active
resource into snapshot/parquet/. A sidecar snapshot/scrape_index.json maps
every resource UUID (DataStore or not) to its source file path relative to
the scrape root, so read_resource can serve non-DataStore files offline.

Input-format detection uses magic bytes, not file extension — the scrape has
10 .csv files that are actually XLSX and 143 .xls files that are actually
XLSX. All files are read as text (dtype=str) to match CKAN's "all fields are
text" invariant.

Usage:
    python scripts/build_snapshot.py
    python scripts/build_snapshot.py --scrape-root /path/to/scrape --out snapshot/
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCRAPE = ROOT / "agridata_TN_Scraped_26-11-2026"
DEFAULT_AUDIT = ROOT / "audit_full.json"
DEFAULT_OUT = ROOT / "snapshot"

PREFIX_RE = re.compile(r"^([0-9a-f]{8})_")
TABULAR_FOLDERS = ("format_csv", "format_api", "format_xlsx", "format_xls")

# Matches the scrape folder convention `agridata_TN_Scraped_DD-MM-YYYY`
_SCRAPE_DATE_RE = re.compile(r"Scraped_(\d{2})-(\d{2})-(\d{4})")


def derive_snapshot_date(scrape_root: Path) -> str | None:
    """Infer the snapshot date from the scrape folder name.

    Expected folder format: `agridata_TN_Scraped_DD-MM-YYYY`. Returns an
    ISO `YYYY-MM-DD` string, or None if the name doesn't match.
    """
    m = _SCRAPE_DATE_RE.search(scrape_root.name)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return f"{yyyy}-{mm}-{dd}"


def sniff_format(path: Path) -> str:
    """Return 'xlsx', 'xls_ole', or 'csv' based on magic bytes."""
    with open(path, "rb") as f:
        sig = f.read(8)
    if sig[:4] == b"PK\x03\x04":
        return "xlsx"
    if sig[:4] == b"\xd0\xcf\x11\xe0":
        return "xls_ole"
    return "csv"


def read_tabular(path: Path):
    """Return a pandas DataFrame (all columns as str), or None on failure."""
    import pandas as pd

    fmt = sniff_format(path)
    try:
        if fmt == "xlsx":
            return pd.read_excel(path, engine="openpyxl", dtype=str)
        if fmt == "xls_ole":
            return pd.read_excel(path, engine="xlrd", dtype=str)
        return pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="skip")
    except Exception:
        return None


def build_prefix_index(scrape_root: Path) -> dict[str, Path]:
    """Map 8-hex prefix → source file path (all format folders)."""
    index: dict[str, Path] = {}
    for folder in os.listdir(scrape_root):
        folder_path = scrape_root / folder
        if not folder_path.is_dir():
            continue
        for fname in os.listdir(folder_path):
            m = PREFIX_RE.match(fname)
            if m:
                index[m.group(1)] = folder_path / fname
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scrape-root", type=Path, default=DEFAULT_SCRAPE)
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--snapshot-date",
        default=None,
        help=(
            "ISO date (YYYY-MM-DD) stamped into the snapshot index. "
            "Overrides the value derived from the scrape folder name. "
            "Use this when the folder name's date is wrong."
        ),
    )
    args = parser.parse_args()

    if not args.scrape_root.exists():
        print(f"ERROR: scrape root not found: {args.scrape_root}", file=sys.stderr)
        return 1
    if not args.audit.exists():
        print(f"ERROR: audit file not found: {args.audit}", file=sys.stderr)
        return 1

    parquet_dir = args.out / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    with open(args.audit, encoding="utf-8") as f:
        audit = json.load(f)

    prefix_to_file = build_prefix_index(args.scrape_root)
    print(f"Indexed {len(prefix_to_file)} files by 8-hex prefix")

    datastore_rids = {r["id"] for r in audit["resources"] if r.get("datastore_active")}
    print(f"DataStore-active resources in audit: {len(datastore_rids)}")

    # scrape_index maps every resource UUID to its source path (relative to scrape root)
    scrape_index: dict[str, str] = {}
    for res in audit["resources"]:
        rid = res["id"]
        src = prefix_to_file.get(rid[:8])
        if src:
            scrape_index[rid] = str(src.relative_to(args.scrape_root)).replace("\\", "/")

    # Convert every DS-active resource that has a source file
    converted = 0
    skipped_no_file = 0
    skipped_non_tabular = 0
    failed = 0
    empty = 0

    for rid in sorted(datastore_rids):
        src = prefix_to_file.get(rid[:8])
        if not src:
            skipped_no_file += 1
            continue
        # Only convert if the source file is in a tabular format folder
        if src.parent.name not in TABULAR_FOLDERS:
            skipped_non_tabular += 1
            continue
        out_path = parquet_dir / f"{rid}.parquet"
        if out_path.exists():
            converted += 1
            continue

        df = read_tabular(src)
        if df is None:
            failed += 1
            continue
        if len(df) == 0:
            empty += 1
            continue
        # Skip Excel overflow artifacts (1,048,575 rows is the Excel row cap)
        if len(df) >= 1_000_000:
            failed += 1
            print(f"  skipped overflow: {rid} ({len(df)} rows)")
            continue

        try:
            df.to_parquet(out_path, compression="zstd", index=False)
            converted += 1
        except Exception as e:
            failed += 1
            print(f"  failed {rid}: {str(e)[:80]}")

    # Write scrape_index.json
    snapshot_date = args.snapshot_date or derive_snapshot_date(args.scrape_root)
    with open(args.out / "scrape_index.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "meta": {
                    "scrape_root": str(args.scrape_root).replace("\\", "/"),
                    "scrape_folder": args.scrape_root.name,
                    "snapshot_date": snapshot_date,
                    "audit_date": audit.get("meta", {}).get("audit_date"),
                    "total_resources": len(audit["resources"]),
                    "resources_with_files": len(scrape_index),
                },
                "resources": scrape_index,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    total_parquet_bytes = sum(p.stat().st_size for p in parquet_dir.glob("*.parquet"))
    print()
    print(f"Converted: {converted}")
    print(f"Skipped (no file in scrape): {skipped_no_file}")
    print(f"Skipped (non-tabular format): {skipped_non_tabular}")
    print(f"Empty: {empty}")
    print(f"Failed: {failed}")
    print(f"Parquet output: {parquet_dir} ({total_parquet_bytes/1e6:.2f} MB)")
    print(f"Scrape index: {args.out / 'scrape_index.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
