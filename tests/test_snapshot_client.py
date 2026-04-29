"""Tests for the SnapshotClient (DuckDB over Parquet) and the `~` translator."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from tanitdata.ckan_client import (
    LiveClient,
    SnapshotClient,
    make_client,
    translate_postgres_tilde,
)

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = ROOT / "snapshot" / "parquet"

# A benchmark resource known to exist in the snapshot (from the inventory pass).
BENCHMARK_RID = "505db350-d619-4f36-9937-a26209ab9b79"  # Kairouan cereal production


# ---------------------------------------------------------------------------
# translator unit tests
# ---------------------------------------------------------------------------


def test_tilde_translates_double_quoted_ident():
    sql = "SELECT * FROM t WHERE \"valeur\" ~ '^-?[0-9.]+$'"
    out = translate_postgres_tilde(sql)
    assert "regexp_matches(\"valeur\", '^-?[0-9.]+$')" in out
    assert " ~ " not in out


def test_tilde_translates_bare_ident():
    sql = "SELECT * FROM t WHERE annee ~ '^\\d{4}$'"
    out = translate_postgres_tilde(sql)
    assert "regexp_matches(annee, '^\\d{4}$')" in out


def test_tilde_translates_qualified_ident():
    sql = 'SELECT * FROM t a WHERE a."col" ~ \'^x$\''
    out = translate_postgres_tilde(sql)
    assert "regexp_matches(a.\"col\", '^x$')" in out


def test_tilde_inside_string_literal_is_preserved():
    # A `~` inside a single-quoted literal must NOT be rewritten.
    sql = "SELECT * FROM t WHERE \"col\" = 'a ~ b'"
    out = translate_postgres_tilde(sql)
    assert out == sql  # unchanged


def test_tilde_with_escaped_quote_in_literal():
    # Doubled single quotes inside a literal should not break the pre-scan
    sql = "SELECT * FROM t WHERE \"col\" = 'it''s fine' AND \"n\" ~ '^\\d+$'"
    out = translate_postgres_tilde(sql)
    assert "it''s fine" in out
    assert "regexp_matches(\"n\", '^\\d+$')" in out


def test_no_tilde_no_change():
    sql = "SELECT \"a\", \"b\" FROM t WHERE \"a\" = '1'"
    assert translate_postgres_tilde(sql) == sql


def test_tilde_preserves_multiple_occurrences():
    sql = (
        'SELECT * FROM t '
        'WHERE "a" ~ \'^x$\' AND "b" ~ \'^y$\''
    )
    out = translate_postgres_tilde(sql)
    assert out.count("regexp_matches") == 2
    assert " ~ " not in out


# ---------------------------------------------------------------------------
# factory tests
# ---------------------------------------------------------------------------


def test_factory_default_is_snapshot(monkeypatch):
    monkeypatch.delenv("DATA_SOURCE", raising=False)
    c = make_client()
    assert isinstance(c, SnapshotClient)


def test_factory_live(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE", "live")
    c = make_client()
    assert isinstance(c, LiveClient)


def test_factory_explicit_snapshot():
    c = make_client("snapshot")
    assert isinstance(c, SnapshotClient)


def test_factory_rejects_unknown():
    with pytest.raises(ValueError):
        make_client("bogus")


# ---------------------------------------------------------------------------
# SnapshotClient query tests — require the built snapshot
# ---------------------------------------------------------------------------


_snapshot_available = pytest.mark.skipif(
    not SNAPSHOT_DIR.exists() or not list(SNAPSHOT_DIR.glob("*.parquet")),
    reason=f"Snapshot not built: run `python scripts/build_snapshot.py` (expected in {SNAPSHOT_DIR})",
)


def _raw_scrape_present() -> bool:
    """True when the raw scrape folder (gitignored, 268 MB) is on disk.

    The Parquet snapshot and scrape_index sidecar are committed; the raw
    source files (XLSX/CSV/PDF) under agridata_TN_Scraped_*/ are not.
    `download_file` reads from those raw files, so its tests can only
    run where the scrape folder is actually available (developer
    machines, not CI).
    """
    idx_path = ROOT / "snapshot" / "scrape_index.json"
    if not idx_path.exists():
        return False
    try:
        import json

        with open(idx_path, encoding="utf-8") as f:
            idx = json.load(f)
        scrape_root = Path(idx.get("meta", {}).get("scrape_root", ""))
        resources = idx.get("resources", {})
        if not resources:
            return False
        first_rel = next(iter(resources.values()))
        return (scrape_root / first_rel).exists()
    except Exception:
        return False


_scrape_available = pytest.mark.skipif(
    not _raw_scrape_present(),
    reason=(
        "raw scrape folder not on disk (CI / fresh clone). "
        "download_file tests read bytes from the gitignored scrape."
    ),
)


@_snapshot_available
def test_snapshot_datastore_search_returns_benchmark_rows():
    client = SnapshotClient()
    result = asyncio.run(client.datastore_search(BENCHMARK_RID, limit=5))
    assert result is not None
    assert result["total"] == 70
    assert len(result["records"]) == 5
    assert result["fields"]
    # Fields are reported as type=text to match CKAN's invariant
    assert all(f["type"] == "text" for f in result["fields"])


@_snapshot_available
def test_snapshot_datastore_search_unknown_resource_returns_none():
    client = SnapshotClient()
    result = asyncio.run(
        client.datastore_search("00000000-0000-0000-0000-000000000000", limit=5)
    )
    assert result is None


@_snapshot_available
def test_snapshot_datastore_search_respects_filters():
    client = SnapshotClient()
    # The resource has a Delegation column. Filter on a known value.
    result = asyncio.run(
        client.datastore_search(BENCHMARK_RID, limit=100)
    )
    assert result is not None
    # Pick the first delegation from the result and re-query with a filter
    first_col = result["fields"][1]["id"] if len(result["fields"]) > 1 else None
    if first_col and result["records"]:
        probe_val = result["records"][0].get(first_col)
        if probe_val:
            filtered = asyncio.run(
                client.datastore_search(
                    BENCHMARK_RID, filters={first_col: probe_val}, limit=100
                )
            )
            assert filtered is not None
            # Every returned row must match the filter
            assert all(r.get(first_col) == probe_val for r in filtered["records"])


@_snapshot_available
def test_snapshot_datastore_sql_basic_select():
    client = SnapshotClient()
    sql = f'SELECT COUNT(*) AS n FROM "{BENCHMARK_RID}"'
    result = asyncio.run(client.datastore_sql(sql))
    assert result is not None
    assert result["records"][0]["n"] == 70


@_snapshot_available
def test_snapshot_datastore_sql_with_tilde_regex():
    """The ~ operator is the regression surface for the DuckDB migration."""
    client = SnapshotClient()
    # Pick a column known to contain year-like strings? The benchmark resource
    # has `Annee` per the inventory. Guard against non-numeric rows.
    sql = (
        f'SELECT COUNT(*) AS n FROM "{BENCHMARK_RID}" '
        f'WHERE "Annee" ~ \'^\\d{{4}}$\''
    )
    result = asyncio.run(client.datastore_sql(sql))
    assert result is not None
    # Don't assert a specific count (the benchmark resource may have blank
    # years); just that the query ran and returned a numeric
    assert isinstance(result["records"][0]["n"], int)


@_snapshot_available
def test_snapshot_table_metadata_enumerates_resources():
    """Schema registry relies on this for live refresh."""
    client = SnapshotClient()
    result = asyncio.run(client.datastore_search("_table_metadata", limit=5000))
    assert result is not None
    assert result["total"] > 100  # well more than 100 DS resources expected
    assert result["records"]
    # Every record has the expected shape
    for rec in result["records"][:5]:
        assert "name" in rec
        assert rec["alias_of"] is None


@_scrape_available
def test_download_file_by_resource_id_returns_bytes():
    client = SnapshotClient()
    # An XLSX non-DataStore resource known to be in the scrape
    rid = "50a4fb83-860f-43bc-968e-f6e47b2ec735"
    data = asyncio.run(client.download_file("", resource_id=rid))
    assert isinstance(data, bytes)
    assert len(data) > 100
    # Magic bytes — every real XLSX starts with PK\x03\x04
    assert data[:4] == b"PK\x03\x04"


@_scrape_available
def test_download_file_parses_resource_id_from_url():
    client = SnapshotClient()
    # Construct a portal-style URL for a resource we know is cached
    rid = "50a4fb83-860f-43bc-968e-f6e47b2ec735"
    url = (
        f"https://catalog.agridata.tn/dataset/any-id/resource/{rid}/"
        f"download/flottille.xlsx"
    )
    data = asyncio.run(client.download_file(url))
    assert isinstance(data, bytes)
    assert data[:4] == b"PK\x03\x04"


@_snapshot_available
def test_download_file_returns_none_for_unknown_resource():
    client = SnapshotClient()
    result = asyncio.run(
        client.download_file(
            "", resource_id="00000000-0000-0000-0000-000000000000"
        )
    )
    assert result is None


@_scrape_available
def test_download_file_respects_size_cap(tmp_path, monkeypatch):
    client = SnapshotClient()
    # Pick a resource known to be in the scrape, then set max_bytes below
    # the on-disk size. 50a4fb83 is ~10 KB, so 100 bytes cap will block it.
    rid = "50a4fb83-860f-43bc-968e-f6e47b2ec735"
    result = asyncio.run(client.download_file("", max_bytes=100, resource_id=rid))
    assert result is None


def test_sniff_format():
    from tanitdata.tools.resource_reader import _sniff_format

    assert _sniff_format(b"PK\x03\x04rest-of-xlsx") == "xlsx"
    assert _sniff_format(b"\xd0\xcf\x11\xe0rest-of-xls") == "xls_ole"
    assert _sniff_format(b"date,nom,valeur\n2025-01-01,a,1\n") == "csv"
    assert _sniff_format(b"") == "csv"  # degenerate case
