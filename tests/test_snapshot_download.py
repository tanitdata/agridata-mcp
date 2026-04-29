"""Tests for SnapshotClient.download_file and magic-byte dispatch."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tanitdata.ckan_client import SnapshotClient
from tanitdata.tools.resource_reader import _sniff_format

ROOT = Path(__file__).resolve().parent.parent
AUDIT_PATH = ROOT / "audit_full.json"
SCRAPE_INDEX = ROOT / "snapshot" / "scrape_index.json"


def _scrape_available() -> bool:
    """True when the raw scrape folder is actually on disk.

    The scrape_index.json sidecar is committed (tiny, deterministic) but
    the 268 MB raw scrape folder is not. In CI only the index is present,
    so tests that read the underlying XLS/XLSX bytes must be skipped.
    We detect by looking up a known file from the index and checking for
    its existence relative to the scrape_root recorded in the sidecar.
    """
    if not AUDIT_PATH.exists() or not SCRAPE_INDEX.exists():
        return False
    try:
        import json

        with open(SCRAPE_INDEX, encoding="utf-8") as f:
            idx = json.load(f)
        scrape_root = Path(idx.get("meta", {}).get("scrape_root", ""))
        # Probe the first indexed file
        resources = idx.get("resources", {})
        if not resources:
            return False
        first_rel = next(iter(resources.values()))
        return (scrape_root / first_rel).exists()
    except Exception:
        return False


_snapshot_available = pytest.mark.skipif(
    not _scrape_available(),
    reason=(
        "raw scrape folder not on disk (CI / fresh clone). "
        "These tests read bytes from files that the gitignored scrape "
        "folder owns; they run locally when the scrape is available."
    ),
)


# ---------------------------------------------------------------------------
# Magic-byte sniffer
# ---------------------------------------------------------------------------


def test_sniff_xlsx():
    assert _sniff_format(b"PK\x03\x04" + b"rest of file") == "xlsx"


def test_sniff_xls_ole():
    assert _sniff_format(b"\xd0\xcf\x11\xe0" + b"rest of file") == "xls_ole"


def test_sniff_csv_default():
    assert _sniff_format(b"col1,col2\n1,2\n") == "csv"


def test_sniff_empty_defaults_to_csv():
    # Short inputs shouldn't raise — they fall through to CSV.
    assert _sniff_format(b"") == "csv"
    assert _sniff_format(b"x") == "csv"


# ---------------------------------------------------------------------------
# URL resource-id extraction
# ---------------------------------------------------------------------------


def test_url_resource_extractor_canonical():
    pattern = SnapshotClient._URL_RESOURCE_RE
    url = (
        "https://catalog.agridata.tn/dataset/"
        "a59de5dd-ffa4-4aa4-a04c-8601b87170ca/"
        "resource/505db350-d619-4f36-9937-a26209ab9b79/download/file.xlsx"
    )
    m = pattern.search(url)
    assert m is not None
    assert m.group(1).lower() == "505db350-d619-4f36-9937-a26209ab9b79"


def test_url_resource_extractor_case_insensitive():
    pattern = SnapshotClient._URL_RESOURCE_RE
    url = "https://x/resource/DF7103B7-0A85-45D6-9309-1D9967445788/download/"
    m = pattern.search(url)
    assert m is not None


def test_url_resource_extractor_no_match():
    pattern = SnapshotClient._URL_RESOURCE_RE
    assert pattern.search("https://example.com/foo") is None


# ---------------------------------------------------------------------------
# download_file integration
# ---------------------------------------------------------------------------


@_snapshot_available
def test_download_file_with_resource_id():
    """Q4 Kairouan production (xls_ole known in the scrape)."""
    client = SnapshotClient()
    rid = "505db350-d619-4f36-9937-a26209ab9b79"
    data = asyncio.run(client.download_file("", resource_id=rid))
    assert data is not None
    # Magic-byte round-trip
    fmt = _sniff_format(data)
    assert fmt in ("xlsx", "xls_ole")


@_snapshot_available
def test_download_file_with_url_only():
    """Falls back to URL parsing when resource_id isn't provided."""
    client = SnapshotClient()
    url = (
        "https://catalog.agridata.tn/dataset/any/"
        "resource/505db350-d619-4f36-9937-a26209ab9b79/download/file.xlsx"
    )
    data = asyncio.run(client.download_file(url))
    assert data is not None


@_snapshot_available
def test_download_file_returns_none_for_unknown_resource():
    client = SnapshotClient()
    data = asyncio.run(
        client.download_file("", resource_id="00000000-0000-0000-0000-000000000000")
    )
    assert data is None


@_snapshot_available
def test_download_file_returns_none_for_unparsable_url():
    client = SnapshotClient()
    data = asyncio.run(client.download_file("not-a-ckan-url"))
    assert data is None


@_snapshot_available
def test_download_file_respects_size_cap():
    """File exists but is over the cap — same contract as LiveClient."""
    client = SnapshotClient()
    rid = "505db350-d619-4f36-9937-a26209ab9b79"
    # Cap of 100 bytes — the real file is a multi-KB spreadsheet
    data = asyncio.run(
        client.download_file("", resource_id=rid, max_bytes=100)
    )
    assert data is None


# ---------------------------------------------------------------------------
# End-to-end: read_resource_tool against a non-DataStore resource
# ---------------------------------------------------------------------------


@_snapshot_available
def test_read_resource_on_unsupported_format_doesnt_crash():
    """Non-DataStore PDF → format-not-supported message (no crash)."""
    from tanitdata.schema_registry import SchemaRegistry
    from tanitdata.tools.resource_reader import read_resource

    client = SnapshotClient()
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()

    # Find the first PDF resource in the audit
    audit = client._load_audit()
    pdf_rid = None
    for r in audit["resources"]:
        if (r.get("format") or "").upper() == "PDF":
            pdf_rid = r["id"]
            break
    if pdf_rid is None:
        pytest.skip("no PDF resources in audit")

    out = asyncio.run(read_resource(client, reg, pdf_rid, limit=10))
    # Exact wording not critical; what matters is that it's a graceful
    # text message rather than an exception.
    assert isinstance(out, str)
    assert "PDF" in out or "format" in out.lower()


@_snapshot_available
def test_read_resource_redirects_for_datastore_active():
    """DataStore-active resource → redirect to query_datastore."""
    from tanitdata.schema_registry import SchemaRegistry
    from tanitdata.tools.resource_reader import read_resource

    client = SnapshotClient()
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()

    out = asyncio.run(
        read_resource(
            client, reg, "505db350-d619-4f36-9937-a26209ab9b79", limit=5
        )
    )
    assert "DataStore-active" in out
    assert "query_datastore" in out
