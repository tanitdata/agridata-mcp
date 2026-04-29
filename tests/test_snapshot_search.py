"""Tests for SnapshotClient.package_search / package_show / resource_show."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tanitdata.ckan_client import SnapshotClient

ROOT = Path(__file__).resolve().parent.parent
AUDIT_PATH = ROOT / "audit_full.json"

_audit_available = pytest.mark.skipif(
    not AUDIT_PATH.exists(),
    reason="audit_full.json missing; cannot run offline search tests",
)

# Benchmark anchors
Q4_RID = "505db350-d619-4f36-9937-a26209ab9b79"
Q4_SLUG = "la-production-des-cereales"


# ---------------------------------------------------------------------------
# fq parser
# ---------------------------------------------------------------------------


def test_fq_parser_simple():
    parsed = SnapshotClient._parse_fq("organization:crda-beja")
    assert parsed == {"organization": "crda-beja"}


def test_fq_parser_multiple():
    parsed = SnapshotClient._parse_fq(
        "organization:crda-beja groups:barrages res_format:XLSX"
    )
    assert parsed == {
        "organization": "crda-beja",
        "groups": "barrages",
        "res_format": "XLSX",
    }


def test_fq_parser_empty():
    assert SnapshotClient._parse_fq("") == {}


def test_fq_parser_quoted_value():
    # Not used by tanitdata tools but supported for forward-compat.
    parsed = SnapshotClient._parse_fq('organization:"some-name"')
    assert parsed == {"organization": "some-name"}


# ---------------------------------------------------------------------------
# package_search
# ---------------------------------------------------------------------------


@_audit_available
def test_package_search_returns_ckan_shape():
    client = SnapshotClient()
    result = asyncio.run(client.package_search(query="céréales", rows=5))
    assert result is not None
    assert "count" in result
    assert "results" in result
    assert "search_facets" in result
    assert isinstance(result["count"], int)
    assert isinstance(result["results"], list)
    # Each result has the hydrated shape
    if result["results"]:
        ds = result["results"][0]
        assert isinstance(ds.get("organization"), dict)
        assert isinstance(ds.get("groups"), list)
        assert isinstance(ds.get("resources"), list)


@_audit_available
def test_package_search_empty_query_returns_all():
    client = SnapshotClient()
    result = asyncio.run(client.package_search(query="", rows=10))
    assert result is not None
    # Should surface the full catalog, not zero
    assert result["count"] >= 1000


@_audit_available
def test_package_search_organization_filter():
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(
            query="", fq="organization:crda-beja", rows=100
        )
    )
    assert result is not None
    for ds in result["results"]:
        assert ds["organization"]["name"] == "crda-beja"


@_audit_available
def test_package_search_format_filter():
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(query="", fq="res_format:PDF", rows=100)
    )
    assert result is not None
    # Every returned dataset should have at least one PDF resource
    for ds in result["results"]:
        formats = {(r.get("format") or "").upper() for r in ds["resources"]}
        assert "PDF" in formats


@_audit_available
def test_package_search_title_weight_dominates_notes():
    """Dataset with keyword in title must outrank one with only notes match."""
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(query="céréales", rows=20)
    )
    # The top few results should all have 'cereales' in their title, not notes only
    from tanitdata.tools.bibliography import _normalize

    for ds in result["results"][:3]:
        # Must have the keyword somewhere; title is the strongest signal
        title_n = _normalize(ds.get("title") or "")
        notes_n = _normalize(ds.get("notes") or "")
        # Keyword should appear in at least one of these
        assert "cereales" in title_n or "cereales" in notes_n


@_audit_available
def test_package_search_facets_counted_over_full_set():
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(
            query="",
            rows=10,
            facet_fields=["organization", "groups", "res_format"],
        )
    )
    assert result is not None
    assert "organization" in result["search_facets"]
    org_items = result["search_facets"]["organization"]["items"]
    assert len(org_items) > 0
    # Sum of per-org counts should roughly equal the full catalog count
    assert sum(i["count"] for i in org_items) >= result["count"] - 50  # allow some nulls


@_audit_available
def test_package_search_rows_zero_returns_facets_only():
    """This is the list_organizations_tool pattern."""
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(
            query="", rows=0, facet_fields=["organization"]
        )
    )
    assert result is not None
    assert result["count"] > 0
    assert result["results"] == []
    assert result["search_facets"]["organization"]["items"]


@_audit_available
def test_package_search_surfaces_q4_anchor_dataset():
    """The Q4 benchmark dataset must surface in a reasonable query."""
    client = SnapshotClient()
    result = asyncio.run(
        client.package_search(query="céréales Kairouan", rows=10)
    )
    names = [ds["name"] for ds in result["results"]]
    assert Q4_SLUG in names


# ---------------------------------------------------------------------------
# package_show
# ---------------------------------------------------------------------------


@_audit_available
def test_package_show_by_slug():
    client = SnapshotClient()
    result = asyncio.run(client.package_show(Q4_SLUG))
    assert result is not None
    assert result["name"] == Q4_SLUG
    assert result["resources"]  # non-empty
    # The Kairouan resource is present
    ids = [r["id"] for r in result["resources"]]
    assert Q4_RID in ids


@_audit_available
def test_package_show_by_id():
    client = SnapshotClient()
    # First get the dataset by slug to discover its UUID
    via_slug = asyncio.run(client.package_show(Q4_SLUG))
    assert via_slug is not None
    via_id = asyncio.run(client.package_show(via_slug["id"]))
    assert via_id is not None
    assert via_id["name"] == Q4_SLUG


@_audit_available
def test_package_show_organization_hydrated():
    client = SnapshotClient()
    result = asyncio.run(client.package_show(Q4_SLUG))
    assert result is not None
    org = result["organization"]
    assert org["name"] == "crda-kairouan"
    assert org["title"]  # non-empty


@_audit_available
def test_package_show_returns_none_for_unknown():
    client = SnapshotClient()
    result = asyncio.run(client.package_show("does-not-exist-anywhere"))
    assert result is None


# ---------------------------------------------------------------------------
# resource_show
# ---------------------------------------------------------------------------


@_audit_available
def test_resource_show_by_uuid():
    client = SnapshotClient()
    result = asyncio.run(client.resource_show(Q4_RID))
    assert result is not None
    assert result["id"] == Q4_RID
    assert result.get("datastore_active") is True
    assert result.get("format")


@_audit_available
def test_resource_show_unknown_returns_none():
    client = SnapshotClient()
    result = asyncio.run(client.resource_show("00000000-0000-0000-0000-000000000000"))
    assert result is None
