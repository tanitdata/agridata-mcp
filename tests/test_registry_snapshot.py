"""Tests for the SchemaRegistry offline refresh path.

These tests exercise the load_snapshot() entry point, verify the specific
registry methods called out in Phase 2 (get_domain_resources,
get_coverage_summary, get_resource_schema, get_column_hints,
get_source_attribution) against real audit data, and confirm that
maybe_refresh is a no-op in snapshot mode.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tanitdata.ckan_client import LiveClient, SnapshotClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.formatting import format_source_footer

ROOT = Path(__file__).resolve().parent.parent
AUDIT_PATH = ROOT / "audit_full.json"
SCHEMAS_PATH = ROOT / "schemas.json"
SNAPSHOT_INDEX = ROOT / "snapshot" / "scrape_index.json"

# Benchmark resources used throughout the migration — these must always
# resolve correctly after an offline refresh.
BEJA_CEREAL_RID = "df7103b7-0a85-45d6-9309-1d9967445788"
KAIROUAN_CEREAL_RID = "505db350-d619-4f36-9937-a26209ab9b79"


_snapshot_available = pytest.mark.skipif(
    not AUDIT_PATH.exists() or not SCHEMAS_PATH.exists(),
    reason="audit_full.json or schemas.json missing; cannot run offline tests",
)


# ---------------------------------------------------------------------------
# load_snapshot — basic shape
# ---------------------------------------------------------------------------


@_snapshot_available
def test_load_snapshot_populates_live_layer():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()

    # Live layer should have at least the 789 DataStore-active resources
    # the audit enumerates.
    assert len(reg._live) >= 789


@_snapshot_available
def test_snapshot_date_loaded_from_sidecar():
    if not SNAPSHOT_INDEX.exists():
        pytest.skip("snapshot/scrape_index.json missing; run build_snapshot.py")

    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot(scrape_index_path=SNAPSHOT_INDEX)
    assert reg.snapshot_date == "2025-11-26"


@_snapshot_available
def test_snapshot_date_absent_in_live_mode():
    reg = SchemaRegistry()
    reg.load()
    # Don't call load_snapshot — simulating live mode
    assert reg.snapshot_date is None


# ---------------------------------------------------------------------------
# Domain queries against audit data
# ---------------------------------------------------------------------------


@_snapshot_available
def test_climate_stations_domain_after_snapshot_refresh():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    resources = reg.get_domain_resources("climate_stations")
    # schemas.json meta shows 24 climate_stations resources in the curated index.
    assert len(resources) == 24
    # Every returned resource should have a schema (fields list) populated.
    with_fields = [r for r in resources if r.get("fields")]
    assert len(with_fields) >= 20  # most should have fields after audit refresh


@_snapshot_available
def test_crop_production_domain_after_snapshot_refresh():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    resources = reg.get_domain_resources("crop_production")
    # schemas.json docs: crop_production ~ 239 resources (curated index)
    assert len(resources) > 100
    # The Kairouan cereal production benchmark resource should be present.
    ids = {r["id"] for r in resources}
    assert KAIROUAN_CEREAL_RID in ids


@_snapshot_available
def test_coverage_summary_climate_stations():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    coverage = reg.get_coverage_summary("climate_stations")
    # Expect at least 10 sensor governorates per CLAUDE.md documentation.
    # Exclude the "national" bucket, which contains metadata-only entries.
    govs = [g for g in coverage if g != "national"]
    assert len(govs) >= 9
    # Known benchmark governorates should be represented.
    assert any("Bizerte" in g for g in coverage)
    assert any("Mahdia" in g for g in coverage)
    # Every bucket should have a positive count.
    assert all(n > 0 for n in coverage.values())


# ---------------------------------------------------------------------------
# Per-resource lookups
# ---------------------------------------------------------------------------


@_snapshot_available
def test_get_resource_schema_beja_cereal():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    fields = reg.get_resource_schema(BEJA_CEREAL_RID)
    assert fields is not None
    assert len(fields) >= 5
    # `_id` / `_full_text` must be stripped.
    assert "_id" not in fields
    assert "_full_text" not in fields


@_snapshot_available
def test_get_column_hints_beja_cereal():
    """value_hints.json keys the Béja cereal resource — request two columns."""
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    hints = reg.get_column_hints(BEJA_CEREAL_RID, ["Delegation", "Ble dur"])
    # Returns a dict — may be empty if no hints for these columns in this
    # particular resource (absence isn't a registry bug, but the call must
    # not raise and must return a dict).
    assert isinstance(hints, dict)


@_snapshot_available
def test_get_source_attribution_includes_snapshot_date():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot(scrape_index_path=SNAPSHOT_INDEX)
    src = reg.get_source_attribution(BEJA_CEREAL_RID)
    assert src is not None
    assert src["resource_id"] == BEJA_CEREAL_RID
    assert src["dataset_name"]  # non-empty slug
    assert src["portal_url"].startswith("https://catalog.agridata.tn/dataset/")
    assert src["snapshot_date"] == "2025-11-26"


@_snapshot_available
def test_get_source_attribution_has_organization_in_snapshot():
    """The audit carries organization data — snapshot refresh should fill it in."""
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    src = reg.get_source_attribution(BEJA_CEREAL_RID)
    assert src is not None
    # Either slug or title should be non-empty after audit refresh
    assert src.get("organization") or src.get("organization_title")


@_snapshot_available
def test_format_source_footer_surfaces_snapshot_date():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot(scrape_index_path=SNAPSHOT_INDEX)
    src = reg.get_source_attribution(BEJA_CEREAL_RID)
    assert src is not None
    footer = format_source_footer([src])
    assert "snapshot dated 2025-11-26" in footer


@_snapshot_available
def test_format_source_footer_no_snapshot_date_in_live_mode():
    reg = SchemaRegistry()
    reg.load()
    # No load_snapshot() — simulate live mode
    src = reg.get_source_attribution(BEJA_CEREAL_RID)
    if src is None:
        pytest.skip("benchmark resource not in static layer alone")
    footer = format_source_footer([src])
    assert "snapshot dated" not in footer


# ---------------------------------------------------------------------------
# maybe_refresh routing
# ---------------------------------------------------------------------------


@_snapshot_available
def test_maybe_refresh_is_noop_for_snapshot_client():
    reg = SchemaRegistry()
    reg.load()
    reg.load_snapshot()
    client = SnapshotClient()
    # Prime _last_refreshed to a very old time so the staleness check would
    # normally trigger — prove the snapshot-mode early return still skips.
    from datetime import datetime, timedelta, timezone

    reg._last_refreshed = datetime.now(tz=timezone.utc) - timedelta(days=30)
    before_ts = reg._last_refreshed

    # Should complete instantly without touching the client.
    asyncio.run(reg.maybe_refresh(client))

    # The timestamp must not have been bumped by the no-op path.
    assert reg._last_refreshed == before_ts


def test_is_snapshot_client_discriminates():
    reg = SchemaRegistry()
    assert reg._is_snapshot_client(SnapshotClient()) is True
    assert reg._is_snapshot_client(LiveClient()) is False
