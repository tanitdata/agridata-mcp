"""Tests for the schema registry."""

import pytest

from tanitdata.schema_registry import SchemaRegistry


@pytest.fixture
def registry():
    reg = SchemaRegistry()
    reg.load()
    return reg


def test_meta_has_totals(registry):
    meta = registry.meta
    assert meta["total_datastore_resources"] == 789
    assert meta["unique_schema_patterns"] == 583


def test_domains_list(registry):
    domains = registry.domains
    assert "climate_stations" in domains
    assert "dams" in domains
    assert "crop_production" in domains


def test_find_resources_by_domain(registry):
    climate = registry.find_resources_by_domain("climate_stations")
    assert climate["count"] >= 21  # was 21 at audit time, now 24 on portal
    assert len(climate["resources"]) >= 21
    assert climate["resources"][0]["fields"] == [
        "Date", "nom_ar", "nom_fr", "unite", "valeur"
    ]


def test_find_resources_unknown_domain(registry):
    result = registry.find_resources_by_domain("nonexistent")
    assert result == {}


def test_get_cluster_resources(registry):
    # Climate station cluster
    resources = registry.get_cluster_resources(
        ["Date", "nom_ar", "nom_fr", "unite", "valeur"]
    )
    assert len(resources) >= 20


def test_get_resource_schema_from_cluster(registry):
    # Known climate station resource
    fields = registry.get_resource_schema("ec7daec9-da4b-47a4-9ea9-f6b5ca820955")
    assert fields == ["Date", "nom_ar", "nom_fr", "unite", "valeur"]


def test_get_resource_schema_not_found(registry):
    assert registry.get_resource_schema("nonexistent-id") is None


def test_arabic_field_mapping(registry):
    mapping = registry.get_arabic_field_mapping()
    assert "field_mapping" in mapping
    assert "lnw`" in mapping["field_mapping"]


def test_get_column_hints_exact_match(registry):
    """Columns that match hint keys exactly should return their values."""
    # Béja cereal resource — value_hints has an unaccented 'Delegation' key.
    rid = "df7103b7-0a85-45d6-9309-1d9967445788"
    hints = registry.get_column_hints(rid, ["Delegation"])
    if not hints:
        pytest.skip("value_hints.json has no entry for this resource")
    assert "Delegation" in hints
    assert len(hints["Delegation"]) >= 2


def test_get_column_hints_accent_folded_fallback(registry):
    """Accented query column should match an unaccented hint key via folding.

    Regression-guards the live-vs-snapshot column-naming asymmetry:
    CKAN strips diacritics (`Delegation`) but openpyxl-parsed XLSX
    preserves them (`Délégation`). Tool responses carry the column name
    the tool actually returned; hints must still resolve.
    """
    rid = "df7103b7-0a85-45d6-9309-1d9967445788"
    hints = registry.get_column_hints(rid, ["Délégation"])
    if not hints:
        pytest.skip("value_hints.json has no entry for this resource")
    # Returned dict is keyed by the requested (accented) name
    assert "Délégation" in hints
    assert len(hints["Délégation"]) >= 2


def test_get_column_hints_unknown_column_returns_empty(registry):
    rid = "df7103b7-0a85-45d6-9309-1d9967445788"
    hints = registry.get_column_hints(rid, ["does_not_exist"])
    assert hints == {}


def test_get_column_hints_unknown_resource_returns_empty(registry):
    hints = registry.get_column_hints(
        "00000000-0000-0000-0000-000000000000", ["Delegation"]
    )
    assert hints == {}
