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
    assert climate["count"] == 21
    assert len(climate["resources"]) == 21
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
