"""Tests for the CKAN client (basic unit tests, no network calls)."""

from tanitdata.ckan_client import CKANClient


def test_default_base_url():
    client = CKANClient()
    assert client.base_url == "https://catalog.agridata.tn"
    assert client._api_base == "https://catalog.agridata.tn/api/3/action"


def test_custom_base_url():
    client = CKANClient(base_url="https://example.com")
    assert client.base_url == "https://example.com"
    assert client._api_base == "https://example.com/api/3/action"
