"""Tests for the (bytes, reason) return contract of download_file and for
the user-facing messages read_resource renders per reason."""

from __future__ import annotations

from tanitdata.tools.resource_reader import _download_error_message


RID = "ed964cb1-52a7-4257-9318-9922eb47ff19"
NAME = "Exportation des produits de la pêche"
URL = (
    "https://catalog.agridata.tn/dataset/5cd39351-7409-4144-b60c-76716fc417c7/"
    "resource/ed964cb1-52a7-4257-9318-9922eb47ff19/download/test.xlsx"
)


def test_size_cap_message_preserves_old_wording():
    """Pre-v3.2.2 path — must still mention the 5 MB cap for oversized files."""
    msg = _download_error_message(RID, NAME, URL, "size_cap")
    assert "5 MB size limit" in msg
    assert URL in msg
    assert RID in msg


def test_http_500_message_is_not_misleading():
    """The v3.2.2 bug: HTTP 500 used to render as 'exceeds size limit'."""
    msg = _download_error_message(RID, NAME, URL, "http_500")
    assert "HTTP 500" in msg
    assert "5 MB" not in msg  # regression guard
    # Tells user the API still works, directing them to query_datastore
    # for structured data
    assert "query_datastore" in msg
    assert URL in msg


def test_http_404_message_reports_the_actual_status():
    msg = _download_error_message(RID, NAME, URL, "http_404")
    assert "HTTP 404" in msg
    assert "5 MB" not in msg


def test_network_message_mentions_network():
    msg = _download_error_message(RID, NAME, URL, "network")
    assert "network" in msg.lower()
    assert "5 MB" not in msg
    assert URL in msg


def test_unknown_reason_is_honest():
    """Defensive: if download_file ever grows a new reason we don't know,
    the message should still be truthful rather than claim size cap."""
    msg = _download_error_message(RID, NAME, URL, "some_future_reason")
    assert "5 MB" not in msg
    assert "some_future_reason" in msg


def test_none_reason_does_not_fabricate_cause():
    msg = _download_error_message(RID, NAME, URL, None)
    assert "5 MB" not in msg
    assert "unknown" in msg
