"""Arabic field name decoding for mojibake-affected Bizerte price datasets."""

from __future__ import annotations

from typing import Any


def annotate_fields_with_arabic(
    fields: list[str], mapping: dict[str, Any]
) -> list[str]:
    """Return field descriptions with Arabic decoding annotations where available."""
    field_map = mapping.get("field_mapping", {})
    result: list[str] = []
    for f in fields:
        if f in field_map:
            result.append(f"`{f}` → {field_map[f]}")
        else:
            result.append(f"`{f}`")
    return result
