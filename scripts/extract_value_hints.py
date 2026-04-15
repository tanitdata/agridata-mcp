#!/usr/bin/env python3
"""Extract per-resource categorical values from vocabulary_raw.json → value_hints.json.

Reads the output of extract_vocabulary.py and produces a lean runtime artifact
that the SchemaRegistry loads at startup. Used by query_datastore to provide
contextual value hints in tool responses — so the LLM knows exact categorical
values before writing follow-up SQL.

Prerequisites:
    vocabulary_raw.json must exist (run extract_vocabulary.py first)

Usage:
    python scripts/extract_value_hints.py
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    vocab_path = ROOT / "vocabulary_raw.json"
    output_path = ROOT / "value_hints.json"

    if not vocab_path.exists():
        print(f"ERROR: {vocab_path.name} not found. Run extract_vocabulary.py first.")
        raise SystemExit(1)

    with open(vocab_path, encoding="utf-8") as f:
        vocab = json.load(f)

    raw_values = vocab.get("categorical_values", {})

    resource_values: dict[str, dict[str, list[str]]] = {}
    total_resources = 0
    total_columns = 0
    total_values = 0

    for rid, columns in raw_values.items():
        cleaned: dict[str, list[str]] = {}
        for col_name, values in columns.items():
            if len(values) < 2:
                continue  # single-value columns aren't useful as hints
            # Sort for consistent output and easier LLM reading
            sorted_vals = sorted(values)
            # Cap at 50 values per column — high-cardinality columns are noise
            capped = sorted_vals[:50]
            cleaned[col_name] = capped
            total_values += len(capped)

        if cleaned:
            resource_values[rid] = cleaned
            total_resources += 1
            total_columns += len(cleaned)

    output = {
        "meta": {
            "description": "Per-resource categorical values for query response enrichment",
            "source": "vocabulary_raw.json",
            "total_resources": total_resources,
            "total_columns": total_columns,
            "total_values": total_values,
            "max_values_per_column": 50,
        },
        "resource_values": resource_values,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"Written {output_path.name}: {size_kb:.0f} KB")
    print(f"  {total_resources} resources, {total_columns} columns, {total_values} values")


if __name__ == "__main__":
    main()
