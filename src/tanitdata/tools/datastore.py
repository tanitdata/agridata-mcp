"""DataStore tool: query_datastore — generic SQL access to any DataStore resource."""

from __future__ import annotations

from tanitdata.ckan_client import CKANClient
from tanitdata.schema_registry import SchemaRegistry
from tanitdata.utils.arabic import annotate_fields_with_arabic
from tanitdata.utils.formatting import format_datastore_result, format_source_footer


async def query_datastore(
    client: CKANClient,
    registry: SchemaRegistry,
    resource_id: str,
    sql: str | None = None,
    limit: int = 100,
) -> str:
    """Execute a SQL query against any DataStore resource.

    If no SQL is provided, returns the first `limit` rows with schema info.
    Always includes the resource's schema in the response so the LLM
    knows what the columns mean.
    """
    # First, get the schema from the registry (fast, local)
    known_fields = registry.get_resource_schema(resource_id)

    if sql:
        # Execute user-provided SQL
        result = await client.datastore_sql(sql)
        if not result:
            return f"SQL query failed for resource `{resource_id}`. Check the resource ID and SQL syntax."
        records = result.get("records", [])
        fields = result.get("fields", [])
        total = len(records)
    else:
        # Simple search with limit
        result = await client.datastore_search(resource_id, limit=limit)
        if not result:
            return f"Could not fetch data from resource `{resource_id}`. It may not be DataStore-active."
        records = result.get("records", [])
        fields = result.get("fields", [])
        total = result.get("total", len(records))

    # Build response
    lines: list[str] = []

    # Add Arabic field annotations if applicable
    arabic_mapping = registry.get_arabic_field_mapping()
    if known_fields and arabic_mapping.get("field_mapping"):
        decoded = annotate_fields_with_arabic(known_fields, arabic_mapping)
        has_arabic = any("→" in d for d in decoded)
        if has_arabic:
            lines.append("**Arabic field decoding:**")
            for d in decoded:
                if "→" in d:
                    lines.append(f"  - {d}")
            lines.append("")

    # Add the formatted result
    lines.append(
        format_datastore_result(
            records=records,
            fields=fields,
            total=total,
            resource_id=resource_id,
        )
    )

    # Value hints for categorical columns (helps LLM write exact WHERE clauses)
    result_col_names = [
        f["id"] if isinstance(f, dict) else f
        for f in fields
        if (f["id"] if isinstance(f, dict) else f) not in ("_id", "_full_text")
    ]
    hints = registry.get_column_hints(resource_id, result_col_names)
    if hints:
        lines.append("")
        lines.append("**Value hints for follow-up queries:**")
        for col, values in hints.items():
            if len(values) <= 20:
                val_str = ", ".join(values)
                lines.append(f"- `{col}`: {len(values)} values — {val_str}")
            else:
                val_str = ", ".join(values[:20])
                lines.append(
                    f"- `{col}`: {len(values)} values — {val_str}, "
                    f"... (+{len(values) - 20} more)"
                )

    # Add a note about the known schema from the registry
    if known_fields:
        lines.append("")
        lines.append(f"**Registry schema:** {', '.join(f'`{f}`' for f in known_fields)}")
        lines.append("*Note: All fields are stored as text. Use `::numeric` or `::timestamp` casts in SQL for math/date operations.*")

    # Append data availability context if we know the domain/governorate
    ctx = registry.get_resource_context(resource_id)
    if ctx:
        gov = ctx.get("gouvernorat")
        for domain in ctx.get("domains", []):
            avail = registry.get_data_availability(domain, gouvernorat=gov)
            if avail:
                lines.append("")
                lines.append(f"**Data availability ({domain}):** {avail}")

    # Source attribution footer
    source = registry.get_source_attribution(resource_id)
    if source:
        lines.append("")
        lines.append(format_source_footer([source]))

    return "\n".join(lines)
