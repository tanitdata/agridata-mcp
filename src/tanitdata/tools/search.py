"""Search tools: search_datasets, get_dataset_details, list_organizations."""

from __future__ import annotations

from typing import Any

from tanitdata.ckan_client import CKANClient
from tanitdata.utils.formatting import format_dataset_list


async def search_datasets(
    client: CKANClient,
    query: str,
    organization: str | None = None,
    group: str | None = None,
    format_filter: str | None = None,
    limit: int = 10,
) -> str:
    """Search portal datasets by keyword, organization, thematic group, or format."""
    fq_parts: list[str] = []
    if organization:
        fq_parts.append(f"organization:{organization}")
    if group:
        fq_parts.append(f"groups:{group}")
    if format_filter:
        fq_parts.append(f"res_format:{format_filter.upper()}")
    fq = " ".join(fq_parts)

    result = await client.package_search(
        query=query,
        fq=fq,
        rows=limit,
        facet_fields=["organization", "groups", "res_format"],
    )
    if not result:
        return "Search failed — no response from the portal API."

    count = result.get("count", 0)
    datasets = result.get("results", [])

    header = f"**Found {count:,} dataset(s)** matching `{query}`"
    if organization:
        header += f" in org `{organization}`"
    if group:
        header += f" in group `{group}`"
    header += f" (showing {len(datasets)}).\n\n"

    return header + format_dataset_list(datasets)


async def get_dataset_details(client: CKANClient, dataset_id: str) -> str:
    """Get full metadata and resource list for a specific dataset."""
    result = await client.package_show(dataset_id)
    if not result:
        return f"Dataset `{dataset_id}` not found."

    title = result.get("title", "Untitled")
    ds_slug = result.get("name", dataset_id)
    org = result.get("organization", {})
    org_title = org.get("title", "Unknown") if isinstance(org, dict) else "Unknown"
    notes = result.get("notes", "No description.")
    groups = [g.get("display_name", g.get("name", "")) for g in result.get("groups", [])]
    portal_url = f"https://catalog.agridata.tn/dataset/{ds_slug}"

    lines = [
        f"# {title}",
        f"**Organization:** {org_title}",
        f"**Groups:** {', '.join(groups) if groups else 'None'}",
        f"**License:** {result.get('license_title', 'N/A')}",
        f"**Portal:** {portal_url}",
        f"**Description:** {notes}",
        "",
        "## Resources",
    ]

    for res in result.get("resources", []):
        ds_active = "Yes" if res.get("datastore_active") else "No"
        lines.append(f"- **{res.get('name', 'Unnamed')}**")
        lines.append(f"  - Resource ID: `{res['id']}`")
        lines.append(f"  - Format: {res.get('format', 'N/A')}")
        lines.append(f"  - DataStore active: {ds_active}")

    return "\n".join(lines)


async def list_organizations(client: CKANClient, query: str = "") -> str:
    """List all data-producing organizations with dataset counts.

    Uses faceted search via package_search to work around the organization_list gap.
    """
    result = await client.package_search(
        query=query,
        rows=0,
        facet_fields=["organization"],
    )
    if not result:
        return "Failed to retrieve organization list."

    facets = result.get("search_facets", {}).get("organization", {}).get("items", [])
    if not facets:
        return "No organizations found."

    facets.sort(key=lambda x: x.get("count", 0), reverse=True)

    lines = [f"**Organizations** ({len(facets)} total):\n"]
    for org in facets:
        lines.append(f"- **{org.get('display_name', org.get('name', '?'))}** — {org.get('count', 0)} dataset(s)")

    return "\n".join(lines)
