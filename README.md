# tanitdata

MCP server for Tunisia's agricultural open data portal ([catalog.agridata.tn](https://catalog.agridata.tn)).

Bridges structured DataStore tables (climate, crops, dams, fisheries, prices) with bibliographic records so that AI assistants can query Tunisia's agricultural data through natural language.

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Tools

| Tool | Description |
|---|---|
| `search_datasets` | Search portal datasets by keyword, org, group, format |
| `get_dataset_details` | Get full metadata and resource list for a dataset |
| `query_datastore` | Execute SQL against any DataStore resource |
| `list_organizations` | List all data-producing organizations with counts |

More tools (climate stations, dams, crops, fisheries, bibliography, dashboards) are planned — see CLAUDE.md for the full roadmap.

## Configure Claude Desktop

Add the following to your Claude Desktop config file:

- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "tanitdata": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/absolute/path/to/TanitData",
        "tanitdata"
      ]
    }
  }
}
```

Replace the path with your actual project directory.

## Test with MCP Inspector

```bash
uv run mcp dev src/tanitdata/server.py
```

In the inspector, try:

- **search_datasets** with `{"query": "olive production"}`
- **query_datastore** with `{"resource_id": "8db5d2d8-586a-4dd3-bedd-9690295b6fe3", "limit": 5}`

## Project Structure

```
src/tanitdata/
├── server.py           # MCP server entry point
├── ckan_client.py      # Async CKAN API client
├── schema_registry.py  # Schema lookup from schemas.json
├── tools/
│   ├── search.py       # search_datasets, get_dataset_details, list_organizations
│   └── datastore.py    # query_datastore (generic SQL)
└── utils/
    ├── formatting.py   # Result formatting
    └── arabic.py       # Arabic field decoding
```

## License

Data sourced from catalog.agridata.tn under the Licence Nationale de Données Publiques Ouvertes.
