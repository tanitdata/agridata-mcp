# tanitdata

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-compatible-purple)](https://modelcontextprotocol.io)

An AI-powered [MCP](https://modelcontextprotocol.io) server that turns Tunisia's agricultural open data portal ([agridata.tn](https://catalog.agridata.tn)) into a conversational data source. Ask questions in natural language — the AI handles querying 1,102 datasets and 789 DataStore resources across climate, crops, fisheries, water, livestock, prices, and more.

Built for researchers, data scientists, agricultural policy analysts, and journalists covering food security in Tunisia and North Africa.

---

## Quick Start — Hosted Instance

**No installation required.** Connect any MCP-compatible client to:

```
https://mcp.tanitdata.org/mcp
```

No authentication needed — all data is public.

### Claude Code

```bash
claude mcp add --transport http tanitdata https://mcp.tanitdata.org/mcp
```

### Claude Desktop

Add to your config (`%APPDATA%\Claude\claude_desktop_config.json` on Windows, `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "tanitdata": {
      "command": "npx",
      "args": ["mcp-remote", "https://mcp.tanitdata.org/mcp"]
    }
  }
}
```

Requires Node.js installed. The `mcp-remote` package is fetched automatically on first launch.

### Cursor / Windsurf

```json
{
  "mcpServers": {
    "tanitdata": {
      "url": "https://mcp.tanitdata.org/mcp"
    }
  }
}
```

### Gemini CLI

Add to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "tanitdata": {
      "url": "https://mcp.tanitdata.org/mcp"
    }
  }
}
```

### Any MCP-compatible client

Point your client to `https://mcp.tanitdata.org/mcp` using its remote server configuration. No headers or tokens needed.

---

## Example Queries

Just ask in natural language. The AI selects and chains the right tools automatically.

> **"Compare cereal production across Beja, Jendouba, and Kef over the last 5 years"**
> Uses `search_datasets` to find crop production resources per governorate, then `query_datastore` to pull and compare yields.

> **"What was the rainfall deficit during the 2022/2023 drought in Siliana?"**
> Uses `query_climate_stations` to fetch precipitation data with monthly aggregation, comparing against historical averages.

> **"Find research documents about olive oil production in Tunisia"**
> Uses `search_bibliography` to search 25,944 bibliographic records, returning ranked results with PDF download links.

> **"What fish species are exported from Tunisia and to which countries?"**
> Uses `search_datasets` to find fisheries and trade export data, then `query_datastore` to cross-query species, quantities, and destinations.

> **"Show me all climate stations and their available sensors"**
> Uses `query_climate_stations` with no arguments to return a full inventory — 23 sensor stations across 10 governorates with live sensor lists.

---

## Tools

### Discovery

| Tool | What it does |
|---|---|
| **search_datasets** | Search portal datasets by keyword, organization, thematic group, or format. *"Find all datasets about olive production in Sfax"* |
| **get_dataset_details** | Get full metadata and resource list for a specific dataset. *"What resources are available in the Bizerte climate dataset?"* |
| **list_organizations** | List all data-producing organizations with dataset counts. *"Which organizations publish the most data?"* |

### Data Query

| Tool | What it does |
|---|---|
| **query_datastore** | Execute SQL against any DataStore resource. Accepts resource UUIDs or dataset slugs (auto-resolved). *"Show average wheat yield by governorate for 2023"* |
| **read_resource** | Download and parse non-DataStore files (CSV, XLSX). *"Read the Excel file from this livestock dataset"* |
| **query_climate_stations** | Query Tunisia's weather monitoring network — 24 resources across 11 governorates with temperature, rainfall, humidity, wind, and solar sensors. Supports time series, aggregation, multi-station comparison. *"Compare temperature trends between Bizerte and Nabeul this winter"* |

### Knowledge

| Tool | What it does |
|---|---|
| **search_bibliography** | Search ONAGRI's bibliographic catalogs (25,944 records) by keyword, year, language, or theme. Returns ranked results with PDF links. *"Find French-language studies on water management published after 2015"* |

### Navigation

| Tool | What it does |
|---|---|
| **get_dashboard_link** | Map a topic to the relevant interactive dashboard on agridata.tn. *"Is there a dashboard for date palm production?"* |

---

## Data Limitations

Working with agridata.tn data, be aware of:

- **All fields are stored as text** — numeric and date operations require SQL casting (`::numeric`, `::timestamp`) with regex guards
- **French-dominant content** — field names, values, and metadata are primarily in French, with some Arabic
- **Geographic gaps** — not all 24 governorates have data in every domain; coverage varies by topic
- **Format mismatches** — 86 resources have CSV/XLSX format mislabeling; the server handles this transparently
- **Arabic field encoding** — 39 resources have mojibake in field names; the 19 Bizerte price datasets are auto-decoded

---

## Local Development

For developers who want to run their own instance or contribute.

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

### Install and run

```bash
git clone https://github.com/tarekddoit/tanitdata.git
cd tanitdata
uv sync
uv run tanitdata          # starts stdio server
```

### Connect locally (Claude Desktop)

```json
{
  "mcpServers": {
    "tanitdata": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/tanitdata", "tanitdata"]
    }
  }
}
```

### Test with MCP Inspector

```bash
uv run mcp dev src/tanitdata/server.py
```

### Run unit tests

```bash
uv run pytest tests/
```

---

## Docker

```bash
docker build -t tanitdata .
docker run -p 8000:8000 tanitdata
```

The container runs in `streamable-http` mode on port 8000. Health check at `/health`, MCP endpoint at `/mcp`.

Override defaults with environment variables: `MCP_TRANSPORT`, `FASTMCP_HOST`, `FASTMCP_PORT`, `SCHEMAS_PATH`.

---

## Contributing

Contributions are welcome.

- One feature or fix per pull request
- Use conventional commit messages (`feat:`, `fix:`, `docs:`)
- All submissions are reviewed before merging

See [CLAUDE.md](CLAUDE.md) for detailed architecture documentation, API patterns, and data quality notes.

---

## License

This project is licensed under the [MIT License](LICENSE).

Data sourced from [catalog.agridata.tn](https://catalog.agridata.tn) under the Licence Nationale de Donnees Publiques Ouvertes.
