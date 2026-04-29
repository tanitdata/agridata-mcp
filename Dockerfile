# --- Build stage ---
FROM python:3.11-slim AS builder

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

# --- Runtime stage ---
FROM python:3.11-slim

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/tanitdata /usr/local/bin/tanitdata

# Static metadata (pre-v3)
COPY schemas.json /app/schemas.json
COPY value_hints.json /app/value_hints.json

# Snapshot data (v3.0.0, DATA_SOURCE=snapshot)
# - audit_full.json: full CKAN catalog dump (datasets, resources, orgs,
#   datastore_schemas). 5.5 MB.
# - snapshot/parquet/: one <uuid>.parquet per DataStore-active resource,
#   ZSTD-compressed. 727 files / 7.8 MB.
# - snapshot/scrape_index.json: UUID → scrape-relative path + snapshot_date
#   metadata. 117 KB.
# Raw scrape files (PDFs, non-DS XLSX) are intentionally NOT included —
# read_resource on non-DataStore resources returns the "download manually"
# message with the portal URL.
COPY audit_full.json /app/audit_full.json
COPY snapshot/parquet/ /app/snapshot/parquet/
COPY snapshot/scrape_index.json /app/snapshot/scrape_index.json

ENV MCP_TRANSPORT=streamable-http
ENV FASTMCP_HOST=0.0.0.0
ENV FASTMCP_PORT=8000
ENV FASTMCP_STATELESS_HTTP=true
ENV SCHEMAS_PATH=/app/schemas.json

# v3.0.0 snapshot-mode defaults. Flip DATA_SOURCE to `live` at deploy time
# to restore the v2.x CKAN API behaviour.
ENV DATA_SOURCE=snapshot
ENV SNAPSHOT_AUDIT_PATH=/app/audit_full.json
ENV SNAPSHOT_PARQUET_DIR=/app/snapshot/parquet
ENV SNAPSHOT_SCRAPE_INDEX=/app/snapshot/scrape_index.json

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health', timeout=5).raise_for_status()" || exit 1

CMD ["tanitdata"]
