# BDR Dagster Hackathon

## Setup

### System requirements

* Install DuckDB, preferrably globally with `brew install duckdb`

### Project installation

1. Sync you local development environment with: `uv sync --dev`
2. Set an environment variable with `export DAGSTER_HOME=/tmp/bdr_dagster_hackaton`
3. Start Dagster with `uv run dg dev` and open the [web UI](http://127.0.0.1:3000)
4. Open the marimo dashboard with `uv run marimo run dashboard.py`
