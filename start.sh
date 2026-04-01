#!/usr/bin/env bash
set -e

RAW_DATA_PATH="${1:-raw_data}"

echo "==> Installing dependencies..."
poetry install

echo "==> Loading raw data from '${RAW_DATA_PATH}'..."
poetry run python scripts/load_raw.py "$RAW_DATA_PATH"

echo "==> Installing dbt packages..."
poetry run dbt deps

echo "==> Running dbt models..."
poetry run dbt run

echo "==> Running dbt tests..."
poetry run dbt test

echo "Done."
