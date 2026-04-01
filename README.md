# olist_mart

A dbt project that transforms the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
into an analytical data mart using DuckDB as the local warehouse engine.

The goal is to demonstrate a complete analytics engineering workflow:
ingesting raw CSV data, modeling it through well-defined layers following
the medallion architecture (bronze, silver, and gold), and validating
the output with data quality tests.

---

## Stack

| Tool | Role |
|---|---|
| Python + DuckDB | Load raw CSVs into a local database |
| dbt-duckdb | Transform and test models |
| Poetry | Dependency management |

---

## Project Structure

```
models/
  staging/      # Cleans and casts raw source tables (views)
  dimensions/   # Customers, products, sellers, dates (tables)
  facts/        # Transactional grain: one row per order (incremental)
  marts/        # Aggregated analytical outputs (tables)

tests/          # Custom SQL data quality assertions
macros/         # Reusable SQL utilities (safe_divide, truncate_date)
scripts/        # Python script to load raw CSVs into DuckDB
```

### Mart outputs

- **mart_revenue_by_category** — monthly revenue, order volume, item
  count, freight, and average delivery time per product category,
  restricted to delivered orders.
- **mart_seller_performance** — per-seller aggregation of revenue,
  items sold, freight, and late delivery rate, restricted to delivered
  orders.

---

## Medallion Architecture

The project follows the medallion (lakehouse) architecture pattern,
organizing data into three progressive layers of quality and abstraction.

### Bronze — raw ingestion

Raw CSV files are loaded as-is into a dedicated `raw` schema in DuckDB
using `scripts/load_raw.py`. No transformations are applied at this
stage. The data reflects the original source exactly, preserving all
original values, types, and potential inconsistencies.

### Silver — cleaned and conformed

The staging models clean and cast the raw data into consistent types
and naming conventions. From there, dimension tables (customers,
products, sellers, dates) and the fact table (orders) are built,
forming a conformed dimensional model. This layer is reliable, tested,
and structured for querying.

### Gold — aggregated analytical outputs

The mart models consume the silver layer and produce business-level
aggregations ready for analysis or visualization. These are the final
outputs of the pipeline and the primary artifact for any downstream
consumer.

---

## Data Flow

```
CSV files (raw_data/)
    |
    v
scripts/load_raw.py     --> DuckDB raw schema           [bronze]
    |
    v
staging models          --> clean, typed views          [silver]
    |
    v
dimensions + facts      --> conformed tables            [silver]
    |
    v
marts                   --> aggregated analytical tables [gold]
```

---

## Getting Started

### Requirements

- Python >= 3.13
- Poetry

### Run everything

```bash
./start.sh
```

This will install dependencies, load the raw data into DuckDB, install
dbt packages, run all models, and execute all tests.

You can optionally pass a custom path to the raw CSV files:

```bash
./start.sh /path/to/csv/files
```

The default path is `raw_data/`.

### Run steps individually

```bash
# Load raw data
poetry run python scripts/load_raw.py

# Run dbt models
poetry run dbt run

# Run tests
poetry run dbt test
```

---

## Data Quality Tests

Custom tests live in `tests/` and cover assertions such as:

- Delivered orders must have a delivery date
- Order items must reference existing sellers
- Payment values must match order totals
- All payment values must be positive
- Timestamps must follow logical ordering (purchase -> approval -> delivery)

dbt's built-in `not_null`, `unique`, and `relationships` tests are also
applied across staging and mart models.

> **Note:** some tests are intentionally failing. This is by design —
> the purpose is to demonstrate that the testing layer is active and
> capable of catching real data issues, not just passing silently. The
> failures reflect actual inconsistencies present in the raw Olist
> dataset (such as orders with mismatched payment totals or missing
> delivery timestamps) and serve as proof that the assertions are
> working as intended.

---

## Dataset

The source data is the Olist Brazilian E-Commerce dataset, available
publicly on Kaggle. It covers ~100k orders placed between 2016 and 2018
across multiple marketplaces in Brazil, including order status, pricing,
payments, freight, customer and seller locations, and product categories.
