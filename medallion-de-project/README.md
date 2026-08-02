# Medallion Batch Pipeline — Retail Orders

A small, self-contained data engineering portfolio project: a **batch
pipeline** built around the **medallion architecture** (Bronze → Silver →
Gold), orchestrated with **Apache Airflow**, processing synthetic daily
e-commerce order data.

It's intentionally scoped small enough to read end-to-end in a few minutes,
but structured the way a production pipeline would be — partitioned data,
config-driven rules, a data quality gate, unit tests, and a real
orchestration DAG rather than a single monolithic script.

> See [`docs/architecture.md`](docs/architecture.md) for diagrams, and
> [`docs/adf_equivalent.md`](docs/adf_equivalent.md) for how this maps onto
> Azure Data Factory instead of Airflow.

## Architecture

```
raw (CSV landing zone)
   │
   ▼
BRONZE   — raw copy + ingestion metadata, partitioned by batch_date
   │
   ▼
SILVER   — cleaned, deduped, typed, conformed into dim/fact tables
   │
   ▼
data quality gate  — fails the pipeline if checks don't pass
   │
   ▼
GOLD     — business marts: daily_sales_summary, customer_lifetime_value, top_products
```

Orchestrated end-to-end by the Airflow DAG in
[`dags/medallion_batch_pipeline.py`](dags/medallion_batch_pipeline.py):

```
generate_raw_data
        │
        ▼
┌───────┼────────┐
▼       ▼        ▼
bronze_orders  bronze_customers  bronze_products      (parallel)
└───────┼────────┘
        ▼
  silver_transform
        ▼
 data_quality_check
        ▼
   gold_aggregate
```

## Project structure

```
.
├── dags/
│   └── medallion_batch_pipeline.py   # Airflow DAG (orchestration)
├── src/
│   ├── generate_raw_data.py          # simulates an upstream daily batch drop
│   ├── bronze_ingest.py              # raw -> bronze
│   ├── silver_transform.py           # bronze -> silver (cleaning + conforming)
│   ├── gold_aggregate.py             # silver -> gold (business marts)
│   ├── data_quality_checks.py        # DQ gate between silver and gold
│   └── utils.py                      # logging, config, partitioned I/O helpers
├── config/
│   └── pipeline_config.yaml          # table names + data quality rules
├── data/
│   ├── raw/2026-08-01/               # sample committed batch (see below)
│   ├── bronze/  silver/  gold/       # generated locally when you run the pipeline
├── tests/
│   └── test_transformations.py       # unit tests for the silver cleaning rules
├── docs/
│   ├── architecture.md               # diagrams + design rationale
│   └── adf_equivalent.md             # how this maps onto Azure Data Factory
├── docker-compose.yml                # local Airflow (LocalExecutor + Postgres)
└── requirements.txt
```

## Running it

### Option A — run the layers directly (no Airflow needed)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# a sample raw batch for 2026-08-01 is already committed under data/raw/,
# but you can generate a fresh one (or a new day) any time:
python src/generate_raw_data.py --batch-date 2026-08-01

python src/bronze_ingest.py --batch-date 2026-08-01
python src/silver_transform.py --batch-date 2026-08-01
python src/data_quality_checks.py --batch-date 2026-08-01
python src/gold_aggregate.py --batch-date 2026-08-01
```

Inspect the result:
```bash
python -c "import pandas as pd; print(pd.read_parquet('data/gold/daily_sales_summary/data.parquet'))"
# or just open data/gold/*/data.csv directly
```

### Option B — run it orchestrated, through Airflow

```bash
docker compose up airflow-init   # one-time: creates the metadata DB + admin user
docker compose up                # starts the webserver + scheduler
```

Open **http://localhost:8080** (`admin` / `admin`), unpause
`medallion_batch_pipeline`, and trigger a run — or just wait for the daily
schedule. Each task's logs (visible in the UI) are the same log lines the
CLI scripts print, since the DAG calls the exact same Python functions.

### Running the tests

```bash
pytest tests/
```

## Data quality

`src/data_quality_checks.py` runs after Silver and before Gold, checking
things like: no nulls in key columns, uniqueness of `order_id`, and no
non-positive quantities. If any check fails, the DAG task fails (with
Airflow's retry policy applied) rather than letting bad data silently reach
the Gold marts. In a larger production setup this would likely be a
dedicated framework (Great Expectations, dbt tests, or Soda) — the pattern
here is the same, just implemented directly for transparency.

## Design notes

- **Partitioning**: Bronze/Silver are partitioned by `batch_date`
  (`table/batch_date=YYYY-MM-DD/`), the standard Hive-style layout used on
  S3/ADLS, which makes re-processing a single past day idempotent.
- **Gold is cumulative**: Gold marts rebuild from *all* Silver history each
  run (not just the triggering batch), since metrics like customer lifetime
  value aren't meaningful scoped to one day.
- **Bronze is immutable and "dumb" on purpose**: it never drops or fixes
  data — that's Silver's job. This keeps Bronze replayable if a cleaning
  rule needs to change later.
- **Config-driven rules**: cleaning/DQ rules live in
  `config/pipeline_config.yaml` rather than being hardcoded, so adding a
  new source table doesn't require touching the transformation code.

## What's synthetic here vs. what's real

- `generate_raw_data.py` — synthetic; stands in for whatever normally lands
  raw data in a lake (a Fivetran sync, a CDC job, a nightly SQL export). In
  a real deployment this task wouldn't exist in the DAG at all.
- Everything else — Bronze/Silver/Gold logic, the DQ gate, the Airflow DAG,
  the tests — runs exactly as it would against real data of this shape.

## Possible extensions

- Swap the local Airflow + local parquet setup for S3/ADLS + a managed
  Airflow (MWAA / Composer) or Azure Data Factory (see
  [`docs/adf_equivalent.md`](docs/adf_equivalent.md))
- Replace `data_quality_checks.py` with Great Expectations or dbt tests
- Add a `dbt` project on top of Silver instead of hand-written Gold
  aggregation scripts
- Add Slack/email alerting on DAG failure
