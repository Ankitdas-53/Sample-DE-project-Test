# Azure Data Factory equivalent

This project orchestrates the medallion pipeline with Airflow because it's
free and runs entirely on a laptop, which makes the repo self-contained and
easy for anyone to clone and run. If you're targeting Azure-focused roles,
the exact same pipeline maps cleanly onto **Azure Data Factory (ADF)** —
the DAG structure doesn't change, only the execution engine does.

## Mapping

| Airflow (this repo)             | ADF equivalent |
|----------------------------------|-----------------|
| DAG (`medallion_batch_pipeline`) | Pipeline |
| `PythonOperator` task            | Copy Activity / Databricks Notebook Activity / Data Flow Activity |
| Task fan-out (`>> [a, b, c]`)    | ForEach Activity (parallel) or three independent Copy Activities |
| `data/raw/`, `data/bronze/`, etc.| ADLS Gen2 containers: `raw/`, `bronze/`, `silver/`, `gold/` |
| `@daily` schedule + `ds`         | Tumbling Window Trigger, with `WindowStart`/`WindowEnd` as the batch key |
| `data_quality_checks.py` gate    | Validation Activity, or an If Condition Activity checking row counts / a Data Flow assert |
| Airflow retries + alerting       | Activity `retry`/`retryIntervalInSeconds` + Web/Alert Activity on failure |
| `config/pipeline_config.yaml`    | ADF Global Parameters / Azure Key Vault-linked linked service parameters |

## Equivalent ADF pipeline shape

```
[Tumbling Window Trigger: daily]
        |
        v
[ForEach: source tables] --parallel--> Copy Activity (raw -> bronze, ADLS)
        |
        v
[Data Flow: silver_transform]   (clean, dedupe, join — same logic as silver_transform.py)
        |
        v
[Validation / If Condition: data quality gate]
        |
        v
[Data Flow: gold_aggregate]     (same three marts: daily_sales_summary, customer_lifetime_value, top_products)
```

## Why this repo ships Airflow instead

ADF pipelines are authored as JSON and tightly coupled to an Azure
subscription (Data Factory instance, ADLS storage account, linked
services), so they can't be cloned and run standalone the way this repo
can. The `src/*.py` transformation logic is intentionally engine-agnostic —
the same `bronze_ingest.py` / `silver_transform.py` / `gold_aggregate.py`
functions could run as Databricks Notebook Activities inside an actual ADF
pipeline with no rewrite needed, only different orchestration wrapping
around them.
