"""
medallion_batch_pipeline.py
-----------------------------
Airflow DAG that orchestrates the daily batch medallion pipeline:

    generate_raw_data (simulates upstream drop — remove in real prod)
            |
            v
    +-------+--------+--------+
    |                |        |
 bronze_orders  bronze_customers  bronze_products      <- run in parallel
    |                |        |
    +-------+--------+--------+
            v
      silver_transform
            v
     data_quality_check
            v
      gold_aggregate

Design notes (why it's built this way):
  - Bronze tasks fan out per source table and run in parallel, since they're
    independent of each other — this is where Airflow's DAG-based execution
    actually pays off over a linear script.
  - Silver depends on ALL bronze tasks completing (fan-in) because it joins
    orders/customers/products together.
  - A data_quality_check task gates Gold: if Silver produced bad data,
    the pipeline stops before it reaches business-facing marts.
  - `execution_date`/`ds` (Airflow's logical date) is used as the batch_date,
    which is the standard pattern for reprocessable, idempotent batch DAGs —
    rerunning a past `ds` reprocesses exactly that day's partition.

To actually run this DAG you need a local Airflow instance — see
docker-compose.yml and the README for the two-command local setup.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

# Make src/ importable from within the Airflow worker
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from generate_raw_data import main as generate_raw_data_main  # noqa: E402
import bronze_ingest  # noqa: E402
import silver_transform  # noqa: E402
import gold_aggregate  # noqa: E402
import data_quality_checks  # noqa: E402


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # would be True + alert email/Slack webhook in prod
}


def _bronze_task(table_name, **context):
    batch_date = context["ds"]
    bronze_ingest.ingest_table(table_name, batch_date)


def _silver_task(**context):
    batch_date = context["ds"]
    silver_transform.run(batch_date)


def _dq_task(**context):
    batch_date = context["ds"]
    data_quality_checks.run(batch_date)


def _gold_task(**context):
    batch_date = context["ds"]
    gold_aggregate.run(batch_date)


with DAG(
    dag_id="medallion_batch_pipeline",
    description="Daily batch pipeline: raw -> bronze -> silver -> gold (medallion architecture)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "batch", "portfolio-project"],
) as dag:

    # NOTE: in a real pipeline this task wouldn't exist — data would already
    # be landing in raw/ from an upstream extraction job. It's kept here so
    # the DAG is runnable end-to-end as a demo.
    generate_raw_data = PythonOperator(
        task_id="generate_raw_data",
        python_callable=lambda **context: generate_raw_data_main(
            ["--batch-date", context["ds"]]
        ),
    )

    bronze_orders = PythonOperator(
        task_id="bronze_ingest_orders",
        python_callable=_bronze_task,
        op_kwargs={"table_name": "orders"},
    )

    bronze_customers = PythonOperator(
        task_id="bronze_ingest_customers",
        python_callable=_bronze_task,
        op_kwargs={"table_name": "customers"},
    )

    bronze_products = PythonOperator(
        task_id="bronze_ingest_products",
        python_callable=_bronze_task,
        op_kwargs={"table_name": "products"},
    )

    silver_task = PythonOperator(
        task_id="silver_transform",
        python_callable=_silver_task,
    )

    dq_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=_dq_task,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregate",
        python_callable=_gold_task,
    )

    generate_raw_data >> [bronze_orders, bronze_customers, bronze_products]
    [bronze_orders, bronze_customers, bronze_products] >> silver_task
    silver_task >> dq_task >> gold_task
