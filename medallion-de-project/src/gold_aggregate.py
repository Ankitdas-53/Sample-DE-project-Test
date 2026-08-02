"""
gold_aggregate.py
------------------
GOLD LAYER — business-level aggregates ("data marts") built on top of Silver.

Responsibility: answer specific business questions. These are the tables
a BI tool (Power BI / Looker / Tableau) or a stakeholder query would hit
directly. Gold tables here are rebuilt from the *full* Silver history each
run (not just the current batch_date) since metrics like lifetime value
are inherently cumulative — a common and intentional pattern, distinct
from Bronze/Silver which are batch-partitioned.
"""
import argparse
from datetime import datetime, timezone

import pandas as pd

from utils import DATA_DIR, get_logger, read_all_partitions

logger = get_logger("gold_aggregate")


def build_daily_sales_summary(fact_orders: pd.DataFrame) -> pd.DataFrame:
    completed = fact_orders[fact_orders["status"] == "COMPLETED"]
    summary = (
        completed.groupby(["order_date", "category"])
        .agg(
            total_revenue=("line_total", "sum"),
            total_orders=("order_id", "nunique"),
            total_units=("quantity", "sum"),
        )
        .reset_index()
        .sort_values(["order_date", "category"])
    )
    return summary


def build_customer_lifetime_value(fact_orders: pd.DataFrame) -> pd.DataFrame:
    completed = fact_orders[fact_orders["status"] == "COMPLETED"]
    clv = (
        completed.groupby(["customer_id", "customer_name", "city"])
        .agg(
            lifetime_revenue=("line_total", "sum"),
            total_orders=("order_id", "nunique"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        )
        .reset_index()
        .sort_values("lifetime_revenue", ascending=False)
    )
    return clv


def build_top_products(fact_orders: pd.DataFrame) -> pd.DataFrame:
    completed = fact_orders[fact_orders["status"] == "COMPLETED"]
    top_products = (
        completed.groupby(["product_id", "product_name", "category"])
        .agg(total_revenue=("line_total", "sum"), total_units_sold=("quantity", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    return top_products


def run(batch_date: str):
    # Gold aggregates over ALL history available so far (cumulative marts),
    # while still logging which batch_date run triggered the refresh.
    fact_orders = read_all_partitions("silver", "fact_orders")

    marts = {
        "daily_sales_summary": build_daily_sales_summary(fact_orders),
        "customer_lifetime_value": build_customer_lifetime_value(fact_orders),
        "top_products": build_top_products(fact_orders),
    }

    for name, df in marts.items():
        out_dir = DATA_DIR / "gold" / name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        df.to_parquet(out_path, index=False)
        # also drop a CSV copy since gold marts are often consumed by
        # non-engineers (analysts, BI tools) who want something they can open directly
        df.to_csv(out_dir / "data.csv", index=False)
        logger.info(f"mart={name} rows={len(df)} refreshed_at={datetime.now(timezone.utc).isoformat()} -> {out_path}")

    logger.info(f"Gold aggregation complete (triggered by batch_date={batch_date})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold layer: silver -> business aggregates")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    run(args.batch_date)
