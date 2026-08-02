"""
silver_transform.py
--------------------
SILVER LAYER — cleaned, conformed, business-rule-applied data.

Responsibility:
  - enforce types
  - deduplicate
  - drop/flag records that violate basic data-quality rules
  - standardize text (casing, trimming)
  - join raw entities into a conformed model (fact_orders + dim tables)

Silver is queryable and trustworthy, but not yet aggregated for a specific
business question — that's what Gold is for.
"""
import argparse

import numpy as np
import pandas as pd

from utils import get_logger, load_config, read_parquet_partition, write_parquet_partitioned

logger = get_logger("silver_transform")


def clean_customers(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    df = df.copy()
    df["email"] = df["email"].str.lower().str.strip()
    df["city"] = df["city"].fillna("UNKNOWN").str.strip()
    df = df.drop_duplicates(subset=rules["dedupe_on"], keep="first")
    return df


def clean_products(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    df = df.copy()
    df["product_name"] = df["product_name"].str.strip()
    df = df.drop_duplicates(subset=rules["dedupe_on"], keep="first")
    return df


def clean_orders(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    df = df.copy()

    before = len(df)
    df = df.dropna(subset=rules["drop_null_columns"])

    # quantity: coerce, then drop nulls / non-positive rows (returns/bad data)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df[df["quantity"] >= rules["min_quantity"]]

    # status: unknowns get normalized to a known bucket rather than dropped,
    # since a missing status shouldn't silently delete a real order
    df["status"] = df["status"].where(df["status"].isin(rules["valid_status_values"]), "UNKNOWN")

    df["order_ts"] = pd.to_datetime(df["order_ts"])
    df["order_date"] = df["order_ts"].dt.date.astype(str)
    df["line_total"] = df["quantity"] * df["unit_price"]

    df = df.drop_duplicates(subset=["order_id"], keep="first")

    after = len(df)
    logger.info(f"orders: {before} raw rows -> {after} clean rows ({before - after} dropped by DQ rules)")
    return df


def run(batch_date: str):
    config = load_config()
    rules = config["silver_rules"]

    customers_raw = read_parquet_partition("bronze", "customers", batch_date)
    products_raw = read_parquet_partition("bronze", "products", batch_date)
    orders_raw = read_parquet_partition("bronze", "orders", batch_date)

    customers = clean_customers(customers_raw, rules["customers"])
    products = clean_products(products_raw, rules["products"])
    orders = clean_orders(orders_raw, rules["orders"])

    # conform: join orders -> customers/products so Silver's fact table
    # already carries descriptive attributes analysts commonly need
    fact_orders = (
        orders
        .merge(customers[["customer_id", "customer_name", "city"]], on="customer_id", how="left")
        .merge(products[["product_id", "product_name", "category"]], on="product_id", how="left")
    )

    for name, df in [
        ("dim_customers", customers),
        ("dim_products", products),
        ("fact_orders", fact_orders),
    ]:
        out_path = write_parquet_partitioned(df, layer="silver", table=name, batch_date=batch_date)
        logger.info(f"table={name} batch_date={batch_date} rows={len(df)} -> {out_path}")

    logger.info(f"Silver transform complete for batch_date={batch_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver layer: bronze -> cleaned/conformed silver")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    run(args.batch_date)
