"""
data_quality_checks.py
-----------------------
Lightweight DQ gate run after Silver, before Gold. In production this
would typically be a dedicated framework (Great Expectations, dbt tests,
Soda), but the pattern is the same: assert expectations about the data
and fail the pipeline loudly if they're violated, rather than letting
bad data silently flow into Gold marts that stakeholders trust.
"""
import argparse
import sys

from utils import get_logger, read_parquet_partition

logger = get_logger("data_quality_checks")


class DataQualityError(Exception):
    pass


def check_no_nulls(df, columns, table_name):
    for col in columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise DataQualityError(f"{table_name}.{col} has {null_count} null values (expected 0)")


def check_unique(df, column, table_name):
    dupes = df[column].duplicated().sum()
    if dupes > 0:
        raise DataQualityError(f"{table_name}.{column} has {dupes} duplicate values (expected unique)")


def check_positive(df, column, table_name):
    bad = (df[column] <= 0).sum()
    if bad > 0:
        raise DataQualityError(f"{table_name}.{column} has {bad} non-positive values")


def run(batch_date: str):
    fact_orders = read_parquet_partition("silver", "fact_orders", batch_date)
    dim_customers = read_parquet_partition("silver", "dim_customers", batch_date)

    checks = [
        lambda: check_no_nulls(fact_orders, ["order_id", "customer_id", "product_id"], "fact_orders"),
        lambda: check_unique(fact_orders, "order_id", "fact_orders"),
        lambda: check_positive(fact_orders, "quantity", "fact_orders"),
        lambda: check_unique(dim_customers, "customer_id", "dim_customers"),
    ]

    failures = []
    for check in checks:
        try:
            check()
        except DataQualityError as e:
            failures.append(str(e))

    if failures:
        for f in failures:
            logger.error(f"DQ FAILED: {f}")
        raise DataQualityError(f"{len(failures)} data quality check(s) failed for batch_date={batch_date}")

    logger.info(f"All data quality checks passed for batch_date={batch_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver-layer data quality checks")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    try:
        run(args.batch_date)
    except DataQualityError as e:
        logger.error(str(e))
        sys.exit(1)
