"""
generate_raw_data.py
---------------------
Simulates a daily batch drop from an upstream OLTP system (e.g. an e-commerce
order database). In a real production setup this file would not exist —
raw data would land in an S3/ADLS bucket or a landing DB table via an
extraction job (Fivetran, a CDC tool, a nightly SQL export, etc).

It exists here purely so this portfolio project is self-contained and
reproducible: running it creates a new day's worth of "raw" CSV files
under data/raw/<batch_date>/, deliberately including the kind of mess
real source systems produce (duplicates, nulls, inconsistent casing,
occasional bad rows) so the Bronze/Silver layers have something real to do.

Usage:
    python src/generate_raw_data.py --batch-date 2026-08-01 --num-orders 500
"""
import argparse
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

PRODUCT_CATALOG = [
    ("P001", "Wireless Mouse", "Electronics", 19.99),
    ("P002", "Mechanical Keyboard", "Electronics", 89.99),
    ("P003", "USB-C Hub", "Electronics", 34.50),
    ("P004", "Standing Desk", "Furniture", 349.00),
    ("P005", "Office Chair", "Furniture", 219.00),
    ("P006", "Notebook Set", "Stationery", 12.75),
    ("P007", "Water Bottle", "Lifestyle", 15.00),
    ("P008", "Desk Lamp", "Furniture", 42.30),
    ("P009", "Backpack", "Lifestyle", 65.00),
    ("P010", "Webcam 1080p", "Electronics", 54.99),
]

CITIES = ["Bangalore", "Mumbai", "Delhi", "Chennai", "Pune", "Hyderabad", None]


def _rand_customer_id(n_customers: int) -> str:
    return f"C{random.randint(1, n_customers):04d}"


def generate_customers(n_customers: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n_customers + 1):
        # Inject messiness: inconsistent casing, occasional missing city,
        # occasional duplicate customer row (simulates upstream re-sends).
        rows.append({
            "customer_id": f"C{i:04d}",
            "customer_name": f"Customer {i}",
            "email": f"customer{i}@example.com".upper() if i % 7 == 0 else f"customer{i}@example.com",
            "city": random.choice(CITIES),
            "signup_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d"),
        })
    df = pd.DataFrame(rows)
    # duplicate a few rows to mimic re-delivered records
    dupes = df.sample(frac=0.03, random_state=1)
    return pd.concat([df, dupes], ignore_index=True)


def generate_products() -> pd.DataFrame:
    return pd.DataFrame(PRODUCT_CATALOG, columns=["product_id", "product_name", "category", "unit_price"])


def generate_orders(batch_date: str, n_orders: int, n_customers: int) -> pd.DataFrame:
    rows = []
    for i in range(n_orders):
        product = random.choice(PRODUCT_CATALOG)
        qty = random.choice([1, 1, 1, 2, 2, 3, -1])  # -1 simulates a bad/return record needing cleaning
        order_id = f"O{batch_date.replace('-', '')}{i:05d}"
        rows.append({
            "order_id": order_id,
            "customer_id": _rand_customer_id(n_customers),
            "product_id": product[0],
            "quantity": qty,
            "unit_price": product[3],
            "order_ts": (
                datetime.strptime(batch_date, "%Y-%m-%d")
                + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "status": random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", None]),
        })
    df = pd.DataFrame(rows)
    # inject a few nulls into quantity to simulate dirty source data
    null_idx = df.sample(frac=0.02, random_state=2).index
    df.loc[null_idx, "quantity"] = np.nan
    return df


def main(argv=None):
    """argv can be passed explicitly (e.g. by the Airflow DAG); defaults to
    sys.argv when run as a CLI script."""
    parser = argparse.ArgumentParser(description="Generate a synthetic daily raw batch drop.")
    parser.add_argument("--batch-date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--num-orders", type=int, default=500)
    parser.add_argument("--num-customers", type=int, default=120)
    args = parser.parse_args(argv)

    out_dir = RAW_DIR / args.batch_date
    out_dir.mkdir(parents=True, exist_ok=True)

    orders = generate_orders(args.batch_date, args.num_orders, args.num_customers)
    customers = generate_customers(args.num_customers)
    products = generate_products()

    orders.to_csv(out_dir / "orders.csv", index=False)
    customers.to_csv(out_dir / "customers.csv", index=False)
    products.to_csv(out_dir / "products.csv", index=False)

    print(f"[generate_raw_data] wrote batch for {args.batch_date} to {out_dir}")
    print(f"  orders.csv    : {len(orders)} rows")
    print(f"  customers.csv : {len(customers)} rows")
    print(f"  products.csv  : {len(products)} rows")


if __name__ == "__main__":
    main()
