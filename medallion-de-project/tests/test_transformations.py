"""
test_transformations.py
------------------------
Unit tests for the Silver-layer cleaning rules. Run with:
    pytest tests/

These test the pure transformation functions directly (no filesystem I/O),
which is the pattern that scales — you don't want to run the whole pipeline
just to check that a negative quantity gets filtered out.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from silver_transform import clean_orders, clean_customers  # noqa: E402


ORDER_RULES = {
    "drop_null_columns": ["order_id", "customer_id"],
    "valid_status_values": ["COMPLETED", "CANCELLED"],
    "min_quantity": 1,
}

CUSTOMER_RULES = {"dedupe_on": ["customer_id"]}


def test_clean_orders_drops_negative_quantity():
    df = pd.DataFrame({
        "order_id": ["O1", "O2"],
        "customer_id": ["C1", "C2"],
        "product_id": ["P1", "P1"],
        "quantity": [2, -1],
        "unit_price": [10.0, 10.0],
        "order_ts": ["2026-08-01 10:00:00", "2026-08-01 11:00:00"],
        "status": ["COMPLETED", "COMPLETED"],
    })
    result = clean_orders(df, ORDER_RULES)
    assert len(result) == 1
    assert result.iloc[0]["order_id"] == "O1"


def test_clean_orders_computes_line_total():
    df = pd.DataFrame({
        "order_id": ["O1"],
        "customer_id": ["C1"],
        "product_id": ["P1"],
        "quantity": [3],
        "unit_price": [10.0],
        "order_ts": ["2026-08-01 10:00:00"],
        "status": ["COMPLETED"],
    })
    result = clean_orders(df, ORDER_RULES)
    assert result.iloc[0]["line_total"] == 30.0


def test_clean_orders_normalizes_unknown_status():
    df = pd.DataFrame({
        "order_id": ["O1"],
        "customer_id": ["C1"],
        "product_id": ["P1"],
        "quantity": [1],
        "unit_price": [10.0],
        "order_ts": ["2026-08-01 10:00:00"],
        "status": [None],
    })
    result = clean_orders(df, ORDER_RULES)
    assert result.iloc[0]["status"] == "UNKNOWN"


def test_clean_customers_dedupes_and_lowercases_email():
    df = pd.DataFrame({
        "customer_id": ["C1", "C1"],
        "customer_name": ["Alice", "Alice"],
        "email": ["ALICE@EXAMPLE.COM", "ALICE@EXAMPLE.COM"],
        "city": ["Bangalore", "Bangalore"],
        "signup_date": ["2024-01-01", "2024-01-01"],
    })
    result = clean_customers(df, CUSTOMER_RULES)
    assert len(result) == 1
    assert result.iloc[0]["email"] == "alice@example.com"


def test_clean_customers_fills_missing_city():
    df = pd.DataFrame({
        "customer_id": ["C1"],
        "customer_name": ["Alice"],
        "email": ["alice@example.com"],
        "city": [None],
        "signup_date": ["2024-01-01"],
    })
    result = clean_customers(df, CUSTOMER_RULES)
    assert result.iloc[0]["city"] == "UNKNOWN"
