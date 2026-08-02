"""
utils.py
--------
Small shared helpers used across the bronze/silver/gold layers, kept in one
place the way a real project would centralize logging config, path
resolution, and I/O helpers rather than repeating them per script.
"""
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"


def get_logger(name: str) -> logging.Logger:
    """Return a standard logger so every layer logs consistently
    (this is what Airflow task logs would show in the UI)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def write_parquet_partitioned(df: pd.DataFrame, layer: str, table: str, batch_date: str) -> Path:
    """Write a dataframe as parquet, partitioned by batch_date, mirroring how
    a lakehouse (S3/ADLS + Hive-style partitioning) is laid out in production."""
    out_path = DATA_DIR / layer / table / f"batch_date={batch_date}"
    out_path.mkdir(parents=True, exist_ok=True)
    file_path = out_path / "data.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def read_parquet_partition(layer: str, table: str, batch_date: str) -> pd.DataFrame:
    file_path = DATA_DIR / layer / table / f"batch_date={batch_date}" / "data.parquet"
    if not file_path.exists():
        raise FileNotFoundError(f"No {layer}/{table} data found for batch_date={batch_date} at {file_path}")
    return pd.read_parquet(file_path)


def read_all_partitions(layer: str, table: str) -> pd.DataFrame:
    """Read every partition for a table (used by Gold, which aggregates
    across all available history rather than a single day)."""
    table_dir = DATA_DIR / layer / table
    frames = []
    for partition_dir in sorted(table_dir.glob("batch_date=*")):
        file_path = partition_dir / "data.parquet"
        if file_path.exists():
            frames.append(pd.read_parquet(file_path))
    if not frames:
        raise FileNotFoundError(f"No partitions found for {layer}/{table}")
    return pd.concat(frames, ignore_index=True)
