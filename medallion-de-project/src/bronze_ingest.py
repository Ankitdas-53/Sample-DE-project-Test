"""
bronze_ingest.py
-----------------
BRONZE LAYER — raw ingestion, no business logic.

Responsibility: land the source files as-is (schema preserved, values
untouched) into the lake, adding only technical metadata:
  - _source_file      : which file this row came from (lineage)
  - _batch_date        : the logical batch/partition this belongs to
  - _ingested_at        : wall-clock ingestion timestamp

This is intentionally "dumb" — Bronze should never silently drop or fix
data, because it's the immutable, replayable copy of the source. If Silver
logic changes later, Bronze lets you reprocess history without re-pulling
from the source system.
"""
import argparse
from datetime import datetime, timezone

import pandas as pd

from utils import DATA_DIR, get_logger, load_config, write_parquet_partitioned

logger = get_logger("bronze_ingest")


def ingest_table(table: str, batch_date: str) -> pd.DataFrame:
    raw_path = DATA_DIR / "raw" / batch_date / f"{table}.csv"
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Expected raw file not found: {raw_path}. "
            f"Run generate_raw_data.py first, or check the upstream drop."
        )

    df = pd.read_csv(raw_path)
    df["_source_file"] = str(raw_path.name)
    df["_batch_date"] = batch_date
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    out_path = write_parquet_partitioned(df, layer="bronze", table=table, batch_date=batch_date)
    logger.info(f"table={table} batch_date={batch_date} rows={len(df)} -> {out_path}")
    return df


def run(batch_date: str):
    config = load_config()
    for table in config["raw_tables"]:
        ingest_table(table, batch_date)
    logger.info(f"Bronze ingestion complete for batch_date={batch_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze layer: raw -> bronze parquet")
    parser.add_argument("--batch-date", required=True, help="Batch date, e.g. 2026-08-01")
    args = parser.parse_args()
    run(args.batch_date)
