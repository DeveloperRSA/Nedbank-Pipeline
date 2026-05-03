"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Strategy
--------
- Read each source file as-is (no transformation).
- Add a single `ingestion_timestamp` column that is the same for all rows
  in a given pipeline run (set once at the start of ingestion).
- Write to Delta format under /data/output/bronze/.
- Configuration (paths, Spark settings) is read from pipeline_config.yaml.
- No hardcoded paths.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from pipeline.config_loader import load_config
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_ingestion(config: dict | None = None) -> None:
    """Execute the Bronze layer ingestion stage."""
    if config is None:
        config = load_config()

    spark = get_spark(config)
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    inp = config["input"]
    out = config["output"]["bronze_path"]

    _ingest_accounts(spark, inp["accounts_path"], out, ingestion_ts)
    _ingest_customers(spark, inp["customers_path"], out, ingestion_ts)
    _ingest_transactions(spark, inp["transactions_path"], out, ingestion_ts)

    logger.info("Bronze ingestion complete.")


# ── Accounts ──────────────────────────────────────────────────────────────────

def _ingest_accounts(spark, src_path: str, bronze_path: str, ingestion_ts: str) -> None:
    logger.info("Ingesting accounts → bronze/accounts/")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # keep everything as STRING at Bronze
        .csv(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{bronze_path}/accounts")
    )
    logger.info("  accounts rows written: %d", df.count())


# ── Customers ─────────────────────────────────────────────────────────────────

def _ingest_customers(spark, src_path: str, bronze_path: str, ingestion_ts: str) -> None:
    logger.info("Ingesting customers → bronze/customers/")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{bronze_path}/customers")
    )
    logger.info("  customers rows written: %d", df.count())


# ── Transactions ──────────────────────────────────────────────────────────────

def _ingest_transactions(spark, src_path: str, bronze_path: str, ingestion_ts: str) -> None:
    logger.info("Ingesting transactions → bronze/transactions/")

    # JSONL — read as multiline=false (default: one JSON object per line)
    df = (
        spark.read
        .option("multiLine", "false")
        .json(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{bronze_path}/transactions")
    )
    logger.info("  transactions rows written: %d", df.count())