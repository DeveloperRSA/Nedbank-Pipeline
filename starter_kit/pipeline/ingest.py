"""
Bronze layer ingestion — Stage 2.

Changes from Stage 1:
- Accounts: NULL account_id records are detected here and routed to the
  quarantine table before the clean table is written. Counts are tracked
  for the DQ report.
- Transactions: merchant_subcategory is handled as an optional field —
  present in Stage 2 data, absent in Stage 1 data. Bronze reads whatever
  is in the file; Silver/Gold add a NULL column if the key is missing.
- All three files are read ONCE each. Counts are returned for the DQ report.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

from pipeline.config_loader import load_config
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_ingestion(config: dict | None = None) -> dict:
    """
    Execute Bronze ingestion.
    Returns raw record counts dict for the DQ report.
    """
    if config is None:
        config = load_config()

    spark = get_spark(config)
    ingestion_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    inp  = config["input"]
    out  = config["output"]["bronze_path"]
    quar = config["output"].get("quarantine_path", f"{out}/quarantine")

    counts = {}

    counts["customers_raw"] = _ingest_customers(
        spark, inp["customers_path"], out, ingestion_ts
    )
    counts["accounts_raw"], counts["accounts_null_pk"] = _ingest_accounts(
        spark, inp["accounts_path"], out, quar, ingestion_ts
    )
    counts["transactions_raw"] = _ingest_transactions(
        spark, inp["transactions_path"], out, ingestion_ts
    )

    logger.info("Bronze ingestion complete. Counts: %s", counts)
    return counts


# ── Customers ──────────────────────────────────────────────────────────────────

def _ingest_customers(
    spark, src_path: str, bronze_path: str, ingestion_ts: str
) -> int:
    logger.info("Ingesting customers → bronze/customers/")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/customers")
    logger.info("  customers bronze rows: %d", count)
    return count


# ── Accounts ───────────────────────────────────────────────────────────────────

def _ingest_accounts(
    spark, src_path: str, bronze_path: str, quarantine_path: str, ingestion_ts: str
) -> tuple[int, int]:
    """
    Returns (total_raw_count, null_pk_count).
    Null-PK records go to quarantine table, not the clean bronze table.
    """
    logger.info("Ingesting accounts → bronze/accounts/")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )

    total = df.count()

    # NULL_REQUIRED: account_id IS NULL → quarantine
    null_pk   = df.filter(F.col("account_id").isNull())
    clean     = df.filter(F.col("account_id").isNotNull())

    null_count = null_pk.count()

    if null_count > 0:
        (
            null_pk
            .withColumn("quarantine_reason", F.lit("NULL_REQUIRED"))
            .write.format("delta").mode("append")
            .save(f"{quarantine_path}/accounts")
        )
        logger.warning("  accounts quarantined (NULL_REQUIRED): %d", null_count)

    clean.write.format("delta").mode("overwrite").save(f"{bronze_path}/accounts")
    logger.info("  accounts bronze rows (clean): %d", total - null_count)
    return total, null_count


# ── Transactions ───────────────────────────────────────────────────────────────

def _ingest_transactions(
    spark, src_path: str, bronze_path: str, ingestion_ts: str
) -> int:
    logger.info("Ingesting transactions → bronze/transactions/")
    df = (
        spark.read
        .option("multiLine", "false")
        .json(src_path)
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )
    # merchant_subcategory: Spark infers it from JSON if present; if absent the
    # column simply won't exist. Silver handles the missing-column case.
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/transactions")
    logger.info("  transactions bronze rows: %d", count)
    return count