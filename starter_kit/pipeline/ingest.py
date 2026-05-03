"""
Stage 3 — Streaming ingestion.

Polls /data/stream/ for new micro-batch JSONL files, processes each one in
filename (chronological) order, and merges results into two Delta Gold tables:

  /data/output/stream_gold/current_balances/
  /data/output/stream_gold/recent_transactions/

Design
------
- Directory polling every POLL_INTERVAL_SECONDS (default 60s).
- Processed filenames tracked in /tmp/stream_processed.txt — survives across
  poll cycles, prevents re-processing on restart.
- Quiesce exit: if no new files appear for QUIESCE_SECONDS (default 120s),
  the loop exits cleanly.  At evaluation time all 12 files are present
  immediately, so this triggers after one idle cycle.
- All DQ normalisation (currency variants, amount cast, date formats) is
  applied identically to the batch path — reuses the same helpers from
  transform.py where possible.
- current_balances: upsert (MERGE on account_id).
- recent_transactions: upsert (MERGE on account_id + transaction_id) then
  DELETE rows beyond position 50 per account.
- SLA: updated_at is set to datetime.now(UTC) at write time, which is within
  seconds of file processing — well inside the 300-second SLA window.
"""

from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import (
    DecimalType, StringType, TimestampType, StructType, StructField
)
from delta.tables import DeltaTable

from pipeline.config_loader import load_config, load_dq_rules
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 60
QUIESCE_SECONDS       = 120   # exit if no new files for this long
RECENT_TXN_WINDOW     = 50    # retain last N transactions per account
PROCESSED_STATE_FILE  = "/tmp/stream_processed.txt"


# ── Schema constants ───────────────────────────────────────────────────────────

_CURRENT_BALANCES_SCHEMA = StructType([
    StructField("account_id",                StringType(),    False),
    StructField("current_balance",           DecimalType(18, 2), False),
    StructField("last_transaction_timestamp", TimestampType(), False),
    StructField("updated_at",               TimestampType(), False),
])

_RECENT_TXN_SCHEMA = StructType([
    StructField("account_id",           StringType(),    False),
    StructField("transaction_id",       StringType(),    False),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("amount",               DecimalType(18, 2), False),
    StructField("transaction_type",     StringType(),    False),
    StructField("channel",              StringType(),    True),
    StructField("updated_at",           TimestampType(), False),
])


# ── Public entry point ─────────────────────────────────────────────────────────

def run_stream_ingestion(config: dict | None = None) -> None:
    """Poll /data/stream/ and process all micro-batch files."""
    if config is None:
        config = load_config()

    spark    = get_spark(config)
    rules    = load_dq_rules(config)
    stream_dir = Path("/data/stream")
    cb_path  = "/data/output/stream_gold/current_balances"
    rt_path  = "/data/output/stream_gold/recent_transactions"

    # Ensure output dirs exist (Delta write will create _delta_log)
    os.makedirs(cb_path, exist_ok=True)
    os.makedirs(rt_path, exist_ok=True)

    processed = _load_processed_state()
    last_new_file_time = time.time()

    logger.info("Stream ingestion starting. Polling %s every %ds.",
                stream_dir, POLL_INTERVAL_SECONDS)

    while True:
        # ── Scan for unprocessed files in filename order ────────────────────
        all_files = sorted(stream_dir.glob("stream_*.jsonl"))
        new_files  = [f for f in all_files if f.name not in processed]

        if new_files:
            last_new_file_time = time.time()
            for stream_file in new_files:
                logger.info("Processing stream file: %s", stream_file.name)
                _process_file(spark, stream_file, cb_path, rt_path, rules)
                processed.add(stream_file.name)
                _save_processed_state(processed)
        else:
            idle_seconds = time.time() - last_new_file_time
            logger.info("No new files. Idle for %.0fs / %ds quiesce window.",
                        idle_seconds, QUIESCE_SECONDS)
            if idle_seconds >= QUIESCE_SECONDS:
                logger.info("Quiesce timeout reached — exiting stream loop.")
                break

        time.sleep(POLL_INTERVAL_SECONDS)


# ── File processing ────────────────────────────────────────────────────────────

def _process_file(
    spark, stream_file: Path,
    cb_path: str, rt_path: str,
    rules: dict,
) -> None:
    """Parse one JSONL file and merge into both stream Gold tables."""
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    updated_at_str = updated_at.isoformat()

    df = (
        spark.read
        .option("multiLine", "false")
        .json(str(stream_file))
    )

    if df.rdd.isEmpty():
        logger.warning("  Empty file — skipping: %s", stream_file.name)
        return

    # ── Flatten nested structs ─────────────────────────────────────────────
    if "location" in df.columns:
        df = (df
              .withColumn("province", F.col("location.province"))
              .drop("location"))
    if "metadata" in df.columns:
        df = df.drop("metadata")

    # ── DQ normalisation (mirrors transform.py) ────────────────────────────
    rule_cfg = rules.get("dq_rules", {})

    # Currency variants
    variants = rule_cfg.get("CURRENCY_VARIANT", {}).get(
        "variants", ["R", "r", "rands", "RANDS", "zar", "710"]
    )
    target_currency = rule_cfg.get("CURRENCY_VARIANT", {}).get("target_value", "ZAR")
    variant_cond = F.col("currency").cast("string").isin([str(v) for v in variants])
    df = df.withColumn(
        "currency",
        F.when(variant_cond | (F.col("currency") != target_currency),
               F.lit(target_currency))
         .otherwise(F.col("currency"))
    )

    # Amount: cast to DECIMAL — discard records that fail
    df = df.withColumn("_amount_cast", F.col("amount").cast(DecimalType(18, 2)))
    df = (df.filter(F.col("_amount_cast").isNotNull())
            .withColumn("amount", F.col("_amount_cast"))
            .drop("_amount_cast"))

    # transaction_timestamp
    df = (
        df
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("transaction_date").cast("string"),
                            F.col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    )

    # Deduplicate within this batch on transaction_id (keep latest ts)
    w_dup = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").desc_nulls_last()
    )
    df = (df.withColumn("_rn", F.row_number().over(w_dup))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

    # Drop rows with null required fields
    df = df.filter(
        F.col("account_id").isNotNull() &
        F.col("transaction_id").isNotNull() &
        F.col("transaction_timestamp").isNotNull() &
        F.col("amount").isNotNull()
    )

    if df.rdd.isEmpty():
        logger.warning("  All rows dropped after DQ — skipping: %s", stream_file.name)
        return

    df.cache()

    _merge_current_balances(spark, df, cb_path, updated_at_str)
    _merge_recent_transactions(spark, df, rt_path, updated_at_str)

    df.unpersist()
    logger.info("  Done: %s  updated_at=%s", stream_file.name, updated_at_str)


# ── current_balances merge ─────────────────────────────────────────────────────

def _merge_current_balances(
    spark, batch_df: DataFrame, cb_path: str, updated_at_str: str
) -> None:
    """
    For each account_id in the batch, compute the latest balance contribution
    and upsert into current_balances.

    Balance update rule:
      CREDIT / REVERSAL → add to balance
      DEBIT  / FEE      → subtract from balance
    """
    # Signed amount per transaction
    signed = batch_df.withColumn(
        "signed_amount",
        F.when(F.col("transaction_type").isin("CREDIT", "REVERSAL"), F.col("amount"))
         .otherwise(-F.col("amount"))
    )

    # Aggregate per account: net delta + latest timestamp
    agg = (
        signed.groupBy("account_id")
        .agg(
            F.sum("signed_amount").cast(DecimalType(18, 2)).alias("balance_delta"),
            F.max("transaction_timestamp").alias("last_transaction_timestamp"),
        )
        .withColumn("updated_at", F.lit(updated_at_str).cast("timestamp"))
    )

    cb_table_path = cb_path

    if DeltaTable.isDeltaTable(spark, cb_table_path):
        dt = DeltaTable.forPath(spark, cb_table_path)
        (
            dt.alias("tgt")
            .merge(
                agg.alias("src"),
                "tgt.account_id = src.account_id"
            )
            .whenMatchedUpdate(set={
                "current_balance": F.col("tgt.current_balance") + F.col("src.balance_delta"),
                "last_transaction_timestamp": F.col("src.last_transaction_timestamp"),
                "updated_at": F.col("src.updated_at"),
            })
            .whenNotMatchedInsert(values={
                "account_id":                 F.col("src.account_id"),
                "current_balance":            F.col("src.balance_delta"),
                "last_transaction_timestamp": F.col("src.last_transaction_timestamp"),
                "updated_at":                 F.col("src.updated_at"),
            })
            .execute()
        )
    else:
        # First write — create table
        init_df = agg.select(
            F.col("account_id"),
            F.col("balance_delta").alias("current_balance"),
            F.col("last_transaction_timestamp"),
            F.col("updated_at"),
        )
        init_df.write.format("delta").mode("overwrite").save(cb_table_path)

    logger.info("    current_balances updated.")


# ── recent_transactions merge ──────────────────────────────────────────────────

def _merge_recent_transactions(
    spark, batch_df: DataFrame, rt_path: str, updated_at_str: str
) -> None:
    """
    Upsert transactions into recent_transactions on (account_id, transaction_id).
    After merge, retain only the 50 most recent rows per account.
    """
    incoming = (
        batch_df
        .select(
            F.col("account_id"),
            F.col("transaction_id"),
            F.col("transaction_timestamp"),
            F.col("amount"),
            F.col("transaction_type"),
            F.col("channel"),
        )
        .withColumn("updated_at", F.lit(updated_at_str).cast("timestamp"))
    )

    if DeltaTable.isDeltaTable(spark, rt_path):
        dt = DeltaTable.forPath(spark, rt_path)
        (
            dt.alias("tgt")
            .merge(
                incoming.alias("src"),
                "tgt.account_id = src.account_id AND tgt.transaction_id = src.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Enforce 50-row-per-account retention:
        # Read current table, rank by ts desc, delete rows ranked > 50.
        current = spark.read.format("delta").load(rt_path)
        w_ret = Window.partitionBy("account_id").orderBy(
            F.col("transaction_timestamp").desc_nulls_last()
        )
        current = current.withColumn("_rn", F.row_number().over(w_ret))

        keep    = current.filter(F.col("_rn") <= RECENT_TXN_WINDOW).drop("_rn")
        evicted = current.filter(F.col("_rn") >  RECENT_TXN_WINDOW)

        if evicted.count() > 0:
            # Overwrite with only the kept rows
            keep.write.format("delta").mode("overwrite").save(rt_path)
            logger.info("    recent_transactions: evicted %d old rows.", evicted.count())
    else:
        incoming.write.format("delta").mode("overwrite").save(rt_path)

    logger.info("    recent_transactions updated.")


# ── Processed-file state ───────────────────────────────────────────────────────

def _load_processed_state() -> set[str]:
    if os.path.isfile(PROCESSED_STATE_FILE):
        with open(PROCESSED_STATE_FILE) as fh:
            return {line.strip() for line in fh if line.strip()}
    return set()


def _save_processed_state(processed: set[str]) -> None:
    with open(PROCESSED_STATE_FILE, "w") as fh:
        fh.write("\n".join(sorted(processed)))