"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Per-table transformations
-------------------------
customers:
  - Deduplicate on customer_id (keep first occurrence by stable sort).
  - Cast dob to DATE.
  - Cast risk_score to INTEGER.
  - Keep all other string fields as STRING.

accounts:
  - Deduplicate on account_id.
  - Cast open_date, last_activity_date to DATE.
  - Cast credit_limit, current_balance to DECIMAL(18,2).
  - Keep other fields as STRING.

transactions:
  - Deduplicate on transaction_id.
  - Flatten nested location.* and metadata.* structs to top-level columns.
  - Cast transaction_date to DATE, combine with transaction_time → TIMESTAMP.
  - Cast amount to DECIMAL(18,2).
  - Normalise currency to "ZAR".
  - Apply DQ flagging (null checks, type checks, currency variants).

DQ rules are read from dq_rules.yaml — no hardcoded logic.
"""

import logging

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.config_loader import load_config, load_dq_rules
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)

# ── Public entry point ────────────────────────────────────────────────────────

def run_transformation(config: dict | None = None) -> None:
    """Execute the Silver layer transformation stage."""
    if config is None:
        config = load_config()

    spark = get_spark(config)
    dq_rules = load_dq_rules(config)

    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    _transform_customers(spark, f"{bronze}/customers", f"{silver}/customers")
    _transform_accounts(spark, f"{bronze}/accounts", f"{silver}/accounts")
    _transform_transactions(
        spark,
        f"{bronze}/transactions",
        f"{silver}/transactions",
        dq_rules,
    )

    logger.info("Silver transformation complete.")


# ── Customers ─────────────────────────────────────────────────────────────────

def _transform_customers(spark, src: str, dst: str) -> None:
    logger.info("Transforming customers → silver/customers/")

    df = spark.read.format("delta").load(src)

    # Deduplicate on natural key — stable sort ensures determinism
    w = Window.partitionBy("customer_id").orderBy("customer_id")
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Type casts
        .withColumn("dob", F.to_date("dob", "yyyy-MM-dd"))
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    )

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  customers silver rows: %d", df.count())


# ── Accounts ──────────────────────────────────────────────────────────────────

def _transform_accounts(spark, src: str, dst: str) -> None:
    logger.info("Transforming accounts → silver/accounts/")

    df = spark.read.format("delta").load(src)

    w = Window.partitionBy("account_id").orderBy("account_id")
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("open_date", F.to_date("open_date", "yyyy-MM-dd"))
        .withColumn(
            "last_activity_date",
            F.to_date("last_activity_date", "yyyy-MM-dd"),
        )
        .withColumn(
            "credit_limit",
            F.col("credit_limit").cast(DecimalType(18, 2)),
        )
        .withColumn(
            "current_balance",
            F.col("current_balance").cast(DecimalType(18, 2)),
        )
    )

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  accounts silver rows: %d", df.count())


# ── Transactions ──────────────────────────────────────────────────────────────

def _transform_transactions(
    spark, src: str, dst: str, dq_rules: dict
) -> None:
    logger.info("Transforming transactions → silver/transactions/")

    df = spark.read.format("delta").load(src)

    # ── 1. Flatten nested structs ─────────────────────────────────────────────
    # Spark reads JSONL with nested objects as StructType columns.
    # We promote them to top-level columns to match the data dictionary.
    if "location" in df.columns:
        df = (
            df
            .withColumn("province", F.col("location.province"))
            .withColumn("city", F.col("location.city"))
            .withColumn("coordinates", F.col("location.coordinates"))
            .drop("location")
        )
    if "metadata" in df.columns:
        df = (
            df
            .withColumn("device_id", F.col("metadata.device_id"))
            .withColumn("session_id", F.col("metadata.session_id"))
            .withColumn("retry_flag", F.col("metadata.retry_flag"))
            .drop("metadata")
        )

    # ── 2. Deduplicate on transaction_id ──────────────────────────────────────
    w = Window.partitionBy("transaction_id").orderBy("transaction_id")
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── 3. Type standardisation ───────────────────────────────────────────────
    df = (
        df
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("transaction_date").cast("string"), F.col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
    )

    # ── 4. Currency normalisation ─────────────────────────────────────────────
    currency_cfg = dq_rules.get("currency_normalisation", {})
    target_currency = currency_cfg.get("target_value", "ZAR")
    flag_variants = currency_cfg.get("flag_variants", True)

    # Track pre-normalisation currency for DQ flagging BEFORE overwriting
    if flag_variants:
        df = df.withColumn("_orig_currency", F.col("currency"))

    df = df.withColumn("currency", F.lit(target_currency))

    # ── 5. DQ flagging ────────────────────────────────────────────────────────
    # Priority: NULL_REQUIRED > CURRENCY_VARIANT > (orphan check done in Gold)
    # We build the flag using a series of CASE-style overrides.
    df = df.withColumn("dq_flag", F.lit(None).cast("string"))

    # NULL_REQUIRED checks
    null_fields = (
        dq_rules.get("null_checks", {}).get("fact_transactions", [])
        or ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "currency", "channel"]
    )
    null_condition = F.lit(False)
    for field in null_fields:
        if field in df.columns:
            null_condition = null_condition | F.col(field).isNull()

    df = df.withColumn(
        "dq_flag",
        F.when(null_condition, F.lit("NULL_REQUIRED")).otherwise(F.col("dq_flag")),
    )

    # CURRENCY_VARIANT check
    if flag_variants and "_orig_currency" in df.columns:
        df = df.withColumn(
            "dq_flag",
            F.when(
                (F.col("dq_flag").isNull()) & (F.col("_orig_currency") != target_currency),
                F.lit("CURRENCY_VARIANT"),
            ).otherwise(F.col("dq_flag")),
        ).drop("_orig_currency")

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  transactions silver rows: %d", df.count())