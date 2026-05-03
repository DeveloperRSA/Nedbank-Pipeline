"""
Silver layer transformation — Stage 2.

Handles all six DQ issue types as per dq_rules.yaml.
Returns a dq_counts dict for the DQ report.

Key behaviours
--------------
DUPLICATE_DEDUPED   — dedup on transaction_id, keep earliest timestamp
ORPHANED_ACCOUNT    — detected in Gold (requires dim_accounts); flagged here
                      as a placeholder, resolved properly in provision.py
TYPE_MISMATCH       — amount as STRING → try cast to DECIMAL; fail → exclude
DATE_FORMAT         — multi-format date parsing (ISO, DD/MM/YYYY, epoch)
CURRENCY_VARIANT    — normalise all ZAR variants to "ZAR"
NULL_REQUIRED       — already quarantined in Bronze; accounts are clean here

merchant_subcategory — added as NULL column when absent from source (Stage 1
                       compatibility).
"""

from __future__ import annotations
import logging

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType

from pipeline.config_loader import load_config, load_dq_rules
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_transformation(config: dict | None = None) -> dict:
    """Execute Silver transformation. Returns dq_counts for the DQ report."""
    if config is None:
        config = load_config()

    spark    = get_spark(config)
    rules    = load_dq_rules(config)
    bronze   = config["output"]["bronze_path"]
    silver   = config["output"]["silver_path"]
    quar     = config["output"].get("quarantine_path", f"{bronze}/quarantine")

    dq_counts: dict[str, int] = {}

    _transform_customers(spark, f"{bronze}/customers", f"{silver}/customers",
                         rules, dq_counts)
    _transform_accounts(spark,  f"{bronze}/accounts",  f"{silver}/accounts",
                         rules, dq_counts)
    _transform_transactions(spark, f"{bronze}/transactions",
                            f"{silver}/transactions", quar, rules, dq_counts)

    logger.info("Silver transformation complete. DQ counts: %s", dq_counts)
    return dq_counts


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _parse_date(col_name: str, formats: list[str]) -> "Column":
    """
    Try ISO first; if null try DD/MM/YYYY; if still null try Unix epoch.
    Returns a DATE column.
    """
    result = F.to_date(F.col(col_name), "yyyy-MM-dd")

    if "dd/MM/yyyy" in formats:
        result = F.when(result.isNull(),
                        F.to_date(F.col(col_name), "dd/MM/yyyy")
                 ).otherwise(result)

    if "epoch" in formats:
        # Unix epoch: column value is an integer (seconds since 1970-01-01)
        result = F.when(
            result.isNull() & F.col(col_name).cast("long").isNotNull(),
            F.from_unixtime(F.col(col_name).cast("long"), "yyyy-MM-dd").cast("date"),
        ).otherwise(result)

    return result


def _count_non_iso_dates(df: DataFrame, col_name: str, formats: list[str]) -> int:
    """Count records where date was NOT in ISO format (needed for DQ report)."""
    iso_parsed = F.to_date(F.col(col_name), "yyyy-MM-dd")
    return df.filter(iso_parsed.isNull() & F.col(col_name).isNotNull()).count()


# ── Customers ──────────────────────────────────────────────────────────────────

def _transform_customers(
    spark, src: str, dst: str, rules: dict, dq_counts: dict
) -> None:
    logger.info("Transforming customers → silver/customers/")
    df = spark.read.format("delta").load(src)

    date_formats = rules.get("dq_rules", {}).get("DATE_FORMAT", {}).get(
        "date_formats", ["yyyy-MM-dd", "dd/MM/yyyy", "epoch"]
    )

    # DATE_FORMAT count for dob
    dob_issues = _count_non_iso_dates(df, "dob", date_formats)
    dq_counts["customers_date_format"] = dob_issues

    # Dedup
    w = Window.partitionBy("customer_id").orderBy("customer_id")
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("dob",        _parse_date("dob", date_formats))
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    )

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  customers silver rows: %d", df.count())


# ── Accounts ───────────────────────────────────────────────────────────────────

def _transform_accounts(
    spark, src: str, dst: str, rules: dict, dq_counts: dict
) -> None:
    logger.info("Transforming accounts → silver/accounts/")
    df = spark.read.format("delta").load(src)

    date_formats = rules.get("dq_rules", {}).get("DATE_FORMAT", {}).get(
        "date_formats", ["yyyy-MM-dd", "dd/MM/yyyy", "epoch"]
    )

    open_date_issues = _count_non_iso_dates(df, "open_date", date_formats)
    last_act_issues  = _count_non_iso_dates(df, "last_activity_date", date_formats)
    dq_counts["accounts_date_format"] = open_date_issues + last_act_issues

    w = Window.partitionBy("account_id").orderBy("account_id")
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("open_date",          _parse_date("open_date",         date_formats))
        .withColumn("last_activity_date", _parse_date("last_activity_date", date_formats))
        .withColumn("credit_limit",    F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  accounts silver rows: %d", df.count())


# ── Transactions ───────────────────────────────────────────────────────────────

def _transform_transactions(
    spark, src: str, dst: str, quarantine_path: str,
    rules: dict, dq_counts: dict
) -> None:
    logger.info("Transforming transactions → silver/transactions/")

    df = spark.read.format("delta").load(src)
    rule_cfg = rules.get("dq_rules", {})
    date_formats = rule_cfg.get("DATE_FORMAT", {}).get(
        "date_formats", ["yyyy-MM-dd", "dd/MM/yyyy", "epoch"]
    )
    currency_variants = rule_cfg.get("CURRENCY_VARIANT", {}).get(
        "variants", ["R", "r", "rands", "RANDS", "zar", "710"]
    )
    currency_target = rule_cfg.get("CURRENCY_VARIANT", {}).get("target_value", "ZAR")

    # ── Flatten nested structs ────────────────────────────────────────────────
    if "location" in df.columns:
        df = (df
              .withColumn("province",    F.col("location.province"))
              .withColumn("city",        F.col("location.city"))
              .withColumn("coordinates", F.col("location.coordinates"))
              .drop("location"))
    if "metadata" in df.columns:
        df = (df
              .withColumn("device_id",  F.col("metadata.device_id"))
              .withColumn("session_id", F.col("metadata.session_id"))
              .withColumn("retry_flag", F.col("metadata.retry_flag"))
              .drop("metadata"))

    # ── merchant_subcategory: Stage 1 compat ──────────────────────────────────
    optional_fields = (
        rules.get("schema", {}).get("optional_transaction_fields", [])
        or ["merchant_subcategory"]
    )
    for field in optional_fields:
        if field not in df.columns:
            df = df.withColumn(field, F.lit(None).cast("string"))

    # ── DUPLICATE_DEDUPED: keep earliest transaction_timestamp ────────────────
    # Count duplicates BEFORE dedup
    w_dup = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").asc_nulls_last()
    )
    df = df.withColumn("_rn_dup", F.row_number().over(w_dup))

    dup_count = df.filter(F.col("_rn_dup") > 1).count()
    dq_counts["DUPLICATE_DEDUPED"] = dup_count

    df = df.filter(F.col("_rn_dup") == 1).drop("_rn_dup")

    # ── DATE_FORMAT: transaction_date ─────────────────────────────────────────
    date_issues = _count_non_iso_dates(df, "transaction_date", date_formats)
    dq_counts["transactions_date_format"] = date_issues

    df = df.withColumn("transaction_date", _parse_date("transaction_date", date_formats))

    # Rebuild transaction_timestamp after date normalisation
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date").cast("string"),
                        F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # ── TYPE_MISMATCH: amount as string ───────────────────────────────────────
    # Detect: any record where the raw JSON type was string (Spark reads it as
    # StringType when the column is heterogeneous).
    amount_col = df.schema["amount"].dataType
    type_mismatch_count = 0

    if str(amount_col) in ("StringType()", "StringType"):
        # Count records where it's genuinely a string (not already numeric-typed)
        type_mismatch_count = df.filter(F.col("amount").isNotNull()).count()
        # Cast — records that fail cast will produce NULL
        df = df.withColumn("_amount_cast",
                           F.col("amount").cast(DecimalType(18, 2)))

        # Exclude records where cast failed (non-numeric string)
        failed_cast = df.filter(
            F.col("amount").isNotNull() & F.col("_amount_cast").isNull()
        )
        failed_count = failed_cast.count()
        if failed_count > 0:
            (failed_cast
             .withColumn("quarantine_reason", F.lit("TYPE_MISMATCH_CAST_FAILED"))
             .write.format("delta").mode("append")
             .save(f"{quarantine_path}/transactions"))
            logger.warning("  TYPE_MISMATCH cast failures quarantined: %d", failed_count)

        df = df.filter(
            F.col("amount").isNull() | F.col("_amount_cast").isNotNull()
        ).withColumn("amount", F.col("_amount_cast")).drop("_amount_cast")
    else:
        # Already numeric — still normalise to DECIMAL(18,2)
        df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))

    dq_counts["TYPE_MISMATCH"] = type_mismatch_count

    # ── CURRENCY_VARIANT: normalise to ZAR ────────────────────────────────────
    variant_condition = F.col("currency").cast("string").isin(
        [str(v) for v in currency_variants]
    )
    currency_variant_count = df.filter(variant_condition).count()
    dq_counts["CURRENCY_VARIANT"] = currency_variant_count

    df = df.withColumn(
        "currency",
        F.when(variant_condition | (F.col("currency") != currency_target),
               F.lit(currency_target))
         .otherwise(F.col("currency"))
    )

    # ── NULL_REQUIRED: mark rows missing required non-PK fields ───────────────
    # PK null (transaction_id) is treated as a data error — flag it
    null_fields = ["transaction_id", "account_id", "transaction_date",
                   "amount", "transaction_type", "currency", "channel"]
    null_cond = F.lit(False)
    for f in null_fields:
        if f in df.columns:
            null_cond = null_cond | F.col(f).isNull()
    null_req_count = df.filter(null_cond).count()
    dq_counts["NULL_REQUIRED_transactions"] = null_req_count

    # ── Build dq_flag ─────────────────────────────────────────────────────────
    # Priority: NULL_REQUIRED > TYPE_MISMATCH > CURRENCY_VARIANT
    # ORPHANED_ACCOUNT is resolved in provision.py after dim_accounts is built.
    df = df.withColumn("dq_flag", F.lit(None).cast("string"))

    df = df.withColumn(
        "dq_flag",
        F.when(null_cond, F.lit("NULL_REQUIRED"))
         .when(F.col("dq_flag").isNull() & variant_condition,
               F.lit("CURRENCY_VARIANT"))
         .otherwise(F.col("dq_flag"))
    )

    df.write.format("delta").mode("overwrite").save(dst)
    logger.info("  transactions silver rows: %d", df.count())