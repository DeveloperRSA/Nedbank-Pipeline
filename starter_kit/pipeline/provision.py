"""
Gold layer provisioning — Stage 2.

Changes from Stage 1:
- fact_transactions now has 15 fields (merchant_subcategory added at pos 9).
- ORPHANED_ACCOUNT is resolved here: transactions with no matching dim_accounts
  row are quarantined and excluded from fact_transactions.
- Returns gold record counts for the DQ report.
"""

from __future__ import annotations
import logging
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType, LongType

from pipeline.config_loader import load_config
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_provisioning(config: dict | None = None) -> dict:
    """Execute Gold provisioning. Returns gold record counts for the DQ report."""
    if config is None:
        config = load_config()

    spark    = get_spark(config)
    silver   = config["output"]["silver_path"]
    gold     = config["output"]["gold_path"]
    quar     = config["output"].get("quarantine_path",
                                    f"{config['output']['bronze_path']}/quarantine")

    silver_customers    = spark.read.format("delta").load(f"{silver}/customers")
    silver_accounts     = spark.read.format("delta").load(f"{silver}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver}/transactions")

    dim_customers = _build_dim_customers(silver_customers)
    dim_accounts  = _build_dim_accounts(silver_accounts)
    fact_txns, orphan_count = _build_fact_transactions(
        silver_transactions, dim_accounts, dim_customers, quar
    )

    dim_customers.write.format("delta").mode("overwrite").save(f"{gold}/dim_customers")
    dim_accounts.write.format("delta").mode("overwrite").save(f"{gold}/dim_accounts")
    fact_txns.write.format("delta").mode("overwrite").save(f"{gold}/fact_transactions")

    counts = {
        "dim_customers":   dim_customers.count(),
        "dim_accounts":    dim_accounts.count(),
        "fact_transactions": fact_txns.count(),
        "ORPHANED_ACCOUNT":  orphan_count,
    }
    logger.info("Gold provisioning complete. Counts: %s", counts)
    return counts


# ── Surrogate key ──────────────────────────────────────────────────────────────

def _sha2_sk(col_expr) -> "Column":
    return F.conv(
        F.sha2(col_expr.cast("string"), 256).substr(1, 15), 16, 10
    ).cast(LongType())


# ── dim_customers ──────────────────────────────────────────────────────────────

def _build_dim_customers(df: DataFrame) -> DataFrame:
    pipeline_run_date = date.today()
    return (
        df
        .withColumn("customer_sk", _sha2_sk(F.col("customer_id")))
        .withColumn("_age",
            F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / F.lit(365.25))
        )
        .withColumn("age_band",
            F.when(F.col("_age") >= 65, "65+")
             .when(F.col("_age") >= 56, "56-65")
             .when(F.col("_age") >= 46, "46-55")
             .when(F.col("_age") >= 36, "36-45")
             .when(F.col("_age") >= 26, "26-35")
             .when(F.col("_age") >= 18, "18-25")
             .otherwise(F.lit(None).cast("string"))
        )
        .drop("_age", "dob")
        .select("customer_sk", "customer_id", "gender", "province",
                "income_band", "segment", "risk_score", "kyc_status", "age_band")
    )


# ── dim_accounts ───────────────────────────────────────────────────────────────

def _build_dim_accounts(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("account_sk", _sha2_sk(F.col("account_id")))
        .withColumnRenamed("customer_ref", "customer_id")
        .select("account_sk", "account_id", "customer_id", "account_type",
                "account_status", "open_date", "product_tier", "digital_channel",
                "credit_limit", "current_balance", "last_activity_date")
    )


# ── fact_transactions ──────────────────────────────────────────────────────────

def _build_fact_transactions(
    silver_txns: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
    quarantine_path: str,
) -> tuple[DataFrame, int]:
    """
    Returns (fact_transactions_df, orphan_count).
    Orphaned transactions are written to the quarantine table and excluded
    from the returned DataFrame.
    """
    # Lightweight lookups
    acc_lookup = dim_accounts.select(
        F.col("account_id").alias("_acc_id"),
        F.col("account_sk").alias("_account_sk"),
        F.col("customer_id").alias("_acc_customer_id"),
    )
    cust_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("customer_sk").alias("_customer_sk"),
    )

    df = silver_txns.join(acc_lookup,
                          silver_txns["account_id"] == acc_lookup["_acc_id"],
                          how="left").drop("_acc_id")

    # ── ORPHANED_ACCOUNT: quarantine and count ─────────────────────────────────
    orphaned   = df.filter(F.col("_account_sk").isNull())
    clean_df   = df.filter(F.col("_account_sk").isNotNull())
    orphan_count = orphaned.count()

    if orphan_count > 0:
        (orphaned
         .withColumn("quarantine_reason", F.lit("ORPHANED_ACCOUNT"))
         .write.format("delta").mode("append")
         .save(f"{quarantine_path}/transactions"))
        logger.warning("  ORPHANED_ACCOUNT quarantined: %d", orphan_count)

    df = clean_df

    # Resolve customer_sk
    df = df.join(cust_lookup,
                 df["_acc_customer_id"] == cust_lookup["_cust_id"],
                 how="left").drop("_cust_id")

    df = df.withColumn("transaction_sk", _sha2_sk(F.col("transaction_id")))

    # 15-field select (Stage 2 schema)
    df = df.select(
        F.col("transaction_sk"),
        F.col("transaction_id"),
        F.col("_account_sk").alias("account_sk"),
        F.col("_customer_sk").alias("customer_sk"),
        F.col("transaction_date"),
        F.col("transaction_timestamp"),
        F.col("transaction_type"),
        F.col("merchant_category"),
        F.col("merchant_subcategory"),      # position 9 — new in Stage 2
        F.col("amount"),
        F.col("currency"),
        F.col("channel"),
        F.col("province"),
        F.col("dq_flag"),
        F.col("ingestion_timestamp"),
    )

    return df, orphan_count