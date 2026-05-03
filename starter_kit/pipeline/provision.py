"""
Gold layer: Build the dimensional model from Silver tables.

Outputs
-------
dim_customers  (9 fields)  — customer_sk, customer_id, gender, province,
                             income_band, segment, risk_score, kyc_status,
                             age_band (derived from dob)

dim_accounts   (11 fields) — account_sk, account_id, customer_id (renamed from
                             customer_ref), account_type, account_status,
                             open_date, product_tier, digital_channel,
                             credit_limit, current_balance, last_activity_date

fact_transactions (14 fields) — transaction_sk, transaction_id, account_sk,
                                customer_sk, transaction_date,
                                transaction_timestamp, transaction_type,
                                merchant_category, amount, currency, channel,
                                province, dq_flag, ingestion_timestamp

Surrogate key strategy
----------------------
sha2(natural_key, 256) cast to BIGINT — deterministic and stable across
pipeline re-runs on the same input data.

age_band derivation
-------------------
As specified in output_schema_spec.md §4:
  age = floor((pipeline_run_date - dob) / 365.25)
  Bucketed into: 18-25, 26-35, 36-45, 46-55, 56-65, 65+
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType, LongType

from pipeline.config_loader import load_config
from pipeline.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_provisioning(config: dict | None = None) -> None:
    """Execute the Gold layer provisioning stage."""
    if config is None:
        config = load_config()

    spark = get_spark(config)
    silver = config["output"]["silver_path"]
    gold = config["output"]["gold_path"]

    # Read Silver tables
    silver_customers = spark.read.format("delta").load(f"{silver}/customers")
    silver_accounts = spark.read.format("delta").load(f"{silver}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver}/transactions")

    # Build Gold dims first, then fact (fact needs SK lookups)
    dim_customers = _build_dim_customers(silver_customers)
    dim_accounts = _build_dim_accounts(silver_accounts)
    fact_transactions = _build_fact_transactions(
        silver_transactions, dim_accounts, dim_customers
    )

    # Write Gold tables as Delta
    dim_customers.write.format("delta").mode("overwrite").save(f"{gold}/dim_customers")
    logger.info("  dim_customers rows: %d", dim_customers.count())

    dim_accounts.write.format("delta").mode("overwrite").save(f"{gold}/dim_accounts")
    logger.info("  dim_accounts rows: %d", dim_accounts.count())

    fact_transactions.write.format("delta").mode("overwrite").save(f"{gold}/fact_transactions")
    logger.info("  fact_transactions rows: %d", fact_transactions.count())

    logger.info("Gold provisioning complete.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sha2_sk(col_expr) -> "Column":
    """Generate a stable BIGINT surrogate key via SHA-256 hash."""
    return F.conv(F.sha2(col_expr.cast("string"), 256).substr(1, 15), 16, 10).cast(LongType())


# ── dim_customers ─────────────────────────────────────────────────────────────

def _build_dim_customers(silver_customers: DataFrame) -> DataFrame:
    """Build dim_customers with 9 fields per output_schema_spec.md §4."""
    pipeline_run_date = date.today()

    df = (
        silver_customers
        .withColumn(
            "customer_sk",
            _sha2_sk(F.col("customer_id")),
        )
        # age_band: floor((pipeline_run_date - dob) / 365.25)
        .withColumn(
            "_age",
            F.floor(
                F.datediff(F.lit(pipeline_run_date), F.col("dob")) / F.lit(365.25)
            ),
        )
        .withColumn(
            "age_band",
            F.when(F.col("_age") >= 65, "65+")
             .when(F.col("_age") >= 56, "56-65")
             .when(F.col("_age") >= 46, "46-55")
             .when(F.col("_age") >= 36, "36-45")
             .when(F.col("_age") >= 26, "26-35")
             .when(F.col("_age") >= 18, "18-25")
             .otherwise(F.lit(None).cast("string")),
        )
        .drop("_age", "dob")  # dob must NOT appear in Gold output
        # Select exactly 9 fields in specified order
        .select(
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        )
    )

    return df


# ── dim_accounts ──────────────────────────────────────────────────────────────

def _build_dim_accounts(silver_accounts: DataFrame) -> DataFrame:
    """Build dim_accounts with 11 fields per output_schema_spec.md §3.

    customer_ref → customer_id rename happens here at the Gold layer.
    """
    df = (
        silver_accounts
        .withColumn("account_sk", _sha2_sk(F.col("account_id")))
        # Rename customer_ref → customer_id
        .withColumnRenamed("customer_ref", "customer_id")
        # Select exactly 11 fields in specified order
        .select(
            "account_sk",
            "account_id",
            "customer_id",          # position 3 — required by Validation Query 2
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        )
    )

    return df


# ── fact_transactions ─────────────────────────────────────────────────────────

def _build_fact_transactions(
    silver_transactions: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """Build fact_transactions with 14 fields per output_schema_spec.md §2."""

    # Build a lightweight lookup: account_id → (account_sk, customer_id)
    account_lookup = dim_accounts.select(
        F.col("account_id").alias("_acc_id"),
        F.col("account_sk").alias("_account_sk"),
        F.col("customer_id").alias("_acc_customer_id"),
    )

    # Build a lightweight lookup: customer_id → customer_sk
    customer_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("customer_sk").alias("_customer_sk"),
    )

    df = silver_transactions

    # ── Resolve account_sk via left join ──────────────────────────────────────
    df = df.join(
        account_lookup,
        df["account_id"] == account_lookup["_acc_id"],
        how="left",
    ).drop("_acc_id")

    # ── Flag orphaned transactions (no matching account) ──────────────────────
    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("_account_sk").isNull(),
            F.lit("ORPHANED_ACCOUNT"),
        ).otherwise(F.col("dq_flag")),
    )

    # ── Resolve customer_sk via account → customer chain ──────────────────────
    df = df.join(
        customer_lookup,
        df["_acc_customer_id"] == customer_lookup["_cust_id"],
        how="left",
    ).drop("_cust_id")

    # ── Generate transaction_sk (deterministic on transaction_id) ─────────────
    df = df.withColumn("transaction_sk", _sha2_sk(F.col("transaction_id")))

    # ── Final column selection — exactly 14 fields ────────────────────────────
    df = df.select(
        F.col("transaction_sk"),
        F.col("transaction_id"),
        F.col("_account_sk").alias("account_sk"),
        F.col("_customer_sk").alias("customer_sk"),
        F.col("transaction_date"),
        F.col("transaction_timestamp"),
        F.col("transaction_type"),
        F.col("merchant_category"),
        F.col("amount"),
        F.col("currency"),
        F.col("channel"),
        F.col("province"),
        F.col("dq_flag"),
        F.col("ingestion_timestamp"),
    )

    return df