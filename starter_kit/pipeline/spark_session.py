"""
Shared SparkSession factory.

All pipeline modules import get_spark() to obtain (or reuse) the single
SparkSession for the run.  Keeping one session alive avoids the ~10-second
JVM startup overhead that would be paid three times if each stage created
its own session.
"""

import os
from pyspark.sql import SparkSession


_spark: SparkSession | None = None


def get_spark(config: dict | None = None) -> SparkSession:
    """Return the active SparkSession, creating it if necessary."""
    global _spark

    if _spark is not None:
        return _spark

    master = "local[2]"
    app_name = "nedbank-de-pipeline"

    if config:
        spark_cfg = config.get("spark", {})
        master = spark_cfg.get("master", master)
        app_name = spark_cfg.get("app_name", app_name)

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Keep shuffle partitions low — we have 2 cores and ~100K–1M rows
        .config("spark.sql.shuffle.partitions", "8")
        # Write temp files to /tmp (512 MB tmpfs provided by the eval system)
        .config("spark.local.dir", "/tmp")
        # Reduce driver/executor overhead
        .config("spark.driver.memory", "1800m")
        .config("spark.executor.memory", "1800m")
    )

    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark