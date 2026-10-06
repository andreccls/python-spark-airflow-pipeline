"""SparkSession factory (local mode — see ADR 0002)."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "pipeline") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # keep `dt=2026-10-01` partitions as strings instead of inferring a date type
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
