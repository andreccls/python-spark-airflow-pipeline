"""Silver: typed, deduplicated, conformed. Bad orders go to a rejects table with a reason."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import Settings
from pipeline.schemas import VALID_STATUSES


def clean_orders(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, rejected). Exact duplicates collapse to one row (not a rejection)."""
    typed = (
        bronze.drop("_ingested_at")
        .dropDuplicates(["order_id"])
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .withColumn("ordered_at", F.to_timestamp("ordered_at"))
        .withColumn("status", F.upper(F.trim("status")))
    )
    reason = (
        F.when(F.col("customer_id").isNull(), "missing_customer")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(~F.col("status").isin(*VALID_STATUSES), "invalid_status")
    )
    flagged = typed.withColumn("reject_reason", reason)
    valid = flagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    return valid, rejected


def clean_customers(bronze: DataFrame) -> DataFrame:
    """Latest version per customer, e-mail normalised."""
    latest = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    return (
        bronze.drop("_ingested_at")
        .withColumn("_rn", F.row_number().over(latest))
        .where("_rn = 1")
        .drop("_rn")
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )


def clean_products(bronze: DataFrame) -> DataFrame:
    return bronze.drop("_ingested_at").withColumn("price", F.col("price").cast("decimal(10,2)"))


def transform(spark: SparkSession, settings: Settings, day: date) -> dict[str, int]:
    dt = f"dt={day.isoformat()}"
    valid, rejected = clean_orders(spark.read.parquet(str(settings.layer("bronze", "orders") / dt)))
    valid.write.mode("overwrite").parquet(str(settings.layer("silver", "orders") / dt))
    rejected.write.mode("overwrite").parquet(str(settings.layer("silver", "orders_rejected") / dt))
    clean_customers(spark.read.parquet(str(settings.layer("bronze", "customers")))).write.mode(
        "overwrite"
    ).parquet(str(settings.layer("silver", "customers")))
    clean_products(spark.read.parquet(str(settings.layer("bronze", "products")))).write.mode(
        "overwrite"
    ).parquet(str(settings.layer("silver", "products")))
    return {
        "orders": spark.read.parquet(str(settings.layer("silver", "orders") / dt)).count(),
        "orders_rejected": spark.read.parquet(
            str(settings.layer("silver", "orders_rejected") / dt)
        ).count(),
    }
