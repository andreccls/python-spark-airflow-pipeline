"""Bronze: raw data as delivered, all strings, plus lineage columns. Re-runnable per day."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline import schemas
from pipeline.config import Settings


def _stamp(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp())


def ingest(spark: SparkSession, settings: Settings, day: date) -> dict[str, int]:
    raw = settings.data_dir / "raw"
    orders = spark.read.schema(schemas.RAW_ORDERS).json(
        str(raw / "orders" / f"dt={day.isoformat()}" / "orders.jsonl")
    )
    customers = spark.read.schema(schemas.RAW_CUSTOMERS).csv(
        str(raw / "customers" / "customers.csv"), header=True
    )
    products = spark.read.schema(schemas.RAW_PRODUCTS).csv(
        str(raw / "products" / "products.csv"), header=True
    )
    # orders: one directory per day, overwritten => idempotent. dims: full snapshots.
    out = settings.layer("bronze", "orders") / f"dt={day.isoformat()}"
    _stamp(orders).write.mode("overwrite").parquet(str(out))
    _stamp(customers).write.mode("overwrite").parquet(str(settings.layer("bronze", "customers")))
    _stamp(products).write.mode("overwrite").parquet(str(settings.layer("bronze", "products")))
    return {
        "orders": spark.read.parquet(str(out)).count(),
        "customers": customers.count(),
        "products": products.count(),
    }
