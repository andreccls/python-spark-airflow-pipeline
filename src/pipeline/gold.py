"""Gold: business-ready aggregate, one partition per day (overwritten => idempotent)."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Settings


def daily_sales(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    return (
        orders.join(customers.select("customer_id", "state"), "customer_id", "left")
        .join(products.select("product_id", "category"), "product_id", "left")
        .groupBy("category", "state")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)").alias("revenue"),
        )
        .orderBy("category", "state")
    )


def build(spark: SparkSession, settings: Settings, day: date) -> int:
    dt = f"dt={day.isoformat()}"
    out = daily_sales(
        spark.read.parquet(str(settings.layer("silver", "orders") / dt)),
        spark.read.parquet(str(settings.layer("silver", "customers"))),
        spark.read.parquet(str(settings.layer("silver", "products"))),
    )
    target = str(settings.layer("gold", "daily_sales") / dt)
    out.write.mode("overwrite").parquet(target)
    return spark.read.parquet(target).count()


def build_all(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Rebuild gold for every silver day. Safe because build() overwrites per day.

    ponytail: rebuilds all days each run; track a watermark if the history grows large.
    """
    days = sorted(
        date.fromisoformat(p.name[3:]) for p in settings.layer("silver", "orders").glob("dt=*")
    )
    return {d.isoformat(): build(spark, settings, d) for d in days}
