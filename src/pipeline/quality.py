"""Quality gate between silver and gold. Raises QualityError so the Airflow task fails."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from pipeline.config import Settings


class QualityError(Exception):
    pass


def check(spark: SparkSession, settings: Settings, day: date) -> dict[str, float]:
    dt = f"dt={day.isoformat()}"
    valid = spark.read.parquet(str(settings.layer("silver", "orders") / dt))
    rejected = spark.read.parquet(str(settings.layer("silver", "orders_rejected") / dt))
    n_valid, n_rej = valid.count(), rejected.count()
    total = n_valid + n_rej
    ratio = n_rej / total if total else 1.0
    problems = []
    if n_valid == 0:
        problems.append("silver has no valid orders")
    if valid.select("order_id").distinct().count() != n_valid:
        problems.append("duplicate order_id in silver")
    if ratio > settings.max_reject_ratio:
        problems.append(f"reject ratio {ratio:.1%} > {settings.max_reject_ratio:.0%}")
    if problems:
        raise QualityError(f"{day}: " + "; ".join(problems))
    return {"valid": n_valid, "rejected": n_rej, "reject_ratio": round(ratio, 4)}
