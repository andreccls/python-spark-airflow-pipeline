"""Row counts per table (audit) and retention cleanup of old day partitions."""

from __future__ import annotations

import shutil
from datetime import date, timedelta

from pyspark.sql import SparkSession

from pipeline.config import Settings

LAYERS = ("raw", "bronze", "silver", "gold")


def audit(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Rows per bronze/silver/gold table (raw is not parquet, so it is skipped)."""
    counts: dict[str, int] = {}
    for layer in LAYERS[1:]:
        base = settings.data_dir / layer
        for table in sorted(p for p in base.glob("*") if p.is_dir()):
            counts[f"{layer}/{table.name}"] = spark.read.parquet(str(table)).count()
    return counts


def cleanup(settings: Settings, today: date, keep_days: int) -> list[str]:
    """Delete `dt=` partitions older than keep_days from every layer. Returns what was removed."""
    cutoff = (today - timedelta(days=keep_days)).isoformat()
    removed = []
    for part in sorted(settings.data_dir.glob("*/*/dt=*")):
        if part.name[3:] < cutoff:
            shutil.rmtree(part)
            removed.append(str(part.relative_to(settings.data_dir)))
    return removed
