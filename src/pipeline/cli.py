"""`python -m pipeline <command> [--date YYYY-MM-DD]` — what the Airflow tasks call."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from datetime import date

from pipeline import generate
from pipeline.config import Settings


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeline")
    p.add_argument(
        "command", choices=["generate", "bronze", "silver", "quality", "gold", "audit", "cleanup"]
    )
    p.add_argument("--date", type=date.fromisoformat, default=date.today())
    p.add_argument("--all", action="store_true", help="gold: rebuild every silver day")
    p.add_argument("--keep-days", type=int, default=30)
    args = p.parse_args(argv)
    settings = Settings.from_env()

    if args.command == "generate":
        generate.land_raw(settings, args.date)
        result: object = {"landed": args.date.isoformat()}
    elif args.command == "cleanup":
        from pipeline import maintenance

        result = maintenance.cleanup(settings, args.date, args.keep_days)
    else:  # everything below needs Spark
        from pipeline import bronze, gold, maintenance, quality, silver
        from pipeline.spark import get_spark

        spark = get_spark(args.command)
        run: dict[str, Callable[[], object]] = {
            "bronze": lambda: bronze.ingest(spark, settings, args.date),
            "silver": lambda: silver.transform(spark, settings, args.date),
            "quality": lambda: quality.check(spark, settings, args.date),
            "gold": lambda: (
                gold.build_all(spark, settings)
                if args.all
                else gold.build(spark, settings, args.date)
            ),
            "audit": lambda: maintenance.audit(spark, settings),
        }
        result = run[args.command]()
    print(json.dumps(result))
    return 0
