# python-spark-airflow-pipeline

Reference data pipeline: synthetic e-commerce data -> PySpark medallion (raw, bronze, silver, gold)
orchestrated by Apache Airflow 2.10, all in Docker.

```
generate_raw -> bronze -> silver -> quality_gate --(Dataset)--> build_gold.gold_daily_sales
 (ingest_orders, daily)                                          (build_gold, data-aware)
maintenance (weekly): audit row counts -> cleanup partitions older than 30 days
```

Silver rejects bad orders (missing customer, quantity <= 0, invalid status) into
`silver/orders_rejected` with a reason. The quality gate fails the run if more than 15% of a
day's orders are rejected, which also stops gold.

## Run

```
make up        # Airflow at http://localhost:8085 (admin / admin); data lands in ./data
make down
make test | make coverage | make lint     # run in the dev image, no local Python needed
```

Trigger a day: `docker compose exec scheduler airflow dags trigger ingest_orders -e 2026-10-03T00:00:00+00:00`
(manual runs with a future logical date stay queued, that is Airflow behaviour).
CLI without Airflow: `python -m pipeline {generate|bronze|silver|quality|gold|audit|cleanup} --date YYYY-MM-DD`.

Layout: `src/pipeline` (logic), `dags/` (orchestration only), `tests/`, `docs/adr/`.
