"""DAG 2 — data-aware: runs whenever DAG 1 publishes fresh, quality-checked silver."""

from airflow import DAG

from common import SILVER_ORDERS, START, step

with DAG(
    "build_gold",
    start_date=START,
    schedule=[SILVER_ORDERS],
    catchup=False,
    default_args={"retries": 1},
    tags=["pipeline"],
) as dag:
    # a dataset-triggered run has no meaningful {{ ds }}, so rebuild all silver days (idempotent)
    step("gold_daily_sales", "gold --all", with_date=False)
