"""DAG 1 — daily: land raw -> bronze -> silver -> quality gate. Publishes the silver Dataset."""

from airflow import DAG

from common import SILVER_ORDERS, START, step

with DAG(
    "ingest_orders",
    start_date=START,
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["pipeline"],
) as dag:
    generate = step("generate_raw", "generate")
    bronze = step("bronze", "bronze")
    silver = step("silver", "silver")
    quality = step("quality_gate", "quality", outlets=[SILVER_ORDERS])
    generate >> bronze >> silver >> quality
