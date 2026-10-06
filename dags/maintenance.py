"""DAG 3 — weekly housekeeping: row-count audit and retention cleanup."""

from airflow import DAG

from common import START, step

with DAG(
    "maintenance", start_date=START, schedule="@weekly", catchup=False, tags=["pipeline"]
) as dag:
    step("audit", "audit") >> step("cleanup", "cleanup --keep-days 30")
