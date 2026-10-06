"""Shared bits for the DAGs. Tasks only shell out to `python -m pipeline`: logic lives in src/."""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

SILVER_ORDERS = Dataset("file:///opt/airflow/data/silver/orders")
START = datetime(2026, 10, 1)


def step(task_id: str, command: str, with_date: bool = True, **kwargs) -> BashOperator:
    # {{ ds }} = the logical date, so reruns and backfills hit the same partition
    return BashOperator(
        task_id=task_id,
        bash_command=f"python -m pipeline {command}" + (" --date {{ ds }}" if with_date else ""),
        **kwargs,
    )
