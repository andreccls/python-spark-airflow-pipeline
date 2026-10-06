# 0002 - Spark in local mode inside the Airflow image

Status: accepted

The data is small and the goal is a reproducible reference, so Spark runs `local[2]` in the same
container as the Airflow task (one image: Airflow 2.10 + Java 17 + PySpark 3.5.4). Tasks are
`BashOperator`s calling `python -m pipeline <command>`: the DAGs hold orchestration only and the
logic is plain, unit-tested Python. For real volume, replace the command with a
`spark-submit` to a cluster; the job code does not change.
