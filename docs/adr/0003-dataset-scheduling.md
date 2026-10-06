# 0003 - DAG 2 is triggered by an Airflow Dataset

Status: accepted

`build_gold` is scheduled on the silver `Dataset`, which `ingest_orders` updates only after its
quality gate passes. A failing gate therefore blocks gold by construction. A dataset-triggered
run has no meaningful `{{ ds }}`, so gold rebuilds every silver day (idempotent per-day overwrite).
