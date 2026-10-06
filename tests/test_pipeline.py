import json
from datetime import date

import pytest

from pipeline import bronze, cli, generate, gold, maintenance, quality, silver
from pipeline.config import Settings
from pipeline.schemas import VALID_STATUSES

DAY = date(2026, 10, 1)


@pytest.fixture
def lake(tmp_path, spark):
    settings = Settings(data_dir=tmp_path, orders_per_day=300)
    generate.land_raw(settings, DAY)
    return settings


def _run_to_silver(spark, s):
    return bronze.ingest(spark, s, DAY), silver.transform(spark, s, DAY)


def test_bronze_keeps_every_raw_row(spark, lake):
    b = bronze.ingest(spark, lake, DAY)
    raw_lines = (lake.data_dir / "raw/orders/dt=2026-10-01/orders.jsonl").read_text().splitlines()
    assert b["orders"] == len(raw_lines)
    assert b["customers"] == 201 and b["products"] == 40


def test_silver_dedups_types_and_rejects_with_reason(spark, lake):
    b, s = _run_to_silver(spark, lake)
    assert s["orders"] + s["orders_rejected"] <= b["orders"]  # duplicates collapsed
    assert s["orders_rejected"] > 0
    rej = spark.read.parquet(str(lake.layer("silver", "orders_rejected") / "dt=2026-10-01"))
    reasons = {r[0] for r in rej.select("reject_reason").distinct().collect()}
    assert reasons <= {"missing_customer", "invalid_quantity", "invalid_status"}
    ok = spark.read.parquet(str(lake.layer("silver", "orders") / "dt=2026-10-01"))
    assert dict(ok.dtypes)["quantity"] == "int"
    assert dict(ok.dtypes)["ordered_at"] == "timestamp"
    assert ok.where("quantity <= 0 or customer_id is null").count() == 0


def test_silver_normalises_status_and_keeps_latest_customer(spark, lake):
    _run_to_silver(spark, lake)
    ok = spark.read.parquet(str(lake.layer("silver", "orders") / "dt=2026-10-01"))
    assert {r[0] for r in ok.select("status").distinct().collect()} <= set(VALID_STATUSES)
    cust = spark.read.parquet(str(lake.layer("silver", "customers")))
    assert cust.count() == 200
    assert cust.where("customer_id = 'C0001'").first()["email"] == "customer1@example.com"


def test_quality_passes_and_fails_on_threshold(spark, lake):
    _run_to_silver(spark, lake)
    res = quality.check(spark, lake, DAY)
    assert 0 < res["reject_ratio"] <= 0.15
    strict = Settings(data_dir=lake.data_dir, max_reject_ratio=0.0)
    with pytest.raises(quality.QualityError, match="reject ratio"):
        quality.check(spark, strict, DAY)


def test_quality_fails_on_empty_silver(spark, tmp_path):
    s = Settings(data_dir=tmp_path)
    empty = spark.createDataFrame([], "order_id string")
    empty.write.parquet(str(s.layer("silver", "orders") / "dt=2026-10-01"))
    empty.write.parquet(str(s.layer("silver", "orders_rejected") / "dt=2026-10-01"))
    with pytest.raises(quality.QualityError, match="no valid orders"):
        quality.check(spark, s, DAY)


def test_quality_fails_on_duplicate_ids(spark, tmp_path):
    s = Settings(data_dir=tmp_path)
    spark.createDataFrame([("a",), ("a",)], "order_id string").write.parquet(
        str(s.layer("silver", "orders") / "dt=2026-10-01")
    )
    spark.createDataFrame([], "order_id string").write.parquet(
        str(s.layer("silver", "orders_rejected") / "dt=2026-10-01")
    )
    with pytest.raises(quality.QualityError, match="duplicate"):
        quality.check(spark, s, DAY)


def test_gold_revenue_reconciles_with_silver_and_is_idempotent(spark, lake):
    _run_to_silver(spark, lake)
    n1 = gold.build(spark, lake, DAY)
    n2 = gold.build(spark, lake, DAY)
    assert n1 == n2 > 0
    g = spark.read.parquet(str(lake.layer("gold", "daily_sales") / "dt=2026-10-01"))
    silver_orders = spark.read.parquet(str(lake.layer("silver", "orders") / "dt=2026-10-01"))
    expected = silver_orders.selectExpr("sum(quantity * unit_price)").first()[0]
    assert g.selectExpr("sum(revenue)").first()[0] == expected
    assert g.selectExpr("sum(orders)").first()[0] == silver_orders.count()


def test_audit_and_cleanup(spark, lake):
    _run_to_silver(spark, lake)
    gold.build(spark, lake, DAY)
    counts = maintenance.audit(spark, lake)
    assert counts["silver/customers"] == 200 and "gold/daily_sales" in counts
    assert maintenance.cleanup(lake, DAY, keep_days=30) == []
    removed = maintenance.cleanup(lake, date(2026, 12, 31), keep_days=30)
    assert "silver/orders/dt=2026-10-01" in removed
    assert not (lake.data_dir / "silver/orders/dt=2026-10-01").exists()


def test_cli_end_to_end(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PIPELINE_ORDERS_PER_DAY", "100")
    out = {}
    for cmd in ("generate", "bronze", "silver", "quality", "gold", "audit", "cleanup"):
        assert cli.main([cmd, "--date", "2026-10-01"]) == 0
        out[cmd] = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert cli.main(["gold", "--all"]) == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["2026-10-01"] > 0
    assert out["audit"]["gold/daily_sales"] > 0
    assert out["cleanup"] == []


def test_gold_build_all_covers_every_silver_day(spark, lake):
    _run_to_silver(spark, lake)
    assert list(gold.build_all(spark, lake)) == ["2026-10-01"]
