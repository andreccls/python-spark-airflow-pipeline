import csv
import json
from datetime import date

from pipeline import generate
from pipeline.config import Settings


def test_orders_are_deterministic_for_same_seed_and_day():
    day = date(2026, 10, 1)
    assert generate.generate_orders(day, 50, 7) == generate.generate_orders(day, 50, 7)


def test_orders_differ_between_days_and_seeds():
    day = date(2026, 10, 1)
    base = generate.generate_orders(day, 50, 7)
    assert base != generate.generate_orders(date(2026, 10, 2), 50, 7)
    assert base != generate.generate_orders(day, 50, 8)


def test_orders_contain_dirty_rows_so_quality_checks_have_work():
    rows = generate.generate_orders(date(2026, 10, 1), 400, 42)
    ids = [r["order_id"] for r in rows]
    assert len(ids) > len(set(ids)), "expected duplicated order ids"
    assert any(r["customer_id"] is None for r in rows)
    assert any(int(r["quantity"]) <= 0 for r in rows)
    valid = {"PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"}
    assert any(r["status"] not in valid for r in rows)


def test_dimensions_are_deterministic_and_have_a_duplicate_customer():
    customers = generate.generate_customers(20, 1)
    assert customers == generate.generate_customers(20, 1)
    assert len({c["customer_id"] for c in customers}) < len(customers)
    assert len(generate.generate_products(10, 1)) == 10


def test_land_raw_writes_files_and_is_idempotent(tmp_path):
    settings = Settings(data_dir=tmp_path, orders_per_day=30)
    day = date(2026, 10, 1)
    generate.land_raw(settings, day)
    orders = tmp_path / "raw" / "orders" / "dt=2026-10-01" / "orders.jsonl"
    first = orders.read_text()
    generate.land_raw(settings, day)
    assert orders.read_text() == first
    assert len(first.splitlines()) >= 30
    assert json.loads(first.splitlines()[0])["order_id"].startswith("20261001-")
    with (tmp_path / "raw" / "customers" / "customers.csv").open() as f:
        assert next(csv.DictReader(f))["customer_id"]
    assert (tmp_path / "raw" / "products" / "products.csv").exists()
