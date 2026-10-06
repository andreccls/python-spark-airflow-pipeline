"""Deterministic synthetic e-commerce data: simulates the upstream source system.

Pure stdlib (no Spark). Orders deliberately contain ~10% dirty rows (duplicates, null
customers, bad quantities, messy statuses) so silver and the quality gate have real work.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.config import Settings

N_CUSTOMERS = 200
N_PRODUCTS = 40
CATEGORIES = ("books", "electronics", "home", "toys", "sports")
STATES = ("MG", "SP", "RJ", "BA", "RS")
STATUSES = ("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")

Row = dict[str, Any]


def generate_customers(n: int, seed: int) -> list[Row]:
    rng = random.Random(f"customers-{seed}")
    rows = [
        {
            "customer_id": f"C{i:04d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "state": rng.choice(STATES),
            "updated_at": "2026-01-01T00:00:00",
        }
        for i in range(1, n + 1)
    ]
    # a later version of C0001 with a messy e-mail: silver must keep only the latest
    rows.append({**rows[0], "email": " Customer1@Example.COM ", "updated_at": "2026-06-01T00:00:00"})
    return rows


def generate_products(n: int, seed: int) -> list[Row]:
    rng = random.Random(f"products-{seed}")
    return [
        {
            "product_id": f"P{i:03d}",
            "name": f"Product {i}",
            "category": rng.choice(CATEGORIES),
            "price": f"{rng.uniform(5, 500):.2f}",
        }
        for i in range(1, n + 1)
    ]


def generate_orders(day: date, n: int, seed: int) -> list[Row]:
    rng = random.Random(f"orders-{seed}-{day.isoformat()}")
    midnight = datetime.combine(day, datetime.min.time())
    rows: list[Row] = []
    for i in range(1, n + 1):
        row: Row = {
            "order_id": f"{day:%Y%m%d}-{i:05d}",
            "customer_id": f"C{rng.randint(1, N_CUSTOMERS):04d}",
            "product_id": f"P{rng.randint(1, N_PRODUCTS):03d}",
            "quantity": str(rng.randint(1, 5)),
            "unit_price": f"{rng.uniform(5, 500):.2f}",
            "status": rng.choice(STATUSES),
            "ordered_at": (midnight + timedelta(seconds=rng.randint(0, 86399))).isoformat(),
        }
        roll = rng.random()
        if roll < 0.03:
            row["customer_id"] = None
        elif roll < 0.06:
            row["quantity"] = str(rng.randint(-3, 0))
        elif roll < 0.08:
            row["status"] = rng.choice(["paid ", "UNKNOWN", "Shipped"])  # messy / invalid
        rows.append(row)
        if rng.random() < 0.02:
            rows.append(dict(row))  # exact duplicate delivery
    return rows


def _write_csv(path: Path, rows: list[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def land_raw(settings: Settings, day: date) -> None:
    """Write the raw landing files for `day`. Overwrites, so re-runs are idempotent."""
    raw = settings.data_dir / "raw"
    _write_csv(raw / "customers" / "customers.csv", generate_customers(N_CUSTOMERS, settings.seed))
    _write_csv(raw / "products" / "products.csv", generate_products(N_PRODUCTS, settings.seed))
    orders_path = raw / "orders" / f"dt={day.isoformat()}" / "orders.jsonl"
    orders_path.parent.mkdir(parents=True, exist_ok=True)
    rows = generate_orders(day, settings.orders_per_day, settings.seed)
    orders_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
