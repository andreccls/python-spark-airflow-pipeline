"""Explicit raw schemas. Everything is a string in bronze: typing is silver's job."""

from pyspark.sql.types import StringType, StructField, StructType


def _strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


RAW_ORDERS = _strings(
    "order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "ordered_at"
)
RAW_CUSTOMERS = _strings("customer_id", "name", "email", "state", "updated_at")
RAW_PRODUCTS = _strings("product_id", "name", "category", "price")
VALID_STATUSES = ("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")
