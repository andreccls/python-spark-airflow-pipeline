import pytest
from pyspark.sql import SparkSession

from pipeline.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_spark("tests")
