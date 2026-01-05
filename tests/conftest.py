import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Created a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
