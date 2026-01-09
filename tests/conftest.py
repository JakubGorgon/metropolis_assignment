"""
Pytest configuration and shared fixtures.
Sets up the SparkSession fixture used across integration tests.
"""
import pytest
import sys
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Creates a shared local Spark session for testing."""
    # Windows Fix for PySpark
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("Integration_Tests") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
