import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("payments-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
