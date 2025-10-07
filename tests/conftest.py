# tests/conftest.py
import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    tmpdir = tempfile.mkdtemp(prefix="spark-test-")
    spark = (
        SparkSession.builder.appName("bikeops-tests")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", os.path.join(tmpdir, "warehouse"))
        .config("spark.local.dir", os.path.join(tmpdir, "local"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
    shutil.rmtree(tmpdir, ignore_errors=True)
