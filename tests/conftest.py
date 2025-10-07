# tests/conftest.py
import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Répertoire temporaire propre pour le warehouse et les checkpoints
    tmpdir = tempfile.mkdtemp(prefix="spark-test-")

    builder = (
        SparkSession.builder.appName("bikeops-tests")
        .master("local[*]")
        # répertoires temp isolés pour éviter les collisions entre tests
        .config("spark.sql.warehouse.dir", os.path.join(tmpdir, "warehouse"))
        .config("spark.local.dir", os.path.join(tmpdir, "local"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # stabilité des résultats
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # Comportements explicites
        .config("spark.sql.legacy.allowUntypedScalaUDF", "false")
        .config("spark.sql.caseSensitive", "false")
    )

    spark = builder.getOrCreate()

    # Nettoyage à la fin de la session de tests
    yield spark
    spark.stop()
    shutil.rmtree(tmpdir, ignore_errors=True)
