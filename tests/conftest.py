# tests/conftest.py
import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext


@pytest.fixture(scope="session")
def spark():
    # 1) Si un SparkContext existe déjà (créé par un test ou un import), on le STOP pour repartir propre
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass  # on ne veut pas faire échouer la fixture pour ça

    # 2) Dossier de travail isolé
    tmpdir = tempfile.mkdtemp(prefix="spark-test-")

    # 3) Créer UNE SEULE SparkSession propre
    builder = (
        SparkSession.builder.appName("bikeops-tests")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", os.path.join(tmpdir, "warehouse"))
        .config("spark.local.dir", os.path.join(tmpdir, "local"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )

    spark = builder.getOrCreate()

    # 4) S’enregistrer explicitement comme session instanciée (évite des récréations sauvages)
    try:
        from pyspark.sql import SparkSession as _SS

        _SS._instantiatedSession = spark  # type: ignore[attr-defined]
    except Exception:
        pass

    yield spark

    # 5) Teardown unique
    try:
        spark.stop()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
