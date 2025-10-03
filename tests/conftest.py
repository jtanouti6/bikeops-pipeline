# tests/conftest.py (extrait pertinent)
import sys

import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# … (tes blocs 0/1/2 restent identiques) …


@pytest.fixture(scope="session")
def spark():
    """Session Spark unique, JVM initialisée via SparkContext + warmup RDD,
    et initialisation forcée de la passerelle Java (fix _jsc=None en CI)."""
    pyexe = sys.executable

    conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("bikeops-tests")
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.ui.showConsoleProgress", "false")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.executorEnv.PYSPARK_PYTHON", pyexe)
        .set("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .set("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    )

    # 1) Démarre (ou récupère) un SparkContext
    sc = SparkContext.getOrCreate(conf)

    # 2) **Forcer** l'initialisation interne PySpark (gateway Java / _jsc)
    #    -> évite AttributeError: 'NoneType' object has no attribute 'sc'
    from pyspark.context import (
        SparkContext as _SC,
    )  # import local pour éviter side-effects

    _SC._ensure_initialized()  # <— ligne clé
    sc = _SC._active_spark_context  # <— récupère le SC correctement initialisé

    # 3) Warmup: petite action RDD pour initialiser le scheduler/executor
    sc.parallelize([1]).count()
    sc.setLogLevel("WARN")

    # 4) Crée la SparkSession par-dessus le SC actif
    spark = SparkSession(sc)

    yield spark

    try:
        spark.stop()
    except Exception:
        pass


@pytest.fixture(autouse=True, scope="session")
def _ensure_spark_session(spark):
    yield
