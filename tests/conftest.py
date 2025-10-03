# tests/conftest.py
# Objectif:
# - Démarrer UNE session Spark réutilisée par tous les tests (scope=session)
# - Éviter les soucis de gateway Py4J résiduels et d'environnement non aligné
# - Fonctionner en local et en CI (GitHub Actions) avec Java 17

import glob
import os
import sys

import pytest
from pyspark import SparkConf, SparkContext  # ⟵ init JVM/SC d'abord
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------
# 0) Assainir l'environnement PySpark (évite de réutiliser une gateway morte)
# ---------------------------------------------------------------------
for var in (
    "PYSPARK_GATEWAY_PORT",
    "PYSPARK_GATEWAY_SECRET",
    "PYSPARK_DRIVER_PYTHON_OPTS",
):
    os.environ.pop(var, None)

# ---------------------------------------------------------------------
# 1) Réseau local & interpréteur Python aligné (driver == executors)
# ---------------------------------------------------------------------
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ---------------------------------------------------------------------
# 2) Java 17 (utile en CI si JAVA_HOME absent)
# ---------------------------------------------------------------------
if not os.environ.get("JAVA_HOME"):
    cands = sorted(glob.glob("/usr/lib/jvm/*-17*"))
    if cands:
        os.environ["JAVA_HOME"] = cands[0]
if os.environ.get("JAVA_HOME"):
    os.environ["PATH"] = (
        os.path.join(os.environ["JAVA_HOME"], "bin") + ":" + os.environ.get("PATH", "")
    )


# ---------------------------------------------------------------------
# 3) Fixture Spark unique (scope=session) via SparkContext → SparkSession
#    - On crée d'abord SparkContext (JVM) avec SparkConf
#    - Warmup d'une petite action RDD pour initialiser le scheduler
#    - Puis SparkSession(sc) par-dessus (évite _jsc=None)
# ---------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Session Spark unique, JVM initialisée via SparkContext + warmup RDD."""
    pyexe = sys.executable

    # Conf explicite pour le SC (JVM)
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

    # Démarrage du SparkContext (JVM)
    sc = SparkContext.getOrCreate(conf)

    # Si la gateway Java n'est pas prête, on redémarre proprement
    if getattr(sc, "_jsc", None) is None:
        sc.stop()
        sc = SparkContext.getOrCreate(conf)

    # Warmup: une petite action RDD pour initialiser le scheduler/executor
    sc.parallelize([1]).count()
    sc.setLogLevel("WARN")

    # Crée la SparkSession à partir du SC existant
    spark = SparkSession(sc)

    yield spark

    # Teardown silencieux
    try:
        spark.stop()
    except Exception:
        pass


# ---------------------------------------------------------------------
# 4) Démarrage automatique pour tous les tests (même sans paramètre `spark`)
# ---------------------------------------------------------------------
@pytest.fixture(autouse=True, scope="session")
def _ensure_spark_session(spark):
    # Le fait de dépendre de `spark` force son initialisation au début de la session.
    yield
