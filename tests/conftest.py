# tests/conftest.py
# Objectif:
# - Démarrer UNE session Spark réutilisée par tous les tests (scope=session)
# - Éviter les soucis de gateway Py4J résiduels et d'environnement non aligné
# - Fonctionner en local et en CI (GitHub Actions) avec Java 17

import glob
import os
import sys

import pytest
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
# 3) Fixture Spark unique (scope=session)
#    - local[2] : 2 threads suffisent pour nos tests
#    - timezone UTC pour des résultats déterministes
#    - IPv4 + bind/host loopback pour éviter des résolutions hasardeuses en CI
#    - executorEnv.PYSPARK_PYTHON = même Python que le driver
# ---------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    pyexe = sys.executable
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("bikeops-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.executorEnv.PYSPARK_PYTHON", pyexe)
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------------------------------------------------------------------
# 4) Démarrage automatique :
#    Même si un test n'accepte pas explicitement le paramètre `spark`,
#    cette fixture autouse garantit que la session est créée au début.
# ---------------------------------------------------------------------
@pytest.fixture(autouse=True, scope="session")
def _ensure_spark_session(spark):
    # Rien à faire : le fait de dépendre de `spark` force son initialisation.
    yield
