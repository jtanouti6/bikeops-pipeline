# tests/conftest.py
# Objectif:
# - Démarrer UNE session Spark réutilisée par tous les tests (scope=session)
# - Éviter les soucis de passerelle Py4J/_jsc=None et d'environnement non aligné
# - Fonctionner en local et en CI (GitHub Actions) avec Java 17

import glob
import os
import sys

import pytest
from pyspark import SparkConf, SparkContext
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
# 3) Fixture Spark unique (scope=session) via SparkContext -> SparkSession
#    - On crée d'abord SparkContext (JVM) avec SparkConf
#    - Warmup RDD pour initialiser le scheduler
#    - Enregistrement explicite des contextes/sessions actifs (PySpark internals)
# ---------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Session Spark unique, JVM initialisée via SparkContext + warmup RDD,
    et enregistrement explicite des contextes actifs pour PySpark (CI)."""

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

    # 2) Forcer l'init interne de la passerelle Java (gateway / _jsc)
    from pyspark.context import (
        SparkContext as _SC,
    )  # import local pour éviter side-effects

    _SC._ensure_initialized()
    sc = _SC._active_spark_context  # récup SC actif correctement initialisé

    # 3) Warmup: petite action RDD pour initialiser le scheduler/executor
    sc.parallelize([1]).count()
    sc.setLogLevel("WARN")

    # 4) Crée la SparkSession par-dessus le SC actif
    spark = SparkSession(sc)

    # 5) Enregistrer explicitement les contextes/sessions "actifs" pour PySpark
    #    (utile en CI quand PySpark ne retrouve pas la session à certains timings)
    from pyspark import context as _ctx
    from pyspark.sql import session as _sess

    _ctx.SparkContext._active_spark_context = sc
    _sess.SparkSession._instantiatedSession = spark
    _sess.SparkSession._activeSession = spark

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
