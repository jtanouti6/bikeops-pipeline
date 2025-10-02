# tests/conftest.py
import glob
import os
import sys

import pytest
from pyspark.sql import SparkSession

# 0) Purge de variables PySpark "fantômes"
for var in (
    "PYSPARK_GATEWAY_PORT",
    "PYSPARK_GATEWAY_SECRET",
    "PYSPARK_DRIVER_PYTHON_OPTS",
):
    os.environ.pop(var, None)

# 1) Réseau local & Python interpréteur identique pour driver + workers
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2) Java 17 (si JAVA_HOME non défini)
if not os.environ.get("JAVA_HOME"):
    cands = sorted(glob.glob("/usr/lib/jvm/*-17*"))
    if cands:
        os.environ["JAVA_HOME"] = cands[0]
if os.environ.get("JAVA_HOME"):
    os.environ["PATH"] = (
        os.path.join(os.environ["JAVA_HOME"], "bin") + ":" + os.environ.get("PATH", "")
    )


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
        .config("spark.executorEnv.PYSPARK_PYTHON", pyexe)  # workers utilisent le même Python
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .getOrCreate()
    )
    yield spark
    spark.stop()
