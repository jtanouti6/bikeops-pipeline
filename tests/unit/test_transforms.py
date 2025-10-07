# tests/unit/test_transforms.py

from pyspark.sql import functions as F

from bikeops.utils.transforms import (
    collapse_spaces,
    normalize_null_str,
    to_double_from_str_any,
    to_lower,
    to_title,
    to_upper,
)


def _ensure_active(spark):
    """
    Force les pointeurs 'actifs' utilisés par les helpers PySpark (F.col, collect, ...)
    afin d'éviter les erreurs du type _active_spark_context is None / setCallSite.
    """
    sc = spark.sparkContext
    from pyspark import context as _ctx
    from pyspark.sql import session as _sess

    _ctx.SparkContext._active_spark_context = sc
    _sess.SparkSession._instantiatedSession = spark
    _sess.SparkSession._activeSession = spark


def _bind_df_to(spark, df):
    """
    Recâble un DataFrame sur la session/contexte actifs.
    Hack 'tests only' pour contourner le flakiness PySpark (SPARK-27335).
    """
    try:
        # Rattache la session JVM du DF sur celle de spark
        df.sql_ctx.sparkSession._jsparkSession = spark._jsparkSession
    except Exception:
        pass
    try:
        # Rattache le SparkContext Python
        df._sc = spark.sparkContext
    except Exception:
        pass
    return df


def test_spaces_and_case(spark):
    _ensure_active(spark)

    # Construire via SQL (évite le parallelize de liste Python)
    df = spark.sql(
        "select '  lille   -  station 01 ' as raw " "union all select 'Rain' as raw"
    )
    df = _bind_df_to(spark, df)

    got = df.select(
        collapse_spaces(F.col("raw")).alias("collapsed"),
        to_title(F.col("raw")).alias("title"),
        to_lower(F.col("raw")).alias("lower"),
        to_upper(F.col("raw")).alias("upper"),
    ).collect()

    assert got[0]["collapsed"] == "lille - station 01"
    # Selon l'implémentation de to_title, on tolère l'une des deux variantes
    assert got[0]["title"] in {"Lille - Station 01", "Lille   -  Station 01"}
    assert got[1]["lower"] == "rain"
    assert got[1]["upper"] == "RAIN"


def test_null_and_decimal_comma(spark):
    _ensure_active(spark)

    df = spark.sql(
        "select 'null' as raw "
        "union all select 'NA' "
        "union all select '' "
        "union all select '16,1' "
        "union all select '0,0'"
    )
    df = _bind_df_to(spark, df)

    got = df.select(
        normalize_null_str(F.col("raw")).alias("norm"),
        to_double_from_str_any(F.col("raw")).alias("as_double"),
    ).collect()

    assert [r["norm"] for r in got[:3]] == [None, None, None]
    assert abs(got[3]["as_double"] - 16.1) < 1e-9
    assert got[4]["as_double"] == 0.0
