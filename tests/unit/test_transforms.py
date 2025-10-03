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
    # Fixe explicitement les contextes/sessions actifs côté PySpark
    from pyspark import context as _ctx
    from pyspark.sql import session as _sess

    _ctx.SparkContext._active_spark_context = spark.sparkContext
    _sess.SparkSession._instantiatedSession = spark
    _sess.SparkSession._activeSession = spark


def test_spaces_and_case(spark):
    _ensure_active(spark)

    # Construire le DF via SQL (évite parallelize/defaultParallelism)
    df = spark.sql(
        "select '  lille   -  station 01 ' as raw " "union all select 'Rain' as raw"
    )

    got = df.select(
        collapse_spaces(F.col("raw")).alias("collapsed"),
        to_title(F.col("raw")).alias("title"),
        to_lower(F.col("raw")).alias("lower"),
        to_upper(F.col("raw")).alias("upper"),
    ).collect()

    assert got[0]["collapsed"] == "lille - station 01"
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

    got = df.select(
        normalize_null_str(F.col("raw")).alias("norm"),
        to_double_from_str_any(F.col("raw")).alias("as_double"),
    ).collect()

    assert [r["norm"] for r in got[:3]] == [None, None, None]
    assert abs(got[3]["as_double"] - 16.1) < 1e-9
    assert got[4]["as_double"] == 0.0
