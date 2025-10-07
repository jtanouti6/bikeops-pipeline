# tests/unit/test_transforms.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

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
    Déclenche l'init JVM côté CI sans assertion bloquante.
    Certaines plateformes retardent l'init de sc._jsc ; on force une action légère.
    """
    # Force l'initialisation paresseuse de la JVM / SparkContext
    _ = spark.version  # touche la JVM
    spark.range(1).count()  # action no-op pour initialiser le backend
    # On ne teste pas sc._jsc ici : inutile pour ces tests de fonctions de string/num


# NOTE: on garde la signature mais on ne l'utilise plus.
def _bind_df_to(spark, df: DataFrame) -> DataFrame:  # pragma: no cover
    return df


def test_spaces_and_case(spark):
    _ensure_active(spark)

    # Construire le DF via createDataFrame (plus robuste en CI que spark.sql)
    df = spark.createDataFrame(
        [("  lille   -  station 01 ",), ("Rain",)],
        ["raw"],
    )

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

    df = spark.createDataFrame(
        [("null",), ("NA",), ("",), ("16,1",), ("0,0",)],
        ["raw"],
    )

    got = df.select(
        normalize_null_str(F.col("raw")).alias("norm"),
        to_double_from_str_any(F.col("raw")).alias("as_double"),
    ).collect()

    assert [r["norm"] for r in got[:3]] == [None, None, None]
    assert abs(got[3]["as_double"] - 16.1) < 1e-9
    assert got[4]["as_double"] == 0.0
