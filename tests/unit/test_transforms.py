from bikeops.utils.transforms import (
    collapse_spaces,
    normalize_null_str,
    to_double_from_str_any,
    to_lower,
    to_title,
    to_upper,
)


def test_spaces_and_case(spark):
    # ⚠️ Evite spark.createDataFrame([...]) pour contourner sc.parallelize/defaultParallelism
    df = spark.sql(
        "select '  lille   -  station 01 ' as raw " "union all select 'Rain' as raw"
    )

    got = df.select(
        collapse_spaces("raw").alias("collapsed"),
        to_title("raw").alias("title"),
        to_lower("raw").alias("lower"),
        to_upper("raw").alias("upper"),
    ).collect()

    assert got[0]["collapsed"] == "lille - station 01"
    assert got[0]["title"] in {"Lille - Station 01", "Lille   -  Station 01"}
    assert got[1]["lower"] == "rain"
    assert got[1]["upper"] == "RAIN"


def test_null_and_decimal_comma(spark):
    # idem : on passe par SQL pour éviter sc.parallelize sur liste Python
    df = spark.sql(
        "select 'null' as raw "
        "union all select 'NA' "
        "union all select '' "
        "union all select '16,1' "
        "union all select '0,0'"
    )

    got = df.select(
        normalize_null_str("raw").alias("norm"),
        to_double_from_str_any("raw").alias("as_double"),
    ).collect()

    assert [r["norm"] for r in got[:3]] == [None, None, None]
    assert abs(got[3]["as_double"] - 16.1) < 1e-9
    assert got[4]["as_double"] == 0.0
