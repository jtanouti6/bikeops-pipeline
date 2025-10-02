from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks


def test_quality_checks_stations(spark):
    schema = StructType(
        [
            StructField("station_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("capacity", IntegerType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("opened_at", DateType(), True),
        ]
    )
    rows = [
        (100, "Lille - Station 01", "Lille", "France", 50.0, 3.0, 24, True, None),
        (200, "Bad", "Lille", "France", 100.0, 200.0, -1, True, None),
    ]
    df = spark.createDataFrame(rows, schema=schema)

    contract = load_contract("contracts/stations.schema.yaml")
    report = run_quality_checks(df, contract).collect()
    m = {r["check_name"]: (r["nb_ko"], r["nb_total"]) for r in report}

    assert m["capacity_non_negative"] == (1, 2)
    assert m["lat_long_valid"] == (1, 2)
