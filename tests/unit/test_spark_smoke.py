from pyspark.sql import SparkSession


def test_spark_session_starts_and_stops():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("test-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    assert spark.version  # la session démarre
    spark.stop()
