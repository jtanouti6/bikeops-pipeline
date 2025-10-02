def test_spark_fixture_starts(spark):
    assert spark.version and isinstance(spark.version, str)
    df = spark.range(0, 5)
    assert df.count() == 5
