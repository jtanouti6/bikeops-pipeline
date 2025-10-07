from pyspark.sql import Row
from pyspark.sql.types import DecimalType
from bikeops.etl.city_price_clean_job import transform_df


def test_price_schema_decimal_12_3(spark):
    src = spark.createDataFrame(
        [
            Row(city="Paris", price="12,34"),
            Row(city="Lyon", price="199.0€"),
        ]
    )
    got = transform_df(src, allow_negative=True)

    # Vérifier le type Decimal(12,3)
    price_type = dict((f.name, f.dataType) for f in got.schema.fields)["price"]
    assert isinstance(price_type, DecimalType)
    assert price_type.precision == 12 and price_type.scale == 3


def test_filter_negative_prices_when_disallowed(spark):
    src = spark.createDataFrame(
        [
            Row(city="Paris", price="12,34"),
            Row(city="Lyon", price="$-5,00"),
            Row(city="Nice", price="-0.001"),
        ]
    )
    kept = (
        transform_df(src, allow_negative=False)
        .select("city")
        .rdd.flatMap(lambda r: r)
        .collect()
    )
    assert set(kept) == {
        "paris"
    }  # seuls les prix >= 0 restent (après normalisation city->lower)


def test_keep_negative_prices_when_allowed(spark):
    src = spark.createDataFrame(
        [
            Row(city="Lyon", price="$-5,00"),
            Row(city="Nice", price="-0.001"),
        ]
    )
    kept = (
        transform_df(src, allow_negative=True)
        .select("city")
        .rdd.flatMap(lambda r: r)
        .collect()
    )
    # On conserve toutes les lignes
    assert set(kept) == {"lyon", "nice"}
