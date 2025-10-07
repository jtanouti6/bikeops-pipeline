from pyspark.sql import Row
from pyspark.sql.types import DecimalType
from bikeops.utils.transforms import clean_price
from tests.unit.utils_assert import assert_df_equal


def test_clean_price_values_and_schema(spark):
    # Given (prix sous formes variées)
    src = spark.createDataFrame(
        [
            Row(price="20.24"),
            Row(price="2,023"),
            Row(price="  199.0€ "),
            Row(price="$-12,5"),
            Row(price="abc"),  # invalide -> NULL
            Row(price=""),  # vide -> NULL
            Row(price=None),  # None -> None
        ]
    )

    # When
    got = clean_price(src, "price", precision=12, scale=3)

    # Then: schéma Decimal(12,3)
    fields = {f.name: f.dataType for f in got.schema.fields}
    assert isinstance(fields["price"], DecimalType)
    assert fields["price"].precision == 12 and fields["price"].scale == 3

    # And: valeurs
    expected = spark.createDataFrame(
        [
            Row(price="20.240"),
            Row(price="2.023"),
            Row(price="199.000"),
            Row(price="-12.500"),
            Row(price=None),
            Row(price=None),
            Row(price=None),
        ],
        schema="price string",
    ).selectExpr("CAST(price AS DECIMAL(12,3)) AS price")

    assert_df_equal(got.select("price"), expected.select("price"), order_by=["price"])
