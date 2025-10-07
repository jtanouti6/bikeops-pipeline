# tests/unit/test_spaces_and_case.py
from pyspark.sql import Row
from bikeops.utils.transforms import clean_city
from tests.unit.utils_assert import assert_df_equal


def test_spaces_and_case(spark):
    # Given
    src = spark.createDataFrame(
        [
            Row(city="lYon", price=20.24),
            Row(city=" Paris", price=2.023),
            Row(city="MARSEILLE  ", price=199.0),
            Row(city="Saint   Denis", price=5.0),
        ]
    )

    # When
    got = clean_city(src, col_in="city")  # modifie la colonne en place

    # Then
    expected = spark.createDataFrame(
        [
            Row(city="lyon", price=20.24),
            Row(city="paris", price=2.023),
            Row(city="marseille", price=199.0),
            Row(city="saint denis", price=5.0),
        ]
    )
    assert_df_equal(
        got.select("city", "price"),
        expected.select("city", "price"),
        order_by=["city", "price"],
    )
