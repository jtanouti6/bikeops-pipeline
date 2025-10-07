# tests/utils_assert.py
from pyspark.sql import DataFrame


def _normalize_schema(df: DataFrame):
    # normalisation simple : noms en lower, ordre des colonnes conservé
    fields = [
        (f.name.lower(), f.dataType.simpleString(), f.nullable)
        for f in df.schema.fields
    ]
    return fields


def assert_df_equal(actual: DataFrame, expected: DataFrame, *, order_by=None):
    # Comparaison schéma (nom/type/nullable), insensible à la casse des noms
    assert _normalize_schema(actual) == _normalize_schema(
        expected
    ), f"Schemas differ:\nactual={actual.schema}\nexpected={expected.schema}"

    a = actual
    e = expected
    if order_by:
        a = a.orderBy(*order_by)
        e = e.orderBy(*order_by)

    assert a.collect() == e.collect(), "DataFrames content differs"
