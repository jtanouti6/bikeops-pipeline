from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType
from bikeops.utils.transforms import clean_city, clean_price


def transform_df(df, allow_negative: bool = True):
    df_clean = clean_city(df, "city")
    df_clean = clean_price(df_clean, "price", precision=12, scale=3)
    if not allow_negative:
        df_clean = df_clean.filter(F.col("price") >= F.lit(0).cast(DecimalType(12, 3)))
    cols = [c for c in ["city", "price"] if c in df_clean.columns] + [
        c for c in df_clean.columns if c not in {"city", "price"}
    ]
    return df_clean.select(*cols)


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    delimiter: str = ";",
    allow_negative: bool = True,
):
    df = (
        spark.read.option("header", True)
        .option("delimiter", delimiter)
        .option("inferSchema", True)
        .csv(input_path)
    )
    df_clean = transform_df(df, allow_negative=allow_negative)
    (
        df_clean.repartition(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("delimiter", delimiter)
        .csv(output_path)
    )
