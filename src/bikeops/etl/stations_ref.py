from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg
from bikeops.utils.transforms import to_title


# helper sûr pour gs:// et chemins locaux
def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main():
    cfg = load_profile("configs/local.yaml")
    spark = build_spark("bikeops-stations-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # --- lecture bronze (CSV virgule)
    src = uri_join(p["bronze"], "stations.csv")
    df = spark.read.option("header", True).option("inferSchema", True).option("sep", ",").csv(src)

    # --- renommage / cast
    df = (
        df.withColumnRenamed("station_name", "name")
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lon", "longitude")
    )

    # --- dérivations & normalisation
    # city = début du station_name avant le tiret
    city_from_name = F.regexp_extract(F.col("name"), r"^\s*([^-\u2013\u2014]+)", 1)
    out = (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("name", to_title(F.col("name")))
        .withColumn("city", to_title(city_from_name))
        .withColumn("country", F.lit("France"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("capacity", F.col("capacity").cast("int"))
        .withColumn("is_active", F.lit(True))  # défaut si non fourni
        .withColumn("opened_at", F.lit(None).cast("date"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    # --- dédup (station_id) : garder la plus récente par ingestion_ts
    w = Window.partitionBy("station_id").orderBy(F.col("ingestion_ts").desc_nulls_last())
    out = out.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # --- qualité (rapide) selon contrat
    contract = load_contract("contracts/stations.schema.yaml")
    report = run_quality_checks(out, contract)
    print("\n=== Stations - quality report ===")
    report.show(truncate=False)

    # --- écriture Silver (Parquet)
    dest = str((Path(p["silver"]) / "stations_silver").resolve())
    (out.coalesce(1).write.mode("overwrite").parquet(dest))  # faible volume → pratique

    print("Stations silver →", dest)
    print("Rows:", out.count())
    spark.stop()


if __name__ == "__main__":
    main()
