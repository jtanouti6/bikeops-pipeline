# stations_ref.py
# Objectif : lire le référentiel stations (Bronze CSV),
# le normaliser et l’écrire en Silver (Parquet) — compatible gs:// (pas de pathlib).

from pyspark.sql import Window
from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg
from bikeops.utils.transforms import to_title


# ---------- Helper URI (pas de pathlib, compatible gs://) ----------
def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main() -> None:
    # 1) Config + session
    cfg = load_profile(
        "configs/local.yaml"
    )  # sur Dataproc, un launcher override la racine vers GCS
    spark = build_spark("bikeops-stations-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # 2) Lecture Bronze (CSV virgule)
    src = uri_join(p["bronze"], "stations.csv")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .csv(src)
    )

    # 3) Renommages simples
    df = (
        df.withColumnRenamed("station_name", "name")
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lon", "longitude")
    )

    # 4) Dérivations & normalisation
    # city = début du name avant un tiret (classique "Lille - Station 01")
    city_from_name = F.regexp_extract(
        F.trim(F.col("name")), r"^\s*([^-\u2013\u2014]+)", 1
    )

    out = (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("name", to_title(F.trim(F.col("name"))))
        .withColumn("city", to_title(F.trim(city_from_name)))
        .withColumn("country", F.lit("France"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("capacity", F.col("capacity").cast("int"))
        .withColumn("is_active", F.lit(True))  # défaut si non fourni
        .withColumn("opened_at", F.lit(None).cast("date"))
        .withColumn("source", F.lit("raw"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    # Garde-fous simples (bornes géo très larges)
    out = (
        out.where(
            (F.col("latitude").isNull())
            | ((F.col("latitude") >= -90) & (F.col("latitude") <= 90))
        )
        .where(
            (F.col("longitude").isNull())
            | ((F.col("longitude") >= -180) & (F.col("longitude") <= 180))
        )
        .dropna(subset=["station_id"])
    )

    # 5) Dédup (station_id) — on garde la + récente
    w = Window.partitionBy("station_id").orderBy(
        F.col("ingestion_ts").desc_nulls_last()
    )
    out = (
        out.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")
    )

    # 6) Qualité rapide (contrat YAML)
    contract = load_contract("contracts/stations.schema.yaml")
    report = run_quality_checks(out, contract)
    print("\n=== Stations - quality report ===")
    report.show(truncate=False)

    # 7) Écriture Silver (Parquet)
    dest = uri_join(p["silver"], "stations_silver")
    out.coalesce(1).write.mode("overwrite").parquet(
        dest
    )  # faible volume → 1 fichier pratique

    print("Stations silver →", dest)
    print("Rows:", out.count())
    spark.stop()


if __name__ == "__main__":
    main()
