from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


def _pick_source_file(bronze_dir: Path) -> str:
    """
    Supporte quelques variantes de nommage, retourne le 1er existant.
    """
    candidates = [
        bronze_dir / "availability_raw.csv",
        bronze_dir / "avaibility_raw.csv",  # typo fréquente
        bronze_dir / "availability.csv",
    ]
    for p in candidates:
        if p.exists():
            return str(p.resolve())
    raise FileNotFoundError(f"Aucun fichier availability* trouvé dans {bronze_dir}")


def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main():
    cfg = load_profile("configs/local.yaml")
    spark = build_spark("bikeops-availability-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    src = uri_join(p["bronze"], "availability_raw.csv")

    # --- lecture bronze: CSV ';'
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(src)
    )

    # --- mapping / cast
    # timestamp -> observed_at ; slots_free -> docks_available
    df = (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("observed_at", F.to_timestamp("timestamp"))  # 'YYYY-MM-DD HH:mm:ss'
        .withColumn("bikes_available", F.col("bikes_available").cast("int"))
        .withColumn("docks_available", F.col("slots_free").cast("int"))
        .drop("timestamp", "slots_free")
        .withColumn("status", F.lit("in_service"))  # par défaut (non présent en brut)
        .withColumn("source", F.lit("raw"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("dt", F.to_date("observed_at"))
        .withColumn("hour", F.hour("observed_at"))
    )

    # --- dédup (station_id, observed_at) : garder la + récente par ingestion_ts
    w = Window.partitionBy("station_id", "observed_at").orderBy(
        F.col("ingestion_ts").desc_nulls_last()
    )
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # --- enrichissement (stations: city, capacity)
    stations_path = str((Path(p["silver"]) / "stations_silver").resolve())
    stations = spark.read.parquet(stations_path)

    clean = df.join(
        stations.select("station_id", "city", "capacity"), on="station_id", how="left"
    ).withColumn(
        "capacity_violation_flag",
        F.when(
            (F.col("capacity").isNotNull())
            & (F.col("bikes_available") > F.col("capacity")),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # --- qualité (rapide) selon contrat availability
    contract = load_contract("contracts/availability.schema.yaml")
    report = run_quality_checks(clean, contract)
    print("\n=== Availability - quality report ===")
    report.show(truncate=False)

    # --- écriture Silver (Parquet partitionné)
    dest = str((Path(p["silver"]) / "availability_silver").resolve())
    (clean.write.mode("overwrite").partitionBy("dt", "hour").parquet(dest))

    print("Availability silver →", dest)
    print("Rows:", clean.count())
    clean.select(
        "station_id",
        "observed_at",
        "city",
        "bikes_available",
        "docks_available",
        "capacity",
        "status",
    ).orderBy("station_id", "observed_at").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
