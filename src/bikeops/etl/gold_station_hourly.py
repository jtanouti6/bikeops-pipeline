# gold_station_hourly.py
# Objectif : agréger la table Availability Silver (partitionnée dt/hour)
# en métriques horaires par station → écrire en zone GOLD (Parquet).
# Compatible GCS (gs://) grâce à uri_join (pas de pathlib).

from pyspark.sql import functions as F

from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


# ---------- Helper URI (sans pathlib, compatible gs:///file:///hdfs://) ----------
def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main() -> None:
    # 1) Config + session Spark
    cfg = load_profile("configs/local.yaml")  # sur Dataproc, le launcher force GCS
    spark = build_spark("bikeops-gold-station-hourly", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # 2) Lecture Availability Silver (partitionnée par dt/hour)
    avail_path = uri_join(p["silver"], "availability_silver")
    df = spark.read.parquet(avail_path)

    # Colonnes minimales attendues (pour éviter des surprises à l’agg)
    required_cols = {
        "station_id",
        "city",
        "dt",
        "hour",
        "bikes_available",
        "capacity",
        "status",
    }
    missing = required_cols.difference(set(df.columns))
    if missing:
        raise RuntimeError(f"Colonnes manquantes dans availability_silver: {missing}")

    # Garde-fous légers : types attendus + retirer station_id null
    df = (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("dt", F.to_date("dt"))
        .withColumn("hour", F.col("hour").cast("int"))
        .filter(F.col("station_id").isNotNull())
    )

    # 3) Agrégats horaires par station / dt / hour
    metrics = df.groupBy("station_id", "city", "dt", "hour").agg(
        F.count(F.lit(1)).alias("n_obs"),
        F.round(F.avg("bikes_available"), 2).alias("bikes_avg"),
        F.min("bikes_available").alias("bikes_min"),
        F.max("bikes_available").alias("bikes_max"),
        F.first("capacity", ignorenulls=True).alias("capacity"),
        # taux d’occupation moyen (moyenne des ratios valides)
        F.round(
            F.avg(
                F.when(
                    (F.col("capacity") > 0) & F.col("bikes_available").isNotNull(),
                    F.col("bikes_available") / F.col("capacity"),
                )
            ),
            4,
        ).alias("occ_rate_avg"),
        # % d'observations à 0 vélo
        F.round(
            100
            * F.avg(
                F.when(F.col("bikes_available") == 0, F.lit(1)).otherwise(F.lit(0))
            ),
            2,
        ).alias("pct_zero_bikes"),
        # % d'observations hors service
        F.round(
            100
            * F.avg(
                F.when(F.col("status") != "in_service", F.lit(1)).otherwise(F.lit(0))
            ),
            2,
        ).alias("pct_out_of_service"),
    )

    # 4) Écriture GOLD (Parquet partitionné par dt)
    dest = uri_join(p["gold"], "station_hourly_metrics")
    metrics.write.mode("overwrite").partitionBy("dt").parquet(dest)

    # 5) Aperçu console
    print("Gold → station_hourly_metrics :", dest)
    metrics.orderBy("station_id", "dt", "hour").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
