from pyspark.sql import functions as F

from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main():
    cfg = load_profile("configs/local.yaml")
    spark = build_spark("bikeops-gold-station-hourly", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # --- Lire la silver availability (partitionnée dt/hour)
    avail_path = uri_join(p["silver"], "availability_silver")
    df = spark.read.parquet(avail_path)

    # colonnes minimales attendues
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

    # --- Agrégats par station / dt / hour
    metrics = df.groupBy("station_id", "city", "dt", "hour").agg(
        F.count(F.lit(1)).alias("n_obs"),
        F.round(F.avg("bikes_available"), 2).alias("bikes_avg"),
        F.min("bikes_available").alias("bikes_min"),
        F.max("bikes_available").alias("bikes_max"),
        F.first("capacity", ignorenulls=True).alias("capacity"),
        F.round(
            F.avg(
                F.when(
                    (F.col("capacity") > 0) & F.col("bikes_available").isNotNull(),
                    F.col("bikes_available") / F.col("capacity"),
                )
            ),
            4,
        ).alias("occ_rate_avg"),
        F.round(
            100
            * F.avg(
                F.when(F.col("bikes_available") == 0, F.lit(1)).otherwise(F.lit(0))
            ),
            2,
        ).alias("pct_zero_bikes"),
        F.round(
            100
            * F.avg(
                F.when(F.col("status") != "in_service", F.lit(1)).otherwise(F.lit(0))
            ),
            2,
        ).alias("pct_out_of_service"),
    )

    # --- Écriture GOLD (partition dt)
    dest = uri_join(p["gold"], "station_hourly_metrics")
    (metrics.write.mode("overwrite").partitionBy("dt").parquet(dest))

    # --- Aperçu
    print("Gold → station_hourly_metrics :", dest)
    metrics.orderBy("station_id", "dt", "hour").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
