from pyspark.sql import functions as F

from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main():
    cfg = load_profile("configs/local.yaml")
    spark = build_spark("bikeops-gold-city-daily", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    avail_path = uri_join(p["silver"], "availability_silver")
    weather_path = uri_join(p["silver"], "weather_silver")

    # --- Lire Silver
    avail = spark.read.parquet(
        avail_path
    )  # attendu: station_id, city, dt, hour, bikes_available, capacity, status
    weather = spark.read.parquet(
        weather_path
    )  # attendu: city, observed_at, temp_c, precip_mm, dt

    # --- Vérifs colonnes minimales
    req_avail = {"city", "dt", "bikes_available", "capacity", "status", "station_id"}
    miss = req_avail.difference(set(avail.columns))
    if miss:
        raise RuntimeError(f"Colonnes manquantes dans availability_silver: {miss}")

    # --- Agrégats ville/jour depuis availability
    city_daily = avail.groupBy("city", "dt").agg(
        F.count(F.lit(1)).alias("n_obs"),
        F.countDistinct("station_id").alias("n_stations"),
        F.round(F.avg("bikes_available"), 2).alias("bikes_avg"),
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
            100 * F.avg(F.when(F.col("bikes_available") == 0, 1).otherwise(0)), 2
        ).alias("pct_zero_bikes"),
        F.round(
            100 * F.avg(F.when(F.col("status") != "in_service", 1).otherwise(0)), 2
        ).alias("pct_out_of_service"),
    )

    # --- Agrégats météo ville/jour (si colonnes présentes)
    # (Permet de rester robuste si certaines colonnes sont absentes)
    weather_cols = set(weather.columns)
    w_aggs = []
    if "temp_c" in weather_cols:
        w_aggs.append(F.round(F.avg("temp_c"), 2).alias("temp_c_avg"))
    if "precip_mm" in weather_cols:
        w_aggs.append(F.round(F.sum("precip_mm"), 2).alias("precip_mm_sum"))

    if w_aggs:
        weather_daily = weather.groupBy("city", "dt").agg(*w_aggs)
    else:
        # Pas d'agg météo possible → table vide mais colonnes clés présentes
        weather_daily = spark.createDataFrame([], "city string, dt date")

    # --- Jointure ville/jour
    gold = city_daily.alias("m").join(
        weather_daily.alias("w"), on=["city", "dt"], how="left"
    )

    # --- Écriture GOLD (partition par dt)
    dest = uri_join(p["gold"], "city_daily_metrics")
    (gold.write.mode("overwrite").partitionBy("dt").parquet(dest))

    print("Gold → city_daily_metrics :", dest)
    gold.orderBy("city", "dt").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
