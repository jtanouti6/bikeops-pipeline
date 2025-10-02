# gold_city_daily.py
# Objectif :
# - Agréger la disponibilité (Availability Silver) au niveau ville/jour
# - Joindre des agrégats météo (Weather Silver) au même grain
# - Écrire la table GOLD "city_daily_metrics" (Parquet, partitionnée par dt)
#
# Points clés :
# - GCS-friendly : on utilise uri_join (pas de pathlib / resolve)
# - Gardes-fous légers : types homogènes, city trim, weather optionnelle

from pyspark.sql import functions as F

from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


# ---------- Helper URI (sans pathlib, compatible gs://, file://, hdfs://) ----------
def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main() -> None:
    # 1) Config + session
    cfg = load_profile("configs/local.yaml")  # sur Dataproc, le launcher force GCS
    spark = build_spark("bikeops-gold-city-daily", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # 2) Entrées Silver
    avail_path = uri_join(p["silver"], "availability_silver")
    weather_path = uri_join(p["silver"], "weather_silver")

    # availability_silver attendu :
    #   station_id, city, dt (date), hour, bikes_available, capacity, status
    avail = spark.read.parquet(avail_path)

    # weather_silver attendu (selon ingestion_weather) :
    #   city, observed_at (ts), temp_c (double), precip_mm (double), dt (date)
    weather = spark.read.parquet(weather_path)

    # 3) Vérifs colonnes minimales (availability)
    req_avail = {"city", "dt", "bikes_available", "capacity", "status", "station_id"}
    miss = req_avail.difference(set(avail.columns))
    if miss:
        raise RuntimeError(f"Colonnes manquantes dans availability_silver: {miss}")

    # 4) Harmonisation types / nettoyage clés de groupby/join
    #    (au cas où des upstream changent le schéma)
    avail = (
        avail.withColumn("city", F.trim(F.col("city")))
        .withColumn("dt", F.to_date("dt"))
        .filter(F.col("city").isNotNull())
    )

    weather = weather.withColumn("city", F.trim(F.col("city"))).withColumn(
        "dt", F.to_date("dt")
    )

    # 5) Agrégats ville/jour (disponibilité)
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

    # 6) Agrégats météo ville/jour (si colonnes présentes)
    weather_cols = set(weather.columns)
    w_aggs = []
    if "temp_c" in weather_cols:
        w_aggs.append(F.round(F.avg("temp_c"), 2).alias("temp_c_avg"))
    if "precip_mm" in weather_cols:
        w_aggs.append(F.round(F.sum("precip_mm"), 2).alias("precip_mm_sum"))

    if w_aggs:
        weather_daily = weather.groupBy("city", "dt").agg(*w_aggs)
    else:
        # Pas d'agg météo possible → table vide avec les clés pour une left join propre
        weather_daily = spark.createDataFrame([], "city string, dt date")

    # 7) Jointure ville/jour
    gold = city_daily.alias("m").join(
        weather_daily.alias("w"), on=["city", "dt"], how="left"
    )

    # 8) Écriture GOLD (Parquet partitionné par dt)
    dest = uri_join(p["gold"], "city_daily_metrics")
    gold.write.mode("overwrite").partitionBy("dt").parquet(dest)

    # 9) Aperçu
    print("Gold → city_daily_metrics :", dest)
    gold.orderBy("city", "dt").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
