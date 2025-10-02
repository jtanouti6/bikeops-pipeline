# ingestion_weather.py
# Objectif : lire la météo brute (Bronze), normaliser/caster,
# appliquer des contrôles qualité, puis écrire la table Weather Silver
# (Parquet, partitionnée par date) de manière compatible GCS (gs://).

from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg
from bikeops.utils.transforms import (
    normalize_null_str,
    to_double_from_str_any,
    to_title,
)


# ---------- Helper URI ----------
def uri_join(base: str, *parts: str) -> str:
    """
    Jointure d'URI sûre pour gs://, file://, hdfs:// (sans pathlib).
    """
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main() -> None:
    # 1) Config & session Spark
    cfg = load_profile("configs/local.yaml")  # sur Dataproc, un launcher forcerait GCS
    spark = build_spark("bikeops-weather-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    # valeur par défaut de la ville (le fichier brut n’a pas de colonne city)
    default_city = cfg.get("weather", {}).get("city", "Lille")

    # 2) Lecture Bronze — CSV séparateur ';'
    # On force tout en string pour maîtriser nos conversions (décimales, nulls…)
    src = uri_join(p["bronze"], "weather_raw.csv")
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)  # tout en string
        .csv(src)
    )

    # 3) Parsing timestamps (formats mixtes)
    #   - "yyyy-MM-dd HH:mm:ss"
    #   - "dd/MM/yyyy HH:mm:ss"
    ts_col = F.coalesce(
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
    )

    # 4) Mapping + normalisation/cast
    # - temperature_c et rain_mm peuvent contenir des virgules, espaces, 'null' etc.
    out = (
        df.withColumn("observed_at", ts_col)
        .withColumn(
            "temp_c", to_double_from_str_any(normalize_null_str(F.col("temperature_c")))
        )
        .withColumn(
            "precip_mm", to_double_from_str_any(normalize_null_str(F.col("rain_mm")))
        )
        .withColumn("weather_code", F.upper(F.trim(F.col("weather_condition"))))
        .withColumn("city", to_title(F.lit(default_city)))
        .withColumn("dt", F.to_date("observed_at"))
        .withColumn("source", F.lit("raw"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .drop("timestamp", "temperature_c", "rain_mm", "weather_condition")
    ).dropna(subset=["observed_at"])  # écarter les lignes non parsables

    # 5) Contrôles qualité (contrat YAML)
    contract = load_contract("contracts/weather.schema.yaml")
    report = run_quality_checks(out, contract)
    print("\n=== Weather - quality report ===")
    report.show(truncate=False)

    # 6) Écriture Silver (Parquet partitionné par dt)
    dest = uri_join(p["silver"], "weather_silver")
    out.write.mode("overwrite").partitionBy("dt").parquet(dest)

    print("Weather silver →", dest)
    print("Rows:", out.count())

    # 7) Aperçu
    out.orderBy("observed_at").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
