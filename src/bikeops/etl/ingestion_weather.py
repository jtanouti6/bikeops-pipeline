from pathlib import Path

from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg
from bikeops.utils.transforms import normalize_null_str, to_double_from_str_any, to_title


# helper sûr pour gs:// et chemins locaux
def uri_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def main():
    cfg = load_profile("configs/local.yaml")
    spark = build_spark("bikeops-weather-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)

    default_city = cfg.get("weather", {}).get("city", "Lille")

    # --- lecture bronze (CSV ; ) : on lit en STRING pour maîtriser le parsing
    src = uri_join(p["bronze"], "weather_raw.csv")
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)  # tout en string
        .csv(src)
    )

    # --- parse timestamps (formats mixtes: 'YYYY-MM-DD HH:mm:ss' OU 'DD/MM/YYYY HH:mm:ss')
    ts_col = F.coalesce(
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
    )

    # --- mapping + normalisation
    out = (
        df.withColumn("observed_at", ts_col)
        .withColumn("temp_c", to_double_from_str_any(normalize_null_str(F.col("temperature_c"))))
        .withColumn("precip_mm", to_double_from_str_any(normalize_null_str(F.col("rain_mm"))))
        .withColumn("weather_code", F.upper(F.trim(F.col("weather_condition"))))
        .withColumn("city", to_title(F.lit(default_city)))
        .withColumn("dt", F.to_date("observed_at"))
        .drop("timestamp", "temperature_c", "rain_mm", "weather_condition")
    )

    # --- qualité selon contrat
    contract = load_contract("contracts/weather.schema.yaml")
    report = run_quality_checks(out, contract)
    print("\n=== Weather - quality report ===")
    report.show(truncate=False)

    # --- écriture Silver (Parquet partitionné par dt)
    dest = str((Path(p["silver"]) / "weather_silver").resolve())
    (out.write.mode("overwrite").partitionBy("dt").parquet(dest))
    print("Weather silver →", dest)
    print("Rows:", out.count())

    # --- aperçu
    out.orderBy("observed_at").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
