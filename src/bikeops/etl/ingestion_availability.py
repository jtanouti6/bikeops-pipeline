# ingestion_availability.py
# Objectif : lire le CSV de disponibilité (Bronze), normaliser, dédupliquer,
# enrichir avec les stations (Silver) puis écrire la table Availability Silver
# en Parquet partitionné par (dt, hour), de façon compatible GCS (gs://).

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from bikeops.config.schema_loader import load_contract
from bikeops.quality.quality_check import run_quality_checks
from bikeops.utils.config import build_spark, load_profile, paths_from_cfg


# ---------- Helpers ----------
def uri_join(base: str, *parts: str) -> str:
    """
    Concatène des morceaux d'URI sans utiliser pathlib (supporte gs://, file://, hdfs://).
    Ex: uri_join("gs://bucket/data", "bronze", "f.csv") -> "gs://bucket/data/bronze/f.csv"
    """
    base = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts)
    return f"{base}/{tail}"


def _pick_source_file(bronze_base: str, spark: SparkSession) -> str:
    """
    Retourne le premier fichier existant parmi quelques variantes,
    en utilisant l'API Hadoop (fs.exists) -> supporte gs://, hdfs://, file://.

    Pourquoi pas pathlib ?
    - pathlib casse les schémas d'URI (gs:/...).
    - L'API Hadoop s'appuie sur le FileSystem du schéma indiqué.
    """
    candidates = ["availability_raw.csv", "avaibility_raw.csv", "availability.csv"]

    jvm = spark._jvm
    jconf = spark._jsc.hadoopConfiguration()
    JPath = jvm.org.apache.hadoop.fs.Path

    # FileSystem basé sur l'URI du préfixe (important pour gs://)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(JPath(bronze_base).toUri(), jconf)

    for name in candidates:
        candidate = uri_join(bronze_base, name)
        if fs.exists(JPath(candidate)):
            return candidate

    raise FileNotFoundError(
        f"Aucun fichier availability* trouvé sous {bronze_base} "
        f"(candidats: {', '.join(candidates)})"
    )


# ---------- Job principal ----------
def main() -> None:
    # 1) Config & session Spark
    cfg = load_profile("configs/local.yaml")  # sur Dataproc, un launcher override ce profil -> GCS
    spark = build_spark("bikeops-availability-silver", cfg["spark"]["timezone"])
    p = paths_from_cfg(cfg)  # renvoie {bronze|silver|gold} compatibles URI

    # Source Bronze : choisit la première variante existante via Hadoop FS
    src = _pick_source_file(p["bronze"], spark)

    # 2) Lecture Bronze (CSV séparateur ';')
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(src)
    )

    # 3) Normalisation / typage colonnes (schéma minimal homogène)
    #    timestamp -> observed_at ; slots_free -> docks_available
    df = (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("observed_at", F.to_timestamp("timestamp"))  # 'YYYY-MM-DD HH:mm:ss'
        .withColumn("bikes_available", F.col("bikes_available").cast("int"))
        .withColumn("docks_available", F.col("slots_free").cast("int"))
        .drop("timestamp", "slots_free")
        .withColumn("status", F.lit("in_service"))       # valeur par défaut (pas en brut)
        .withColumn("source", F.lit("raw"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("dt", F.to_date("observed_at"))
        .withColumn("hour", F.hour("observed_at"))
    )

    # 4) Déduplication (station_id, observed_at) -> on garde la ligne la + récente par ingestion_ts
    w = Window.partitionBy("station_id", "observed_at").orderBy(
        F.col("ingestion_ts").desc_nulls_last()
    )
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # 5) Enrichissement depuis Stations Silver (city, capacity)
    stations_path = uri_join(p["silver"], "stations_silver")
    stations = spark.read.parquet(stations_path)

    clean = (
        df.join(
            stations.select("station_id", "city", "capacity"),
            on="station_id",
            how="left",
        )
        .withColumn(
            "capacity_violation_flag",
            F.when(
                (F.col("capacity").isNotNull())
                & (F.col("bikes_available") > F.col("capacity")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
    )

    # 6) Contrôles qualité (rapides) via contrat YAML
    contract = load_contract("contracts/availability.schema.yaml")
    report = run_quality_checks(clean, contract)
    print("\n=== Availability - quality report ===")
    report.show(truncate=False)

    # 7) Écriture Silver (Parquet) partitionnée par (dt, hour)
    dest = uri_join(p["silver"], "availability_silver")
    clean.write.mode("overwrite").partitionBy("dt", "hour").parquet(dest)

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
