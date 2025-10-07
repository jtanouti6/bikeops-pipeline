import os
import tempfile
import glob
from bikeops.etl.city_price_clean_job import run


def _write_tmp_csv(path: str, rows: list[str]):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))


def test_city_price_clean_job_csv_io(spark):
    tmp = tempfile.mkdtemp(prefix="bikeops_it-")
    src_dir = os.path.join(tmp, "in")
    out_dir = os.path.join(tmp, "out")
    os.makedirs(src_dir, exist_ok=True)

    # Arrange: fichier source
    src_csv = os.path.join(src_dir, "cities.csv")
    _write_tmp_csv(
        src_csv,
        [
            "city;price",
            "lYon;20.24",
            " Paris;2,023",
            "MARSEILLE  ;199.0€",
            "Saint   Denis;$-12,5",
        ],
    )

    # Act
    run(spark, src_csv, out_dir, delimiter=";")

    # Assert: lire la sortie SANS inferschema pour vérifier les chaînes écrites
    out_df = (
        spark.read.option("header", True)
        .option("delimiter", ";")
        .option("inferSchema", False)
        .csv(out_dir)
        .select("city", "price")
        .orderBy("city")
    )

    expected = spark.createDataFrame(
        [
            ("lyon", "20.240"),
            ("marseille", "199.000"),
            ("paris", "2.023"),
            ("saint denis", "-12.500"),
        ],
        ["city", "price"],
    ).orderBy("city")

    assert out_df.collect() == expected.collect()

    # Bonus: vérifie qu'un part-* existe
    parts = glob.glob(os.path.join(out_dir, "part-*"))
    assert parts, "Aucun fichier part-* écrit en sortie"
