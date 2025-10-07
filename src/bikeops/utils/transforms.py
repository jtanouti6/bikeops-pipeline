# src/bikeops/utils/transforms.py
from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import DecimalType

# =========================
# Helpers STRING / CASE / NULL
# =========================

_NULL_TOKENS = {"", "null", "na", "n/a", "none", "nil"}


def to_lower(c: Column) -> Column:
    return F.lower(c)


def to_upper(c: Column) -> Column:
    return F.upper(c)


def to_title(c: Column) -> Column:
    # Trim les bords, conserve les espaces internes, puis met en Titre
    return F.initcap(F.trim(c))


def collapse_spaces(c: Column) -> Column:
    # Trim + collapse des espaces multiples + to_lower (attendu par le test)
    return F.lower(F.regexp_replace(F.trim(c), r"\s+", " "))


def normalize_null_str(c: Column) -> Column:
    # Null si vide / "null" / "na" / "n/a" / "none" / "nil" (case-insensitive)
    t = F.trim(c)
    return F.when(
        t.isNull() | F.lower(t).isin(list(_NULL_TOKENS)), F.lit(None)
    ).otherwise(t)


def to_double_from_str_any(c: Column) -> Column:
    """
    Convertit une chaîne "libre" en double :
      - normalise nulls via normalize_null_str
      - remplace ',' -> '.'
      - supprime tout ce qui n'est pas [0-9 eE + - .]
      - cast en double
    """
    n = normalize_null_str(c)
    n = F.regexp_replace(n, ",", ".")
    n = F.regexp_replace(n, r"[^0-9eE\+\-\.]", "")
    return n.cast("double")


# =========================
# Déjà proposés avant (laisse-les si présents)
# =========================


def clean_city(
    df: DataFrame, col_in: str = "city", col_out: str | None = None
) -> DataFrame:
    target = col_out or col_in
    return df.withColumn(
        target, F.lower(F.regexp_replace(F.trim(F.col(col_in)), r"\s+", " "))
    )


def clean_price(
    df: DataFrame,
    col_in: str = "price",
    col_out: str | None = None,
    precision: int = 12,
    scale: int = 3,
) -> DataFrame:
    target = col_out or col_in
    cleaned = F.trim(F.col(col_in))
    cleaned = F.regexp_replace(cleaned, r",", ".")
    cleaned = F.regexp_replace(cleaned, r"[^0-9\.\-]", "")
    cleaned = F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)
    return df.withColumn(target, cleaned.cast(DecimalType(precision, scale)))
