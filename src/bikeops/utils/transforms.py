from pyspark.sql import functions as F


def collapse_spaces(col):
    return F.regexp_replace(F.trim(col), r"\s+", " ")


def to_title(col):
    return F.initcap(collapse_spaces(col))


def to_lower(col):
    return F.lower(collapse_spaces(col))


def to_upper(col):
    return F.upper(collapse_spaces(col))


def normalize_null_str(col):
    """Transforme 'null', 'None', 'NA', '' -> NULL"""
    return F.when(F.trim(F.lower(col)).isin("null", "none", "na", ""), F.lit(None)).otherwise(col)


def to_double_from_str_any(col):
    """Remplace la virgule décimale par un point et caste en double, en traitant les 'null'."""
    cleaned = F.regexp_replace(F.trim(col), r",", ".")
    return F.when(F.trim(F.lower(col)).isin("null", ""), F.lit(None).cast("double")).otherwise(
        cleaned.cast("double")
    )
