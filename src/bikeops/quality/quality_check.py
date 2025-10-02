from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from bikeops.config.schema_loader import quality_checks


def run_quality_checks(df, contract):
    checks = quality_checks(contract)
    totals = df.count()
    rows = []
    for c in checks:
        name = c["name"]
        expr = c["expr"]
        try:
            nb_ko = df.filter(F.expr(f"NOT ({expr})")).count()
        except AnalysisException as e:
            raise RuntimeError(
                f"Quality check '{name}' failed. Expr: {expr}\n"
                f"Available columns: {df.columns}\nUnderlying error: {e}"
            ) from e
        taux = round(100.0 * nb_ko / totals, 2) if totals else 0.0
        rows.append((name, nb_ko, totals, taux))
    return df.sparkSession.createDataFrame(
        rows, "check_name string, nb_ko long, nb_total long, taux_ko double"
    )
