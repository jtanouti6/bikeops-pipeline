import re
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

# Racine du repo: .../src/bikeops/utils/config.py -> parents[3] = repo root
REPO_ROOT = Path(__file__).resolve().parents[3]


def load_profile(profile_file: str = "configs/local.yaml"):
    p = Path(profile_file)
    if not p.is_absolute() and not p.exists():
        p = REPO_ROOT / profile_file
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str, timezone: str = "UTC"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", timezone)
        .getOrCreate()
    )


_SCHEME = re.compile(r"^[a-zA-Z0-9+.-]+://")


def _join_uri(base, *parts):
    if _SCHEME.match(base):  # gs://, s3://, hdfs:// …
        base = base.rstrip("/")
        tail = "/".join(p.strip("/") for p in parts)
        return f"{base}/{tail}"
    return str(Path(base, *parts))


def paths_from_cfg(cfg):
    root = cfg["data_root"]
    return {
        "bronze": _join_uri(root, "bronze"),
        "silver": _join_uri(root, "silver"),
        "gold": _join_uri(root, "gold"),
    }
