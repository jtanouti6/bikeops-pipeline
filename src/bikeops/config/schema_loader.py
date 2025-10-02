from pathlib import Path
from typing import Any, Dict, List

import yaml


def load_contract(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    # validations minimales
    assert "name" in doc and "fields" in doc, "Contrat invalide"
    return doc


def field_specs(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    return contract.get("fields", [])


def quality_checks(contract: Dict[str, Any]) -> List[Dict[str, str]]:
    return contract.get("quality_checks", [])
