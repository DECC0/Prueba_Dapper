"""
Utilidades para cargar las reglas de validación utilizadas por el pipeline.
El archivo por defecto se ubica en `configs/validation_rules.json`, pero puede
sobrescribirse mediante la variable de entorno `VALIDATION_RULES_PATH`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

DEFAULT_RULES: Dict = {
    "fields": {
        "title": {
            "type": "string",
            "regex": r".+",
            "required": True,
            "max_length": 65,
        },
        "created_at": {
            "type": "date",
            "regex": r"^\d{4}-\d{2}-\d{2}",
            "required": True,
        },
        "external_link": {
            "type": "string",
            "regex": r"^https?://.+",
            "required": True,
        },
        "summary": {
            "type": "string",
            "required": False,
        },
        "rtype_id": {
            "type": "integer",
            "required": True,
        },
        "classification_id": {
            "type": "integer",
            "required": True,
        },
        "is_active": {
            "type": "boolean",
            "required": True,
        },
        "update_at": {
            "type": "string",
            "regex": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "required": True,
        },
    }
}

DEFAULT_RULES_PATH = Path(os.environ.get("VALIDATION_RULES_PATH", "configs/validation_rules.json"))


def load_rules(path: Path | None = None) -> Dict:
    """
    Carga reglas desde el archivo indicado. Si no existe, retorna las reglas por defecto.
    """
    config_path = path or DEFAULT_RULES_PATH
    if config_path.is_file():
        with config_path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    return DEFAULT_RULES

