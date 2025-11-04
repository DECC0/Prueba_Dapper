"""
Aplicación de reglas de validación sobre los datos extraídos.
Las reglas se cargan mediante `src.validaciones` y permiten definir el tipo,
regex y obligatoriedad de cada campo. Cuando un dato no cumple, se convierte
en nulo; si un campo obligatorio queda nulo, la fila completa se descarta.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd


def _coerce_type(series: pd.Series, expected_type: str) -> Tuple[pd.Series, pd.Series]:
    """
    Convierte y valida el tipo de dato según lo solicitado.
    Retorna una serie convertida y una máscara booleana indicando cuáles valores son válidos.
    """
    converted = []
    mask_valid = []

    for value in series:
        if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
            converted.append(pd.NA)
            mask_valid.append(False)
            continue

        try:
            if expected_type == "string":
                text = str(value).strip()
                if not text:
                    raise ValueError("empty string")
                converted.append(text)
                mask_valid.append(True)

            elif expected_type == "integer":
                text = str(value).strip()
                if text == "":
                    raise ValueError("empty integer")
                converted.append(int(text))
                mask_valid.append(True)

            elif expected_type == "float":
                text = str(value).strip()
                if text == "":
                    raise ValueError("empty float")
                converted.append(float(text))
                mask_valid.append(True)

            elif expected_type == "boolean":
                if isinstance(value, bool):
                    converted.append(value)
                    mask_valid.append(True)
                else:
                    text = str(value).strip().lower()
                    truthy = {"true", "1", "t", "yes", "si", "sí"}
                    falsy = {"false", "0", "f", "no"}
                    if text in truthy:
                        converted.append(True)
                        mask_valid.append(True)
                    elif text in falsy:
                        converted.append(False)
                        mask_valid.append(True)
                    else:
                        raise ValueError("invalid boolean")

            elif expected_type == "date":
                if isinstance(value, datetime):
                    dt = value
                else:
                    text = str(value).strip()
                    if not text:
                        raise ValueError("empty date")
                    dt = None
                    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
                        try:
                            dt = datetime.strptime(text, fmt)
                            break
                        except ValueError:
                            continue
                    if dt is None:
                        raise ValueError("invalid date format")
                converted.append(dt.strftime("%Y-%m-%d"))
                mask_valid.append(True)

            else:
                # Tipo no reconocido: dejar el valor como está.
                converted.append(value)
                mask_valid.append(True)

        except Exception:
            converted.append(pd.NA)
            mask_valid.append(False)

    return (
        pd.Series(converted, index=series.index, dtype="object"),
        pd.Series(mask_valid, index=series.index),
    )


def apply_validation(df: pd.DataFrame, rules: Dict) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Aplica las reglas de validación configuradas y retorna datos limpios junto con estadísticas.
    """
    if df.empty:
        return df.copy(), {
            "rows_received": 0,
            "rows_discarded": 0,
            "rows_valid": 0,
            "invalid_cells": {},
        }

    rules_fields = (rules or {}).get("fields", {})
    df_clean = df.copy()
    invalid_cells = {}
    required_fields = []

    for field, config in rules_fields.items():
        if config.get("required"):
            required_fields.append(field)

        if field not in df_clean.columns:
            # Crear columna con valores nulos para que se marque como inválida si es requerida
            df_clean[field] = pd.NA
            invalid_cells[field] = len(df_clean)
            continue

        series = df_clean[field]
        mask_valid = pd.Series(True, index=series.index)
        coerced = series.astype("object")

        expected_type = config.get("type")
        if expected_type:
            coerced, mask_type = _coerce_type(series, expected_type)
            mask_valid &= mask_type

        regex_pattern = config.get("regex")
        if regex_pattern:
            regex = re.compile(regex_pattern)
            mask_regex = coerced.fillna("").astype(str).str.match(regex)
            coerced = coerced.where(mask_regex, pd.NA)
            mask_valid &= mask_regex

        max_length = config.get("max_length")
        if max_length is not None:
            lengths = coerced.fillna("").astype(str).str.len()
            mask_length = lengths <= max_length
            coerced = coerced.where(mask_length, pd.NA)
            mask_valid &= mask_length

        invalid_count = int((~mask_valid).sum())
        if invalid_count:
            invalid_cells[field] = invalid_count

        df_clean[field] = coerced

    before_drop = len(df_clean)
    if required_fields:
        df_clean = df_clean.dropna(subset=required_fields)

    after_drop = len(df_clean)
    stats = {
        "rows_received": len(df),
        "rows_discarded": before_drop - after_drop,
        "rows_valid": after_drop,
        "invalid_cells": invalid_cells,
    }

    return df_clean.reset_index(drop=True), stats

