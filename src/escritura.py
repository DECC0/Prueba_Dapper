"""
Módulo de escritura responsable de persistir la información en Postgres.
Utiliza las mismas reglas de idempotencia del script original, pero ahora la
conexión se obtiene desde variables de entorno que expone el docker-compose de
Airflow.
"""

from __future__ import annotations

import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import psycopg2

# Variables de entorno que se inyectan desde docker-compose
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "airflow")
DB_USER = os.environ.get("DB_USER", "airflow")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "airflow")


@dataclass
class DatabaseConfig:
    host: str = DB_HOST
    port: int = DB_PORT
    name: str = DB_NAME
    user: str = DB_USER
    password: str = DB_PASSWORD


class DatabaseManager(AbstractContextManager):
    """
    Encapsula la conexión a Postgres y expone helpers de ejecución masiva.
    Permite usarse tanto con `with` como de forma manual mediante `connect`.
    """

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self.config = config or DatabaseConfig()
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None

    # Context manager -----------------------------------------------------
    def __enter__(self) -> "DatabaseManager":
        if not self.connect():
            raise ConnectionError("Could not establish database connection")
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()

    # Conexión -------------------------------------------------------------
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                dbname=self.config.name,
                user=self.config.user,
                password=self.config.password,
                host=self.config.host,
                port=self.config.port,
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as exc:
            print(f"Database connection error: {exc}")
            return False

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    # Helpers --------------------------------------------------------------
    def execute_query(self, query: str, params: Optional[Iterable] = None) -> List[Tuple]:
        if not self.cursor:
            raise RuntimeError("Database not connected")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df: pd.DataFrame, table_name: str) -> int:
        if not self.cursor or not self.connection:
            raise RuntimeError("Database not connected")

        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]

            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as exc:
            self.connection.rollback()
            raise RuntimeError(f"Error inserting into {table_name}: {str(exc)}") from exc


# -------------------------------------------------------------------------
# Funciones auxiliares de inserción

REGULATIONS_TABLE = os.environ.get("REGULATIONS_TABLE", "regulations")
REGULATIONS_COMPONENT_TABLE = os.environ.get("REGULATIONS_COMPONENT_TABLE", "regulations_component")
ENTITY_VALUE = "Agencia Nacional de Infraestructura"


def insert_regulations_component(db_manager: DatabaseManager, new_ids: List[int]) -> Tuple[int, str]:
    """
    Inserta los componentes de las regulaciones replicando la lógica original.
    """
    if not new_ids:
        return 0, "No new regulation IDs provided"

    try:
        id_rows = pd.DataFrame(new_ids, columns=["regulations_id"])
        id_rows["components_id"] = 7

        inserted_count = db_manager.bulk_insert(id_rows, REGULATIONS_COMPONENT_TABLE)
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"

    except Exception as exc:
        return 0, f"Error inserting regulation components: {str(exc)}"


def insert_new_records(db_manager: DatabaseManager, df: pd.DataFrame, entity: str = ENTITY_VALUE) -> Tuple[int, str]:
    """
    Inserta nuevos registros evitando duplicados. Conserva la lógica del script
    original (claves compuestas por título, fecha y enlace).
    """
    try:
        query = f"""
            SELECT title, created_at::text, entity, COALESCE(external_link, '') AS external_link
            FROM {REGULATIONS_TABLE}
            WHERE entity = %s
        """

        existing_records = db_manager.execute_query(query, (entity,))

        if not existing_records:
            db_df = pd.DataFrame(columns=["title", "created_at", "entity", "external_link"])
        else:
            db_df = pd.DataFrame(existing_records, columns=["title", "created_at", "entity", "external_link"])

        print(f"Registros existentes en BD para {entity}: {len(db_df)}")

        entity_df = df[df["entity"] == entity].copy()

        if entity_df.empty:
            return 0, f"No records found for entity {entity}"

        print(f"Registros a procesar para {entity}: {len(entity_df)}")

        if not db_df.empty:
            db_df["created_at"] = db_df["created_at"].astype(str)
            db_df["external_link"] = db_df["external_link"].fillna("").astype(str)
            db_df["title"] = db_df["title"].astype(str).str.strip()

        entity_df["created_at"] = entity_df["created_at"].astype(str)
        entity_df["external_link"] = entity_df["external_link"].fillna("").astype(str)
        entity_df["title"] = entity_df["title"].astype(str).str.strip()

        print("=== INICIANDO VALIDACIÓN DE DUPLICADOS OPTIMIZADA ===")

        if db_df.empty:
            new_records = entity_df.copy()
            duplicates_found = 0
            print("No hay registros existentes, todos son nuevos")
        else:
            entity_df["unique_key"] = (
                entity_df["title"] + "|" + entity_df["created_at"] + "|" + entity_df["external_link"]
            )

            db_df["unique_key"] = db_df["title"] + "|" + db_df["created_at"] + "|" + db_df["external_link"]

            existing_keys = set(db_df["unique_key"])
            entity_df["is_duplicate"] = entity_df["unique_key"].isin(existing_keys)

            new_records = entity_df[~entity_df["is_duplicate"]].copy()
            duplicates_found = len(entity_df) - len(new_records)

            if duplicates_found > 0:
                print(f"Duplicados encontrados: {duplicates_found}")
                duplicate_records = entity_df[entity_df["is_duplicate"]]
                print("Ejemplos de duplicados:")
                for _, row in duplicate_records.head(3).iterrows():
                    print(f"  - {row['title'][:50]}... | {row['created_at']}")

        print(f"Antes de remover duplicados internos: {len(new_records)}")
        new_records = new_records.drop_duplicates(subset=["title", "created_at", "external_link"], keep="first")
        internal_duplicates = len(entity_df) - duplicates_found - len(new_records)
        if internal_duplicates > 0:
            print(f"Duplicados internos removidos: {internal_duplicates}")

        print(f"Después de remover duplicados internos: {len(new_records)}")
        print(f"=== DUPLICADOS IDENTIFICADOS: {duplicates_found + internal_duplicates} ===")

        if new_records.empty:
            return 0, f"No new records found for entity {entity} after duplicate validation"

        columns_to_drop = ["unique_key", "is_duplicate"]
        for col in columns_to_drop:
            if col in new_records.columns:
                new_records = new_records.drop(columns=[col])

        print(f"Registros finales a insertar: {len(new_records)}")

        try:
            print(f"=== INSERTANDO {len(new_records)} REGISTROS ===")
            total_rows_processed = db_manager.bulk_insert(new_records, REGULATIONS_TABLE)

            if total_rows_processed == 0:
                return 0, f"No records were actually inserted for entity {entity}"

            print(f"Registros insertados exitosamente: {total_rows_processed}")

        except Exception as insert_error:
            print(f"Error en inserción: {insert_error}")
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                print("Error de duplicados detectado - algunos registros ya existían")
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            raise insert_error

        print("=== OBTENIENDO IDS DE REGISTROS INSERTADOS ===")

        new_ids_query = f"""
            SELECT id FROM {REGULATIONS_TABLE}
            WHERE entity = %s 
            ORDER BY id DESC
            LIMIT %s
        """

        new_ids_result = db_manager.execute_query(new_ids_query, (entity, total_rows_processed))
        new_ids = [row[0] for row in new_ids_result]

        print(f"IDs obtenidos: {len(new_ids)}")

        inserted_count_comp = 0
        component_message = ""

        if new_ids:
            try:
                inserted_count_comp, component_message = insert_regulations_component(db_manager, new_ids)
                print(f"Componentes: {component_message}")
            except Exception as comp_error:
                print(f"Error insertando componentes: {comp_error}")
                component_message = f"Error inserting components: {str(comp_error)}"

        total_duplicates = duplicates_found + internal_duplicates
        stats = (
            f"Processed: {len(entity_df)} | "
            f"Existing: {len(db_df)} | "
            f"Duplicates skipped: {total_duplicates} | "
            f"New inserted: {total_rows_processed}"
        )

        message = f"Entity {entity}: {stats}. {component_message}"
        print("=== RESULTADO FINAL ===")
        print(message)
        print("=" * 50)

        return total_rows_processed, message

    except Exception as exc:
        if hasattr(db_manager, "connection") and db_manager.connection:
            db_manager.connection.rollback()
        error_msg = f"Error processing entity {entity}: {str(exc)}"
        print(f"ERROR CRÍTICO: {error_msg}")
        import traceback

        print(traceback.format_exc())
        return 0, error_msg


def fetch_latest_created_at(db_manager: DatabaseManager, entity: str = ENTITY_VALUE) -> Optional[datetime]:
    """
    Obtiene la última fecha de creación almacenada para una entidad.
    """
    query = f"SELECT MAX(created_at) FROM {REGULATIONS_TABLE} WHERE entity = %s"
    result = db_manager.execute_query(query, (entity,))

    if result and result[0][0]:
        latest_db_date = result[0][0]
        if isinstance(latest_db_date, str):
            try:
                latest_db_date = datetime.strptime(latest_db_date, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    latest_db_date = datetime.strptime(latest_db_date.split()[0], "%Y-%m-%d")
                except Exception:
                    latest_db_date = None

        if latest_db_date and hasattr(latest_db_date, "tzinfo") and latest_db_date.tzinfo is not None:
            latest_db_date = latest_db_date.replace(tzinfo=None)

        return latest_db_date

    return None

