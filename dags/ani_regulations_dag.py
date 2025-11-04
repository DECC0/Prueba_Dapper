"""
DAG de Airflow que orquesta el flujo Extract -> Validate -> Write para las
normativas de la ANI. Ejecuta el scraping, aplica validaciones configurables y
persiste las filas nuevas en Postgres reutilizando las reglas de idempotencia.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from src import escritura, extraccion, validacion, validaciones

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_conf(context: Dict[str, Any]) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def extract_task(**context) -> List[Dict[str, Any]]:
    conf = _get_conf(context)
    num_pages = int(conf.get("num_pages_to_scrape", 9))
    num_pages = max(num_pages, 1)
    force_scrape = bool(conf.get("force_scrape", False))

    def fetch_latest():
        with escritura.DatabaseManager() as db_manager:
            return escritura.fetch_latest_created_at(db_manager, extraccion.ENTITY_VALUE)

    if not force_scrape:
        has_new_content = extraccion.check_for_new_content(fetch_latest, min(3, num_pages))
        if not has_new_content:
            raise AirflowSkipException("No se detectó contenido nuevo en las primeras páginas")

    records = extraccion.scrape_recent_pages(0, num_pages - 1, verbose=True)
    context["ti"].xcom_push(key="records_scraped", value=len(records))
    return records


def validate_task(**context) -> List[Dict[str, Any]]:
    records = context["ti"].xcom_pull(task_ids="extract")
    if not records:
        raise AirflowSkipException("No hay registros para validar")

    df = pd.DataFrame(records)
    rules = validaciones.load_rules()
    validated_df, stats = validacion.apply_validation(df, rules)

    context["ti"].xcom_push(key="validation_stats", value=stats)
    context["ti"].xcom_push(key="records_validated", value=len(validated_df))

    if validated_df.empty:
        raise AirflowSkipException("Todos los registros fueron descartados por validación")

    return validated_df.to_dict(orient="records")


def write_task(**context) -> Dict[str, Any]:
    records = context["ti"].xcom_pull(task_ids="validate")
    if not records:
        raise AirflowSkipException("No hay registros validados para insertar")

    df = pd.DataFrame(records)

    with escritura.DatabaseManager() as db_manager:
        inserted_count, status_message = escritura.insert_new_records(db_manager, df, escritura.ENTITY_VALUE)

    context["ti"].xcom_push(key="records_inserted", value=inserted_count)
    return {
        "inserted": inserted_count,
        "message": status_message,
    }


with DAG(
    dag_id="ani_regulations_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracción, validación y escritura de normatividad ANI",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ani", "scraping", "regulations"],
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    write = PythonOperator(
        task_id="write",
        python_callable=write_task,
    )

    extract >> validate >> write

