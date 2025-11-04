import json
from typing import Any, Dict, Optional

import pandas as pd

from src import escritura, extraccion, validacion, validaciones


def _fetch_latest_created_at() -> Optional[pd.Timestamp]:
    with escritura.DatabaseManager() as db_manager:
        return escritura.fetch_latest_created_at(db_manager, escritura.ENTITY_VALUE)


def lambda_handler(event: Optional[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    """
    Orquestación local del proceso Extract -> Validate -> Write reutilizando los módulos refactorizados.
    Permite ejecutar la lógica fuera de Airflow (o para pruebas manuales).
    """
    try:
        num_pages_to_scrape = int(event.get("num_pages_to_scrape", 9)) if event else 9
        num_pages_to_scrape = max(num_pages_to_scrape, 1)
        force_scrape = bool(event.get("force_scrape", False)) if event else False

        print(f"Iniciando scraping de ANI - Páginas a procesar: {num_pages_to_scrape}")

        if not force_scrape:
            has_new_content = extraccion.check_for_new_content(
                _fetch_latest_created_at,
                min(3, num_pages_to_scrape),
            )
            if not has_new_content:
                return {
                    "statusCode": 200,
                    "body": json.dumps(
                        {
                            "message": "No se detectó contenido nuevo. Scraping omitido.",
                            "records_scraped": 0,
                            "records_inserted": 0,
                            "content_check": "no_new_content",
                            "success": True,
                        }
                    ),
                }

        start_page = 0
        end_page = num_pages_to_scrape - 1
        print(f"Procesando páginas más recientes desde {start_page} hasta {end_page}")

        all_normas_data = extraccion.scrape_recent_pages(start_page, end_page, verbose=True)

        if not all_normas_data:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "No se encontraron datos válidos durante el scraping",
                        "records_scraped": 0,
                        "records_inserted": 0,
                        "pages_processed": f"{start_page}-{end_page}",
                        "success": True,
                    }
                ),
            }

        df_normas = pd.DataFrame(all_normas_data)
        print(f"Total de registros extraídos: {len(df_normas)}")

        rules = validaciones.load_rules()
        validated_df, validation_stats = validacion.apply_validation(df_normas, rules)
        print(f"Validación aplicada: {validation_stats}")

        if validated_df.empty:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Todos los registros fueron descartados por validación",
                        "records_scraped": len(df_normas),
                        "records_validated": 0,
                        "records_inserted": 0,
                        "pages_processed": f"{start_page}-{end_page}",
                        "success": True,
                    }
                ),
            }

        with escritura.DatabaseManager() as db_manager:
            inserted_count, status_message = escritura.insert_new_records(
                db_manager,
                validated_df,
                escritura.ENTITY_VALUE,
            )

        response_body = {
            "message": status_message,
            "records_scraped": len(df_normas),
            "records_validated": len(validated_df),
            "records_inserted": inserted_count,
            "pages_processed": f"{start_page}-{end_page}",
            "content_check": "new_content_found" if not force_scrape else "forced_scrape",
            "success": True,
        }

        print(f"Operación completada: {status_message}")
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:
        error_message = f"Error en la ejecución del proceso: {str(exc)}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": error_message, "success": False}),
        }


if __name__ == "__main__":
    test_event = {"num_pages_to_scrape": 3, "force_scrape": True}
    result = lambda_handler(test_event, {})
    print(json.dumps(result, indent=2))

