"""
Módulo de extracción encargado de scrapear la información publicada por la ANI.
La lógica se mantuvo conforme al script original para garantizar el mismo
comportamiento frente a omisiones, limpieza y reglas de negocio.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# Constantes del scraping (idénticas a la lambda original)
ENTITY_VALUE = "Agencia Nacional de Infraestructura"
FIXED_CLASSIFICATION_ID = 13
URL_BASE = (
    "https://www.ani.gov.co/informacion-de-la-ani/normatividad?"
    "field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="
)

CLASSIFICATION_KEYWORDS = {
    "resolución": 15,
    "resolucion": 15,
    "decreto": 14,
}

DEFAULT_RTYPE_ID = 14


def clean_quotes(text: Optional[str]) -> Optional[str]:
    """
    Elimina comillas y variantes Unicode para dejar el texto limpio y consistente.
    """
    if not text:
        return text
    quotes_map = {
        "\u201C": "",
        "\u2018": "",
        "\u2019": "",
        "\u00AB": "",
        "\u00BB": "",
        "\u201E": "",
        "\u201A": "",
        "\u2039": "",
        "\u203A": "",
        '"': "",
        "'": "",
        "´": "",
        "`": "",
        "′": "",
        "″": "",
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r"['\"\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]"
    cleaned_text = re.sub(quotes_pattern, "", cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = " ".join(cleaned_text.split())
    return cleaned_text


def get_rtype_id(title: str) -> int:
    """
    Determina el tipo de regulación (`rtype_id`) a partir de palabras clave del título.
    """
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(created_at_value: Optional[str]) -> bool:
    """
    Verifica que el campo `created_at` contenga información utilizable.
    """
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False


def normalize_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Quita información de timezone para facilitar comparaciones.
    """
    if dt is None:
        return None
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def extract_title_and_link(row, norma_data: Dict[str, Optional[str]], verbose: bool, row_num: int) -> bool:
    """
    Extrae título y enlace de una fila; devuelve False si faltan datos requeridos.
    """
    title_cell = row.find("td", class_="views-field views-field-title")
    if not title_cell:
        if verbose:
            print(f"No se encontró celda de título en la fila {row_num}. Saltando.")
        return False

    title_link = title_cell.find("a")
    if not title_link:
        if verbose:
            print(f"No se encontró enlace en la fila {row_num}. Saltando.")
        return False

    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)

    if len(cleaned_title) > 65:
        if verbose:
            print(
                f"Saltando norma con título demasiado largo: '{cleaned_title}' "
                f"(longitud: {len(cleaned_title)})"
            )
        return False

    norma_data["title"] = cleaned_title

    external_link = title_link.get("href")
    if external_link and not external_link.startswith("http"):
        external_link = "https://www.ani.gov.co" + external_link

    norma_data["external_link"] = external_link
    norma_data["gtype"] = "link" if external_link else None

    if not norma_data["external_link"]:
        if verbose:
            print(f"Saltando norma '{norma_data['title']}' por no tener enlace externo válido.")
        return False

    return True


def extract_summary(row, norma_data: Dict[str, Optional[str]]) -> None:
    """
    Extrae el resumen/descripción de la fila.
    """
    summary_cell = row.find("td", class_="views-field views-field-body")
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize()
        norma_data["summary"] = formatted_summary
    else:
        norma_data["summary"] = None


def extract_creation_date(row, norma_data: Dict[str, Optional[str]], verbose: bool, row_num: int) -> bool:
    """
    Extrae la fecha de creación y la normaliza a formato YYYY-MM-DD si es posible.
    """
    fecha_cell = row.find("td", class_="views-field views-field-field-fecha--1")
    if fecha_cell:
        fecha_span = fecha_cell.find("span", class_="date-display-single")
        if fecha_span:
            created_at_raw = fecha_span.get("content", fecha_span.get_text(strip=True))
            if "T" in created_at_raw:
                norma_data["created_at"] = created_at_raw.split("T")[0]
            elif "/" in created_at_raw:
                try:
                    day, month, year = created_at_raw.split("/")
                    norma_data["created_at"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    norma_data["created_at"] = created_at_raw
            else:
                norma_data["created_at"] = created_at_raw
        else:
            norma_data["created_at"] = fecha_cell.get_text(strip=True)
    else:
        norma_data["created_at"] = None

    if not is_valid_created_at(norma_data["created_at"]):
        if verbose:
            print(
                f"Saltando norma '{norma_data['title']}' por no tener fecha de creación válida "
                f"(created_at: {norma_data['created_at']})."
            )
        return False

    return True


def scrape_page(page_num: int, verbose: bool = False) -> List[Dict[str, Optional[str]]]:
    """
    Scrapea una página específica de la ANI y devuelve los registros válidos encontrados.
    """
    page_url = URL_BASE if page_num == 0 else f"{URL_BASE}&page={page_num}"

    if verbose:
        print(f"Scrapeando página {page_num}: {page_url}")

    try:
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        tbody = soup.find("tbody")

        if not tbody:
            if verbose:
                print(f"No se encontró tabla en página {page_num}")
            return []

        rows = tbody.find_all("tr")
        if verbose:
            print(f"Encontradas {len(rows)} filas en página {page_num}")

        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                norma_data: Dict[str, Optional[str]] = {
                    "created_at": None,
                    "update_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_active": True,
                    "title": None,
                    "gtype": None,
                    "entity": ENTITY_VALUE,
                    "external_link": None,
                    "rtype_id": None,
                    "summary": None,
                    "classification_id": FIXED_CLASSIFICATION_ID,
                }

                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue

                extract_summary(row, norma_data)

                if not extract_creation_date(row, norma_data, verbose, i):
                    continue

                norma_data["rtype_id"] = get_rtype_id(norma_data["title"])

                page_data.append(norma_data)

            except Exception as exc:
                if verbose:
                    print(f"Error procesando fila {i} en página {page_num}: {str(exc)}")
                continue

        return page_data

    except requests.RequestException as exc:
        print(f"Error HTTP en página {page_num}: {exc}")
        return []
    except Exception as exc:
        print(f"Error procesando página {page_num}: {exc}")
        return []


def scrape_recent_pages(start_page: int, end_page: int, verbose: bool = False) -> List[Dict[str, Optional[str]]]:
    """
    Scrapea un rango de páginas comenzando por las más recientes.
    """
    all_normas_data: List[Dict[str, Optional[str]]] = []
    total_pages = end_page - start_page + 1

    for page_num in range(start_page, end_page + 1):
        if verbose:
            print(f"Procesando página {page_num}...")
        page_data = scrape_page(page_num, verbose=verbose)
        all_normas_data.extend(page_data)

        if verbose and (page_num - start_page + 1) % 3 == 0:
            processed = page_num - start_page + 1
            print(f"Procesadas {processed}/{total_pages} páginas. Encontrados {len(all_normas_data)} registros válidos.")

    return all_normas_data


def check_for_new_content(
    fetch_latest_created_at: Callable[[], Optional[datetime]],
    num_pages_to_check: int = 3,
) -> bool:
    """
    Verifica si hay contenido nuevo comparando las primeras páginas contra la fecha almacenada.
    `fetch_latest_created_at` debe devolver un datetime naive o None.
    """
    print(f"Verificando contenido nuevo en las primeras {num_pages_to_check} páginas...")

    try:
        latest_db_date = fetch_latest_created_at()
        latest_db_date = normalize_datetime(latest_db_date)

        print(f"Fecha más reciente en BD: {latest_db_date}")

        for page_num in range(num_pages_to_check):
            try:
                page_data = scrape_page(page_num, verbose=False)

                for record in page_data:
                    created_at_val = record.get("created_at")
                    if not created_at_val or not is_valid_created_at(created_at_val):
                        continue

                    web_date: Optional[datetime] = None
                    try:
                        web_date = datetime.strptime(created_at_val, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        try:
                            web_date = datetime.strptime(created_at_val.split()[0], "%Y-%m-%d")
                        except Exception:
                            continue

                    web_date = normalize_datetime(web_date)

                    if not latest_db_date or (web_date and web_date > latest_db_date):
                        print(f"Nuevo contenido detectado - Fecha web: {web_date}, Fecha BD: {latest_db_date}")
                        return True

            except Exception as exc:
                print(f"Error verificando página {page_num}: {exc}")
                continue

        print("No se detectó contenido nuevo")
        return False

    except Exception as exc:
        print(f"Error en verificación de contenido nuevo: {exc}")
        return True

