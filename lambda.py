import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import psycopg2
import boto3
from botocore.exceptions import ClientError
import json
import os
from typing import Dict, Any

# Configuración de AWS Secrets Manager
SECRET_NAME = os.environ.get("SECRET_NAME", "Test")
REGION_NAME = os.environ.get("AWS_REGION", "us-east-1")

# Constantes para el scraping
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
URL_BASE = "https://www.ani.gov.co/informacion-de-la-ani/normatividad?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="

# Clasificaciones de documentos
CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}

DEFAULT_RTYPE_ID = 14

# Cliente de Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)

def get_secret():
    """
    Recupera las credenciales de la base de datos de AWS Secrets Manager.
    """
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

#  Clase para manejar la conexión a la base de datos y realizar operaciones de inserción de datos.
class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            secrets = get_secret()
            
            self.connection = psycopg2.connect(
                dbname=secrets['DB_NAME'],
                user=secrets['DB_USERNAME'],
                password=secrets['DB_PASSWORD'],
                host=secrets['DB_HOST'],
                port=secrets['DB_PORT']
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        if not self.cursor:
            raise Exception("Database not connected")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df, table_name):
        if not self.connection or not self.cursor:
            raise Exception("Database not connected")
        
        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            
            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]
            
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error inserting into {table_name}: {str(e)}")
        
# Función eliminar comillas
def clean_quotes(text):
    if not text:
        return text
    quotes_map = {
        '\u201C': '', '\u2018': '', '\u2019': '', '\u00AB': '', '\u00BB': '',
        '\u201E': '', '\u201A': '', '\u2039': '', '\u203A': '', '"': '',
        "'": '', '´': '', '`': '', '′': '', '″': '',
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned_text = re.sub(quotes_pattern, '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

# Obtener el rtype_id basado en el título del documento
def get_rtype_id(title):
    """
    Obtiene el rtype_id basado en el título del documento.
    """
    title_lower = title.lower()
    
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    
    return DEFAULT_RTYPE_ID

# Validar el campo created_at
def is_valid_created_at(created_at_value):
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False

def normalize_datetime(dt):
    """
    Normaliza un datetime para quitar información de timezone.
    """
    if dt is None:
        return None
    
    # Si es un datetime con timezone, convertir a naive
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    
    return dt

def extract_title_and_link(row, norma_data, verbose, row_num):
    """
    Extrae título y enlace de una fila
    
    Returns:
        bool: True si se extrajo correctamente, False si debe saltarse
    """
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        if verbose:
            print(f"No se encontró celda de título en la fila {row_num}. Saltando.")
        return False
    
    title_link = title_cell.find('a')
    if not title_link:
        if verbose:
            print(f"No se encontró enlace en la fila {row_num}. Saltando.")
        return False
    
    # Procesar título
    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)
    
    # Validar longitud del título
    if len(cleaned_title) > 65:
        if verbose:
            print(f"Saltando norma con título demasiado largo: '{cleaned_title}' (longitud: {len(cleaned_title)})")
        return False
    
    norma_data['title'] = cleaned_title
    
    # Procesar enlace
    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link
    
    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None
    
    # Validar que tenga enlace
    if not norma_data['external_link']:
        if verbose:
            print(f"Saltando norma '{norma_data['title']}' por no tener enlace externo válido.")
        return False
    
    return True

def extract_summary(row, norma_data):
    """
    Extrae el resumen/descripción de una fila
    """
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize()
        norma_data['summary'] = formatted_summary
    else:
        norma_data['summary'] = None

def extract_creation_date(row, norma_data, verbose, row_num):
    """
    Extrae la fecha de creación de una fila
    
    Returns:
        bool: True si se extrajo correctamente, False si debe saltarse
    """
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            # Procesar diferentes formatos de fecha
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None
    
    # Validar fecha
    if not is_valid_created_at(norma_data['created_at']):
        if verbose:
            print(f"Saltando norma '{norma_data['title']}' por no tener fecha de creación válida (created_at: {norma_data['created_at']}).")
        return False
    
    return True

def scrape_page(page_num, verbose=False):
    """
    Scrapea una página específica de ANI
    
    Args:
        page_num (int): Número de página a scrapear
        verbose (bool): Si mostrar logs detallados
    
    Returns:
        list: Lista de diccionarios con los datos extraídos
    """
    # Construir URL de la página
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"
    
    if verbose:
        print(f"Scrapeando página {page_num}: {page_url}")
    
    try:
        # Realizar solicitud HTTP
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()
        
        # Parsear HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tbody = soup.find('tbody')
        
        if not tbody:
            if verbose:
                print(f"No se encontró tabla en página {page_num}")
            return []
        
        rows = tbody.find_all('tr')
        if verbose:
            print(f"Encontradas {len(rows)} filas en página {page_num}")
        
        # Procesar filas
        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                # Estructura base del registro
                norma_data = {
                    'created_at': None,
                    'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': True,
                    'title': None,
                    'gtype': None,
                    'entity': ENTITY_VALUE,
                    'external_link': None,
                    'rtype_id': None,
                    'summary': None,
                    'classification_id': FIXED_CLASSIFICATION_ID,
                }
                
                # Extraer datos
                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue
                
                extract_summary(row, norma_data)
                
                if not extract_creation_date(row, norma_data, verbose, i):
                    continue
                
                # Establecer rtype_id basado en título
                norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
                
                page_data.append(norma_data)
                
            except Exception as e:
                if verbose:
                    print(f"Error procesando fila {i} en página {page_num}: {str(e)}")
                continue
        
        return page_data
        
    except requests.RequestException as e:
        print(f"Error HTTP en página {page_num}: {e}")
        return []
    except Exception as e:
        print(f"Error procesando página {page_num}: {e}")
        return []

def insert_regulations_component(db_manager, new_ids):
    """
    Inserta los componentes de las regulaciones.
    """
    if not new_ids:
        return 0, "No new regulation IDs provided"

    try:
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = 7
        
        inserted_count = db_manager.bulk_insert(id_rows, 'regulations_component')
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"
        
    except Exception as e:
        return 0, f"Error inserting regulation components: {str(e)}"

def insert_new_records(db_manager, df, entity):
    """
    Inserta nuevos registros en la base de datos evitando duplicados.
    Optimizada para velocidad y precisión.
    """
    regulations_table_name = 'regulations'
    
    try:
        # 1. OBTENER REGISTROS EXISTENTES INCLUYENDO EXTERNAL_LINK
        query = """
            SELECT title, created_at, entity, COALESCE(external_link, '') as external_link 
            FROM {} 
            WHERE entity = %s
        """.format(regulations_table_name)
        
        existing_records = db_manager.execute_query(query, (entity,))
        
        if not existing_records:
            db_df = pd.DataFrame(columns=['title', 'created_at', 'entity', 'external_link'])
        else:
            db_df = pd.DataFrame(existing_records, columns=['title', 'created_at', 'entity', 'external_link'])
        
        print(f"Registros existentes en BD para {entity}: {len(db_df)}")
        
        # 2. PREPARAR DATAFRAME DE LA ENTIDAD
        entity_df = df[df['entity'] == entity].copy()
        
        if entity_df.empty:
            return 0, f"No records found for entity {entity}"
        
        print(f"Registros a procesar para {entity}: {len(entity_df)}")
        
        # 3. NORMALIZAR DATOS PARA COMPARACIÓN CONSISTENTE
        # Normalizar created_at a string
        if not db_df.empty:
            db_df['created_at'] = db_df['created_at'].astype(str)
            db_df['external_link'] = db_df['external_link'].fillna('').astype(str)
            db_df['title'] = db_df['title'].astype(str).str.strip()
        
        entity_df['created_at'] = entity_df['created_at'].astype(str)
        entity_df['external_link'] = entity_df['external_link'].fillna('').astype(str)
        entity_df['title'] = entity_df['title'].astype(str).str.strip()
        
        # 4. IDENTIFICAR DUPLICADOS DE MANERA OPTIMIZADA
        print("=== INICIANDO VALIDACIÓN DE DUPLICADOS OPTIMIZADA ===")
        
        if db_df.empty:
            # Si no hay registros existentes, todos son nuevos
            new_records = entity_df.copy()
            duplicates_found = 0
            print("No hay registros existentes, todos son nuevos")
        else:
            # Crear claves únicas para comparación super rápida
            entity_df['unique_key'] = (
                entity_df['title'] + '|' + 
                entity_df['created_at'] + '|' + 
                entity_df['external_link']
            )
            
            db_df['unique_key'] = (
                db_df['title'] + '|' + 
                db_df['created_at'] + '|' + 
                db_df['external_link']
            )
            
            # Usar set para comparación O(1) - súper rápido
            existing_keys = set(db_df['unique_key'])
            entity_df['is_duplicate'] = entity_df['unique_key'].isin(existing_keys)
            
            new_records = entity_df[~entity_df['is_duplicate']].copy()
            duplicates_found = len(entity_df) - len(new_records)
            
            # Log para debugging
            if duplicates_found > 0:
                print(f"Duplicados encontrados: {duplicates_found}")
                duplicate_records = entity_df[entity_df['is_duplicate']]
                print("Ejemplos de duplicados:")
                for idx, row in duplicate_records.head(3).iterrows():
                    print(f"  - {row['title'][:50]}... | {row['created_at']}")
        
        # 5. REMOVER DUPLICADOS INTERNOS DEL DATAFRAME
        print(f"Antes de remover duplicados internos: {len(new_records)}")
        new_records = new_records.drop_duplicates(
            subset=['title', 'created_at', 'external_link'], 
            keep='first'
        )
        internal_duplicates = len(entity_df) - duplicates_found - len(new_records)
        if internal_duplicates > 0:
            print(f"Duplicados internos removidos: {internal_duplicates}")
        
        print(f"Después de remover duplicados internos: {len(new_records)}")
        print(f"=== DUPLICADOS IDENTIFICADOS: {duplicates_found + internal_duplicates} ===")
        
        if new_records.empty:
            return 0, f"No new records found for entity {entity} after duplicate validation"
        
        # 6. LIMPIAR DATAFRAME ANTES DE INSERTAR
        # Remover columnas auxiliares
        columns_to_drop = ['unique_key', 'is_duplicate']
        for col in columns_to_drop:
            if col in new_records.columns:
                new_records = new_records.drop(columns=[col])
        
        print(f"Registros finales a insertar: {len(new_records)}")
        
        # 7. INSERTAR NUEVOS REGISTROS
        try:
            print(f"=== INSERTANDO {len(new_records)} REGISTROS ===")
            
            total_rows_processed = db_manager.bulk_insert(new_records, regulations_table_name)
            
            if total_rows_processed == 0:
                return 0, f"No records were actually inserted for entity {entity}"
            
            print(f"Registros insertados exitosamente: {total_rows_processed}")
            
        except Exception as insert_error:
            print(f"Error en inserción: {insert_error}")
            # Si es error de duplicados, algunos se escaparon
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                print("Error de duplicados detectado - algunos registros ya existían")
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            else:
                raise insert_error
        
        # 8. OBTENER IDS DE REGISTROS INSERTADOS - MÉTODO OPTIMIZADO
        print("=== OBTENIENDO IDS DE REGISTROS INSERTADOS ===")
        
        # Método simple y eficiente - obtener los últimos N IDs
        new_ids_query = f"""
            SELECT id FROM {regulations_table_name}
            WHERE entity = %s 
            ORDER BY id DESC
            LIMIT %s
        """
        
        new_ids_result = db_manager.execute_query(
            new_ids_query, 
            (entity, total_rows_processed)
        )
        new_ids = [row[0] for row in new_ids_result]
        
        print(f"IDs obtenidos: {len(new_ids)}")
        
        # 9. INSERTAR COMPONENTES DE REGULACIÓN
        inserted_count_comp = 0
        component_message = ""
        
        if new_ids:
            try:
                inserted_count_comp, component_message = insert_regulations_component(db_manager, new_ids)
                print(f"Componentes: {component_message}")
            except Exception as comp_error:
                print(f"Error insertando componentes: {comp_error}")
                component_message = f"Error inserting components: {str(comp_error)}"
        
        # 10. MENSAJE FINAL CON ESTADÍSTICAS DETALLADAS
        total_duplicates = duplicates_found + internal_duplicates
        stats = (
            f"Processed: {len(entity_df)} | "
            f"Existing: {len(db_df)} | "
            f"Duplicates skipped: {total_duplicates} | "
            f"New inserted: {total_rows_processed}"
        )
        
        message = f"Entity {entity}: {stats}. {component_message}"
        print(f"=== RESULTADO FINAL ===")
        print(message)
        print("=" * 50)
        
        return total_rows_processed, message
        
    except Exception as e:
        if hasattr(db_manager, 'connection') and db_manager.connection:
            db_manager.connection.rollback()
        error_msg = f"Error processing entity {entity}: {str(e)}"
        print(f"ERROR CRÍTICO: {error_msg}")
        import traceback
        print(traceback.format_exc())
        return 0, error_msg

def check_for_new_content(num_pages_to_check=3):
    """
    Verifica si hay contenido nuevo en las primeras páginas.
    Retorna True si se detecta nuevo contenido, False en caso contrario.
    """
    print(f"Verificando contenido nuevo en las primeras {num_pages_to_check} páginas...")
    
    try:
        # Conectar a la base de datos para obtener la fecha más reciente
        db_manager = DatabaseManager()
        if not db_manager.connect():
            print("Error conectando a la base de datos para verificación")
            return True  # En caso de error, proceder con el scraping
        
        # Obtener la fecha de creación más reciente en la base de datos
        query = "SELECT MAX(created_at) FROM dapper_regulations_regulations WHERE entity = %s"
        result = db_manager.execute_query(query, (ENTITY_VALUE,))
        
        latest_db_date = None
        if result and result[0][0]:
            latest_db_date = result[0][0]
            
            # Normalizar fecha de la base de datos
            if isinstance(latest_db_date, str):
                try:
                    latest_db_date = datetime.strptime(latest_db_date, '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        latest_db_date = datetime.strptime(latest_db_date.split()[0], '%Y-%m-%d')
                    except:
                        latest_db_date = None
            
            # Normalizar datetime (quitar timezone info)
            latest_db_date = normalize_datetime(latest_db_date)
        
        db_manager.close()
        
        print(f"Fecha más reciente en BD: {latest_db_date}")
        
        # Verificar las primeras páginas en busca de contenido más reciente
        for page_num in range(num_pages_to_check):
            try:
                page_data = scrape_page(page_num, verbose=False)
                
                for record in page_data:
                    created_at_val = record.get('created_at')
                    
                    if created_at_val and is_valid_created_at(created_at_val):
                        web_date = None
                        try:
                            web_date = datetime.strptime(created_at_val, '%Y-%m-%d %H:%M:%S')
                        except:
                            try:
                                web_date = datetime.strptime(created_at_val.split()[0], '%Y-%m-%d')
                            except:
                                continue
                        
                        # Normalizar fecha web (quitar timezone info)
                        web_date = normalize_datetime(web_date)
                        
                        # Si encontramos contenido más reciente que el de la base de datos
                        if not latest_db_date or web_date > latest_db_date:
                            print(f"Nuevo contenido detectado - Fecha web: {web_date}, Fecha BD: {latest_db_date}")
                            return True
                
            except Exception as e:
                print(f"Error verificando página {page_num}: {e}")
                continue
        
        print("No se detectó contenido nuevo")
        return False
        
    except Exception as e:
        print(f"Error en verificación de contenido nuevo: {e}")
        return True  # En caso de error, proceder con el scraping

def lambda_handler(event, context):
    """
    AWS Lambda handler function para el scraping de normativas ANI.
    Modificado para procesar las páginas más recientes (0-8) y detectar contenido nuevo.
    """
    try:
        # Obtener parámetros del evento
        num_pages_to_scrape = event.get('num_pages_to_scrape', 9) if event else 9
        force_scrape = event.get('force_scrape', False) if event else False
        
        print(f"Iniciando scraping de ANI - Páginas a procesar: {num_pages_to_scrape}")
        
        # Verificar si hay contenido nuevo (a menos que se fuerce el scraping)
        if not force_scrape:
            has_new_content = check_for_new_content(min(3, num_pages_to_scrape))
            if not has_new_content:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'No se detectó contenido nuevo. Scraping omitido.',
                        'records_scraped': 0,
                        'records_inserted': 0,
                        'content_check': 'no_new_content',
                        'success': True
                    })
                }
        
        # Procesar las páginas más recientes (0 a num_pages_to_scrape-1)
        start_page = 0
        end_page = num_pages_to_scrape - 1
        
        print(f"Procesando páginas más recientes desde {start_page} hasta {end_page}")
        
        # Proceso principal de scraping
        all_normas_data = []
        
        for page_num in range(start_page, end_page + 1):
            print(f"Procesando página {page_num}...")
            page_data = scrape_page(page_num)
            all_normas_data.extend(page_data)
            
            # Indicador de progreso cada 3 páginas
            if (page_num + 1) % 3 == 0:
                print(f"Procesadas {page_num + 1}/{num_pages_to_scrape} páginas. Encontrados {len(all_normas_data)} registros válidos.")
        
        if not all_normas_data:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No se encontraron datos válidos durante el scraping',
                    'records_scraped': 0,
                    'records_inserted': 0,
                    'pages_processed': f"{start_page}-{end_page}",
                    'success': True
                })
            }
        
        # Crear DataFrame
        df_normas = pd.DataFrame(all_normas_data)
        print(f"Total de registros extraídos: {len(df_normas)}")
        
        # Operaciones de base de datos
        db_manager = DatabaseManager()
        if not db_manager.connect():
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error de conexión a la base de datos',
                    'success': False
                })
            }
        
        try:
            # Insertar nuevos registros
            inserted_count, status_message = insert_new_records(db_manager, df_normas, ENTITY_VALUE)
            
            response = {
                'statusCode': 200,
                'body': json.dumps({
                    'message': status_message,
                    'records_scraped': len(df_normas),
                    'records_inserted': inserted_count,
                    'pages_processed': f"{start_page}-{end_page}",
                    'content_check': 'new_content_found' if not force_scrape else 'forced_scrape',
                    'success': True
                })
            }
            
            print(f"Operación completada: {status_message}")
            return response
            
        finally:
            db_manager.close()
        
    except Exception as e:
        error_message = f"Error en la ejecución de Lambda: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': error_message,
                'success': False
            })
        }

# Para pruebas locales
if __name__ == "__main__":
    # Evento de prueba
    test_event = {
        'num_pages_to_scrape': 3,
        'force_scrape': True
    }
    
    # Contexto de prueba (vacío para pruebas locales)
    test_context = {}
    
    # Ejecutar función
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))