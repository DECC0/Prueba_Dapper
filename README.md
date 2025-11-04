# Prueba Técnica – Scraping ANI con Airflow

Automatización end-to-end para capturar normatividad de la Agencia Nacional de Infraestructura (ANI), validar los datos y persistirlos en Postgres, orquestando el flujo con Apache Airflow.

## 1. Arquitectura y Flujo

```
Extracción (src/extraccion.py)
        ↓
Validación (src/validacion.py + configs/validation_rules.json)
        ↓
Escritura (src/escritura.py)
        ↓
Tablas Postgres (regulations / regulations_component)
```

- **Extracción**: replica la lógica original de la lambda, manteniendo cada criterio de exclusión y limpieza.
- **Validación**: aplica reglas configurables (tipo, regex, longitud y obligatoriedad) sobre cada campo.
- **Escritura**: conecta a Postgres con credenciales de ambiente, inserta en lote y evita duplicados según la clave `(title, created_at, external_link)`.
- **Orquestación**: DAG `ani_regulations_pipeline` encadena las tareas `extract → validate → write`. La detección previa de contenido nuevo permite saltar ejecuciones innecesarias.

## 2. Estructura del Repositorio

| Ruta | Descripción |
|------|-------------|
| `src/extraccion.py` | Scraping, limpieza y detección de novedades. |
| `src/validacion.py` | Aplicación de reglas (tipos, regex, campos obligatorios). |
| `src/validaciones.py` | Carga de reglas desde JSON (con fallback por defecto). |
| `src/escritura.py` | Conexión a Postgres, inserción idempotente, fetch de IDs. |
| `configs/validation_rules.json` | Reglas actuales por campo. |
| `configs/schema.sql` | DDL para crear tablas `regulations` y `regulations_component`. |
| `dags/ani_regulations_dag.py` | DAG con tres `PythonOperator`. |
| `lambda.py` | Wrapper para ejecutar el flujo completo fuera de Airflow (útil para pruebas manuales). |
| `Dockerfile` | Imagen base (Airflow 2.7.1 + dependencias). |
| `docker-compose.yml` | Stack con Postgres, scheduler y webserver. |
| `Makefile` | Atajos opcionales para limpiar/inicializar/levantar el stack. |

## 3. Prerrequisitos

- Docker y Docker Compose (v2).
- GNU Make (opcional).
- Acceso a Internet (el contenedor debe llegar a `www.ani.gov.co`).
- Puerto `8080` disponible en el host.


## 4. Puesta en Marcha

### 4.1 Preparar directorios (solo la primera vez)

```bash
mkdir -p logs dags plugins configs src
chmod 777 logs dags plugins
```

Si prefieres usar Make:

```bash
sudo make reset-airflow
```

### 4.2 Construir imagen y preparar Airflow

```bash
sudo docker compose build

# Inicializar metadatos
sudo docker compose run --rm webserver airflow db init

# Crear usuario administrador
sudo docker compose run --rm webserver \
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 4.3 Levantar servicios

```bash
sudo docker compose up -d
sudo docker compose ps  # Debes ver postgres, scheduler y webserver en estado "Up"
```

### 4.4 Crear tablas destino

```bash
cat configs/schema.sql | sudo docker compose exec -T postgres psql -U airflow -d airflow
```

El script crea la tabla principal, la relación de componentes y el índice único basado en `(title, created_at, COALESCE(external_link, ''))`.

## 5. Configuración y Variables

| Variable | Origen | Comentario |
|----------|--------|------------|
| `DB_HOST` | docker-compose.yml | Host del contenedor Postgres (`postgres`). |
| `DB_PORT` | docker-compose.yml | Puerto interno (`5432`). |
| `DB_NAME` | docker-compose.yml | Base de datos objetivo (`airflow`). |
| `DB_USER` | docker-compose.yml | Usuario (`airflow`). |
| `DB_PASSWORD` | docker-compose.yml | Password (`airflow`). |
| `REGULATIONS_TABLE` | opcional | Tabla destino (por defecto `regulations`). |
| `REGULATIONS_COMPONENT_TABLE` | opcional | Tabla puente (por defecto `regulations_component`). |
| `VALIDATION_RULES_PATH` | opcional | Ruta al archivo JSON de reglas (montado en `/opt/airflow/configs`). |

## 6. Ejecución del DAG

1. Accede a `http://localhost:8080` y entra con `admin/admin`.
2. Activa el DAG `ani_regulations_pipeline`.
3. Lanza una corrida manual. Puedes pasar parámetros en **Trigger DAG w/ Config**:

   ```json
   {
     "num_pages_to_scrape": 5,
     "force_scrape": false
   }
   ```

4. Monitorea los logs:
   - **extract**: páginas procesadas, filas válidas, detección de novedades.
   - **validate**: `rows_received`, `rows_discarded`, `invalid_cells`.
   - **write**: duplicados detectados, filas insertadas y componentes asociados.

Si no hay novedades y `force_scrape=false`, la tarea `extract` emitirá un `AirflowSkipException` y el DAG marcará `skipped` sin tocar la base.

## 7. Validación Configurable

- Las reglas se ubican en `configs/validation_rules.json`. Cada campo admite:
  - `type`: `string`, `integer`, `float`, `boolean`, `date`.
  - `regex`: patrón adicional.
  - `max_length`: longitud máxima (strings).
  - `required`: marcar en `true` para descartar filas si el campo queda vacío.
- Para añadir nuevas validaciones, ajusta el JSON o crea otro archivo y apunta `VALIDATION_RULES_PATH` a la nueva ruta.

## 8. Idempotencia y Verificación

- Se mantiene la clave compuesta `(title, created_at, external_link)` para no insertar duplicados.
- Tras las inserciones se recuperan los IDs y se pobla `regulations_component` con `components_id = 7`.
- Para validar manualmente:

```bash
sudo docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM regulations;"
sudo docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM regulations_component;"
```

Reejecuta el DAG; el conteo solo aumentará si aparecen nuevos documentos.

## 9. Ejecución Local (fuera de Airflow)

Configura las mismas variables de entorno y ejecuta:

```bash
python lambda.py
```

La función `lambda_handler` repite la secuencia y respeta las validaciones/reglas de idempotencia.

## 10. Resolución de Problemas Frecuentes

| Problema | Solución |
|----------|----------|
| `permission denied ... /var/run/docker.sock` | Ejecutar comandos con `sudo` o agregar el usuario al grupo `docker`. |
| Webserver no arranca, error `IsADirectoryError: '/opt/airflow/airflow.cfg'` | Asegurarse de no montar `./config/airflow.cfg` como directorio (la línea fue eliminada del compose). |
| Mensaje `You need to initialize the database` | Ejecutar `sudo docker compose run --rm webserver airflow db init`. |
| Advertencias `PYTHONPATH` | Definir `PYTHONPATH=` en `.env` si se desea silenciar (no afecta la ejecución). |
| No se detecta contenido nuevo | Forzar ejecución con `{"force_scrape": true}` o esperar a nuevas publicaciones en el sitio. |

## 11. Limpieza

Para detener los contenedores y borrar volúmenes:

```bash
sudo docker compose down --volumes
```