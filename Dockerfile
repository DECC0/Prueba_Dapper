# Dockerfile
FROM apache/airflow:2.7.1-python3.9

# Copiamos el requirements al contenedor
COPY requirements.txt /requirements.txt

# Instalamos las dependencias como el usuario airflow
USER airflow
RUN pip install --user --no-cache-dir -r /requirements.txt

# Devolvemos al usuario airflow para la ejecución normal
USER airflow
