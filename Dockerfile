FROM apache/airflow:2.10.5-python3.10

USER root

# Instalación de utilidades del sistema
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       curl \
       git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalación de dbt-postgres y librerías analíticas
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
