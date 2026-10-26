"""
DAG de Orquestación ELT: Análisis de Demanda Territorial y Logística en Argentina
Pipeline integral: Ingesta RAW -> dbt Seed -> dbt Run -> dbt Test -> Generación de Reporte HTML
Configuración: Ejecución manual (schedule=None) según requerimiento de la prueba técnica.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import psycopg2

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def check_postgres_connection():
    """Valida la conectividad con el almacén PostgreSQL antes de iniciar el pipeline."""
    host = os.environ.get('POSTGRES_HOST', 'postgres_warehouse')
    port = int(os.environ.get('POSTGRES_PORT', '5432'))
    db = os.environ.get('POSTGRES_DB', 'warehouse_db')
    user = os.environ.get('POSTGRES_USER', 'postgres')
    pwd = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
        connect_timeout=10
    )
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        ver = cur.fetchone()[0]
        print(f"Conexión exitosa a PostgreSQL: {ver}")
    conn.close()

with DAG(
    dag_id='dag_demanda_logistica',
    default_args=default_args,
    description='Pipeline ELT end-to-end de demanda territorial y logística (IGN, INDEC, ENACOM, dbt)',
    schedule=None,  # Disparo manual con botón Play según requerimiento técnico
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['logistica', 'argentina', 'elt', 'dbt', 'indec', 'ign', 'enacom', 'marts'],
) as dag:

    # 1. Verificación de conectividad
    t0_check_db = PythonOperator(
        task_id='verificar_conexion_warehouse',
        python_callable=check_postgres_connection,
    )

    # 2. Ingesta de fuentes RAW (IGN, INDEC, ENACOM)
    t1_ingesta_raw = BashOperator(
        task_id='ingesta_fuentes_raw',
        bash_command='python /opt/scripts/extract_sources.py',
    )

    # 3. dbt Seed (Zonificación macro-regional estratégica)
    t2_dbt_seed = BashOperator(
        task_id='dbt_seed_zonificacion',
        bash_command='dbt seed --project-dir /opt/dbt --profiles-dir /opt/dbt --full-refresh',
    )

    # 4. dbt Run (Staging, Intermediate, Marts)
    t3_dbt_run = BashOperator(
        task_id='dbt_run_transformacion',
        bash_command='dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt',
    )

    # 5. dbt Test (Suite de calidad: Unicidad, No nulos, Integridad referencial, Suma 100%)
    t4_dbt_test = BashOperator(
        task_id='dbt_test_calidad',
        bash_command='dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt',
    )

    # 6. Generación de Reporte Ejecutivo HTML con Mapa Interactivo Leaflet
    t5_generar_reporte = BashOperator(
        task_id='generar_reporte_html',
        bash_command='python /opt/scripts/generate_html_report.py',
    )

    # Flujo de dependencias lineal y atómico
    t0_check_db >> t1_ingesta_raw >> t2_dbt_seed >> t3_dbt_run >> t4_dbt_test >> t5_generar_reporte
