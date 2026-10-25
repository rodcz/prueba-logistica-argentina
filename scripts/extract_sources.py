#!/usr/bin/env python3
"""
scripts/extract_sources.py
==========================
Módulo de extracción e ingesta idempotente (Truncate & Load) a la capa RAW de PostgreSQL 18.

Fuentes Oficiales:
1. IGN Georef API (Argentina): Departamentos, categorías, IDs y coordenadas centroides.
2. INDEC / RENAPER: Censo Nacional y población identificada por departamento.
3. ENACOM: Serie histórica y actual de penetración de Internet de banda ancha fija por provincia.
"""

import os
import sys
import time
import io
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("extract_sources")

# Variables de conexión a base de datos
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "warehouse_db")

# URLs de Fuentes Oficiales
URL_IGN = "https://apis.datos.gob.ar/georef/api/v2.0/departamentos.csv"
URL_RENAPER = "https://datosabiertos.renaper.gob.ar/poblacion_identificada_departamento_agosto_2024.csv"
URL_ENACOM = "https://indicadores.enacom.gob.ar/Files/DatosAbiertos/internet_accesos_penetracion_provincias.csv"

def get_connection():
    """Crea y retorna una conexión psycopg2 a PostgreSQL con reintentos."""
    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                dbname=DB_NAME
            )
            return conn
        except Exception as e:
            logger.warning(f"Intento {attempt}/5 de conexión a BD fallido: {e}. Reintentando en 3s...")
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a PostgreSQL en warehouse_db tras 5 intentos.")

def download_csv(url: str, name: str, max_retries: int = 3, timeout: int = 45) -> pd.DataFrame:
    """Descarga un archivo CSV desde una URL pública con reintentos y validación de status HTTP."""
    logger.info(f"Iniciando descarga de {name} desde {url}...")
    headers = {"User-Agent": "Antigravity-Logistica-Argentina/1.0 (Data Engineering Pipeline)"}
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            content = response.content.decode('utf-8', errors='replace')
            df = pd.read_csv(io.StringIO(content), dtype=str)
            logger.info(f"✓ Descarga exitosa de {name}: {len(df):,} filas obtenidas.")
            return df
        except Exception as e:
            logger.warning(f"Intento {attempt}/{max_retries} fallido al descargar {name}: {e}")
            if attempt < max_retries:
                time.sleep(3 * attempt)
            else:
                raise RuntimeError(f"Error crítico: No se pudo descargar {name} tras {max_retries} intentos: {e}")

def bulk_insert(conn, table_name: str, columns: list, values: list, page_size: int = 5000):
    """Inserta registros en lotes mediante psycopg2.extras.execute_values de forma eficiente."""
    col_str = ", ".join(columns)
    query = f"INSERT INTO raw.{table_name} ({col_str}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, query, values, page_size=page_size)
    conn.commit()

def ingest_ign(conn):
    """Descarga y carga la fuente IGN Georef a raw.ign_departamentos."""
    df = download_csv(URL_IGN, "IGN Georef Departamentos")
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Preparación de datos
    df['centroide_lat'] = pd.to_numeric(df['centroide_lat'], errors='coerce')
    df['centroide_lon'] = pd.to_numeric(df['centroide_lon'], errors='coerce')
    df['provincia_interseccion'] = pd.to_numeric(df['provincia_interseccion'], errors='coerce').fillna(0.0)
    
    cols = ['categoria', 'centroide_lat', 'centroide_lon', 'fuente', 'id', 
            'nombre', 'nombre_completo', 'provincia_id', 'provincia_interseccion', 'provincia_nombre']
    df = df[cols].where(pd.notnull(df), None)
    values = [tuple(x) for x in df.to_numpy()]
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.ign_departamentos (
                categoria VARCHAR(100),
                centroide_lat DOUBLE PRECISION,
                centroide_lon DOUBLE PRECISION,
                fuente VARCHAR(255),
                id VARCHAR(10),
                nombre VARCHAR(255),
                nombre_completo VARCHAR(255),
                provincia_id VARCHAR(10),
                provincia_interseccion DOUBLE PRECISION,
                provincia_nombre VARCHAR(255)
            );
            TRUNCATE TABLE raw.ign_departamentos;
        """)
        conn.commit()
    
    logger.info(f"Cargando {len(values):,} registros en raw.ign_departamentos (Truncate & Load)...")
    bulk_insert(conn, "ign_departamentos", cols, values)
    logger.info("✓ Carga completada en raw.ign_departamentos.")

def ingest_renaper(conn):
    """Descarga y carga la fuente INDEC/RENAPER a raw.indec_poblacion_raw y raw.indec_poblacion_departamentos."""
    df = download_csv(URL_RENAPER, "RENAPER / Censo 2022 Población")
    df.columns = [c.strip().lower() for c in df.columns]
    df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce').fillna(0).astype('int64')
    
    cols_raw = ['provincia_id', 'nombre_provincia', 'departamento_id', 'nombre_departamento', 
                'sexo', 'edad_quinquenal', 'nacionalidad', 'pais_nacimiento', 'cantidad']
    df_raw = df[cols_raw].where(pd.notnull(df), None)
    values_raw = [tuple(x) for x in df_raw.to_numpy()]
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.indec_poblacion_raw (
                provincia_id VARCHAR(10),
                nombre_provincia VARCHAR(255),
                departamento_id VARCHAR(10),
                nombre_departamento VARCHAR(255),
                sexo VARCHAR(50),
                edad_quinquenal VARCHAR(50),
                nacionalidad VARCHAR(50),
                pais_nacimiento VARCHAR(100),
                cantidad BIGINT
            );
            TRUNCATE TABLE raw.indec_poblacion_raw;
        """)
        conn.commit()
    
    logger.info(f"Cargando {len(values_raw):,} registros en raw.indec_poblacion_raw (Truncate & Load)...")
    bulk_insert(conn, "indec_poblacion_raw", cols_raw, values_raw)
    logger.info("✓ Carga completada en raw.indec_poblacion_raw.")

    # Agregación consolidada por departamento para modelado dbt y auditoría rápida
    df_agg = df.groupby(
        ['departamento_id', 'nombre_departamento', 'provincia_id', 'nombre_provincia'],
        as_index=False
    )['cantidad'].sum().rename(columns={'cantidad': 'poblacion_total'})
    
    cols_agg = ['departamento_id', 'nombre_departamento', 'provincia_id', 'nombre_provincia', 'poblacion_total']
    values_agg = [tuple(x) for x in df_agg[cols_agg].to_numpy()]
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.indec_poblacion_departamentos (
                departamento_id VARCHAR(10),
                nombre_departamento VARCHAR(255),
                provincia_id VARCHAR(10),
                nombre_provincia VARCHAR(255),
                poblacion_total BIGINT
            );
            TRUNCATE TABLE raw.indec_poblacion_departamentos;
        """)
        conn.commit()
        
    logger.info(f"Cargando {len(values_agg):,} departamentos consolidados en raw.indec_poblacion_departamentos...")
    bulk_insert(conn, "indec_poblacion_departamentos", cols_agg, values_agg)
    logger.info("✓ Carga completada en raw.indec_poblacion_departamentos.")

def ingest_enacom(conn):
    """Descarga y carga la fuente ENACOM a raw.enacom_conectividad."""
    df = download_csv(URL_ENACOM, "ENACOM Conectividad a Internet")
    df.columns = [c.strip().lower() for c in df.columns]
    
    for col in ['accesos_cada_100_hogares', 'accesos_cada_100_habitantes']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    df['anio'] = pd.to_numeric(df['anio'], errors='coerce').fillna(0).astype(int)
    df['trimestre'] = pd.to_numeric(df['trimestre'], errors='coerce').fillna(0).astype(int)
    
    cols = ['anio', 'trimestre', 'provincia', 'accesos_cada_100_hogares', 'accesos_cada_100_habitantes']
    df = df[cols].where(pd.notnull(df), None)
    values = [tuple(x) for x in df.to_numpy()]
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.enacom_conectividad (
                anio INT,
                trimestre INT,
                provincia VARCHAR(255),
                accesos_cada_100_hogares NUMERIC(10,2),
                accesos_cada_100_habitantes NUMERIC(10,2)
            );
            TRUNCATE TABLE raw.enacom_conectividad;
        """)
        conn.commit()
        
    logger.info(f"Cargando {len(values):,} registros en raw.enacom_conectividad (Truncate & Load)...")
    bulk_insert(conn, "enacom_conectividad", cols, values)
    logger.info("✓ Carga completada en raw.enacom_conectividad.")

def main():
    start_total = time.time()
    logger.info("==================================================================")
    logger.info("INICIO DE INGESTA DE FUENTES PÚBLICAS A CAPA RAW (PostgreSQL 18)")
    logger.info("==================================================================")
    
    conn = get_connection()
    try:
        ingest_ign(conn)
        ingest_renaper(conn)
        ingest_enacom(conn)
        
        # Auditoría de resultados
        logger.info("------------------------------------------------------------------")
        logger.info("AUDITORÍA DE INTEGRIDAD EN POSTGRESQL:")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.ign_departamentos;")
            c_ign = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM raw.indec_poblacion_raw;")
            c_pob = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*), SUM(poblacion_total) FROM raw.indec_poblacion_departamentos;")
            c_dep, s_pob = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM raw.enacom_conectividad;")
            c_ena = cur.fetchone()[0]
            
            logger.info(f"• raw.ign_departamentos:             {c_ign:,} departamentos georreferenciados.")
            logger.info(f"• raw.indec_poblacion_raw:          {c_pob:,} registros demográficos.")
            logger.info(f"• raw.indec_poblacion_departamentos: {c_dep:,} departamentos consolidados.")
            logger.info(f"• Población Total Nacional:          {s_pob:,} habitantes oficiales.")
            logger.info(f"• raw.enacom_conectividad:          {c_ena:,} registros de penetración.")
        
        elapsed_total = time.time() - start_total
        logger.info(f"Tiempo total de ejecución: {elapsed_total:.2f}s.")
        logger.info("==================================================================")
        logger.info("INGESTA A CAPA RAW COMPLETADA CON ÉXITO")
        logger.info("==================================================================")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Falla fatal en la ingesta: {e}", exc_info=True)
        sys.exit(1)
