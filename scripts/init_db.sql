-- ==============================================================================
-- INICIALIZACIÓN DE BASES DE DATOS Y ESQUEMAS: POSTGRESQL 18
-- ==============================================================================

-- 1. Base de datos para el Metastore de Apache Airflow
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- 2. Conexión al Data Warehouse
\connect warehouse_db;

-- 3. Esquemas analíticos de la arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 4. Concesión de privilegios al usuario principal
GRANT ALL ON SCHEMA raw TO postgres;
GRANT ALL ON SCHEMA analytics TO postgres;

COMMENT ON SCHEMA raw IS 'Capa Raw / Ingesta inicial idempotente desde fuentes abiertas (IGN, INDEC, ENACOM)';
COMMENT ON SCHEMA analytics IS 'Capa Analytics / Modelos dimensionales dbt (Staging, Curated y Marts)';
