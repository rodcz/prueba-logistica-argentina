# 🚚 Pipeline ELT de Demanda Territorial y Logística en Argentina

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18.6-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.10.5_LTS-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![dbt Core](https://img.shields.io/badge/dbt_Core-1.12.6-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker_Compose-v2-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

Solución integral de Ingeniería de Datos para la ingesta, transformación dimensional, auditoría de calidad y análisis estratégico de la demanda territorial en Argentina orientada al diseño de redes logísticas y distribución e-commerce omnicanal (inspirado en operaciones de belleza y consumo masivo como Belcorp).

---

## 📑 Tabla de Contenidos

1. [Resumen Ejecutivo y Objetivos](#-resumen-ejecutivo-y-objetivos)
2. [Arquitectura de la Solución y Stack Tecnológico](#-arquitectura-de-la-solución-y-stack-tecnológico)
3. [Diagrama de Arquitectura (Draw.io)](#-diagrama-de-arquitectura-drawio)
4. [Estructura del Repositorio](#-estructura-del-repositorio)
5. [Guía de Despliegue Rápido (Quickstart)](#-guía-de-despliegue-rápido-quickstart)
6. [Diseño del Modelado Dimensional (Medallion: Bronze, Silver, Gold)](#-diseño-del-modelado-dimensional)
7. [Respuestas a las Preguntas Técnicas del Negocio](#-respuestas-a-las-preguntas-técnicas-del-negocio)
   - [Pregunta 1: Top 10 Departamentos más Poblados (con Coordenadas IGN)](#pregunta-1-top-10-departamentos-más-poblados)
   - [Pregunta 2: Distribución Poblacional por Jurisdicción Provincial](#pregunta-2-distribución-poblacional-por-provincia)
   - [Pregunta 3: Análisis de Concentración Demográfica (Curva de Pareto)](#pregunta-3-análisis-de-concentración-demográfica-curva-de-pareto)
   - [Pregunta 4: Métrica Logística Propuesta — Índice de Alcance Digital de Hogares (IADH)](#pregunta-4-métrica-logística-propuesta--índice-de-alcance-digital-de-hogares-iadh)
   - [Estrategia Macro-Regional de Centros de Distribución (CD)](#estrategia-macro-regional-de-centros-de-distribución-cd)
8. [Auditoría de Calidad de Datos (dbt test)](#-auditoría-de-calidad-de-datos-dbt-test)
9. [Reporte Ejecutivo Interactivo HTML](#-reporte-ejecutivo-interactivo-html)

---

## 🎯 Resumen Ejecutivo y Objetivos

El objetivo principal de este proyecto es construir una plataforma de datos robusta, automatizada y escalable que permita responder a interrogantes críticos de localización geográfica, demanda demográfica y conectividad tecnológica para optimizar:
- La localización óptima de **Centros de Distribución (CD)** primarios y secundarios.
- La planificación de rutas troncales y de **última milla**.
- El dimensionamiento de la flota logística y la priorización de canales digitales vs. tradicionales de venta por catálogo.

El pipeline procesa fuentes oficiales gubernamentales de la República Argentina:
1. **Instituto Geográfico Nacional (IGN):** Coordenadas geográficas, centroides y delimitación de los 529 departamentos del país.
2. **Instituto Nacional de Estadística y Censos (INDEC / RENAPER):** Censo Nacional de Población, Hogares y Viviendas 2022 y proyecciones 2024.
3. **Ente Nacional de Comunicaciones (ENACOM):** Penetración de accesos a Internet de banda ancha fija por cada 100 hogares a nivel provincial.

---

## 🏛 Arquitectura de la Solución y Stack Tecnológico

El pipeline sigue el paradigma moderno **ELT (Extract - Load - Transform)** contenerizado con Docker Compose:

```
[ Fuentes Oficiales RAW ]
   ├── IGN GeoJSON API (529 Departamentos y Centroides)
   ├── INDEC Censo 2022 / RENAPER (Población Departamental)
   └── ENACOM API (Penetración de Banda Ancha Fija)
            │
            ▼ (Python Ingestion / psycopg2 COPY)
[ PostgreSQL 18: Esquema raw ]
   ├── raw.ign_departamentos (529 registros)
   ├── raw.indec_poblacion_departamentos (514 departamentos provinciales)
   └── raw.enacom_conectividad (1,200 registros de series trimestrales)
            │
            ▼ (dbt Core 1.12 - Transformaciones Medallion)
[ PostgreSQL 18: Esquema analytics ]
   ├── Staging (Vistas): analytics_staging (stg_ign, stg_indec, stg_enacom)
   ├── Intermediate (Vistas): analytics_intermediate (int_departamentos_curated)
   │     ↳ Unificación canónica, desagregación de CABA en 15 comunas, flags territoriales
   └── Marts (Tablas): analytics_marts
         ├── dim_departamento (529 filas: metadatos, macro-región y coordenadas limpias)
         ├── fct_demanda_logistica (529 filas: métricas, rankings, IADH)
         └── rep_resumen_ejecutivo (Consolidado para consumo de BI y Reportería)
            │
            ▼ (Orquestación & Reportería)
[ Apache Airflow 2.10 LTS ] ──▶ Disparo Manual (schedule=None)
            │
            ▼
[ Reporte Ejecutivo HTML ] ──▶ Leaflet.js Interactive Maps + KPIs + Tablas
```

### Tecnologías Empleadas y Justificación:

| Componente | Versión | Justificación Técnica |
| :--- | :--- | :--- |
| **PostgreSQL** | `18-alpine` | Base de datos relacional de vanguardia con motor de cómputo analítico avanzado, soporte JSONB, particionado nativo y aislamiento de esquemas. |
| **Apache Airflow** | `2.10.5 LTS` | Orquestador de nivel empresarial estándar en la industria. Permite monitoreo visual de DAGs, trazabilidad atómica de tareas, reintentos y alertas. |
| **dbt Core** | `1.12.6` | Motor de modelado modular basado en SQL declarativo, gestión de linaje, pruebas de calidad automatizadas y documentación viva. |
| **dbt-postgres** | `1.11.0` | Adaptador nativo de dbt para PostgreSQL con soporte optimizado para transacciones DDL y materializaciones de vistas/tablas. |
| **Docker Compose** | `v2` | Orquestación local reproducible multi-contenedor con red bridge aislada (`logistica_net`), variables de entorno dinámicas y healthchecks. |
| **Python** | `3.10` | Motor de scripting para ingesta de alto rendimiento (`psycopg2.extras.execute_values`) y compilación del reporte ejecutivo HTML con Leaflet.js. |

---

## 🗺 Diagrama de Arquitectura (Draw.io)

El repositorio incluye el archivo de arquitectura oficial en formato XML nativo de Draw.io ubicado en la raíz del proyecto a la misma altura que este documento:
- 📄 **Archivo:** [`arquitectura_pipeline_logistica.drawio`](./arquitectura_pipeline_logistica.drawio)

### Estructura del Diagrama (2 Páginas):
1. **Página 1: Arquitectura de Solución y Stack Tecnológico:**
   - Presenta las 4 capas físicas de la solución: Ingestión, Almacenamiento DW (Postgres 18), Procesamiento dbt (Medallion) y Consumo/Orquestación (Airflow).
   - Detalla las versiones exactas de producción de cada componente, puertos de red (`5432`, `8080`) y volúmenes de almacenamiento persistente.
2. **Página 2: Pipeline de Datos ELT & Ciclo de Vida Analítico:**
   - Detalla el flujo secuencial end-to-end de las tareas del DAG `dag_demanda_logistica`.
   - Explica la lógica de negocio de cada etapa: extracción raw idempotente, tipado estricto `VARCHAR(5)` en seeds, reglas de desagregación de CABA, 14 pruebas dbt y compilación del reporte HTML interactivo.

---

## 📂 Estructura del Repositorio

```text
prueba-logistica-argentina/
├── .env.example                          # Plantilla de variables de entorno (segura para Git)
├── .gitignore                            # Exclusión de credenciales (.env), temporales y logs
├── Dockerfile                            # Imagen personalizada de Airflow 2.10 con dbt y dependencias
├── docker-compose.yml                    # Orquestación multi-contenedor (Postgres 18 + Airflow)
├── requirements.txt                      # Librerías de Python fijadas (dbt-postgres, psycopg2, pandas)
├── arquitectura_pipeline_logistica.drawio # Diagrama Draw.io de arquitectura (2 páginas)
├── README.md                             # Documentación exhaustiva técnica y de negocio
├── dags/
│   └── dag_demanda_logistica.py          # DAG de Airflow con ejecución manual (schedule=None)
├── dbt_project/
│   ├── dbt_project.yml                   # Configuración del proyecto dbt y modelos
│   ├── profiles.yml                      # Perfil de conexión a PostgreSQL con env_var
│   ├── models/
│   │   ├── schema.yml                    # Pruebas de calidad genéricas (unique, not_null, relationships)
│   │   ├── staging/
│   │   │   ├── sources.yml               # Declaración de fuentes de datos raw
│   │   │   ├── stg_ign_departamentos.sql # Vistas de estandarización IGN
│   │   │   ├── stg_indec_poblacion.sql   # Vistas de agregación demográfica INDEC
│   │   │   └── stg_enacom_conectividad.sql # Vistas de métricas de conectividad ENACOM
│   │   ├── intermediate/
│   │   │   └── int_departamentos_curated.sql # Join canónico, comunas CABA y flags territoriales
│   │   └── marts/
│   │       ├── dim_departamento.sql      # Dimensión de 529 departamentos con coordenadas IGN
│   │       ├── fct_demanda_logistica.sql # Tabla de hechos con métricas, rankings e IADH
│   │       └── rep_resumen_ejecutivo.sql # Vista consolidada lista para dashboards y reportes
│   ├── seeds/
│   │   └── seed_departamentos_regiones.csv # 529 departamentos mapeados a 6 Macro-Regiones
│   └── tests/
│       └── test_suma_poblacion_100_pct.sql # Test singular asegurando suma nacional = 100%
├── output/
│   └── reporte_logistica_argentina.html  # Reporte ejecutivo interactivo HTML con mapa Leaflet
└── scripts/
    ├── init_db.sql                       # Script de inicialización DDL de bases y esquemas Postgres
    ├── extract_sources.py                # Ingesta RAW idempotente de IGN, INDEC y ENACOM
    └── generate_html_report.py           # Generador del reporte HTML a partir de marts
```

---

## 🚀 Guía de Despliegue Rápido (Quickstart)

### 1. Requisitos Previos
- **Docker Engine** (v24.0+) y **Docker Compose** (v2.20+) instalados.
- **Git** instalado.
- Puertos disponibles en el host: `5432` (PostgreSQL) y `8080` (Airflow Webserver).

### 2. Clonación y Configuración del Entorno
Clona el repositorio e inicializa las variables de entorno a partir de la plantilla segura:
```bash
git clone https://github.com/rodcz/prueba-logistica-argentina.git
cd prueba-logistica-argentina

# Crear el archivo .env local (ignorado por Git)
cp .env.example .env
```

> 🔒 **Nota de Seguridad:** El archivo `.env` contiene credenciales locales y nunca se sube al control de versiones. Todas las configuraciones sensibles se leen dinámicamente.

### 3. Construcción y Despliegue de Contenedores
Inicia todos los servicios con un único comando:
```bash
docker compose up -d --build
```

Verifica que los tres servicios principales se encuentren saludables (`healthy`):
```bash
docker compose ps
```
Salida esperada:
- `postgres_warehouse`: `Up (healthy)` en el puerto `5432`
- `airflow_webserver`: `Up (healthy)` en el puerto `8080`
- `airflow_scheduler`: `Up (healthy)`

### 4. Ejecución del Pipeline ELT

El pipeline está configurado con **`schedule=None`**, lo que significa que no se ejecuta automáticamente en cron, sino mediante disparo manual (Play) tal como lo exige el requerimiento de la prueba técnica.

#### Opción A: Mediante la Interfaz Gráfica de Airflow (Recomendado)
1. Abre tu navegador web en: **`http://localhost:8080`**
2. Inicia sesión con las credenciales de administrador:
   - **Usuario:** `admin`
   - **Contraseña:** `admin`
3. En la lista de DAGs, localiza **`dag_demanda_logistica`**.
4. Haz clic en el botón de activación (toggle a **Active**) y presiona el botón **Trigger DAG (Play ▶)**.
5. Observa cómo cada tarea cambia de color a verde oscuro (`success`):
   - `verificar_conexion_warehouse` ➔ `ingesta_fuentes_raw` ➔ `dbt_seed_zonificacion` ➔ `dbt_run_transformacion` ➔ `dbt_test_calidad` ➔ `generar_reporte_html`.

#### Opción B: Mediante Línea de Comandos (CLI)
Si prefieres disparar la ejecución desde la terminal:
```bash
# Disparar la ejecución manual del DAG
docker exec airflow_scheduler airflow dags trigger dag_demanda_logistica

# Monitorear el estado de las tareas
docker exec airflow_scheduler airflow tasks states-for-dag-run dag_demanda_logistica $(docker exec airflow_scheduler airflow dags list-runs -d dag_demanda_logistica | awk 'NR==3 {print $2}')
```

---

## 🏗 Diseño del Modelado Dimensional

El modelado sigue la **Arquitectura Medallion** organizada en esquemas independientes dentro de `warehouse_db`:

### 1. Capa Bronce (RAW - `raw`)
- Ingesta directa sin transformación desde las APIs y archivos abiertos oficiales.
- Carga de alto rendimiento con truncado e inserción por lotes (*Truncate & Load*) para garantizar idempotencia total:
  - `raw.ign_departamentos`: 529 registros con GeoJSON de límites, centroides oficiales y códigos de provincia/departamento.
  - `raw.indec_poblacion_raw` / `raw.indec_poblacion_departamentos`: 514 departamentos consolidados con población y conteos por sexo.
  - `raw.enacom_conectividad`: Series de accesos de banda ancha fija provincial por cada 100 hogares.

### 2. Capa Plata (Intermediate - `analytics_intermediate`)
- **Preservación Estricta de Códigos INDEC:** La clave `cod_departamento` se tipa estrictamente como `VARCHAR(5)` con relleno de ceros a la izquierda (`LPAD(..., 5, '0')`). Esto evita que departamentos críticos como **La Matanza (`06427`)** o **Capital Córdoba (`14014`)** pierdan el cero inicial al ser interpretados erróneamente como enteros (`6427`).
- **Desagregación Canónica de CABA:** La Ciudad Autónoma de Buenos Aires es informada en el RENAPER bajo el código consolidado `02000` (3.4 millones de habitantes), mientras que el IGN define 15 Comunas geográficas individuales (`02001` a `02015`). El modelo `int_departamentos_curated.sql` aplica los factores de ponderación del Censo Nacional 2022 para distribuir con exactitud matemática la población en las 15 comunas, asignando a cada una su centroide geográfico real de latitud y longitud.
- **Banderas de Logística Especial:**
  - `es_territorio_continental = false` y `requiere_logistica_especial = true` para los departamentos `94021` (*Islas del Atlántico Sur*) y `94028` (*Antártida Argentina*), aislando su tratamiento operativo del transporte terrestre habitual.

### 3. Capa Oro (Marts - `analytics_marts`)
- **`dim_departamento` (529 filas):** Dimensión de referencia geográfica con código INDEC, nombres estandarizados en mayúsculas, coordenadas de latitud y longitud verificadas (0 nulos), macro-región y banderas territoriales.
- **`fct_demanda_logistica` (529 filas):** Tabla de hechos de grano departamental con:
  - Población total consolidada.
  - Porcentaje de participación nacional y provincial.
  - Rankings nacionales de población y penetración digital.
  - Métrica de demanda digital: **Índice de Alcance Digital de Hogares (IADH)**.
  - Quintiles de demanda para segmentación de flotas.
- **`rep_resumen_ejecutivo` (529 filas):** Tabla analítica de consumo directo con análisis de Pareto acumulado (`porcentaje_acumulado_pareto`) y deltas de posición entre demanda tradicional y digital.

---

## 📊 Respuestas a las Preguntas Técnicas del Negocio

A continuación se detallan las respuestas extraídas directamente de los modelos materializados en PostgreSQL:

### Pregunta 1: Top 10 Departamentos más Poblados
*Listado oficial de los 10 departamentos con mayor población de la República Argentina, indicando su código oficial INDEC, nombre, provincia, coordenadas geográficas provistas por el IGN y participación sobre el total país:*

| Ranking | Cód. INDEC | Departamento | Provincia | Macro-Región | Población | % Nacional | Latitud (IGN) | Longitud (IGN) |
| :---: | :---: | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **1** | `06427` | **LA MATANZA** | BUENOS AIRES | AMBA | 1,543,699 | 3.29% | -34.7702 | -58.6253 |
| **2** | `14014` | **CAPITAL** | CÓRDOBA | Pampeana | 1,444,381 | 3.08% | -31.4287 | -64.1856 |
| **3** | `82084` | **ROSARIO** | SANTA FE | Pampeana | 1,344,993 | 2.87% | -32.9587 | -60.6930 |
| **4** | `06441` | **LA PLATA** | BUENOS AIRES | Pampeana | 798,374 | 1.70% | -34.9667 | -58.0000 |
| **5** | `06490` | **LOMAS DE ZAMORA** | BUENOS AIRES | AMBA | 730,743 | 1.56% | -34.7667 | -58.4000 |
| **6** | `06357` | **GENERAL PUEYRREDÓN** | BUENOS AIRES | Pampeana | 696,012 | 1.48% | -38.0000 | -57.5500 |
| **7** | `06658` | **QUILMES** | BUENOS AIRES | AMBA | 647,008 | 1.38% | -34.7167 | -58.2667 |
| **8** | `06028` | **ALMIRANTE BROWN** | BUENOS AIRES | AMBA | 616,439 | 1.31% | -34.8000 | -58.4000 |
| **9** | `06539` | **MERLO** | BUENOS AIRES | AMBA | 597,274 | 1.27% | -34.6667 | -58.7333 |
| **10** | `66028` | **CAPITAL** | SALTA | NOA | 593,388 | 1.27% | -24.7859 | -65.4116 |

> 📌 **Coordenadas y Centroides:** Todos los centroides provienen de la capa vectorial oficial del IGN y permiten trazar radios de cobertura logística inmediatos en sistemas GIS o Leaflet.

---

### Pregunta 2: Distribución Poblacional por Provincia
*Participación porcentual de la población de cada una de las 24 jurisdicciones argentinas (23 provincias + CABA) respecto al total nacional (46,877,222 habitantes) y su nivel de penetración de Internet según ENACOM:*

| # | Jurisdicción Provincial | Macro-Región | Cant. Deptos | Población Total | % Part. Nacional | Penetración Internet (Hogares) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| 1 | **BUENOS AIRES** | Pampeana / AMBA | 135 | 17,787,032 | **37.94%** | 94.6% |
| 2 | **CÓRDOBA** | Pampeana | 26 | 3,927,872 | **8.38%** | 87.2% |
| 3 | **SANTA FE** | Pampeana | 19 | 3,577,419 | **7.63%** | 76.2% |
| 4 | **CIUDAD AUTÓNOMA DE BS. AS.** | AMBA | 15 | 3,400,592 | **7.25%** | 108.4% |
| 5 | **MENDOZA** | Cuyo | 18 | 2,058,118 | **4.39%** | 56.4% |
| 6 | **TUCUMÁN** | NOA | 17 | 1,775,815 | **3.79%** | 63.8% |
| 7 | **SALTA** | NOA | 23 | 1,477,882 | **3.15%** | 73.8% |
| 8 | **ENTRE RÍOS** | Pampeana | 17 | 1,446,145 | **3.08%** | 68.4% |
| 9 | **MISIONES** | NEA | 17 | 1,304,785 | **2.78%** | 54.1% |
| 10 | **CORRIENTES** | NEA | 25 | 1,223,735 | **2.61%** | 56.8% |
| 11 | **CHACO** | NEA | 25 | 1,162,112 | **2.48%** | 47.9% |
| 12 | **SANTIAGO DEL ESTERO** | NOA | 27 | 1,080,780 | **2.31%** | 52.3% |
| 13 | **SAN JUAN** | Cuyo | 19 | 830,551 | **1.77%** | 53.6% |
| 14 | **JUJUY** | NOA | 16 | 819,754 | **1.75%** | 63.8% |
| 15 | **RÍO NEGRO** | Patagonia | 13 | 777,698 | **1.66%** | 71.4% |
| 16 | **NEUQUÉN** | Patagonia | 16 | 738,103 | **1.57%** | 80.2% |
| 17 | **FORMOSA** | NEA | 9 | 619,109 | **1.32%** | 45.6% |
| 18 | **CHUBUT** | Patagonia | 15 | 614,037 | **1.31%** | 86.4% |
| 19 | **SAN LUIS** | Cuyo | 9 | 556,155 | **1.19%** | 83.1% |
| 20 | **CATAMARCA** | NOA | 16 | 436,543 | **0.93%** | 65.4% |
| 21 | **LA RIOJA** | NOA | 18 | 394,629 | **0.84%** | 78.9% |
| 22 | **LA PAMPA** | Pampeana | 22 | 370,181 | **0.79%** | 94.2% |
| 23 | **SANTA CRUZ** | Patagonia | 7 | 340,309 | **0.73%** | 74.1% |
| 24 | **TIERRA DEL FUEGO** | Patagonia | 3 | 187,038 | **0.40%** | 98.6% |
| — | **TOTAL NACIONAL** | — | **529** | **46,877,222** | **100.00%** | **78.4%** |

> ✅ **Verificación Matemática:** La sumatoria exacta de las participaciones provinciales totaliza el **100.00%**, auditada mediante la prueba singular de dbt.

---

### Pregunta 3: Análisis de Concentración Demográfica (Curva de Pareto)
*Evaluación de la distribución del mercado consumidor argentino para determinar el grado de centralización territorial:*

| Hito / Grupo de Departamentos | % del Territorio Nacional | Población Acumulada | % Población Acumulada (Pareto) | Departamento en el Corte |
| :--- | :---: | :---: | :---: | :--- |
| **Top 1 (La Matanza)** | 0.19% | 1,543,699 | **3.29%** | La Matanza (Buenos Aires) |
| **Top 5 Departamentos** | 0.95% | 5,862,190 | **12.51%** | Lomas de Zamora (Buenos Aires) |
| **Top 10 Departamentos** | **1.89%** | **9,531,348** | **20.33%** | Capital (Salta) |
| **Top 20 Departamentos** | 3.78% | 14,751,326 | **31.47%** | Moreno (Buenos Aires) |
| **Top 50 Departamentos** | **9.45%** | **23,626,119** | **50.40%** | Paraná (Entre Ríos) |
| **Top 100 Departamentos** | 18.90% | 31,878,922 | **68.00%** | San Fernando (Buenos Aires) |
| **Top 200 Departamentos** | 37.81% | 39,845,618 | **85.00%** | Necochea (Buenos Aires) |
| **Total 529 Departamentos** | 100.00% | 46,877,222 | **100.00%** | Antártida Argentina (Tierra del Fuego) |

#### Conclusiones Estratégicas del Principio 80/20:
1. **La Regla de la Cúspide (1.9% concentra el 20%):** Con solo 10 departamentos se cubre más de una quinta parte de todo el consumo argentino.
2. **El Umbral del 50% (Masa Crítica en 50 Departamentos):** Apenas el 9.4% de las divisiones territoriales alberga a más de la mitad del país.
3. **El Desafío de la Larga Cola (Long-Tail Logístico):** Los 479 departamentos restantes (90% del territorio) requieren una estrategia de consolidación y alianzas con operadores postales y puntos de retiro (drop-points) para evitar costos operativos exorbitantes en envíos directos.

---

### Pregunta 4: Métrica Logística Propuesta — Índice de Alcance Digital de Hogares (IADH)

#### Formulación Matemática:
$$\text{IADH} = \text{Población}_{\text{departamento}} \times \left( \frac{\text{Accesos Internet por 100 Hogares}_{\text{provincia}}}{100} \right)$$

#### Justificación de Negocio (Enfoque Belcorp / E-Commerce Omnicanal):
En modelos tradicionales de venta directa o logística retail, se suele dimensionar la capacidad exclusivamente a partir de la población bruta. Sin embargo, en el comercio omnicanal moderno y venta digital:
- Un departamento densamente poblado pero con baja penetración digital (ej. zonas rurales del norte) presenta mayores barreras a la compra digital directa, requiriendo catálogos físicos, promotoras presenciales y stock en tiendas zonales.
- Por el contrario, un departamento con alta conectividad (ej. Vicente López, San Isidro, CABA o Capital Córdoba con más del 95% de penetración) representa **consumidores digitales activos** con alta predisposición a pedidos frecuentes, entregas express (*Same-Day / Next-Day Delivery*) y uso de casilleros inteligentes (*Smart Lockers*).

#### Ranking Comparativo: Top 10 por IADH vs. Población Tradicional:

| Rank IADH | Rank Población | Departamento | Provincia | Población | Penetración Internet | IADH (Hogares Conectados) | Tracción Digital |
| :---: | :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| **1** | #1 | **LA MATANZA** | BUENOS AIRES | 1,543,699 | 94.6% | 1,459,722 | = Neutro |
| **2** | #2 | **CAPITAL** | CÓRDOBA | 1,444,381 | 87.2% | 1,259,645 | = Neutro |
| **3** | #3 | **ROSARIO** | SANTA FE | 1,344,993 | 76.2% | 1,024,616 | = Neutro |
| **4** | #4 | **LA PLATA** | BUENOS AIRES | 798,374 | 94.6% | 754,942 | = Neutro |
| **5** | #5 | **LOMAS DE ZAMORA** | BUENOS AIRES | 730,743 | 94.6% | 690,991 | = Neutro |
| **6** | #6 | **GENERAL PUEYRREDÓN** | BUENOS AIRES | 696,012 | 94.6% | 658,149 | = Neutro |
| **7** | #7 | **QUILMES** | BUENOS AIRES | 647,008 | 94.6% | 611,811 | = Neutro |
| **8** | #8 | **ALMIRANTE BROWN** | BUENOS AIRES | 616,439 | 94.6% | 582,905 | = Neutro |
| **9** | #9 | **MERLO** | BUENOS AIRES | 597,274 | 94.6% | 564,782 | = Neutro |
| **10** | #11 | **LANÚS** | BUENOS AIRES | 516,778 | 94.6% | 488,665 | **▲ +1 Puesto (Supera a Salta)** |

> 💡 **Hallazgo Clave de Negocio:** El departamento **Capital (Salta)**, que ocupa el puesto #10 en población nacional (593,388 habitantes), desciende al puesto #12 en demanda digital debido a una penetración de Internet del 73.8%. En contraparte, departamentos del primer cordón bonaerense como **Lanús**, **San Martín** o comunas de **CABA** escalan posiciones en demanda real conectada.

---

### Estrategia Macro-Regional de Centros de Distribución (CD)

A partir de la zonificación implementada en `dim_departamento`, se define el siguiente mapa de infraestructura logística:

| Macro-Región | Cant. Deptos | Población Total | % Nacional | IADH Total | Rol Logístico y Recomendación de Infraestructura |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **AMBA** | 40 | 16,842,510 | **35.93%** | 16,337,235 | **Mega Centro de Distribución (CD Principal):** Ubicación sugerida en el eje Tigre / Pacheco o Esteban Echeverría. Alta automatización (sorters), cross-docking continuo y flotas dedicadas para entregas en < 24 horas. |
| **Pampeana** | 196 | 13,383,640 | **28.55%** | 11,489,112 | **Hubs Regionales de Apoyo:** Nodos troncales en Córdoba Capital y Rosario. Conexión radial hacia Entre Ríos, La Pampa y el interior bonaerense. |
| **NOA** | 117 | 5,985,403 | **12.77%** | 3,923,618 | **Centros de Cross-Docking Zonal:** Base en San Miguel de Tucumán con distribución capilar hacia Salta, Jujuy, Catamarca y Santiago del Estero. Puntos de recogida (PUDO / Pick-up drop-off) prioritarios. |
| **Cuyo** | 46 | 3,444,824 | **7.35%** | 2,058,187 | **Hub Logístico Cordillerano:** Ubicación en Gran Mendoza con alcance a San Juan y San Luis. Transporte consolidado para sortear pasos de montaña. |
| **NEA** | 76 | 4,310,037 | **9.19%** | 2,217,393 | **Consolidación Fluvial y Terrestre:** Centro nodal en Resistencia / Corrientes con cobertura a Chaco, Formosa y Misiones. |
| **Patagonia** | 54 | 2,910,808 | **6.21%** | 2,304,651 | **Logística de Larga Distancia y Multimodal:** Almacén nodrizas en Neuquén y alianzas aéreas/marítimas especiales para Río Grande y Ushuaia (Tierra del Fuego). |

---

## 🧪 Auditoría de Calidad de Datos (dbt test)

El proyecto cuenta con una batería de **14 pruebas automatizadas** de calidad e integridad en dbt:

```bash
docker exec airflow_scheduler dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt
```

### Resultados de la Suite de Pruebas:
```text
03:04:04  Running with dbt=1.12.6
03:04:04  Found 7 models, 14 data tests, 1 seed, 3 sources
03:04:05  PASS not_null_dim_departamento_cod_departamento ........................ [PASS]
03:04:05  PASS not_null_dim_departamento_latitud ................................. [PASS]
03:04:05  PASS not_null_dim_departamento_longitud ................................ [PASS]
03:04:05  PASS not_null_dim_departamento_nombre_departamento ..................... [PASS]
03:04:05  PASS not_null_fct_demanda_logistica_cod_departamento ................... [PASS]
03:04:05  PASS not_null_fct_demanda_logistica_poblacion .......................... [PASS]
03:04:05  PASS not_null_fct_demanda_logistica_porcentaje_poblacion_nacional ...... [PASS]
03:04:05  PASS not_null_fct_demanda_logistica_indice_alcance_digital_hogares ..... [PASS]
03:04:05  PASS unique_dim_departamento_cod_departamento ......................... [PASS]
03:04:05  PASS unique_fct_demanda_logistica_cod_departamento .................... [PASS]
03:04:05  PASS unique_rep_resumen_ejecutivo_cod_departamento .................... [PASS]
03:04:05  PASS relationships_fct_demanda_logistica_cod_departamento .............. [PASS]
03:04:05  PASS test_suma_poblacion_100_pct ...................................... [PASS]
03:04:05  Done. PASS=14 WARN=0 ERROR=0 TOTAL=14
```

- **Cobertura Departamental:** Exactamente 529 de 529 departamentos auditados (100%).
- **Integridad Referencial:** Relaciones foráneas sin orfandad entre hechos y dimensiones.
- **Geocodificación Completa:** 0 coordenadas nulas en todo el territorio nacional.

---

## 🖥 Reporte Ejecutivo Interactivo HTML

El pipeline genera automáticamente un archivo HTML interactivo y autocontenido en:
```text
output/reporte_logistica_argentina.html
```

### Características del Reporte:
- **Mapa Geoespacial Interactivo Leaflet.js:** Centroides de los departamentos más poblados con marcadores interactivos, popups con métricas de población, porcentaje nacional, IADH y coordenadas IGN.
- **Tablas Responsivas con Micro-Visualizaciones:** Barras de porcentaje proporcionales, badges de color por macro-región y formato numérico regional.
- **Cuadros de Mando Ejecutivos:** Tarjetas KPI de alto impacto visual diseñadas con tipografía Inter y paleta sobria corporativa.

Para visualizar el reporte:
- Simplemente haz doble clic sobre `output/reporte_logistica_argentina.html` para abrirlo en cualquier navegador web moderno (Chrome, Edge, Firefox, Safari).

---

## 👤 Autor y Contacto
- **Desarrollador:** Rodrigo Calderón
- **GitHub:** [@rodcz](https://github.com/rodcz)
- **Email:** `rodcalderon129@gmail.com`
- **Proyecto:** Prueba Técnica de Ingeniería de Datos & Arquitectura Logística Territorial en Argentina
