# Explicación del Modelado de Datos y Arquitectura de Capas

Documento técnico que detalla la arquitectura de modelado, tablas, relaciones y justificación de ingeniería para el pipeline de demanda territorial y logística en Argentina.

---

## 1. Arquitectura en Capas (Medallion Architecture)

El pipeline implementa una arquitectura en tres capas desacopladas dentro del Data Warehouse (PostgreSQL 18) utilizando **dbt Core 1.12**:

```
[ Fuentes Oficiales: IGN / RENAPER / ENACOM ]
                    │
                    ▼  (Extract & Load - Airflow Task 1)
 ┌─────────────────────────────────────────────────────────┐
 │ CAPA BRONCE (raw)                                       │
 │ • ign_departamentos | renaper_poblacion | enacom_internet│
 └──────────────────────────┬──────────────────────────────┘
                            │
                            ▼  (dbt Transformations - Task 3)
 ┌─────────────────────────────────────────────────────────┐
 │ CAPA PLATA (analytics_intermediate - Vistas Lógicas)    │
 │ • int_departamentos_curated                             │
 │   - Estandarización VARCHAR(5) con ceros a la izquierda │
 │   - Resolución de homónimos por clave compuesta         │
 │   - Escalamiento censal proporcional de CABA (15 comunas)│
 │   - Asignación de banderas operativas de ruteo          │
 └──────────────────────────┬──────────────────────────────┘
                            │
                            ▼  (Star Schema & Marts - Task 3)
 ┌─────────────────────────────────────────────────────────┐
 │ CAPA ORO (analytics_marts - Tablas Físicas)             │
 │ • dim_departamento (Dimensión Territorial)              │
 │ • fct_demanda_logistica (Tabla de Hechos de Demanda)    │
 │ • rep_resumen_ejecutivo (Data Mart Desnormalizado)      │
 └─────────────────────────────────────────────────────────┘
```

### Detalle de Capas:
* **Capa Bronce (`raw`):** 
  * *Esquema:* Tablas físicas cargadas mediante estrategia `Truncate & Load` idempotente.
  * *Propósito:* Almacenamiento inmutable y fiel al dato de origen sin transformaciones destructivas.
* **Capa Plata (`analytics_staging` / `analytics_intermediate`):**
  * *Materialización:* Vistas lógicas en PostgreSQL.
  * *Propósito:* Limpieza de tipos de datos, tipado estricto `VARCHAR(5)` en códigos INDEC, normalización de cadenas de texto (`UPPER(TRIM())`), cruce no destructivo de homónimos, calibración matemática de CABA y enriquecimiento con flags operacionales (`es_territorio_continental`, `requiere_logistica_especial`).
* **Capa Oro (`analytics_marts`):**
  * *Materialización:* Tablas físicas optimizadas para reporting analítico.
  * *Propósito:* Modelo dimensional en estrella (*Star Schema*) con métricas pre-computadas para consumo directo de dashboards y analistas de negocio.

---

## 2. Modelo Dimensional: Tablas y Granularidad

### A. `dim_departamento` (Dimensión Territorial)
* **Grano:** 1 fila = 1 departamento o comuna (529 registros en total).
* **Clave Primaria (PK):** `cod_departamento` (`VARCHAR(5)`).
* **Atributos:**
  * `cod_departamento`: Identificador oficial del departamento según codificación INDEC.
  * `cod_provincia`: Código oficial de provincia de 2 dígitos.
  * `nombre_departamento`: Denominación oficial estandarizada.
  * `nombre_provincia`: Jurisdicción provincial correspondiente.
  * `macro_region`: Agrupación territorial logística (AMBA, Pampeana, Cuyo, NOA, NEA, Patagonia).
  * `latitud`, `longitud`: Coordenadas geodésicas oficiales provistas por el IGN.
  * `es_territorio_continental`: Indicador booleano de conexión vial terrestre continua.
  * `requiere_logistica_especial`: Indicador booleano para destinos que demandan puentes aéreos o fluviales/marítimos (Antártida y Malvinas).

### B. `fct_demanda_logistica` (Tabla de Hechos)
* **Grano:** 1 fila = métricas analíticas por departamento (529 registros).
* **Clave Foránea (FK):** `cod_departamento` (enlace 1:1 con `dim_departamento`).
* **Métricas y Medidas:**
  * `poblacion`: Volumen poblacional consolidado y auditado (RENAPER / Censo).
  * `porcentaje_poblacion_nacional`: Participación porcentual relativa sobre el total país a 4 decimales.
  * `penetracion_internet_hogares`: Tasa oficial de accesos de banda ancha fija por cada 100 hogares (ENACOM).
  * `indice_alcance_digital_hogares` (IADH): Métrica compuesta que refleja el volumen absoluto de hogares conectables ($\text{Población} \times \frac{\text{Penetración}}{100}$).
  * `ranking_poblacion_nacional`: Posición ordinal según volumen demográfico (1 a 529).
  * `ranking_alcance_digital`: Posición ordinal según IADH (1 a 529).
  * `porcentaje_acumulado_pareto`: Curva de concentración acumulada (80/20).

### C. `rep_resumen_ejecutivo` (Data Mart Desnormalizado)
* **Grano:** 1 fila por departamento (529 registros).
* **Propósito:** Vista desnormalizada que fusiona los atributos descriptivos de la dimensión y las métricas de la tabla de hechos. Permite alimentar al reporte interactivo HTML ([`reporte_logistica_argentina.html`](output/reporte_logistica_argentina.html)) con tiempos de respuesta en milisegundos sin sobrecosto de `JOINs` en tiempo de consulta.

---

### D. `seed_departamentos_regiones` (Catálogo Maestro Territorial y Macro-Regiones)
* **Grano:** 1 fila = 1 departamento o comuna (529 registros).
* **Fuentes y Metodología de Construcción:**
  1. **Marco Geoestadístico Censo 2022 (INDEC) + Georef (IGN):** Nomenclador oficial de 529 entidades territoriales de segundo orden con código INDEC unificado de 5 dígitos (`cod_departamento` = 2 dígitos de provincia + 3 dígitos de departamento), incluyendo las 15 comunas de CABA (`02001` a `02015`) y los 135 partidos de la Provincia de Buenos Aires (`06007` a `06891`).
  2. **Estándar de Macro-Regiones (MINCyT / CTI e INDEC EPH):** Agrupación canónica en 6 macro-regiones del Estado Argentino:
     * **CABA y GBA / AMBA:** 15 comunas de CABA + 38 partidos del Gran Buenos Aires (`es_amba = True`).
     * **Cuyo:** Mendoza, San Juan y San Luis.
     * **NEA:** Chaco, Corrientes, Formosa y Misiones.
     * **NOA:** Catamarca, Jujuy, La Rioja, Salta, Santiago del Estero y Tucumán.
     * **Pampeana:** Resto del interior de la Provincia de Buenos Aires (95 partidos), Córdoba, Entre Ríos, La Pampa y Santa Fe.
     * **Patagonia:** Chubut, Neuquén, Río Negro, Santa Cruz y Tierra del Fuego, Antártida e Islas del Atlántico Sur.
* **Justificación de dbt Seed:** La Provincia de Buenos Aires (`06`) se encuentra dividida entre el conurbano metropolitano (`AMBA`) y el interior agropecuario (`Pampeana`). Por ende, una macro-región no puede inferirse a nivel de provincia (`06`); requiere una dimensión puente a nivel departamental (`VARCHAR(5)`) versionada en dbt bajo control de versiones (`git`).

---

## 3. Relaciones e Integridad Referencial

```
 ┌────────────────────────────────────────┐       ┌──────────────────────────────────────────────────┐
 │           dim_departamento             │       │               fct_demanda_logistica              │
 ├────────────────────────────────────────┤       ├──────────────────────────────────────────────────┤
 │ PK  cod_departamento     VARCHAR(5)    │ 1 ─── 1  FK  cod_departamento              VARCHAR(5)    │
 │     nombre_departamento  VARCHAR(100)  │       │      poblacion                     INTEGER       │
 │     nombre_provincia     VARCHAR(100)  │       │      porcentaje_poblacion_nacional NUMERIC(7,4)  │
 │     macro_region         VARCHAR(50)   │       │      penetracion_internet_hogares  NUMERIC(6,2)  │
 │     latitud              NUMERIC(10,6) │       │      indice_alcance_digital_hogares INTEGER      │
 │     longitud             NUMERIC(10,6) │       │      ranking_poblacion_nacional    INTEGER       │
 │     es_territorio_cont.  BOOLEAN       │       │      ranking_alcance_digital       INTEGER       │
 │     requiere_log_esp.    BOOLEAN       │       │      porcentaje_acumulado_pareto   NUMERIC(7,4)  │
 └────────────────────────────────────────┘       └──────────────────────────────────────────────────┘
```

* **Cardinalidad:** 1 a 1 exacta (529 departamentos en dimensión y 529 departamentos en hechos).
* **Validación Automatizada (dbt Tests):**
  * `unique` y `not_null` sobre `cod_departamento` en ambas entidades.
  * `relationships` para asegurar que todo registro en `fct_demanda_logistica` exista previamente en `dim_departamento`.
  * `test_suma_poblacion_100_pct` garantizando que la suma de participaciones relativas sea exactamente `100.0000%`.

---

## 4. Justificación Técnica: ¿Por qué se modeló así?

1. **Desacoplamiento y Mantenibilidad:**
   Separar la capa de ingesta (*bronze/staging*) del modelado analítico (*marts*) protege a los consumidores de datos frente a cambios estructurales en los archivos fuente del gobierno o entes reguladores.
2. **Eficiencia en Cómputo (Pre-cálculo de Ventanas Analíticas):**
   Las funciones analíticas complejas (`ROW_NUMBER()`, `SUM() OVER ()` y la curva acumulada de Pareto) se resuelven durante la fase de transformación en dbt y se persisten físicamente, evitando ejecutar cómputos de agregación pesados en cada consulta del usuario final.
3. **Optimización para Herramientas de BI y Autoservicio:**
   El diseño dimensional en estrella (*Star Schema*) es el estándar de la industria (Kimball). Facilita la conexión directa con herramientas de Business Intelligence (PowerBI, Tableau, Looker) y permite a perfiles de negocio construir análisis cruzados sin ambigüedad en las relaciones de datos.
