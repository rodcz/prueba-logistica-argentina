# Pipeline ELT: Demanda Territorial y Logística en Argentina

Pipeline de datos automatizado y contenerizado para la consolidación, modelado dimensional y análisis de demanda territorial y conectividad en Argentina.

---

## 1. Cómo ejecutarlo

### Requisitos previos
* **Docker** y **Docker Compose** instalados y en ejecución.

### Pasos de ejecución
1. **Configurar el entorno:**
   ```bash
   cp .env.example .env
   ```
2. **Levantar los servicios:**
   ```bash
   docker compose up -d --build
   ```
3. **Ejecutar el pipeline en Apache Airflow:**
   * Ingresar a `http://localhost:8080` (Usuario: `admin` | Contraseña: `admin`).
   * Activar y disparar el DAG `dag_demanda_logistica`.
   * *O dispararlo por terminal:*
     ```bash
     docker compose exec airflow-webserver airflow dags trigger dag_demanda_logistica
     ```
4. **Verificar los resultados:**
   * **Base de datos (PostgreSQL 18 en `localhost:5432`):**
     ```bash
     docker exec -it postgres_warehouse psql -U postgres -d warehouse_db -c "SELECT cod_departamento, nombre_departamento, nombre_provincia, poblacion, indice_alcance_digital_hogares FROM analytics_marts.rep_resumen_ejecutivo LIMIT 10;"
     ```
   * **Reporte HTML interactivo:** Generado automáticamente en `output/reporte_logistica_argentina.html`.

---

## 2. Qué fuentes usaste y por qué

* **Instituto Geográfico Nacional (IGN) - División Política:**
  * *Dataset:* [API Georef (Documentación)](https://datosgobar.github.io/georef-ar-api/) | [Descarga directa CSV](https://apis.datos.gob.ar/georef/api/v2.0/departamentos.csv).
  * *Por qué:* Es la autoridad cartográfica oficial de la República Argentina. Proporciona centroides geodésicos (latitud y longitud) indispensables para la georreferenciación y el análisis territorial de cobertura logística de los 529 departamentos.
* **RENAPER (Agosto 2024) - Población Identificada por Departamento:**
  * *Dataset:* [Portal Estadístico de Población](https://estadisticas.renaper.gob.ar/app_poblacion/) | [Descarga directa CSV](https://datosabiertos.renaper.gob.ar/poblacion_identificada_departamento_agosto_2024.csv).
  * *Por qué:* Padrón de 46.877.222 personas con DNI registrado a nivel departamental. Representa la fuente de población más actualizada y accesible en un único archivo estructurado (48.983 registros), permitiendo una ingesta automatizada, repetible y sin scraping frágil.
* **INDEC - Censo Nacional 2022 (Población y Nomenclador de 529 Departamentos):**
  * *Dataset:* [Portal Temático Censo 2022](https://www.indec.gob.ar/indec/web/Nivel4-Tema-2-41-165) | [Resultados Definitivos - Cuadro 1.1, Pág. 25 (PDF)](https://www.indec.gob.ar/ftp/cuadros/poblacion/censo2022_indicadores_demograficos.pdf).
  * *Por qué:* Establece el marco geoestadístico oficial de 529 entidades (15 comunas de CABA + 135 partidos de PBA + 379 departamentos del interior) con código INDEC de 5 dígitos. Dado que RENAPER agrupa a CABA bajo el código comodín `02000` (3,4M hab), el Censo 2022 aportó la distribución porcentual oficial para calibrar las 15 comunas porteñas de forma matemáticamente exacta con 0 error contable residual.
* **ENACOM - Conectividad e Internet Fijo (2014-2024):**
  * *Dataset:* [Tablero de Penetración Provincial](https://indicadores.enacom.gob.ar/DatosAbiertos/Internet/penetracion-provincias) | [Descarga directa CSV](https://indicadores.enacom.gob.ar/Files/DatosAbiertos/internet_accesos_penetracion_provincias.csv).
  * *Por qué:* Es el ente regulador oficial de telecomunicaciones. Su serie trimestral de accesos y penetración de internet por cada 100 hogares a nivel provincial permite formular el Índice de Alcance Digital de Hogares (IADH) para identificar el potencial del comercio electrónico.
* **DAMI / AMBA (Subsecretaría de Asuntos Municipales):**
  * *Dataset:* [Delimitación Oficial del AMBA (CABA + 40 municipios)](https://www.argentina.gob.ar/noticias/sabias-que-es-el-amba).
  * *Por qué:* Delimitación geográfica formal para categorizar el nodo de mayor concentración demográfica y volumen logístico del país.
* **MINCyT (Indicadores CTI) / INDEC (EPH) - Macro-Regiones Estadísticas:**
  * *Dataset:* [Estándar de Macro-Regiones del Estado Argentino](https://www.argentina.gob.ar/ciencia/indicadores-cti/datasets/dataset-empresas-innovadoras-en-productos-innovacion-y-region).
  * *Por qué:* Define el estándar canónico de 6 macro-regiones del país (CABA y GBA, Pampeana, Cuyo, NOA, NEA y Patagonia). Permite desacoplar a nivel de departamento la heterogeneidad de la Provincia de Buenos Aires (diferenciando los 38 partidos del Conurbano/AMBA del interior bonaerense pampeano) mediante la semilla `seed_departamentos_regiones.csv`.

---

## 3. Decisiones importantes y alternativas que descartaste

* **Decisión 1: `groupby` compuesto en la ingesta de RENAPER (`['departamento_id', 'nombre_departamento', 'provincia_id', 'nombre_provincia']`)**
  * *Alternativa descartada:* Agrupar únicamente por `departamento_id` o por nombre.
  * *Motivo:* En Argentina existen departamentos homónimos en distintas provincias (por ejemplo, el departamento *Capital* existe en Córdoba, Mendoza, Salta y Catamarca; o *San Martín* en San Juan y Corrientes). La clave compuesta previene fusiones accidentales de registros y preserva los nombres en memoria en una sola pasada, sin requerir un `JOIN` secundario.
* **Decisión 2: Escalamiento censal proporcional en CABA vs. Promedio simple o dejar en cero.**
  * *Alternativa descartada:* Dividir los 3,4M de CABA en 15 partes iguales (distorsionaría la demanda real) o dejar las comunas en cero (rompería la georreferenciación).
  * *Motivo:* Se aplicó la ponderación demográfica oficial del Censo 2022 ($\text{Pob}_i = \text{round}(P_{\text{censo}_i} \times \frac{3.400.592}{3.121.707})$), garantizando la representación real de cada comuna con 0 error residual en el control contable.
* **Decisión 3: Métrica IADH basada en hogares conectados vs. Habitantes individuales.**
  * *Alternativa descartada:* Medir penetración por cada 100 habitantes (~25%) o considerar únicamente población bruta.
  * *Motivo:* La tasa por habitante mide contratos individuales de facturación. En logística comercial y e-commerce de retail, las entregas de paquetería se dirigen al **hogar**, donde los convivientes comparten la misma conexión Wi-Fi fija. El IADH refleja el volumen real de hogares contactables.
* **Decisión 4: Materialización en dbt (Vistas lógicas en Staging/Intermediate vs. Tablas físicas).**
  * *Alternativa descartada:* Persistir tablas intermedias físicas innecesarias.
  * *Motivo:* Configurar staging e intermediate como vistas lógicas reduce el consumo de almacenamiento en disco en PostgreSQL y asegura datos siempre frescos sin sobrecosto de I/O, materializando únicamente los marts analíticos finales.

---

## 4. Qué tan confiable es el resultado y cómo lo medís

### Cómo se mide (Métricas Cuantitativas y Quality Gates)
* **18 de 18 tests automatizados de dbt aprobados (100% pass rate):** Validaciones de unicidad, no nulidad, llaves foráneas y consistencia ejecutadas en ~1.4s con política *fail-fast*.
* **100% de Cobertura Territorial:** 529 de 529 departamentos cubiertos y 24 de 24 jurisdicciones analizadas (0 faltantes).
* **100% de Georreferenciación:** 0 registros nulos en coordenadas geodésicas oficiales del IGN.
* **Control de Integridad Contable 1:1:** Suma de participaciones relativas = **100.0000% exacto** y suma poblacional nacional = **46.877.222 habitantes**.
* **Tipado estricto:** 100% de códigos departamentales en formato `VARCHAR(5)` con ceros a la izquierda preservados (e.g., `02001`).

### Qué quedó afuera del alcance regular (Exclusiones Cuantificadas)
* **Exclusión de Red Logística Terrestre Regular (147 habitantes / 0.00031% del país):**
  * *Islas del Atlántico Sur / Malvinas* (Cód. `94021`, 17 hab) y *Antártida Argentina* (Cód. `94028`, 130 hab) quedaron excluidos de la red troncal terrestre y etiquetados con los flags `es_territorio_continental = false` y `requiere_logistica_especial = true` por aislamiento geográfico y necesidad de abastecimiento aéreo/marítimo militar.
* **Granularidad Municipal de Conectividad (ENACOM):**
  * ENACOM publica la penetración fija exclusivamente a nivel de **24 provincias**. Quedaron afuera las diferencias de conectividad intradepartamentales y rurales (se asume tasa provincial uniforme en el IADH).
* **Topología de Red Vial y Ruteo Dinámico:**
  * Se opera con distancias geodésicas y centroides IGN; quedó afuera el grafo vial de Vialidad Nacional (rutas pavimentadas vs. ripio) y tiempos de tránsito vehicular.

---

## 5. Limitaciones y qué harías con más tiempo

* **1. Visualización Geoespacial Dependiente de Conexión Externa:**
  * *Limitación actual:* El reporte HTML consume mapas base vía CDN web (OpenStreetMap/CartoDB). En redes cerradas u offline, el mapa base no renderiza las calles de fondo.
  * *Con más tiempo:* Empaquetar los assets estáticos de Leaflet y servir teselas vectoriales offline (`.mbtiles`) mediante un contenedor ligero de TileServer GL.
* **2. Análisis Estático (Snapshot) vs. Modelado Predictivo con Series de Tiempo:**
  * *Limitación actual:* El modelo actual es descriptivo: evalúa la foto estática del último trimestre disponible de ENACOM y del padrón 2024.
  * *Con más tiempo:* Como la capa raw ya cuenta con los **10 años de series históricas trimestrales de ENACOM (2014-2024)**, entrenar un modelo de series de tiempo (Prophet o ARIMA) para proyectar el crecimiento del IADH a 24 meses y anticipar la saturación de Centros de Distribución.

---

## 6. Declaración de Uso de Inteligencia Artificial y Validación

Se utilizó un agente de IA como asistente de ingeniería de datos (*pair programming*) y un subagente especializado de aseguramiento de calidad (**`qa-data-validator`**) para auditar cada hito técnico antes de su integración.

### ¿Para qué se usó?
1. **Construcción Integral del Pipeline ELT:** Desarrollo de scripts de extracción en Python (`extract_sources.py`), orquestación con Apache Airflow 2.10 (`dag_demanda_logistica.py`), modelos dbt Core (capas Staging, Intermediate y Marts) y entorno Docker Compose.
2. **Detección y Fix Demográfico de CABA:** Identificación de la omisión comunal en el comodín `02000` de RENAPER (3,4M hab) y diseño de la fórmula de escalamiento censal proporcional.
3. **Resolución de Homónimos Territoriales:** Detección de colisiones potenciales entre departamentos con el mismo nombre en distintas provincias (ej. *Capital* o *San Martín*) e implementación de la agrupación cuádruple compuesta en Pandas (`['departamento_id', 'nombre_departamento', 'provincia_id', 'nombre_provincia']`).
4. **Preservación Cartográfica de Códigos INDEC:** Detección del truncamiento de ceros a la izquierda (`02001` → `2001`) y forzado de tipado estricto `VARCHAR(5)` con `lpad` en la capa Staging de dbt para garantizar los cruces espaciales.
5. **Aislamiento Operativo de Soberanía (Antártida y Malvinas):** Creación de banderas booleanas (`es_territorio_continental = false`, `requiere_logistica_especial = true`) para cuantificar los territorios especiales sin excluirlos de la base nacional.
6. **Modelado Star Schema, Métrica IADH y Dashboard:** Modelado de `dim_departamento` y `fct_demanda_logistica` con pre-cálculo de funciones de ventana (Rankings y curva de Pareto 80/20), formulación del IADH y maquetación del reporte interactivo HTML con Leaflet.js.

### ¿Cómo se validó lo que produjo?
1. **Auditoría del Agente `qa-data-validator` y Ejecución Real:** El subagente especializado de QA validó la sintaxis con `docker compose config`, ejecutó el DAG de 6 tareas de Airflow de punta a punta y auditó los logs de los contenedores (`docker ps`, Airflow y PostgreSQL), confirmando cero fallas de dependencias.
2. **Cruce contra Documento Primario Oficial INDEC (Pág. 25):** Se contrastó fila por fila contra el PDF del Censo 2022 (Cuadro 1.1, Pág. 25). Se validó por SQL que la distribución comunal conserve las participaciones censales a 4 decimales y sume exactamente **3.400.592 habitantes** (0 error residual contable).
3. **Test de Unicidad dbt y Consulta Anti-Duplicados:** El agente de QA corrió el test `unique` sobre `cod_departamento` en dbt y una query de verificación de duplicados (`HAVING COUNT(*) > 1`), certificando que los 529 departamentos mantienen identidad única sin fusiones erróneas.
4. **Test de Integridad Referencial `relationships` y Cobertura IGN:** Se ejecutó el test foráneo de dbt cruzando hechos con dimensiones. Se comprobó que el 100% de los 529 departamentos coincidan con la cartografía del IGN con **0 nulos en coordenadas de latitud y longitud**.
5. **Control Contable y Test `test_suma_poblacion_100_pct`:** Se auditó por SQL que exactamente 147 habitantes (17 de Malvinas y 130 de Antártida, el 0.00031% del país) tengan las banderas activas, y que el test contable de dbt apruebe confirmando que el **100.0000% de la población (46.877.222 hab)** esté contemplada.
6. **Benchmarking de Latencia y Verificación en Navegador:** Se validó que la vista `rep_resumen_ejecutivo` responda en **menos de 2 milisegundos** sin recalcular ventanas al vuelo. Se inspeccionó el archivo HTML en navegador validando el renderizado de los 529 marcadores y métricas sin errores de consola.

