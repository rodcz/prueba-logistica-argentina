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
  * *Dataset:* Centroides y geometrías oficiales de los 529 departamentos del país.
  * *Por qué:* Es la autoridad cartográfica oficial de la República Argentina. Proporciona coordenadas geodésicas (latitud y longitud) indispensables para la georreferenciación y el análisis territorial de cobertura logística.
* **RENAPER (Agosto 2024) - Población Identificada por Departamento:**
  * *Dataset:* Padrón de 46.877.222 personas con DNI registrado a nivel departamental.
  * *Por qué:* Representa la fuente de población más actualizada y accesible en un único archivo CSV estructurado (48.983 registros), permitiendo una ingesta automatizada, repetible y sin scraping frágil.
* **INDEC - Censo Nacional 2022 (Página 25, Cuadro 1.1):**
  * *Dataset:* Distribución poblacional por departamento y comuna de CABA.
  * *Por qué:* RENAPER registra a la Ciudad Autónoma de Buenos Aires bajo un único código comodín (`02000`). El Censo 2022 aportó la distribución porcentual oficial para calibrar las 15 comunas porteñas de forma matemáticamente exacta.
* **ENACOM - Conectividad e Internet Fijo (2014-2024):**
  * *Dataset:* Serie histórica trimestral de accesos y penetración de internet por cada 100 hogares a nivel provincial.
  * *Por qué:* Es el ente regulador oficial de telecomunicaciones. Permite formular el Índice de Alcance Digital de Hogares (IADH) para identificar el potencial del comercio electrónico.
* **DAMI / AMBA (Subsecretaría de Asuntos Municipales):**
  * *Dataset:* Delimitación oficial del Área Metropolitana de Buenos Aires (CABA + 40 municipios).
  * *Por qué:* Es la delimitación geográfica formal para categorizar el nodo de mayor concentración demográfica y volumen logístico del país.

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
* **14 de 14 tests automatizados de dbt aprobados (100% pass rate):** Validaciones de unicidad, no nulidad, llaves foráneas y consistencia ejecutadas en 0.8s con política *fail-fast*.
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
