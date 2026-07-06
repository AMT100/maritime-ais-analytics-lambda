# Real-Time Maritime Analytics & Anomaly Detection (Lambda Architecture)

Este repositorio contiene la implementación de una **Arquitectura Lambda extremo a extremo** diseñada para la ingesta, almacenamiento, procesamiento masivo y detección de anomalías cinemáticas en tiempo real de buques comerciales en el Estrecho de Gibraltar utilizando datos de telemetría **AIS (Automatic Identification System)**.

El entorno de producción ha sido desplegado e integrado sobre una instancia de computación en la nube **AWS EC2**.

---

## Arquitectura del Sistema

El flujo de datos se gestiona bajo el concepto de un **Data Lake de tres capas (Medallion Architecture)** implementado de forma local:

1. **Bronze (Data Ingestion):** Captura de datos en bruto (*raw packets*) desde un WebSocket persistente.
2. **Silver (Data Cleaning & Refinement):** Limpieza, tipificación (*Schema-on-read*), filtrado geográfico y almacenamiento en formato optimizado **Parquet** particionado por fecha.
3. **Gold (Business Level KPIs):** Agregaciones horarias por buque y cálculo analítico de anomalías en el comportamiento de navegación.

                  [ Fuente Externa: API AISStream.io (WebSocket) ]
                                         │
                                         ▼
                            [ ingestor_ais.py (Python) ]
                                         │
                                 (Escritura Atómica)
                                         │
                                         ▼
                              [ Capa BRONZE: JSONs ]
                                         │
                                         ▼
         ┌──────────────────────────────────────────────────────────────┐
         │                MOTOR DE PROCESAMIENTO (SPARK)                │
         ├──────────────────────────────┬───────────────────────────────┤
         │ ► VÍA BATCH                  │ ► VÍA STREAMING               │
         │   (pipeline_batch.py)        │   (pipeline_streaming_...)    │
         │                              │                               │
         │   Procesamiento masivo de    │   Procesamiento continuo      │
         │   histórico inicial (.csv)   │   de Micro-Lotes (JSON)       │
         └──────────────────────────────┴───────────────────────────────┘
                                         │
                                         ▼
                         [ Capa SILVER: Parquet Histórico ]
                                         │
                            (Funciones de Ventana / Lag)
                                         │
                                         ▼
                           [ Capa GOLD: KPIs agregados ]
                                         │
                                         ▼
                            [ exportar_gold_a_csv.py ]
                                         │
                                         ▼
                          [ Consumo Final: Power BI / Excel ]

---
## Componentes Técnicos y Scripts

* **`ingestor_ais.py`**: Script en Python nativo encargado de la conexión continua por WebSocket con la API de *AISStream.io*. Filtra los reportes de posición en las coordenadas geográficas del Estrecho de Gibraltar y aplica un mecanismo de **escritura atómica** (`.tmp` + `os.rename`) para garantizar que el motor de streaming no lea fragmentos corruptos.
* **`pipeline_batch.py`**: Script en PySpark encargado de procesar los bloques de datos históricos iniciales, limpiar inconsistencias y poblar la base histórica de las capas Silver y Gold.
* **`pipeline_streaming_nativo.py`**: Corazón del procesamiento en tiempo real ejecutado con **Spark Structured Streaming**. Lee incrementalmente la capa Bronze, actualiza la capa Silver y calcula mediante funciones analíticas de ventana (`Window` y `lag`) variaciones críticas de velocidad y rumbo para detectar anomalías cinemáticas en caliente.
* **`exportar_gold_a_csv.py`**: Componente auxiliar de Spark que realiza un `coalesce(1)` sobre la capa Gold para unificar los KPIs resumidos en un único fichero `.csv` plano y legible por herramientas de BI.

---

## Reglas Analíticas de Anomalías

Las alertas cinemáticas se disparan calculando el diferencial entre la transmisión actual de un buque y su estado inmediatamente anterior:
* **Desviación de Velocidad ($\Delta SOG$):** $> 5.0$ nudos.
* **Desviación de Rumbo ($\Delta COG$):** $> 45.0$ grados.

---

## Tecnologías Utilizadas

* **Lenguajes:** Python, PySpark (SQL Analytics).
* **Procesamiento:** Apache Spark, Spark Structured Streaming.
* **Infraestructura:** AWS EC2 (Ubuntu/Amazon Linux Enterprise).
* **Formatos de Almacenamiento:** Apache Parquet, JSON, CSV.
* **Visualización:** Power BI / Excel.
