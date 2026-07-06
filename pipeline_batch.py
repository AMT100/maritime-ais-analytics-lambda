import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, lag, date_format, to_date, when, count, avg
from pyspark.sql.window import Window
 
# 1. Inicialización de la sesión de spark
spark = SparkSession.builder \
    .appName("AIS_Gibraltar_Pipeline_Batch") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()
 
# Ruta del Data Lake en el entorno local de la instancia EC2
BASE_PATH = "/home/ec2-user/datalake"
 
# 2. Fase Bronze a Silver: limpieza, tipificación y filtrado espacial 
 
# Lectura de los datos crudos (csv)
try:
    df_bronze = spark.read.csv(f"{BASE_PATH}/bronze/ais_raw.csv", header=True, inferSchema=False)
except Exception as e:
    print(f"[!] Error crítico al leer el archivo de origen: {e}")
    spark.stop()
    sys.exit(1)
 
# Delimitación del Estrecho de Gibraltar (Cuadrante Estricto)
LAT_MIN, LAT_MAX = 35.8, 36.2
LON_MIN, LON_MAX = -5.8, -5.2
 
# Schema-on-read: Conversión de tipos y filtros de calidad
df_silver = df_bronze \
    .withColumn("MMSI", col("MMSI").cast("string")) \
    .withColumn("LAT", col("LAT").cast("float")) \
    .withColumn("LON", col("LON").cast("float")) \
    .withColumn("SOG", col("SOG").cast("float")) \
    .withColumn("COG", col("COG").cast("float")) \
    .withColumn("BaseDateTime", col("BaseDateTime").cast("timestamp")) \
    .withColumn("Fecha", to_date(col("BaseDateTime"))) \
    .filter(
        col("MMSI").isNotNull() & 
        (col("LAT").between(LAT_MIN, LAT_MAX)) & 
        (col("LON").between(LON_MIN, LON_MAX)) & 
        (col("SOG") >= 0.0)
    )
 
# Capa Silver optimizada en formato Parquet y particionada por fecha
df_silver.write.mode("append").partitionBy("Fecha").parquet(f"{BASE_PATH}/silver/ais_refined")
 
# 3. Fase Silver a Gold: Cálculo de KPIS y análisis analítico 
 
# Lectura capa Silver
df_silver_source = spark.read.parquet(f"{BASE_PATH}/silver/ais_refined")
 
# Definición de ventana particionada por MMSI y ordenada por fecha
window_buque = Window.partitionBy("MMSI").orderBy("BaseDateTime")
 
# Identificación de anomalías comparando el registro actual con el anterior
df_anomalias = df_silver_source \
    .withColumn("SOG_previo", lag("SOG", 1).over(window_buque)) \
    .withColumn("COG_previo", lag("COG", 1).over(window_buque)) \
    .withColumn("Delta_SOG", abs(col("SOG") - col("SOG_previo"))) \
    .withColumn("Delta_COG", abs(col("COG") - col("COG_previo"))) \
    .withColumn("Alerta_Anomalia", when((col("Delta_SOG") > 5.0) | (col("Delta_COG") > 45.0), 1).otherwise(0))
 
# Agregación final por bloques horarios
df_gold_kpis = df_anomalias \
    .withColumn("Intervalo_Horario", date_format(col("BaseDateTime"), "yyyy-MM-dd HH:00:00")) \
    .groupBy("Intervalo_Horario", "MMSI") \
    .agg(
        avg("SOG").alias("Velocidad_Media_Nudos"),
        count("Alerta_Anomalia").alias("Total_Señales_Intervalo"),
        avg("Alerta_Anomalia").alias("Ratio_Anomalas")
    )
 
# Escritura final en la capa Gold
df_gold_kpis.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/ais_kpis")
 
print("[✓] Pipeline Batch completado con éxito. Capas Silver y Gold actualizadas.")
spark.stop()
