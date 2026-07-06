import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, lag, date_format, to_date, when, count, avg
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
 
# 1. Inicialización de la sesión de spark
spark = SparkSession.builder \
    .appName("AIS_Gibraltar_Structured_Streaming") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()
 
BASE_PATH = "/home/ec2-user/datalake"
 
# 2. Definición del esquema
schema_ais = StructType([
    StructField("MMSI", StringType(), True),
    StructField("BaseDateTime", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True)
])
 
# 3. Lectura en streaming 
df_stream_crudo = spark.readStream \
    .schema(schema_ais) \
    .json(f"{BASE_PATH}/bronze/stream_input/")
 
# 4. Función micro-batch
def aplicar_logica_batch_a_streaming(df_micro_batch, batch_id):
    
    if df_micro_batch.isEmpty():
        return
 
    # Coordenadas reales utilizadas (Gibraltar Estricto)
    LAT_MIN, LAT_MAX = 35.8, 36.2
    LON_MIN, LON_MAX = -5.8, -5.2
 
    # Bronze a Silver 
    df_silver = df_micro_batch \
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
    
    # Diagnóstico 1: Filtro
    if df_silver.isEmpty():
        print(f"Lote {batch_id}: Abortado. El DataFrame Silver está vacío tras el filtro.")
        return
        
    df_silver.write.mode("append").partitionBy("Fecha").parquet(f"{BASE_PATH}/silver/ais_refined")
 
    # Diagnóstico 2: Lectura Parquet
    try:
        df_silver_historico = spark.read.parquet(f"{BASE_PATH}/silver/ais_refined")
    except Exception as e:
        print(f"Lote {batch_id}: Abortado. Excepción en Parquet: {e}")
        return
    
    window_buque = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    
    df_anomalias = df_silver_historico \
        .withColumn("SOG_previo", lag("SOG", 1).over(window_buque)) \
        .withColumn("COG_previo", lag("COG", 1).over(window_buque)) \
        .withColumn("Delta_SOG", abs(col("SOG") - col("SOG_previo"))) \
        .withColumn("Delta_COG", abs(col("COG") - col("COG_previo"))) \
        .withColumn("Alerta_Anomalia", when((col("Delta_SOG") > 5.0) | (col("Delta_COG") > 45.0), 1).otherwise(0))
    
    df_gold_kpis = df_anomalias \
        .withColumn("Intervalo_Horario", date_format(col("BaseDateTime"), "yyyy-MM-dd HH:00:00")) \
        .groupBy("Intervalo_Horario", "MMSI") \
        .agg(
            avg("SOG").alias("Velocidad_Media_Knot"),
            count("Alerta_Anomalia").alias("Total_Señales_Intervalo"),
            avg("Alerta_Anomalia").alias("Ratio_Anomalas")
        )
    
    df_gold_kpis.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/ais_kpis")
    print(f"Lote de datos {batch_id} procesado correctamente e inyectado en Gold.")
 
# 5. Escritura en streaming
flujo_streaming = df_stream_crudo.writeStream \
    .foreachBatch(aplicar_logica_batch_a_streaming) \
    .outputMode("update") \
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/ais_streaming") \
    .start()
 
flujo_streaming.awaitTermination()
