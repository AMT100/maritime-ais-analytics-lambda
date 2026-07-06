from pyspark.sql import SparkSession
 
# Inicialización de la sesión de Spark
spark = SparkSession.builder \
    .appName("Exportar_Capa_Gold") \
    .getOrCreate()
 
# Leer los datos Parquet de la capa Gold
df_gold = spark.read.parquet("/home/ec2-user/datalake/gold/ais_kpis")
 
# Consolidar en un único archivo (coalesce) y escribir en formato CSV plano con cabeceras
df_gold.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/home/ec2-user/datalake/exportacion_csv")
 
spark.stop()
