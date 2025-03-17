from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("EventosProcesamiento") \
    .getOrCreate()

# 2. Leer el archivo CSV comprimido
df = spark.read.csv("eventos.csv.gz", header=True, inferSchema=True)

# 3. Filtrar registros nulos
df_clean = df.dropna(subset=["id_source", "id_destination"])

# 4. Validar que calls, seconds y sms sean valores no negativos
df_clean = df_clean.filter((col("calls") >= 0) & (col("seconds") >= 0) & (col("sms") >= 0))

# 5. Guardar los datos procesados en formato Parquet
df_clean.write.mode("overwrite").parquet("output/eventos_procesados")

# Finalizar sesión de Spark
spark.stop()
