from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, upper, length

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes_sucios.csv",
    inferSchema=True,
    header=True
)

print("DATOS ORIGINALES")
df.show()

df_limpio = df.withColumn(
    "nombre",
    trim(col("nombre"))
)
df_limpio.show()

df_limpio = df_limpio.withColumn(
    "nombre",
    initcap(col("nombre"))
)
df_limpio.show()

df_limpio = df_limpio.withColumn(
    "ciudad",
    upper(trim(col("ciudad")))
)

print("DATOS LIMPIOS")
df_limpio.show()

df_limpio.select(
    col("nombre"),
    length(col("nombre")).alias("longitud")
).show()