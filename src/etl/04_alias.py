from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

clientes = df.select(
    col("nombre").alias("cliente"),
    col("saldo").alias("saldo_actual")
)
clientes.show()

clientesPremium = df.select(
    col("id").alias("ID cliente"),
    col("nombre").alias("cliente"),
    col("saldo").alias("saldo disponible")
)
clientesPremium.show()