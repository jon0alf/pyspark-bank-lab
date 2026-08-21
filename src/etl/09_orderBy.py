from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv (
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

clientes_ordenados = df.orderBy("saldo")
clientes_ordenados.show()

clientes_ordenados = df.orderBy(
    col("saldo").desc()
)

clientes_ordenados.show()

clientes_ordenados = df.orderBy(
    col("nombre").asc()
)

