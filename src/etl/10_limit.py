from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/clientes.csv",
    header="True",
    inferSchema="True"
)

clientes_muestra = df.limit(2)
clientes_muestra.show()

top_clientes = (
    df.orderBy(col("nombre").desc()).limit(1)
)

top_clientes.show()


saldos_menores = df.orderBy(col("saldo").asc()).limit(2)
saldos_menores.show()