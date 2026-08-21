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

clientes = df.withColumn(
    "saldo_doble",
    col("saldo")*2
)

clientes.show()

clientesPremium = df.withColumn(
    "saldo",
    col("saldo")*1.16
)
clientesPremium.show()

saldos_clientes = df.withColumn(
    "saldo_mensual",
    col("saldo")/12
)
saldos_clientes.show()