from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

clientes = df.filter(df.saldo>20000)
clientes.show()

clientesPremium = df.filter(df.saldo>30000)
clientesPremium.show()