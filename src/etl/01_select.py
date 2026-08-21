from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

df.printSchema()

clientes = df.select(
    "id",
    "nombre"
    )
clientes.show()

saldos = df.select("nombre","saldo")
saldos.show()

saldos = df.select("*")
saldos.show()