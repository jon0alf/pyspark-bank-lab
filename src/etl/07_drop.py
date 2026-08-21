from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True   
)

df.show()
df.printSchema()
clientes = df.drop("saldo")

clientes.show()
clientes.printSchema()

clientes = df.drop("saldo","id")
clientes.show()
clientes.printSchema()