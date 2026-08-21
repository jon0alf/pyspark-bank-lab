from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)
df.printSchema()

clientes = df.withColumnRenamed("nombre","cliente")

clientes.printSchema()