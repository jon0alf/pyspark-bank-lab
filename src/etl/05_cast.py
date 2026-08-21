from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=False
)

df.printSchema()

clientes = df.withColumn(
    "saldo",
    col("saldo").cast("double")
)

clientes.printSchema()

clientes = df.withColumn(
    "id",
    col("id").cast("integer")
)

clientes.printSchema()
clientes.show()