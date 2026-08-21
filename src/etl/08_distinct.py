from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

data = [
    (1,"Ana",15000),
    (2, "Luis", 23000),
    (2, "Luis",23000),
    (3,"Maria",18500),
    (3, "Maria",18500)
]

df = spark.createDataFrame(
    data,
    ["id","nombre", "saldo"]
)

print("DATOS ORIGINALES")
df.show()
print(df.count())

clientes_unicos = df.distinct()
print("DATOS SIN DUPLICADOS")
clientes_unicos.show()
print(clientes_unicos.count())