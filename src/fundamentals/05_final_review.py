from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Reto final").getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
    )

print(f"="*50)
print(f"Aplicacion: {spark.sparkContext.appName}")
print(f"Versión: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
print(f"="*50)

df.show()
df.printSchema()
print(f"Columnas: {df.columns}")
print(f"Total de registros: {df.count()}")





