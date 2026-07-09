from pyspark.sql import SparkSession

spark=(
    SparkSession.builder
    .appName("BBVA Batch Simulator")
    .getOrCreate()
)

data = [
    (1,"Ana",15000,"Madrid"),
    (2,"Luis",23000,"Barcelona"),
    (3,"Maria",18500,""),
    (4,"Carlos",42000,"Sevilla")
]

df = spark.createDataFrame(
    data,
    ["id","nombre","salario","ciudad"]
)

df.show()

print("\n===== ESQUEMA =====")
print(df.printSchema())

print("\n===== COLUMNAS =====")
print(df.columns)

print("\n===== TOTAL DE REGISTROS =====")
print(df.count())
