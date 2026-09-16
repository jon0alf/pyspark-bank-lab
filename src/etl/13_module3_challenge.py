from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

reporte_final = (
    df 
    .filter(col("saldo")>= 18000)
    .withColumn(
        "saldo_anual",
        col("saldo")*12
    )
    .select(
        col("id"),
        col("nombre").alias("cliente"),
        col("saldo").cast("double"),
        col("saldo_anual")
    )
    .orderBy(
        col("saldo").desc(),
        col("cliente").asc()
    )
    .limit(5)
    
)
reporte_final.show()
reporte_final.printSchema()
print(reporte_final.count())

reporte_publico = reporte_final.drop("id")
reporte_publico.show()

data = [
    (1,"Luis",13000),
    (1,"Luis",13000),
    (2,"Carlos",14900),
    (3,"Luis",43000),
]

df2 = spark.createDataFrame(
    data,
    ["id","nombre","saldo"]
)
print(f"Cantidad de registros:  {df2.count()}")
unicos = df2.distinct()
print(f"Cantidad de registros:  {unicos.count()}")