from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/clientes.csv",
    header="True",
    inferSchema="True"
)

print("DATOS ORIGINALES")
df.show()

reporte = (
    df
    .filter(col("saldo")>=20000)
    .withColumn(
        "saldo_anual",
        col("saldo")*12
    )
    .select(
        col("id"),
        col("nombre").alias("Nombre"),
        col("saldo").cast("double"),
        col("saldo_anual")
    )
    .orderBy(
        col("saldo").desc()
    )
    .limit(4)
)

print("REPORTE ANUAL")
reporte.show()
reporte.printSchema()

print("REPORTE TRIMESTRAL")

reporte_trimestral = (
    df
    .filter(col("saldo")>15000)
    .withColumn(
        "saldo_trimestral",
        col("saldo")*3
    )
    .select(
        col("nombre").alias("nombre_cliente"),
        col("saldo").cast("double"),
        col("saldo_trimestral")
    )
    .orderBy(col("saldo").desc())
    .limit(4)
    
)
reporte_trimestral.show()

print("REPORTE SEMESTRAL")

reporte_semestral = (
    df
    .filter(col("saldo")>18000)
    .withColumn(
        "saldo_semestral",
        col("saldo")*6
    )
    .select(
        col("id"),
        col("nombre").alias("cliente"),
        col("saldo").cast("double"),
        col("saldo_semestral")
    )
    .orderBy(
        col("saldo").desc(),
        col("cliente").asc()
    )
    .limit(3)
)

reporte_semestral.show()
print(reporte_semestral.count())
reporte_semestral.printSchema()

cliente_revision = reporte_semestral.drop("id")
cliente_revision.show()