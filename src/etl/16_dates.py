from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, datediff, current_date
)

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv(
    "data/raw/movimientos_fechas.csv",
    header=True,
    inferSchema=False
)

df.printSchema()
df.show()

movimientos = df.withColumn(
    "fecha_movimiento",
    to_date(col("fecha_movimiento"),"yyyy-mm-dd")
)

movimientos.printSchema()

movimientos = (
    movimientos
        .withColumn("año",year(col("fecha_movimiento")))
        .withColumn("mes",month(col("fecha_movimiento")))
        .withColumn("dia",dayofmonth(col("fecha_movimiento")))
)

movimientos.show()

movimientos = (
    movimientos
        .withColumn("dias_desde_movimiento",datediff(current_date(),col("fecha_movimiento")))
        .withColumn("Fecha actual",current_date())
)

movimientos.show()