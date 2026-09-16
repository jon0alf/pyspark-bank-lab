from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date

from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, DoubleType
)
spark = (SparkSession.builder.appName("Bank Daily Movements").getOrCreate())

schema = StructType([
    StructField("id_movimiento", IntegerType(), True),
    StructField("cuenta", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("importe", DoubleType(), True),
    StructField("moneda", StringType(), True)
])

df = spark.read.csv(
    "data/raw/movimientos_banco.csv",
    header=True,
    sep=";",
    schema=schema
)
print(f"Entrada: {df.count()}")
df.show()

procesados = (
df
    .withColumn("cuenta", upper(trim(col("cuenta"))))
    .withColumn("tipo", upper(trim(col("tipo"))))
    .withColumn("moneda", upper(trim(col("moneda"))))
    .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
    .na.fill({"moneda": "MXN"})
)

condicion_valida = (
    col("id_movimiento").isNotNull() &
    col("cuenta").isNotNull() &
    col("fecha").isNotNull() &
    col("tipo").isin("CARGO", "ABONO") &
    col("importe").isNotNull() &
    (col("importe") > 0)
)

aceptados = procesados.filter(condicion_valida)
rechazados = procesados.filter(~condicion_valida)
print("ACEPTADOS")
aceptados.show()
print("RECHAZADOS")
rechazados.show()

total = procesados.count()
total_aceptados = aceptados.count()
total_rechazados = rechazados.count()
if total != total_aceptados + total_rechazados:
    raise ValueError("ERROR: descuadre de registros")

print("VALIDACION OK")
print(f"Entrada: {total}")
print(f"Aceptados: {total_aceptados}")
print(f"Rechazados: {total_rechazados}")

aceptados.write.mode("overwrite").parquet("data/processed/movimientos_banco_aceptados")
rechazados.write.mode("overwrite").option("header",True).csv("output/movimientos_banco_rechazados")

salida = spark.read.parquet(
    "data/processed/movimientos_banco_aceptados"
)
if salida.count() != total_aceptados:
    raise ValueError("ERROR: cantidad escrita no coincide")

print("LOAD VALIDADO")