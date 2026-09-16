from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = ( SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/transacciones_validacion.csv",
    header=True,
    inferSchema=True
)
df.show()
df.printSchema()

total_entrada = df.count()
print(f"Registros de entrada: {total_entrada}")

incompletos = df.filter(
    col("cuenta").isNull() |
    col("importe").isNull()
)
incompletos.show()
print(f"Registros incompletos: {incompletos.count()}")

importe_invalido = df.filter(
col("importe") <= 0
)
importe_invalido.show()

validos = df.filter(
    col("cuenta").isNotNull() &
    col("importe").isNotNull() &
    (col("importe") > 0)
)
invalidos = df.filter(
    col("cuenta").isNull() |
    col("importe").isNull() |
    (col("importe") <= 0)
)
print("VALIDOS")
validos.show()

print("INVALIDOS")
invalidos.show()

total_validos = validos.count()
total_invalidos = invalidos.count()
print(f"Entrada: {total_entrada}")
print(f"Validos: {total_validos}")
print(f"Invalidos: {total_invalidos}")
print(f"Procesados: {total_validos + total_invalidos}")