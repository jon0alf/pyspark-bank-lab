from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.json(
    "data/raw/clientes.jsonl"
)
df.show()
df.printSchema()

detalle = spark.read.json(
    "data/raw/clientes_detalle.jsonl"
)

detalle.printSchema()
detalle.show(truncate=False) #instruccion para que spark  no recorte los textos largos al mostrarlos en pantalla

detalle.select(
    "id",
    "nombre",
    "cuenta.tipo",
    "cuenta.saldo"
).show()

from pyspark.sql.functions import col

cuentas = detalle.select(
    col("id"),
    col("nombre"),
    col("cuenta.tipo").alias("tipo_cuenta"),
    col("cuenta.saldo").alias("saldo")
)
cuentas.show()
cuentas.printSchema()