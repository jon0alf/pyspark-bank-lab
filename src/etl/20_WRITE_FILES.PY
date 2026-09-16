from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/cuentas.csv",
    header=True,
    sep=";",
    inferSchema=True
)

resultado = df.filter(
    col("saldo") >= 20000
)
resultado.show()

resultado.write.mode("overwrite").option("header",True).csv("output/cuentas_filtradas_csv")
resultado.write.mode("overwrite").json("output/cuentas_filtradas_json")
resultado.write.mode("overwrite").parquet("output/cuentas_filtradas_parquet")
resultado.write.mode("append").parquet("output/historico_cuentas")

#overwrite: reemplaza la salida existente.
#append: agrega nuevos datos a una salida existente.
#ignore: si la salida ya existe, no la reemplaza.
#error / errorifexists: produce un error si la salida ya existe; es el comportamiento predeterminado

validacion = spark.read.parquet(
    "output/cuentas_filtradas_parquet"
)
validacion.show()
validacion.printSchema()