from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()
)

df = spark.read.csv (
    "data/raw/clientes_nulos.csv",
    inferSchema=True,
    header=True
)

df.show()
df.printSchema()

df.filter(
    col("saldo").isNotNull()
).show()

clientes_completos = df.na.drop() #esta instruccion elimina registros que contenga un valor nulo en cualquier campo
clientes_completos.show()

#aca eliminamos registro con valor nulo en un campo en especifico
clientes_con_saldo = df.na.drop(
    subset=["saldo"]
)
clientes_con_saldo.show()

#Aca le indicamos que si encuentra un nulo en alguno de esos campos, lo reemplace por el valor indicado
clientes_reemplazados = df.na.fill({
    "ciudad": "DESCONOCIDA",
    "saldo": 0
})
clientes_reemplazados.show()

