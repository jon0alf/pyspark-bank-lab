from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate())

df = spark.read.csv(
    "data/raw/cuentas.csv",
    header=True,
    sep=";",
    inferSchema=True
)
df.show()
df.printSchema()

from pyspark.sql.types import(

    StructType, StructField, IntegerType, StringType, DoubleType
)

esquema = StructType([
    StructField("id", IntegerType(), True),
    StructField("cliente", StringType(), True),
    StructField("tipo_cuenta", StringType(), True),
    StructField("saldo", DoubleType(), True)
])
df_controlado = spark.read.csv(
    "data/raw/cuentas.csv",
    header=True,
    sep=";",
    schema=esquema
)
df_controlado.printSchema()
df_controlado.show()