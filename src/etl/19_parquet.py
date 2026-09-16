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

df.write.mode("overwrite").parquet(
    "data/processed/cuentas_parquet"
)

parquet_df = spark.read.parquet(
    "data/processed/cuentas_parquet"
)
parquet_df.show()
parquet_df.printSchema()

from pyspark.sql.functions import col

parquet_df.select(
    "cliente",
    "tipo_cuenta",
    "saldo"
).filter(
    col("saldo") >= 20000
).show()