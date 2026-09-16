from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, upper, to_date

#EXTRACT
spark = (SparkSession.builder.appName("BBVA ETL Pipeline").getOrCreate()
         )
df = spark.read.csv(
    "data/raw/movimientos_etl.csv",
    header=True,
    sep=";",
    inferSchema=True
)
print("ENTRADA")
df.show()
print(f"Registros de entrada: {df.count()}")

#TRANSFORM

limpios = (
    df 
        .withColumn("Cliente",initcap(trim(col("cliente"))))
        .withColumn("Ciudad",upper(trim(col("cliente"))))
        .withColumn("Fecha",to_date("fecha","yyyy-mm-dd"))
        .withColumn("Importe",col("importe").cast("double"))
)

limpios = limpios.na.fill({
    "ciudad":"DESCONOCIDA"
})

validos = limpios.filter(
    col("cliente").isNotNull() &
    col("importe").isNotNull() &
    (col("importe") > 0)
)

invalidos = limpios.filter(
    col("cliente").isNull() |
    col("importe").isNull() |
    (col("importe") <= 0)
)

total_entrada = limpios.count()
total_validos = validos.count()
total_invalidos = invalidos.count()

if total_entrada != total_validos + total_invalidos:
    raise ValueError("Error en cuadre de registros")

print("VALIDACION OK")
print(f"Entrada: {total_entrada}")
print(f"Validos: {total_validos}")
print(f"Invalidos: {total_invalidos}")

#LOAD

validos.write.mode("overwrite").parquet("data/processed/movimientos_validos")
invalidos.write.mode("overwrite").option("header",True).csv("output/movimientos_invalidos")

resultado = spark.read.parquet(
    "data/processed/movimientos_validos"
)

print("RESULTADO FINAL")
resultado.show()
resultado.printSchema()