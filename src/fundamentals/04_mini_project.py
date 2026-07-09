from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("BBVA Batch Simulator").getOrCreate()   
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True, #instruccion para que detecte que la primera fila es el nombre de las columnas
    inferSchema=True #instruccion para decirle que detecte el tipo de dato de cada columna
)

print("="*50)
print(f"Aplicacion: {spark.sparkContext.appName}")
print(f"Version de Spark: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
print("="*50)

df.show()
df.printSchema()
print(f"Columnas: {df.columns}")
print(f"Numero de filas: {df.count()}")


