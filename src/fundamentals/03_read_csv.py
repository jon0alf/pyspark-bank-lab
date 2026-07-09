from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BBVA Batch Simulator")
    .getOrCreate()
)

df = spark.read.csv(
    "data/raw/clientes.csv",
    header=True, #instruccion para que detecte que la primera fila es el nombre de las columnas
    inferSchema=True #instruccion para decirle que detecte el tipo de dato de cada columna
)

df.show()

df.printSchema()