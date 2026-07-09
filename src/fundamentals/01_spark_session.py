from pyspark.sql import SparkSession

spark = (
    SparkSession.builder # instruccion que inicia la construccion de la aplicacion
    .appName("BBVA Batch Simulator") # Asigna nombre a la aplicacion
    .getOrCreate() #instruccion que crea o reutiliza un SparkSession
)

print("="*50)
print(f"Aplicacion: {spark.sparkContext.appName}")
print(f"Version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
print("="*50)
print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
