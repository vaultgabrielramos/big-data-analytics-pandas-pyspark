from pyspark.sql import SparkSession

# Inicializar a SparkSession
spark = SparkSession.builder \
    .appName("Big Data Analytics com PySpark") \
    .getOrCreate()

# Carregar um dataset fictício com Spark
data = [("Teclado", 120, 10), 
        ("Mouse", 75, 25), 
        ("Monitor", 250, 5), 
        ("CPU", 1500, 2), 
        ("Impressora", 700, 7)]

columns = ["Produto", "Preço", "Quantidade"]

# Criar DataFrame distribuído
df_spark = spark.createDataFrame(data, schema=columns)

# Exibir o DataFrame original
print("Dataset original com PySpark:")
df_spark.show()

# Calcular o valor total para cada produto
df_spark = df_spark.withColumn("Valor Total", df_spark["Preço"] * df_spark["Quantidade"])

# Exibir o DataFrame modificado com o valor total
print("\nDataset com o cálculo do valor total com PySpark:")
df_spark.show()

# Exibir estatísticas descritivas usando PySpark
print("\nEstatísticas descritivas com PySpark:")
df_spark.describe().show()

# Parar a SparkSession após o término
spark.stop()