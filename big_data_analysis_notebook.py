#Importações
import pandas as pd
from pyspark.sql import SparkSession

# ---- Parte 1: Análise Local com pandas ----
# Carregar o dataset local com pandas
data = {
    'Produto': ['Teclado', 'Mouse', 'Monitor', 'CPU', 'Impressora'],
    'Preço': [120, 75, 250, 1500, 700],
    'Quantidade': [10, 25, 5, 2, 7]
}
df_pandas = pd.DataFrame(data)

# Exibir o DataFrame com pandas
print("Análise local com pandas:")
print(df_pandas)

# Calcular o valor total com pandas
df_pandas['Valor Total'] = df_pandas['Preço'] * df_pandas['Quantidade']
print("\nDataset modificado com pandas:")
print(df_pandas)


# ---- Parte 2: Análise Distribuída com PySpark ----
# Inicializar a SparkSession
spark = SparkSession.builder \
    .appName("Big Data Analytics com PySpark") \
    .getOrCreate()

# Criar DataFrame distribuído em PySpark
df_spark = spark.createDataFrame(df_pandas)

# Exibir o DataFrame com PySpark
print("\nAnálise distribuída com PySpark:")
df_spark.show()

# Calcular o valor total com PySpark
df_spark = df_spark.withColumn("Valor Total", df_spark["Preço"] * df_spark["Quantidade"])
df_spark.show()

# Exibir estatísticas descritivas com PySpark
df_spark.describe().show()

# Parar a SparkSession
spark.stop()
