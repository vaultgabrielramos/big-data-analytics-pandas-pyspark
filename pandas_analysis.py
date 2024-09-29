import pandas as pd

# Carregar o dataset local
data = {
    'Produto': ['Teclado', 'Mouse', 'Monitor', 'CPU', 'Impressora'],
    'Preço': [120, 75, 250, 1500, 700],
    'Quantidade': [10, 25, 5, 2, 7]
}

df = pd.DataFrame(data)

# Visualizar as primeiras linhas do dataset
print("Dataset original:")
print(df)

# Calcular o valor total para cada produto
df['Valor Total'] = df['Preço'] * df['Quantidade']

# Exibir o dataset com os valores totais
print("\nDataset com o cálculo do valor total:")
print(df)

# Exibir estatísticas descritivas
print("\nEstatísticas descritivas:")
print(df.describe())