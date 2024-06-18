from pyspark.sql import SparkSession

# Iniciar Spark
spark = SparkSession.builder \
    .appName("Análise de Big Data") \
    .getOrCreate()

df = spark.read.csv('dados.csv', header=True, inferSchema=True)

df.printSchema()

df.show()

# Exemplo de limpeza (remover linhas com valores nulos)
df_limpo = df.dropna()

# Exemplo de processamento (calcular estatísticas)
estatisticas = df_limpo.describe()

# Corrigindo para agrupar por 'job_title' e contar o número de ocorrências
contagem_por_job_title = df_limpo.groupBy('job_title').count()

# Exibindo os resultados
contagem_por_job_title.show()
