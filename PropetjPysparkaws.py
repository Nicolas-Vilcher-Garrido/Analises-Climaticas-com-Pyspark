from pyspark.sql import SparkSession
import os

# Criação da SparkSession
spark = SparkSession.builder.appName("ClimateAnalysis").getOrCreate()   

# Configurar credenciais da AWS através de variáveis de ambiente
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("SUA CHAVE DE ACESSO AWS")) - Não coloquei a minha por questões de segurança
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("SUA CHAVE DE ACESSO SECRETA AWS")) - Não coloquei a minha por questões de segurança
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "SUA REGIÃO DE ACESSO") - Não coloquei a minha por questões de segurança

# Ler o arquivo CSV do S3
df = spark.read.csv("s3a://projetops/01001099999.csv", header=True, inferSchema=True)
df.show(5)

# Selecionar colunas importantes e renomear
df = df.select("STATION", "DATE", "TEMP", "DEWP", "WDSP", "MAX", "MIN")

# Filtrar dados de um ano específico, por exemplo, 2020
df_filtered = df.filter(df["DATE"].contains("2020"))

# Agrupar por estação e calcular a média de temperatura
df_grouped = df_filtered.groupBy("STATION").avg("TEMP")
df_grouped.show()

# Salvar resultados no S3
df_grouped.write.csv("s3a://projetops/climate_analysis_output.csv", header=True)
