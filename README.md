# Analises-Climaticas-com-Pyspark

Este projeto utiliza o PySpark para analisar dados climáticos provenientes de um arquivo CSV armazenado no Amazon S3. O objetivo é filtrar dados de um ano específico e calcular a média de temperatura por estação, salvando os resultados de volta no S3.


Como Usar: 

Configurar suas credenciais AWS: Altere as chaves de acesso diretamente no código ou configure variáveis de ambiente.

Ajustar o caminho do CSV: Verifique se o caminho do arquivo CSV no S3 está correto: df = spark.read.csv("s3a://nomedoseuarquivocsv.csv", header=True, inferSchema=True)

