-- Databricks notebook source
-- MAGIC %fs
-- MAGIC ls /databricks-datasets/learning-spark-v2/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC airbnb = spark.read.parquet("/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")
-- MAGIC airbnb.createOrReplaceTempView("airbnb")

-- COMMAND ----------

SELECT * FROM airbnb;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC mkdirs /sandbox

-- COMMAND ----------

-- MAGIC %python
-- MAGIC airbnb.write.parquet("/sandbox/sf-airbnb-clean.parquet/")

-- COMMAND ----------

-- MAGIC %fs ls /sandbox/sf-airbnb-clean.parquet/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Leitura de dados em streaming utilizando o Spark
-- MAGIC     # Define o formato dos arquivos a serem lidos como "cloudFiles"
-- MAGIC     # Define o formato dos arquivos como "parquet"
-- MAGIC     # Define o local onde está localizado o esquema dos arquivos
-- MAGIC     # Define o modo de evolução do esquema como "rescue"
-- MAGIC     # Carrega os dados da fonte especificada ("/sandbox/sf-airbnb-clean.parquet/") 
-- MAGIC     # Define a saída do fluxo de dados
-- MAGIC     # Define a opção para mesclar esquemas, caso necessário, como verdadeira
-- MAGIC     # Define o local onde será armazenado o checkpoint do fluxo de dados
-- MAGIC     # Escreve os dados no formato de tabela do Spark SQL com o nome "AirBnbAutoLoader"
-- MAGIC
-- MAGIC spark.readStream \
-- MAGIC     .format("cloudFiles") \
-- MAGIC     .option("cloudFiles.format", "parquet") \
-- MAGIC     .option("cloudFiles.schemaLocation", "/sandbox/_schema") \
-- MAGIC     .option("cloudFiles.schemaEvolutionMode", "rescue") \
-- MAGIC     .load("/sandbox/sf-airbnb-clean.parquet/") \
-- MAGIC     .writeStream \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .option("checkpointLocation", "/sandbox/_checkpoint") \
-- MAGIC     .toTable("AirBnbAutoLoader")

-- COMMAND ----------

SELECT * FROM AirBnbAutoLoader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC airbnb_modified = spark.sql("""
-- MAGIC     SELECT *, 'smeagol' AS gollum
-- MAGIC     FROM AIRBNB
-- MAGIC     """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(airbnb_modified)

-- COMMAND ----------

SELECT COUNT(1) FROM AirBnbAutoLoader

-- COMMAND ----------

-- MAGIC %python
-- MAGIC airbnb_modified.write.mode("append").parquet("sandbox/sf-airbnb-clean.parquet/")

-- COMMAND ----------

SELECT * FROM AirBnbAutoLoader LIMIT 5;
