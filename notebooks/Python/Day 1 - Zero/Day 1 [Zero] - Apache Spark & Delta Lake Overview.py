# Databricks notebook source
# MAGIC %md
# MAGIC ### Nota importante!
# MAGIC
# MAGIC Este notebook funcionou com uma versão 12.2 LTS Runtime!
# MAGIC
# MAGIC Não há preocupações quanto ao tamanho do cluster (memória e núcleos).
# MAGIC
# MAGIC O objetivo deste notebook é apenas mostrar a versão dos comandos SQL para Python, **sempre use a versão SQL como referência**, como foi o usado durante o curso Databricks SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos mágicos do Databricks Notebook
# MAGIC
# MAGIC | Comando | Resultado |
# MAGIC |--------|--------|
# MAGIC | `%lsmagic` | Lista todos os comandos mágicos. |
# MAGIC | `%md` | Célula como um Markdown. |
# MAGIC | `%sql` | UsarSQL. |
# MAGIC | `%python` | Utilize Python. |
# MAGIC | `%scala` | Usar Scala. |
# MAGIC | `%r` | Usar R. |
# MAGIC | `%run` | Executa um arquivo Python ou outro notebook. |
# MAGIC | `%who` | Mostra todas as variáveis. |
# MAGIC | `%env` | Permite inserir variáveis ​​de ambiente. |
# MAGIC | `%fs` | Permite usar comandos do sistema de arquivos dos utilitários DBFD. |
# MAGIC | `%sh` | Executa comandos Shell no cluster. |
# MAGIC | `%matplotlib` | Recursos de back-end do Matplotlib. |
# MAGIC | `%config` | Pode definir configurações para notebook. |
# MAGIC | `%pip` | Instale pacotes Python. |
# MAGIC | `%load` | Carrega o conteúdo de um arquivo em uma célula. |
# MAGIC | `%reload` | Recarrega o conteúdo. |
# MAGIC | `%jobs` | Lista trabalhos em execução. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Apache Spark?
# MAGIC
# MAGIC Apache Spark é um mecanismo de processamento capaz de analisar dados usando SQL, Python, Scala, R e Java. Também possui estruturas para permitir aprendizado de máquina, processamento de gráficos ou streaming:
# MAGIC
# MAGIC ![Spark Engines](https://github.com/tiluan/trn-databricks-sql-zero-to-hero/blob/main/images/spark-engine.png?raw=true)
# MAGIC <br/>
# MAGIC <br/>
# MAGIC
# MAGIC * A API DataFrames funciona devido a uma abstração acima dos RDDs, porém ganhando algo em torno de 5 a 20x em relação aos RDDs tradicionais com seu Catalyst Optimizer.
# MAGIC
# MAGIC ![Spark Unified Engine](https://github.com/tiluan/trn-databricks-sql-zero-to-hero/blob/main/images/unified-engine.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Arquitetura Spark Cluster: Drivers, Executores, Slots e Tarefas
# MAGIC ![Spark Physical Cluster, slots](https://github.com/tiluan/trn-databricks-sql-zero-to-hero/blob/main/images/spark-driver.png?raw=true)
# MAGIC
# MAGIC Ao criar um cluster, você pode escolher entre **nó único** ou **vários nós**, onde deve incluir o tipo de máquina do driver e também o tipo de máquina de trabalho

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Spark Jobs, avaliação preguiçosa, transformações e ações
# MAGIC ![Spark Jobs, Lazy Evaluation, Transformations & Actions](https://github.com/tiluan/trn-databricks-sql-zero-to-hero/blob/main/images/spark_bookclub.png?raw=true)
# MAGIC
# MAGIC
# MAGIC * Toda execução do Apache Spark é classificada como Spark Job, que posteriormente é dividida em etapas que irão agrupar tarefas (menor unidade de execução de trabalho).
# MAGIC * Como o Apache Spark é anexado ao modelo Lazy Evaluation, cada processo pode ser classificado em **ação** OU **transformação**.
# MAGIC * Cada Spark Job é gerado por um comando de ação, enquanto os estágios serão organizados devido a funções de consulta embaralhadas e de otimização.
# MAGIC * Embaralhar? Sim, as transformações Spark também podem ser divididas em: **transformações estreitas** e **transformações amplas (shuffle)**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alguns exemplos de Spark Action
# MAGIC
# MAGIC | Comando | Resultado |
# MAGIC |--------|--------|
# MAGIC | `collect()` | Retorna uma matriz que contém todas as linhas deste conjunto de dados. |
# MAGIC | `count()` | Retorna o número de linhas no conjunto de dados. |
# MAGIC | `first()` | Retorna a primeira linha. |
# MAGIC | `foreach(f)` | Aplica uma função f a todas as linhas. |
# MAGIC | `head()` | Retorna a primeira linha. |
# MAGIC | `show(..)` | Exibe as 20 principais linhas do conjunto de dados em formato tabular. |
# MAGIC | `take(n)` | Retorna as primeiras n linhas do conjunto de dados. |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alguns exemplos de transformação Spark
# MAGIC
# MAGIC | Comando | Tipo | Resultado |
# MAGIC |--------|--------|-------------|
# MAGIC | `filter()` | Estreito | Retorna um novo DataFrame após aplicar a função de filtro no conjunto de dados de origem. |
# MAGIC | `distinct()` | Largo | Retorna um novo DataFrame contendo as linhas distintas neste DataFrame. |
# MAGIC | `join()` | Largo | Junta-se a outro DataFrame, usando a expressão de junção fornecida. |
# MAGIC | `union()` | Estreito | Combina dois DataFrames e retorna o novo DataFrame. |
# MAGIC | `repartition()` | Largo | Retorna um novo DataFrame (particionado por hash) particionado pelas expressões de particionamento fornecidas. |
# MAGIC | `groupBy()` | Largo | Agrupa o DataFrame usando as colunas especificadas, para que possamos executar a agregação nelas. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Delta Lake?

# COMMAND ----------

# MAGIC %md
# MAGIC ![Delta Lake](https://github.com/tiluan/trn-databricks-sql-zero-to-hero/blob/main/images/delta-lake.png?raw=true)
# MAGIC
# MAGIC * Delta Lake é uma estrutura de armazenamento de código aberto que permite construir uma arquitetura Lakehouse com mecanismos de computação, atuando como um **formato de tabela nativo em Databricks**, usando Parquet como formato físico para anexar todos os dados.
# MAGIC * As tabelas Delta são criadas com base nas **garantias ACID** fornecidas pelo protocolo Delta Lake de código aberto. ACID significa atomicidade, consistência, isolamento e durabilidade.
# MAGIC * O Delta Lake pode implementar as propriedades ACID devido aos **controles de log de transações** salvos durante **cada confirmação** nos dados. Durante uma transação, os arquivos de dados são gravados no diretório de arquivos que suporta a tabela. Quando a transação for concluída, uma nova entrada será confirmada no log de transações que inclui os caminhos para todos os arquivos gravados durante a transação. Cada commit incrementa a versão da tabela e torna novos arquivos de dados visíveis para operações de leitura.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando uma tabela Delta

# COMMAND ----------

df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
table_name = "people_10millions"
df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/learning-spark-v2/people/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo uma tabela Delta

# COMMAND ----------

df_people = spark.read.table("default.people_10millions")
display(df_people.take(5))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/databricks-datasets/learning-spark-v2/people/'

# COMMAND ----------

display(df_people.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtendo detalhes sobre a tabela Delta (propriedades, tamanho, localização, etc.)

# COMMAND ----------

display(df_people.describe())

# COMMAND ----------

df_people = spark.read.format("delta").table("people_10millions")
df_people.createOrReplaceTempView("people_10millions")
display(spark.sql("DESCRIBE TABLE EXTENDED people_10millions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando o histórico da tabela Delta (criação, upserts, exclusões, etc)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL people_10millions;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hive_metastore.default.people_10millions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alterando Tabela Delta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserindo novos dados

# COMMAND ----------

from pyspark.sql.functions import when

df_people = df_people.withColumn("gender", when(df_people.gender == "M", "H")
                                        .when(df_people.gender == "F", "M")
                                        .otherwise(df_people.gender))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removendo linhas/valores

# COMMAND ----------

# df_people.delete("birthDate >= '2000-01-01'")
# DELETE FROM hive_metastore.default.people_10millions WHERE birthDate >= '2000-01-01'

df_people = df_people.filter("birthDate < '2000-01-01'")

# COMMAND ----------

display(df_people.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando novamente o histórico da tabela Delta (criação, upserts, exclusões, etc)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hive_metastore.default.people_10millions;
# MAGIC
# MAGIC -- display(df_people.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resgatando através da Viagem no Tempo para recuperar a versão antiga da Tabela Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM hive_metastore.default.people_10millions VERSION AS OF 0
# MAGIC CREATE OR REPLACE TABLE hive_metastore.default.people_10millions AS SELECT * FROM hive_metastore.default.people_10millions VERSION AS OF 2
# MAGIC
# MAGIC -- spark.sql("CREATE OR REPLACE TABLE hive_metastore.default.people_10millions AS SELECT * FROM hive_metastore.default.people_10millions VERSION AS OF 0")

# COMMAND ----------

df_people.describe()

# COMMAND ----------

display(df_people.describe())

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/people_10millions/_delta_log')

# COMMAND ----------

spark.read.json('/user/hive/warehouse/people_10millions/_delta_log/00000000000000000003.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando e se livrando da história

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM hive_metastore.default.people_10millions
# MAGIC -- df_people.vacuum()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM hive_metastore.default.people_10millions
# MAGIC
# MAGIC --display(df_people.count())
