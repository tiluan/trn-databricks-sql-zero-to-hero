-- Databricks notebook source
CREATE OR REFRESH TREAMING TABLE bronze_remuneracao (
  CONSTRAINT table_not_null EXPECT (count > 0) ON VALIDATION FAIL UPDATE
)
TBLPROPERTIES("quality" = "bronze")
AS SELECT * 
FROM cloud_files(
  "mnt/sd_dbfs/",
  "csv",
  map(
    "multiLine", "true",
    "delimiter", ";",
    "header", "true",
    "encondig", "ISO-8859-1"
  )
)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_remuneracao(
  CONSTRAINT person_exists EXPECT(cpf IS NOT NULL and funcao IS NOT NULL) ON DROP ROW
)
TBLPROPERTIES("quality" = "silver")
AS SELECT
  cpf COMMENT "N CADASTRO",
  orgao COMMENT "ORGAO VINCULADO AO SERVIDOR",
  cargo,
  funcao COOMENT "DESIGNACAO TEMPORARIA DE DETERMINADA FUNCAO",
  CAST(mes AS INT) AS mes,
  CAST(ano AS INT) AS ano,
  CAST(REPLACE(bruto, ',', '.') AS DOUBLE) AS salario_bruto,
  CAST(REPLACE(liquido, ',', '.') AS DOUBLE) AS salario_liquido
FROM STREAM(LIVE.bronze_remuneracao)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_remuneracao
COMMENT "Conjunto de dados para agregação de valores de remunaração"
TBLPROPERTIES("quality", "gold")
AS SELECT
  orgao COMMENT "ORGAO VINCULADO AO SERVIDOR",
  cargo,
  funcao COOMENT "DESIGNACAO TEMPORARIA DE DETERMINADA FUNCAO",
  mes,
  ano,
  SUM(salario_bruto),
  SUM(salario_liquido)
FROM STREAM(LIVE.silver_remuneracao)
GROUP BY 1, 2, 3, 4, 5, 6
