-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE bronze_remuneracao (
  CONSTRAINT table_not_null EXPECT (nome IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT have_remuneration EXPECT (liquido != ",00") ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * 
FROM cloud_files(
  "mnt/sd_dbfs/",
  "csv",
  map(
    "multiLine", "true",
    "delimiter", ";",
    "header", "true",
    "enconding", "ISO-8859-1",
    "schema", "nome STRING, cpf STRING, cargo STRING, funcao STRING, situacaO STRING, ano STRING, codigo_do_orgao STRING, matricula STRING, remuneracao_basica STRING, beneficios STRING, valor_das_funcoes STRING, comissao_conselheiro STRING, hora_extra STRING, verbas_eventuais STRING, verbas_judiciais STRING, descontos_a_maior STRING, licenca_premio STRING, irrf STRING, seguridade_social STRING, teto_redutor STRING, outros_recebimentos STRING, outros_descontos_obrigatorios STRING, pagamento_a_maior STRING, bruto STRING, liquido STRING"
  )
)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_remuneracao(
  cpf STRING COMMENT "N CADASTRO",
  orgao STRING COMMENT "ORGAO VINCULADO AO SERVIDOR",
  cargo STRING,
  funcao STRING COOMENT "DESIGNACAO TEMPORARIA DE DETERMINADA FUNCAO",
  mes INT,
  ano INT,
  salario_bruto DOUBLE,
  salario_liquido DOUBLE
  CONSTRAINT person_exists EXPECT(cpf IS NOT NULL and funcao IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Domínios de remuneração dos servidores do Governo"
TBLPROPERTIES("quality" = "silver")
AS SELECT
  cpf,
  orgao,
  cargo,
  funcao,
  CAST(mes AS INT) AS mes,
  CAST(ano AS INT) AS ano,
  CAST(REPLACE(bruto, ',', '.') AS DOUBLE) AS salario_bruto,
  CAST(REPLACE(liquido, ',', '.') AS DOUBLE) AS salario_liquido
FROM STREAM(LIVE.bronze_remuneracao)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_remuneracao(
  cpf STRING COMMENT "N CADASTRO",
  orgao STRING COMMENT "ORGAO VINCULADO AO SERVIDOR",
  cargo STRING,
  funcao STRING COOMENT "DESIGNACAO TEMPORARIA DE DETERMINADA FUNCAO",
  mes INT,
  ano INT,
  salario_bruto DOUBLE,
  salario_liquido DOUBLE
  COMMENT "Conjunto de dados para agregação de valores de remunaração"
)
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
