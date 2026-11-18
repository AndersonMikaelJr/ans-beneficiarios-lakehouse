# Databricks notebook source
# MAGIC %md
# MAGIC # ANS Operadoras: cadastro (Silver) e dimensão (Gold)
# MAGIC Baixa o `Relatorio_cadop.csv` (operadoras ativas), grava `silver.operadoras`, monta `gold.dim_operadora`
# MAGIC e valida o join com broadcast contra `silver.beneficiarios`. Parâmetros: `competencia` (AAAAMM) e `uf`.

# COMMAND ----------

import os
import urllib.request
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

dbutils.widgets.text("competencia", "202609")
dbutils.widgets.text("uf", "ES")

COMPETENCIA = dbutils.widgets.get("competencia")
UF = dbutils.widgets.get("uf").upper()
PREDICADO = f"competencia = '{COMPETENCIA}' AND uf = '{UF}'"

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
CADOP = "/Volumes/workspace/bronze/raw/cadop/Relatorio_cadop.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download do cadastro

# COMMAND ----------

os.makedirs(os.path.dirname(CADOP), exist_ok=True)
urllib.request.urlretrieve(URL, CADOP)

with open(CADOP, encoding="utf-8", errors="replace") as f:
    for _ in range(3):
        print(f.readline())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura e seleção das colunas

# COMMAND ----------

cadop = (spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(CADOP))

cad = cadop.select(
    F.trim(F.col("REGISTRO_OPERADORA")).alias("CD_OPERADORA"),
    F.col("NOME_FANTASIA"),
    F.col("MODALIDADE").alias("MODALIDADE_CADASTRO"),
    F.col("UF").alias("UF_SEDE"),
    F.to_date(F.col("DATA_REGISTRO_ANS")).alias("DT_REGISTRO_ANS"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Join com broadcast (teste de cobertura)
# MAGIC O cadastro é pequeno (1.101 linhas), então vai por broadcast. O join não pode multiplicar linhas nem deixar beneficiários sem cadastro.

# COMMAND ----------

cad_unico = cad.dropDuplicates(["CD_OPERADORA"]).withColumn("NO_CADASTRO", F.lit(True))

silver = spark.table("workspace.silver.beneficiarios").filter(PREDICADO)
juntado = silver.join(broadcast(cad_unico), on="CD_OPERADORA", how="left")

total = silver.count()
linhas_juntadas = juntado.count()
sem_cadastro = juntado.filter(F.col("NO_CADASTRO").isNull()).count()

print(f"Silver: {total} | Após join: {linhas_juntadas} | Sem cadastro: {sem_cadastro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: `silver.operadoras`

# COMMAND ----------

cad_silver = cad.dropDuplicates(["CD_OPERADORA"])

cad_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.operadoras")

print(spark.table("workspace.silver.operadoras").count(), "operadoras gravadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: `gold.dim_operadora`

# COMMAND ----------

dim_operadora = (spark.table("workspace.silver.operadoras")
    .select(
        "CD_OPERADORA",
        F.coalesce(F.col("NOME_FANTASIA"), F.lit("(sem nome fantasia)")).alias("NOME_FANTASIA"),
        "MODALIDADE_CADASTRO",
        "UF_SEDE",
        "DT_REGISTRO_ANS",
    ))

dim_operadora.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.dim_operadora")

print(spark.table("workspace.gold.dim_operadora").count(), "operadoras na dimensão")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check: operadoras da fato sem cadastro na dimensão

# COMMAND ----------

fato = spark.table("workspace.gold.fato_beneficiarios").filter(PREDICADO)
dim = spark.table("workspace.gold.dim_operadora")

orfas = fato.select("CD_OPERADORA").distinct().join(dim, "CD_OPERADORA", "left_anti").count()
print("Operadoras da fato fora da dimensão:", orfas)
assert orfas == 0, "Há operadoras na fato sem cadastro na dimensão"
print("Check aprovado.")
