# Databricks notebook source
# MAGIC %md
# MAGIC # ANS Beneficiários: carga parametrizada (Bronze → Silver → Gold)
# MAGIC Parâmetros: `competencia` (AAAAMM), `uf` (ex.: ES) e `forcar_falha` (para testar retry).

# COMMAND ----------

import os
import urllib.request
import zipfile
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

dbutils.widgets.text("competencia", "202609")
dbutils.widgets.text("uf", "ES")
dbutils.widgets.dropdown("forcar_falha", "nao", ["nao", "sim"])

COMPETENCIA = dbutils.widgets.get("competencia")
UF = dbutils.widgets.get("uf").upper()
FORCAR_FALHA = dbutils.widgets.get("forcar_falha") == "sim"

PREDICADO = f"competencia = '{COMPETENCIA}' AND uf = '{UF}'"
LIMITE_REJEICAO = 0.01

BRONZE = "workspace.bronze.beneficiarios"
SILVER = "workspace.silver.beneficiarios"
REJEITADOS = "workspace.silver.rejeitados"
FATO = "workspace.gold.fato_beneficiarios"
AGREGADO = "workspace.gold.beneficiarios_mes"
CONTROLE = "workspace.ops.carga_controle"

QT = ["QT_BENEFICIARIO_ATIVO", "QT_BENEFICIARIO_ADERIDO", "QT_BENEFICIARIO_CANCELADO"]
CHAVE = [
    "ID_CMPT_MOVEL", "CD_OPERADORA", "CD_MUNICIPIO", "TP_SEXO",
    "DE_FAIXA_ETARIA", "DE_FAIXA_ETARIA_REAJ", "CD_PLANO",
    "TP_VIGENCIA_PLANO", "DE_CONTRATACAO_PLANO", "DE_SEGMENTACAO_PLANO",
    "DE_ABRG_GEOGRAFICA_PLANO", "COBERTURA_ASSIST_PLAN", "TIPO_VINCULO",
]
COLUNAS = [
    "ID_CMPT_MOVEL", "CD_OPERADORA", "NM_RAZAO_SOCIAL", "NR_CNPJ",
    "MODALIDADE_OPERADORA", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO",
    "TP_SEXO", "DE_FAIXA_ETARIA", "DE_FAIXA_ETARIA_REAJ", "CD_PLANO",
    "TP_VIGENCIA_PLANO", "DE_CONTRATACAO_PLANO", "DE_SEGMENTACAO_PLANO",
    "DE_ABRG_GEOGRAFICA_PLANO", "COBERTURA_ASSIST_PLAN", "TIPO_VINCULO",
    "QT_BENEFICIARIO_ATIVO", "QT_BENEFICIARIO_ADERIDO",
    "QT_BENEFICIARIO_CANCELADO", "DT_CARGA",
]

ANO, MES = COMPETENCIA[:4], COMPETENCIA[4:]
CSV_DIR = f"/Volumes/workspace/bronze/raw/competencia={COMPETENCIA}"
CSV_PATH = f"{CSV_DIR}/pda-024-icb-{UF}-{ANO}_{MES}.csv"

# COMMAND ----------

def garantir_csv():
    """Baixa e extrai o ZIP da ANS só se o CSV ainda não estiver no volume."""
    if os.path.exists(CSV_PATH):
        return
    url = (
        "https://dadosabertos.ans.gov.br/FTP/PDA/"
        "informacoes_consolidadas_de_beneficiarios-024/"
        f"{COMPETENCIA}/pda-024-icb-{UF}-{ANO}_{MES}.zip"
    )
    os.makedirs(CSV_DIR, exist_ok=True)
    zip_local = f"/tmp/{UF}_{COMPETENCIA}.zip"
    urllib.request.urlretrieve(url, zip_local)
    with zipfile.ZipFile(zip_local) as z:
        z.extractall(CSV_DIR)


def padronizar_textos(df):
    for c, tipo in df.dtypes:
        if tipo == "string":
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", F.lit(None).cast("string"))
                 .otherwise(F.trim(F.col(c))),
            )
    return df


def separar(df_texto):
    df = padronizar_textos(df_texto)
    for c in QT:
        df = df.withColumn(f"{c}_num", F.expr(f"try_cast({c} AS INT)"))

    regras = [(
        F.col("ID_CMPT_MOVEL").isNull() | F.col("CD_OPERADORA").isNull() | F.col("CD_MUNICIPIO").isNull(),
        "chave_nula",
    )]
    for c in QT:
        regras += [
            (F.col(c).isNull(), "quantidade_vazia"),
            (F.col(c).isNotNull() & F.col(f"{c}_num").isNull(), "falha_conversao"),
            (F.col(f"{c}_num") < 0, "quantidade_negativa"),
        ]

    motivo = F.coalesce(*[F.when(cond, F.lit(nome)) for cond, nome in regras])
    df = df.withColumn("motivo_rejeicao", motivo)
    rejeitados = df.filter(F.col("motivo_rejeicao").isNotNull())
    aceitos = df.filter(F.col("motivo_rejeicao").isNull())
    return aceitos, rejeitados


def tipar(df):
    df = (
        df.withColumn("ID_CMPT_MOVEL", F.to_date(F.concat(F.col("ID_CMPT_MOVEL"), F.lit("-01"))))
          .withColumn("DT_CARGA", F.to_date(F.col("DT_CARGA")))
    )
    for c in QT:
        df = df.withColumn(c, F.col(f"{c}_num")).drop(f"{c}_num")
    return df.drop("motivo_rejeicao")


def gravar(df, tabela, predicado):
    """Sobrescreve só o recorte do predicado; se a tabela não existir, cria."""
    existe = spark.catalog.tableExists(tabela)
    escrita = df.write.format("delta").mode("overwrite")
    if existe:
        escrita = escrita.option("replaceWhere", predicado)
    escrita.saveAsTable(tabela)


def registrar(status, inicio, bronze=None, silver=None, rej=None, msg=None):
    schema = (
        "competencia string, uf string, status string, linhas_bronze long, "
        "linhas_silver long, linhas_rejeitadas long, iniciado_em timestamp, "
        "finalizado_em timestamp, mensagem string"
    )
    linha = [(COMPETENCIA, UF, status, bronze, silver, rej, inicio, datetime.now(), msg)]
    spark.createDataFrame(linha, schema).write.format("delta").mode("append").saveAsTable(CONTROLE)


spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE} (
    competencia string, uf string, status string,
    linhas_bronze long, linhas_silver long, linhas_rejeitadas long,
    iniciado_em timestamp, finalizado_em timestamp, mensagem string
) USING DELTA
""")

# COMMAND ----------

inicio = datetime.now()

try:
    # 1. Extração e Bronze
    garantir_csv()
    schema = StructType([StructField(c, StringType(), True) for c in COLUNAS])
    bronze_df = (
        spark.read.schema(schema)
        .option("header", True).option("sep", ";").option("encoding", "UTF-8")
        .csv(CSV_PATH)
        .select("*", F.col("_metadata.file_name").alias("arquivo_origem"))
        .withColumn("competencia", F.lit(COMPETENCIA))
        .withColumn("uf", F.lit(UF))
        .withColumn("carregado_em", F.current_timestamp())
    )
    gravar(bronze_df, BRONZE, PREDICADO)
    n_bronze = spark.table(BRONZE).filter(PREDICADO).count()
    print(f"Bronze: {n_bronze} linhas")

    # Ponto de falha provocada, para testar o retry do Job.
    if FORCAR_FALHA:
        raise RuntimeError("Falha provocada para teste de retry.")

    # 2. Silver: separa, valida o limite e grava
    bronze = spark.table(BRONZE).filter(PREDICADO)
    aceitos, rejeitados = separar(bronze)
    n_rej = rejeitados.count()
    taxa = n_rej / n_bronze
    print(f"Rejeitados: {n_rej} | Taxa: {taxa:.4%}")
    if taxa > LIMITE_REJEICAO:
        raise ValueError(f"Taxa de rejeição {taxa:.4%} acima do limite. Nada foi gravado na Silver.")

    gravar(tipar(aceitos.dropDuplicates(CHAVE)), SILVER, PREDICADO)
    gravar(rejeitados.select(*bronze.columns, "motivo_rejeicao"), REJEITADOS, PREDICADO)
    n_silver = spark.table(SILVER).filter(PREDICADO).count()

    # 3. Gold: fato e agregado, a partir da Silver
    fato = (spark.table(SILVER).filter(PREDICADO)
        .select("competencia", "uf", *CHAVE, *QT, "DT_CARGA"))
    gravar(fato, FATO, PREDICADO)

    agregado = (spark.table(FATO).filter(PREDICADO)
        .groupBy("competencia", "uf", "CD_OPERADORA", "CD_MUNICIPIO")
        .agg(
            F.sum("QT_BENEFICIARIO_ATIVO").alias("QT_ATIVO"),
            F.sum("QT_BENEFICIARIO_ADERIDO").alias("QT_ADERIDO"),
            F.sum("QT_BENEFICIARIO_CANCELADO").alias("QT_CANCELADO"),
        ))
    gravar(agregado, AGREGADO, PREDICADO)

    # 4. Checks de reconciliação: se algo divergir, a carga falha
    soma_silver = spark.table(SILVER).filter(PREDICADO).agg(F.sum("QT_BENEFICIARIO_ATIVO")).first()[0]
    soma_fato = spark.table(FATO).filter(PREDICADO).agg(F.sum("QT_BENEFICIARIO_ATIVO")).first()[0]
    soma_agreg = spark.table(AGREGADO).filter(PREDICADO).agg(F.sum("QT_ATIVO")).first()[0]
    assert soma_silver == soma_fato == soma_agreg, "Totais divergentes entre as camadas"
    assert n_silver + n_rej <= n_bronze, "Silver + rejeitados maior que a Bronze"

    # 5. Log de sucesso
    registrar("SUCESSO", inicio, bronze=n_bronze, silver=n_silver, rej=n_rej)
    print(f"Carga concluída: {COMPETENCIA} / {UF}")

except Exception as erro:
    registrar("FALHA", inicio, msg=str(erro)[:500])
    raise

# COMMAND ----------

spark.table(CONTROLE).orderBy(F.col("finalizado_em").desc()).show(10, truncate=False)
