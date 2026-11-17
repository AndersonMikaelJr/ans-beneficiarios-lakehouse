# Databricks notebook source
# MAGIC %md
# MAGIC # ANS Beneficiários: backfill (carga histórica)
# MAGIC Executa o notebook parametrizado `ans_lakehouse` uma vez por competência.
# MAGIC Como cada camada grava com `replaceWhere` (competência e UF), reexecutar um mês não duplica dados.

# COMMAND ----------

from datetime import datetime

NOTEBOOK = "/Workspace/Users/<seu-usuario>/ans_lakehouse"
UF = "ES"
COMPETENCIAS = ["202510", "202511", "202512"] + [f"2026{m:02d}" for m in range(1, 10)]

resultados = []

for comp in COMPETENCIAS:
    inicio = datetime.now()
    try:
        dbutils.notebook.run(
            NOTEBOOK, 1800,
            {"competencia": comp, "uf": UF, "forcar_falha": "nao"},
        )
        status = "OK"
    except Exception as erro:
        status = "ERRO: " + str(erro)[:80]
    duracao = (datetime.now() - inicio).seconds
    resultados.append((comp, status, duracao))
    print(comp, status, f"{duracao}s")

falhas = [r for r in resultados if r[1] != "OK"]
print(f"\n{len(resultados) - len(falhas)} OK | {len(falhas)} com erro")
