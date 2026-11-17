-- Documentação das tabelas no Unity Catalog (comentários e tags).
-- Rodar no SQL Editor do Databricks.

COMMENT ON TABLE workspace.bronze.beneficiarios IS
  'Camada Bronze: dados brutos de beneficiários da ANS (PDA 024), todas as colunas como texto, mais arquivo_origem, competencia, uf e carregado_em. Recarga idempotente por competencia e uf (replaceWhere).';
COMMENT ON TABLE workspace.silver.beneficiarios IS
  'Camada Silver: beneficiários tipados e sem duplicatas na chave natural de 13 colunas. Quantidades como inteiro e competência como data. Fonte: bronze.beneficiarios.';
COMMENT ON TABLE workspace.silver.rejeitados IS
  'Camada Silver: linhas da Bronze rejeitadas na validação (chave nula, quantidade vazia, falha de conversão ou negativa), com a coluna motivo_rejeicao. Limite de rejeição da carga: 1%.';
COMMENT ON TABLE workspace.silver.operadoras IS
  'Camada Silver: cadastro de operadoras ativas da ANS (Relatorio_cadop.csv), uma linha por CD_OPERADORA. Usada para montar a dimensão gold.dim_operadora.';
COMMENT ON TABLE workspace.gold.dim_operadora IS
  'Camada Gold: dimensão de operadoras (código, nome fantasia, modalidade, UF da sede e data de registro na ANS). Chave: CD_OPERADORA. Fonte: silver.operadoras.';
COMMENT ON TABLE workspace.gold.fato_beneficiarios IS
  'Beneficiários ANS por competência, UF, operadora, município e plano. Grão: uma linha por combinação da CHAVE. Fonte: silver.beneficiarios.';
COMMENT ON COLUMN workspace.gold.fato_beneficiarios.QT_BENEFICIARIO_ATIVO IS
  'Quantidade de beneficiários ativos na competência.';
COMMENT ON TABLE workspace.gold.beneficiarios_mes IS
  'Camada Gold: totais de beneficiários ativos, aderidos e cancelados por competência, UF, operadora e município. Fonte: gold.fato_beneficiarios.';
COMMENT ON TABLE workspace.ops.carga_controle IS
  'Controle operacional: uma linha por tentativa de carga (competência, UF, status SUCESSO ou FALHA, contagens por camada, horários em UTC e mensagem de erro). Tabela somente de inserção.';

ALTER TABLE workspace.bronze.beneficiarios SET TAGS ('camada' = 'bronze', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.silver.beneficiarios SET TAGS ('camada' = 'silver', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.silver.rejeitados SET TAGS ('camada' = 'silver', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.silver.operadoras SET TAGS ('camada' = 'silver', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.gold.dim_operadora SET TAGS ('camada' = 'gold', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.gold.fato_beneficiarios SET TAGS ('camada' = 'gold', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.gold.beneficiarios_mes SET TAGS ('camada' = 'gold', 'dominio' = 'saude_suplementar');
ALTER TABLE workspace.ops.carga_controle SET TAGS ('camada' = 'ops', 'dominio' = 'saude_suplementar');
