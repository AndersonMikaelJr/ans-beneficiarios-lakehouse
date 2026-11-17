-- Conferência do backfill: contagens por competência e camada (ES).
-- Bronze, Silver e Fato devem coincidir; rejeitados devem ser 0 (ou explicados).
SELECT b.competencia,
       b.n AS bronze, s.n AS silver, COALESCE(r.n, 0) AS rej,
       f.n AS fato, g.n AS agregado,
       s.ativos AS ativos_silver, f.ativos AS ativos_fato
FROM (SELECT competencia, COUNT(*) AS n
      FROM workspace.bronze.beneficiarios WHERE uf = 'ES' GROUP BY competencia) b
LEFT JOIN (SELECT competencia, COUNT(*) AS n, SUM(QT_BENEFICIARIO_ATIVO) AS ativos
           FROM workspace.silver.beneficiarios WHERE uf = 'ES' GROUP BY competencia) s
       ON s.competencia = b.competencia
LEFT JOIN (SELECT competencia, COUNT(*) AS n
           FROM workspace.silver.rejeitados WHERE uf = 'ES' GROUP BY competencia) r
       ON r.competencia = b.competencia
LEFT JOIN (SELECT competencia, COUNT(*) AS n, SUM(QT_BENEFICIARIO_ATIVO) AS ativos
           FROM workspace.gold.fato_beneficiarios WHERE uf = 'ES' GROUP BY competencia) f
       ON f.competencia = b.competencia
LEFT JOIN (SELECT competencia, COUNT(*) AS n
           FROM workspace.gold.beneficiarios_mes WHERE uf = 'ES' GROUP BY competencia) g
       ON g.competencia = b.competencia
ORDER BY b.competencia;

-- Perfil por competência: ajuda a achar meses atípicos (ex.: 202605).
SELECT competencia,
       COUNT(*) AS linhas,
       SUM(CASE WHEN QT_BENEFICIARIO_ATIVO = 0 THEN 1 ELSE 0 END) AS ativo_zero,
       COUNT(DISTINCT CD_PLANO) AS planos,
       COUNT(DISTINCT CD_OPERADORA) AS operadoras,
       COUNT(DISTINCT CD_MUNICIPIO) AS municipios
FROM workspace.silver.beneficiarios
WHERE uf = 'ES'
GROUP BY competencia
ORDER BY competencia;
