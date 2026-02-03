
/*Query 1: Quais as 5 operadoras com maior crescimento percentual de despesas entre
o primeiro e o último trimestre analisado?*/

WITH despesas_ordenadas AS (
    SELECT
        registroans,
        razaosocial,
        ano,
        trimestre,
        CAST(REPLACE(valordespesas, ',', '.') AS DECIMAL(15,2)) AS despesas,
        ROW_NUMBER() OVER (PARTITION BY registroans ORDER BY ano, trimestre) AS rn_inicio,
        ROW_NUMBER() OVER (PARTITION BY registroans ORDER BY ano DESC, trimestre DESC) AS rn_fim
    FROM despesasconsolidadas
),
inicio_fim AS (
    SELECT
        registroans,
        razaosocial,
        MAX(CASE WHEN rn_inicio = 1 THEN despesas END) AS despesas_inicio,
        MAX(CASE WHEN rn_fim = 1 THEN despesas END) AS despesas_fim
    FROM despesas_ordenadas
    GROUP BY registroans, razaosocial
)
SELECT
    registroans,
    razaosocial,
    despesas_inicio,
    despesas_fim,
    ROUND(
        ((despesas_fim - despesas_inicio) / despesas_inicio) * 100,
        2
    ) AS crescimento_percentual
FROM inicio_fim
WHERE despesas_inicio > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;


/*Query 2: Qual a distribuição de despesas por UF? Liste os 5 estados com maiores
despesas totais.*/

SELECT
    uf,
    ROUND(
        SUM(CAST(REPLACE(valordespesas, ',', '.') AS DECIMAL(15,2))),
        2
    ) AS despesas_totais
FROM despesasconsolidadas
WHERE uf IS NOT NULL
GROUP BY uf
ORDER BY despesas_totais DESC
LIMIT 5;


/*Query 3: Quantas operadoras tiveram despesas acima da média geral em pelo menos
2 dos 3 trimestres analisados?*/

WITH media_geral AS (
    SELECT
        AVG(CAST(REPLACE(valordespesas, ',', '.') AS DECIMAL(15,2))) AS media
    FROM despesasconsolidadas
),
despesas_acima_media AS (
    SELECT
        d.registroans,
        d.trimestre,
        CAST(REPLACE(d.valordespesas, ',', '.') AS DECIMAL(15,2)) AS despesas
    FROM despesasconsolidadas d
    CROSS JOIN media_geral m
    WHERE CAST(REPLACE(d.valordespesas, ',', '.') AS DECIMAL(15,2)) > m.media
)
SELECT
    COUNT(*) AS qtd_operadoras
FROM (
    SELECT
        registroans
    FROM despesas_acima_media
    GROUP BY registroans
    HAVING COUNT(DISTINCT trimestre) >= 2
) t;
