-- 0.1 Contagem de linhas por tabela
SELECT 'dim_data'            AS tabela, COUNT(*) AS total FROM dw.dim_data
UNION ALL
SELECT 'dim_titular',                   COUNT(*)         FROM dw.dim_titular
UNION ALL
SELECT 'dim_categoria',                 COUNT(*)         FROM dw.dim_categoria
UNION ALL
SELECT 'dim_estabelecimento',           COUNT(*)         FROM dw.dim_estabelecimento
UNION ALL
SELECT 'fato_transacao',                COUNT(*)         FROM dw.fato_transacao
ORDER BY tabela;

-- 0.2 Verificar se existem FK órfãs na fato
SELECT 'FK id_data órfã'       AS verificacao,
       COUNT(*) AS ocorrencias
FROM dw.fato_transacao f
LEFT JOIN dw.dim_data d ON f.id_data = d.id_data
WHERE d.id_data IS NULL
UNION ALL
SELECT 'FK id_titular órfã',
       COUNT(*)
FROM dw.fato_transacao f
LEFT JOIN dw.dim_titular t ON f.id_titular = t.id_titular
WHERE t.id_titular IS NULL
UNION ALL
SELECT 'FK id_categoria órfã',
       COUNT(*)
FROM dw.fato_transacao f
LEFT JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
WHERE c.id_categoria IS NULL
UNION ALL
SELECT 'FK id_estabelecimento órfã',
       COUNT(*)
FROM dw.fato_transacao f
LEFT JOIN dw.dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
WHERE e.id_estabelecimento IS NULL;

-- 0.3 Distribuição de transações por arquivo de origem (confirmar 12 faturas)
SELECT arquivo_origem,
       COUNT(*)            AS qtd_transacoes,
       SUM(valor_brl)      AS total_brl,
       MIN(d.data)         AS data_mais_antiga,
       MAX(d.data)         AS data_mais_recente
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
GROUP BY arquivo_origem
ORDER BY arquivo_origem;

-- 0.4 Checagem de estornos carregados corretamente
SELECT COUNT(*)       AS qtd_estornos,
       SUM(valor_brl) AS total_estornos_brl
FROM dw.fato_transacao
WHERE valor_brl < 0;

-- 1.1 Gasto total por titular no período completo
--     (excluindo estornos da soma bruta; apresentando ambos)
SELECT
    t.nome_titular,
    t.final_cartao,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl)                                    AS saldo_liquido_brl,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl < 0)    AS total_estornos_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl
FROM dw.fato_transacao f
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
GROUP BY t.nome_titular, t.final_cartao
ORDER BY total_compras_brl DESC;

-- 1.2 Gasto total por titular, discriminado por mês
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    t.nome_titular,
    t.final_cartao,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl)                                    AS saldo_liquido_brl,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl
FROM dw.fato_transacao f
JOIN dw.dim_data    d ON f.id_data    = d.id_data
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
GROUP BY d.ano, d.mes, d.nome_mes, t.nome_titular, t.final_cartao
ORDER BY d.ano, d.mes, total_compras_brl DESC;


-- 2.1 Top 10 categorias por valor total de compras
SELECT
    c.nome_categoria,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl,
    ROUND(
        100.0 * SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)
        / SUM(SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)) OVER (),
        2
    )                                                   AS pct_do_total
FROM dw.fato_transacao f
JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl > 0
GROUP BY c.nome_categoria
ORDER BY total_compras_brl DESC
LIMIT 10;

-- 2.2 Top 10 categorias por titular (quem gasta mais em cada categoria)
SELECT
    c.nome_categoria,
    t.nome_titular,
    t.final_cartao,
    COUNT(*)                 AS qtd_transacoes,
    SUM(f.valor_brl)         AS total_brl
FROM dw.fato_transacao f
JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
JOIN dw.dim_titular   t ON f.id_titular   = t.id_titular
WHERE f.valor_brl > 0
GROUP BY c.nome_categoria, t.nome_titular, t.final_cartao
ORDER BY c.nome_categoria, total_brl DESC;

-- 3.1 Total mensal geral (todos os titulares)
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    TO_CHAR(DATE_TRUNC('month', d.data), 'YYYY-MM') AS ano_mes,
    COUNT(*)                                         AS qtd_transacoes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0) AS total_compras_brl,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl < 0) AS total_estornos_brl,
    SUM(f.valor_brl)                                 AS saldo_liquido_brl
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
GROUP BY d.ano, d.mes, d.nome_mes, DATE_TRUNC('month', d.data)
ORDER BY d.ano, d.mes;

-- 3.2 Variação mês a mês (crescimento percentual)
WITH mensal AS (
    SELECT
        d.ano,
        d.mes,
        d.nome_mes,
        SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0) AS total_compras_brl
    FROM dw.fato_transacao f
    JOIN dw.dim_data d ON f.id_data = d.id_data
    GROUP BY d.ano, d.mes, d.nome_mes
)
SELECT
    ano,
    mes,
    nome_mes,
    total_compras_brl,
    LAG(total_compras_brl) OVER (ORDER BY ano, mes)  AS mes_anterior_brl,
    ROUND(
        100.0 * (total_compras_brl - LAG(total_compras_brl) OVER (ORDER BY ano, mes))
        / NULLIF(LAG(total_compras_brl) OVER (ORDER BY ano, mes), 0),
        2
    ) AS variacao_pct
FROM mensal
ORDER BY ano, mes;


-- 4.1 KPIs por titular: ticket médio, quantidade e total
SELECT
    t.nome_titular,
    t.final_cartao,
    COUNT(*)                                                AS qtd_total_transacoes,
    COUNT(*) FILTER (WHERE f.valor_brl > 0)                 AS qtd_compras,
    COUNT(*) FILTER (WHERE f.valor_brl < 0)                 AS qtd_estornos,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)         AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl,
    MAX(f.valor_brl) FILTER (WHERE f.valor_brl > 0)         AS maior_compra_brl,
    MIN(f.valor_brl) FILTER (WHERE f.valor_brl > 0)         AS menor_compra_brl
FROM dw.fato_transacao f
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
GROUP BY t.nome_titular, t.final_cartao
ORDER BY total_compras_brl DESC;

-- 4.2 Ranking de titulares por mês (quem gastou mais em cada mês)
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    t.nome_titular,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)  AS total_compras_brl,
    RANK() OVER (
        PARTITION BY d.ano, d.mes
        ORDER BY SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0) DESC
    )                                                  AS ranking_mes
FROM dw.fato_transacao f
JOIN dw.dim_data    d ON f.id_data    = d.id_data
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
GROUP BY d.ano, d.mes, d.nome_mes, t.nome_titular
ORDER BY d.ano, d.mes, ranking_mes;



-- 5.1 Top 15 estabelecimentos por valor total
SELECT
    e.nome_estabelecimento,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl
FROM dw.fato_transacao f
JOIN dw.dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
WHERE f.valor_brl > 0
GROUP BY e.nome_estabelecimento
ORDER BY total_compras_brl DESC
LIMIT 15;

-- 5.2 Top 10 estabelecimentos por titular
SELECT
    t.nome_titular,
    e.nome_estabelecimento,
    COUNT(*)         AS qtd_visitas,
    SUM(f.valor_brl) AS total_brl
FROM dw.fato_transacao f
JOIN dw.dim_titular        t ON f.id_titular        = t.id_titular
JOIN dw.dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
WHERE f.valor_brl > 0
GROUP BY t.nome_titular, e.nome_estabelecimento
ORDER BY t.nome_titular, total_brl DESC;

-- 6.1 Compras à vista vs parceladas (quantidade e valor)
SELECT
    CASE
        WHEN num_parcela IS NULL THEN 'À vista (Única)'
        ELSE 'Parcelada'
    END                                                 AS tipo_pagamento,
    COUNT(*)                                            AS qtd_transacoes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_quantidade,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl
FROM dw.fato_transacao f
WHERE f.valor_brl > 0
GROUP BY tipo_pagamento
ORDER BY qtd_transacoes DESC;

-- 6.2 Valor total de compras parceladas por titular
SELECT
    t.nome_titular,
    COUNT(*) FILTER (WHERE f.num_parcela IS NULL AND f.valor_brl > 0)    AS qtd_a_vista,
    COUNT(*) FILTER (WHERE f.num_parcela IS NOT NULL AND f.valor_brl > 0) AS qtd_parceladas,
    SUM(f.valor_brl) FILTER (WHERE f.num_parcela IS NULL AND f.valor_brl > 0)    AS valor_a_vista,
    SUM(f.valor_brl) FILTER (WHERE f.num_parcela IS NOT NULL AND f.valor_brl > 0) AS valor_parcelado
FROM dw.fato_transacao f
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
GROUP BY t.nome_titular
ORDER BY t.nome_titular;


-- 7.1 Volume e valor por dia da semana (todos os titulares)
SELECT
    d.dia_semana,
    d.nome_dia,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
WHERE f.valor_brl > 0
GROUP BY d.dia_semana, d.nome_dia
ORDER BY d.dia_semana;

-- 7.2 Fim de semana vs dias úteis
SELECT
    CASE WHEN d.eh_fim_semana THEN 'Fim de semana' ELSE 'Dia útil' END AS tipo_dia,
    COUNT(*)                                            AS qtd_transacoes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS total_compras_brl,
    ROUND(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 2) AS ticket_medio_brl
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
WHERE f.valor_brl > 0
GROUP BY d.eh_fim_semana
ORDER BY tipo_dia;

-- 8.1 Estornos por titular
SELECT
    t.nome_titular,
    t.final_cartao,
    COUNT(*)          AS qtd_estornos,
    SUM(f.valor_brl)  AS total_estornos_brl,
    MIN(f.valor_brl)  AS maior_estorno_brl 
FROM dw.fato_transacao f
JOIN dw.dim_titular t ON f.id_titular = t.id_titular
WHERE f.valor_brl < 0
GROUP BY t.nome_titular, t.final_cartao
ORDER BY total_estornos_brl ASC; 

-- 8.2 Estornos por categoria
SELECT
    c.nome_categoria,
    COUNT(*)          AS qtd_estornos,
    SUM(f.valor_brl)  AS total_estornos_brl
FROM dw.fato_transacao f
JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl < 0
GROUP BY c.nome_categoria
ORDER BY total_estornos_brl ASC;

-- 8.3 Impacto dos estornos no total líquido por mês
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)  AS total_bruto_brl,
    SUM(f.valor_brl) FILTER (WHERE f.valor_brl < 0)  AS total_estornos_brl,
    SUM(f.valor_brl)                                  AS saldo_liquido_brl
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
GROUP BY d.ano, d.mes, d.nome_mes
ORDER BY d.ano, d.mes;

-- 9.1 Transações com conversão de moeda
SELECT
    t.nome_titular,
    d.ano,
    d.mes,
    d.nome_mes,
    e.nome_estabelecimento,
    f.valor_usd,
    f.cotacao,
    f.valor_brl
FROM dw.fato_transacao f
JOIN dw.dim_data           d ON f.id_data           = d.id_data
JOIN dw.dim_titular        t ON f.id_titular         = t.id_titular
JOIN dw.dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
WHERE f.valor_usd IS NOT NULL
  AND f.valor_brl > 0
ORDER BY d.ano, d.mes, f.valor_usd DESC;

-- 9.2 Cotação média utilizada por mês
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    COUNT(*)                       AS qtd_transacoes_usd,
    ROUND(AVG(f.cotacao), 4)       AS cotacao_media,
    MIN(f.cotacao)                 AS cotacao_min,
    MAX(f.cotacao)                 AS cotacao_max,
    SUM(f.valor_usd)               AS total_usd,
    SUM(f.valor_brl)               AS total_brl_convertido
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data
WHERE f.valor_usd IS NOT NULL
  AND f.valor_brl > 0
GROUP BY d.ano, d.mes, d.nome_mes
ORDER BY d.ano, d.mes;


-- 10 — KPIs RESUMO (VISÃO EXECUTIVA)
SELECT
    'Período'                       AS kpi,
    MIN(d.data)::TEXT || ' a ' || MAX(d.data)::TEXT AS valor
FROM dw.fato_transacao f
JOIN dw.dim_data d ON f.id_data = d.id_data

UNION ALL

SELECT 'Total de transações',       COUNT(*)::TEXT
FROM dw.fato_transacao

UNION ALL

SELECT 'Total de compras (R$)',
       TO_CHAR(SUM(valor_brl) FILTER (WHERE valor_brl > 0), 'FM999G999G990D00')
FROM dw.fato_transacao

UNION ALL

SELECT 'Total de estornos (R$)',
       TO_CHAR(SUM(valor_brl) FILTER (WHERE valor_brl < 0), 'FM999G999G990D00')
FROM dw.fato_transacao

UNION ALL

SELECT 'Saldo líquido (R$)',
       TO_CHAR(SUM(valor_brl), 'FM999G999G990D00')
FROM dw.fato_transacao

UNION ALL

SELECT 'Ticket médio por compra (R$)',
       TO_CHAR(AVG(valor_brl) FILTER (WHERE valor_brl > 0), 'FM999G990D00')
FROM dw.fato_transacao

UNION ALL

SELECT 'Titulares distintos',       COUNT(DISTINCT id_titular)::TEXT
FROM dw.fato_transacao

UNION ALL

SELECT 'Categorias distintas',      COUNT(DISTINCT id_categoria)::TEXT
FROM dw.fato_transacao

UNION ALL

SELECT 'Estabelecimentos distintos', COUNT(DISTINCT id_estabelecimento)::TEXT
FROM dw.fato_transacao

UNION ALL

SELECT 'Transações parceladas',
       COUNT(*) FILTER (WHERE num_parcela IS NOT NULL AND valor_brl > 0)::TEXT
FROM dw.fato_transacao

UNION ALL

SELECT 'Transações em dólar',
       COUNT(*) FILTER (WHERE valor_usd IS NOT NULL AND valor_brl > 0)::TEXT
FROM dw.fato_transacao;
