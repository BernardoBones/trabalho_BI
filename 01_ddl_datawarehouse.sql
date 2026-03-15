CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================
-- DIMENSÃO: DIM_DATA
-- Granularidade: 1 linha por dia único presente nas transações
-- ============================================================
CREATE TABLE dw.dim_data (
    id_data       SERIAL        PRIMARY KEY,
    data          DATE          NOT NULL UNIQUE,
    dia           SMALLINT      NOT NULL CHECK (dia BETWEEN 1 AND 31),
    mes           SMALLINT      NOT NULL CHECK (mes BETWEEN 1 AND 12),
    nome_mes      VARCHAR(20)   NOT NULL,          -- 'Janeiro', 'Fevereiro', ...
    trimestre     SMALLINT      NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    ano           SMALLINT      NOT NULL,
    dia_semana    SMALLINT      NOT NULL CHECK (dia_semana BETWEEN 1 AND 7),
                                                    -- 1=Segunda ... 7=Domingo (ISO)
    nome_dia      VARCHAR(20)   NOT NULL,           -- 'Segunda-feira', etc.
    eh_fim_semana BOOLEAN       NOT NULL DEFAULT FALSE
);


-- ============================================================
-- DIMENSÃO: DIM_TITULAR
-- Granularidade: 1 linha por combinação única (nome + final_cartao)
-- ============================================================
CREATE TABLE dw.dim_titular (
    id_titular    SERIAL        PRIMARY KEY,
    nome_titular  VARCHAR(100)  NOT NULL,
    final_cartao  CHAR(4)       NOT NULL CHECK (final_cartao ~ '^\d{4}$'),
    UNIQUE (nome_titular, final_cartao)
);

-- ============================================================
-- DIMENSÃO: DIM_CATEGORIA
-- Granularidade: 1 linha por categoria MCC única
-- ============================================================
CREATE TABLE dw.dim_categoria (
    id_categoria    SERIAL        PRIMARY KEY,
    nome_categoria  VARCHAR(150)  NOT NULL UNIQUE
);


-- ============================================================
-- DIMENSÃO: DIM_ESTABELECIMENTO
-- Granularidade: 1 linha por nome de estabelecimento único
-- ============================================================
CREATE TABLE dw.dim_estabelecimento (
    id_estabelecimento  SERIAL        PRIMARY KEY,
    nome_estabelecimento VARCHAR(255) NOT NULL UNIQUE
);

-- ============================================================
-- TABELA FATO: FATO_TRANSACAO
-- Granularidade: 1 linha por transação de cartão de crédito
-- ============================================================
CREATE TABLE dw.fato_transacao (
    id_transacao        BIGSERIAL     PRIMARY KEY,

    -- Chaves estrangeiras para as dimensões
    id_data             INT           NOT NULL REFERENCES dw.dim_data(id_data),
    id_titular          INT           NOT NULL REFERENCES dw.dim_titular(id_titular),
    id_categoria        INT           NOT NULL REFERENCES dw.dim_categoria(id_categoria),
    id_estabelecimento  INT           NOT NULL REFERENCES dw.dim_estabelecimento(id_estabelecimento),

    -- Medidas financeiras
    valor_brl           NUMERIC(12,2) NOT NULL,   -- Valor em R$ (negativo = estorno/crédito)
    valor_usd           NUMERIC(12,2),             -- Valor em US$ (NULL quando não aplicável)
    cotacao             NUMERIC(10,4),             -- Cotação USD→BRL usada na conversão

    -- Atributos de parcelamento (mantidos na fato conforme decisão de modelagem)
    parcela_texto       VARCHAR(10),               -- Texto original: 'Única', '1/3', '2/10', etc.
    num_parcela         SMALLINT,                  -- Número da parcela atual (NULL quando 'Única')
    total_parcelas      SMALLINT,                  -- Total de parcelas (NULL quando 'Única')

    -- Rastreabilidade ETL
    arquivo_origem      VARCHAR(50),               -- Nome do arquivo CSV de origem (ex.: Fatura_2025-03-20.csv)
    data_carga          TIMESTAMP DEFAULT NOW()    -- Timestamp de quando o registro foi carregado
);


-- ============================================================
-- ÍNDICES DE PERFORMANCE (consultas analíticas)
-- ============================================================

-- Filtragem por período (série temporal)
CREATE INDEX idx_fato_id_data          ON dw.fato_transacao(id_data);

-- Filtragem por titular / cartão
CREATE INDEX idx_fato_id_titular       ON dw.fato_transacao(id_titular);

-- Filtragem por categoria
CREATE INDEX idx_fato_id_categoria     ON dw.fato_transacao(id_categoria);

-- Filtragem por estabelecimento (top N)
CREATE INDEX idx_fato_id_estabelecimento ON dw.fato_transacao(id_estabelecimento);

-- Consultas de estorno (valor_brl negativo)
CREATE INDEX idx_fato_valor_brl        ON dw.fato_transacao(valor_brl);

-- Índice na data real para join rápido na dimensão
CREATE INDEX idx_dimdata_data          ON dw.dim_data(data);


-- ============================================================
-- REGISTRO DE METADADOS DO PROJETO
-- ============================================================
CREATE TABLE dw.meta_carga (
    id_carga        SERIAL       PRIMARY KEY,
    arquivo         VARCHAR(100) NOT NULL,
    registros_lidos INT,
    registros_cargados INT,
    registros_rejeitados INT,
    inicio_carga    TIMESTAMP,
    fim_carga       TIMESTAMP,
    status          VARCHAR(20)  CHECK (status IN ('SUCESSO', 'ERRO', 'PARCIAL')),
    observacao      TEXT
);
