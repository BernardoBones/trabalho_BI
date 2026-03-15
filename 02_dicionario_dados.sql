/*
=================================================================
DICIONÁRIO DE DADOS — DATA WAREHOUSE DE TRANSAÇÕES DE CARTÃO
=================================================================

MODELO: Star Schema (Esquema Estrela)
SCHEMA: dw
BANCO:  PostgreSQL

TABELAS:
  1. dw.dim_data             — Dimensão de tempo
  2. dw.dim_titular          — Dimensão de titulares/cartões
  3. dw.dim_categoria        — Dimensão de categorias MCC
  4. dw.dim_estabelecimento  — Dimensão de estabelecimentos
  5. dw.fato_transacao       — Tabela fato central
  6. dw.meta_carga           — Log de controle ETL

=================================================================
1. dw.dim_data — Dimensão de Tempo
=================================================================

Origem no CSV   : coluna "Data de Compra" (formato DD/MM/AAAA)
Granularidade   : 1 linha por dia único existente nas transações
Chave natural   : data (DATE)
Chave substituta: id_data (SERIAL)

Coluna          | Tipo         | Nulo | Descrição
----------------|--------------|------|--------------------------------------------
id_data         | SERIAL       | NÃO  | PK — chave substituta gerada automaticamente
data            | DATE         | NÃO  | Data completa (YYYY-MM-DD), única na tabela
dia             | SMALLINT     | NÃO  | Dia do mês (1–31)
mes             | SMALLINT     | NÃO  | Mês do ano (1–12)
nome_mes        | VARCHAR(20)  | NÃO  | Nome do mês em pt-BR (ex.: Janeiro)
trimestre       | SMALLINT     | NÃO  | Trimestre (1–4)
ano             | SMALLINT     | NÃO  | Ano com 4 dígitos (ex.: 2025)
dia_semana      | SMALLINT     | NÃO  | ISO: 1=Segunda, 2=Terça, ..., 7=Domingo
nome_dia        | VARCHAR(20)  | NÃO  | Nome do dia em pt-BR (ex.: Segunda-feira)
eh_fim_semana   | BOOLEAN      | NÃO  | TRUE se sábado ou domingo

Regras ETL:
  - Converter "DD/MM/AAAA" para DATE no formato ISO antes da carga.
  - Derivar todos os atributos calculados (mes, trimestre, dia_semana, etc.)
    a partir da data usando funções do banco ou pandas.
  - Não inserir duplicatas — usar INSERT ... ON CONFLICT DO NOTHING.

=================================================================
2. dw.dim_titular — Dimensão de Titulares / Cartões
=================================================================

Origem no CSV   : colunas "Nome no Cartão" + "Final do Cartão"
Granularidade   : 1 linha por cartão lógico (nome + 4 últimos dígitos)
Chave natural   : (nome_titular, final_cartao)
Chave substituta: id_titular (SERIAL)

Coluna          | Tipo         | Nulo | Descrição
----------------|--------------|------|--------------------------------------------
id_titular      | SERIAL       | NÃO  | PK — chave substituta
nome_titular    | VARCHAR(100) | NÃO  | Nome conforme coluna "Nome no Cartão" (anonimizado)
final_cartao    | CHAR(4)      | NÃO  | Últimos 4 dígitos numéricos do cartão

Regras ETL:
  - Normalizar espaços e caixa do nome (strip + título).
  - Validar final_cartao: deve conter exatamente 4 dígitos numéricos.
  - Um mesmo titular pode ter mais de um cartão (final_cartao diferente).
  - Constraint UNIQUE(nome_titular, final_cartao) garante integridade.

=================================================================
3. dw.dim_categoria — Dimensão de Categorias MCC
=================================================================

Origem no CSV   : coluna "Categoria"
Granularidade   : 1 linha por categoria única
Chave natural   : nome_categoria
Chave substituta: id_categoria (SERIAL)

Coluna          | Tipo         | Nulo | Descrição
----------------|--------------|------|--------------------------------------------
id_categoria    | SERIAL       | NÃO  | PK — chave substituta
nome_categoria  | VARCHAR(150) | NÃO  | Nome da categoria MCC, único na tabela

Regras ETL:
  - Tratar valores nulos ou "-" como "Não Categorizado".
  - Normalizar espaços extras e caracteres especiais.
  - Constraint UNIQUE(nome_categoria) garante sem duplicatas.

Exemplos de valores esperados:
  - "Restaurante / Lanchonete / Bar"
  - "Supermercado"
  - "Combustível"
  - "Não Categorizado"

=================================================================
4. dw.dim_estabelecimento — Dimensão de Estabelecimentos
=================================================================

Origem no CSV   : coluna "Descrição"
Granularidade   : 1 linha por nome de estabelecimento único
Chave natural   : nome_estabelecimento
Chave substituta: id_estabelecimento (SERIAL)

Coluna              | Tipo         | Nulo | Descrição
--------------------|--------------|------|--------------------------------------------
id_estabelecimento  | SERIAL       | NÃO  | PK — chave substituta
nome_estabelecimento| VARCHAR(255) | NÃO  | Nome bruto do estabelecimento/operador

Regras ETL:
  - Tratar valores nulos ou "-" como "Não Informado".
  - Manter o nome bruto conforme consta no CSV (sem normalização de marca).
  - Pode conter caracteres especiais, siglas e abreviações — codificação UTF-8.

=================================================================
5. dw.fato_transacao — Tabela Fato Central
=================================================================

Origem no CSV   : todas as colunas dos arquivos Fatura_*.csv
Granularidade   : 1 linha por evento de transação (compra ou estorno)
Chave substituta: id_transacao (BIGSERIAL)

Coluna               | Tipo          | Nulo | Descrição
---------------------|---------------|------|--------------------------------------------
id_transacao         | BIGSERIAL     | NÃO  | PK — chave substituta da transação
id_data              | INT           | NÃO  | FK → dim_data.id_data
id_titular           | INT           | NÃO  | FK → dim_titular.id_titular
id_categoria         | INT           | NÃO  | FK → dim_categoria.id_categoria
id_estabelecimento   | INT           | NÃO  | FK → dim_estabelecimento.id_estabelecimento
valor_brl            | NUMERIC(12,2) | NÃO  | Valor em R$; negativo = estorno ou crédito
valor_usd            | NUMERIC(12,2) | SIM  | Valor em US$ (NULL se não aplicável)
cotacao              | NUMERIC(10,4) | SIM  | Cotação USD/BRL (NULL se não aplicável)
parcela_texto        | VARCHAR(10)   | SIM  | Texto original da parcela (ex.: "Única","1/3")
num_parcela          | SMALLINT      | SIM  | Parcela atual; NULL para compras à vista
total_parcelas       | SMALLINT      | SIM  | Total parcelas; NULL para compras à vista
arquivo_origem       | VARCHAR(50)   | SIM  | Nome do arquivo CSV (rastreabilidade ETL)
data_carga           | TIMESTAMP     | NÃO  | Timestamp de inserção no DW (automático)

Regras ETL:
  - Cada linha do CSV origina 1 linha na fato (sem deduplicação por padrão).
  - Parcela "Única" → num_parcela = NULL, total_parcelas = NULL.
  - Parcela "X/Y"  → num_parcela = X, total_parcelas = Y (extrair com regex).
  - Valor_brl negativo é válido — representa estorno ou crédito.
  - Para análises monetárias em R$: usar apenas valor_brl.
  - Para análises em dólar: usar valor_usd com cotacao.
  - Registrar arquivo_origem para auditoria da carga.

Medidas disponíveis para análise:
  - SUM(valor_brl)          → Total gasto em R$
  - AVG(valor_brl)          → Ticket médio em R$
  - COUNT(id_transacao)     → Quantidade de transações
  - SUM(valor_brl) FILTER (WHERE valor_brl < 0) → Total de estornos
  - SUM(valor_brl) FILTER (WHERE total_parcelas > 1) → Total parcelado

=================================================================
6. dw.meta_carga — Log de Controle ETL
=================================================================

Uso             : Controle e auditoria das execuções do pipeline ETL
Granularidade   : 1 linha por arquivo CSV processado em cada execução

Coluna               | Tipo         | Nulo | Descrição
---------------------|--------------|------|--------------------------------------------
id_carga             | SERIAL       | NÃO  | PK — identificador da execução
arquivo              | VARCHAR(100) | NÃO  | Nome do arquivo processado
registros_lidos      | INT          | SIM  | Total de linhas lidas no CSV
registros_cargados   | INT          | SIM  | Total de registros inseridos no DW
registros_rejeitados | INT          | SIM  | Total de registros descartados/com erro
inicio_carga         | TIMESTAMP    | SIM  | Timestamp de início do processamento
fim_carga            | TIMESTAMP    | SIM  | Timestamp de fim do processamento
status               | VARCHAR(20)  | SIM  | SUCESSO | ERRO | PARCIAL
observacao           | TEXT         | SIM  | Mensagem de erro ou observações livres

=================================================================
RELACIONAMENTOS (Star Schema)
=================================================================

fato_transacao.id_data             → dim_data.id_data        (N:1)
fato_transacao.id_titular          → dim_titular.id_titular   (N:1)
fato_transacao.id_categoria        → dim_categoria.id_categoria (N:1)
fato_transacao.id_estabelecimento  → dim_estabelecimento.id_estabelecimento (N:1)

=================================================================
DECISÕES DE MODELAGEM DOCUMENTADAS
=================================================================

1. PARCELAS NA FATO (não em dimensão separada)
   Motivo: Parcelamento é um atributo da transação e não possui
   hierarquia ou análise dimensional própria. Manter na fato reduz
   joins e simplifica o modelo.

2. UMA LINHA POR LINHA DO CSV
   Motivo: Cada linha do CSV representa um evento distinto registrado
   na fatura. Não há deduplicação automática — a qualidade do
   dado-fonte é preservada para auditoria.

3. ESTORNOS NA MESMA FATO
   Motivo: Estornos são eventos financeiros válidos. Mantê-los com
   valor_brl negativo permite calcular saldo líquido e total bruto
   com simples filtros, sem necessidade de tabela separada.

4. CHAVES SUBSTITUTAS (SURROGATE KEYS)
   Motivo: Evitar texto como PK nas dimensões melhora performance de
   join e desacopla o DW de mudanças no dado-fonte (ex.: correção
   de nome de estabelecimento).

5. CATEGORIAS NULAS → "Não Categorizado"
   Motivo: Garantir integridade referencial sem perder transações
   que não possuem categoria no extrato.

=================================================================
MAPEAMENTO CSV → DW
=================================================================

Coluna CSV              | Tabela destino          | Coluna destino
------------------------|-------------------------|---------------------------
Data de Compra          | dim_data                | data
Nome no Cartão          | dim_titular             | nome_titular
Final do Cartão         | dim_titular             | final_cartao
Categoria               | dim_categoria           | nome_categoria
Descrição               | dim_estabelecimento     | nome_estabelecimento
Parcela                 | fato_transacao          | parcela_texto, num_parcela, total_parcelas
Valor (em US$)          | fato_transacao          | valor_usd
Cotação (em R$)         | fato_transacao          | cotacao
Valor (em R$)           | fato_transacao          | valor_brl
(nome arquivo)          | fato_transacao          | arquivo_origem

=================================================================
*/
