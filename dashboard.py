"""
=============================================================
Projeto: Data Warehouse - Transações de Cartão de Crédito
Fase 4  : Dashboard Streamlit
=============================================================

Instalação:
    pip install streamlit plotly psycopg2-binary pandas

Execução:
    streamlit run dashboard.py

Ajuste DB_CONFIG abaixo conforme seu ambiente.
=============================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from etl import DB_CONFIG  

st.set_page_config(
    page_title="DW Cartão de Crédito",
    page_icon="💳",
    layout="wide",
)

# ─────────────────────────────────────────────
# CONEXÃO
# ─────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data(ttl=300)
def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)


# ─────────────────────────────────────────────
# DADOS BASE (com cache)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def carregar_titulares():
    return query("SELECT id_titular, nome_titular, final_cartao FROM dw.dim_titular ORDER BY nome_titular, final_cartao")

@st.cache_data(ttl=300)
def carregar_categorias():
    return query("SELECT id_categoria, nome_categoria FROM dw.dim_categoria ORDER BY nome_categoria")

@st.cache_data(ttl=300)
def carregar_periodo():
    return query("SELECT MIN(data) AS data_min, MAX(data) AS data_max FROM dw.dim_data d JOIN dw.fato_transacao f ON d.id_data = f.id_data")


# ─────────────────────────────────────────────
# SIDEBAR — FILTROS
# ─────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/bank-card-back-side.png", width=60)
    st.title("Filtros")

    titulares_df = carregar_titulares()
    opcoes_titular = ["Todos"] + [
        f"{r.nome_titular} (****{r.final_cartao})"
        for r in titulares_df.itertuples()
    ]
    titular_sel = st.multiselect(
        "Titular / Cartão",
        options=opcoes_titular[1:],
        default=opcoes_titular[1:],
        placeholder="Selecione titulares...",
    )
    if not titular_sel:
        titular_sel = opcoes_titular[1:]

    ids_titular = titulares_df[
        titulares_df.apply(
            lambda r: f"{r['nome_titular']} (****{r['final_cartao']})" in titular_sel,
            axis=1,
        )
    ]["id_titular"].tolist()

    periodo = carregar_periodo()
    data_min = pd.to_datetime(periodo["data_min"].iloc[0]).date()
    data_max = pd.to_datetime(periodo["data_max"].iloc[0]).date()

    data_inicio, data_fim = st.date_input(
        "Período",
        value=(data_min, data_max),
        min_value=data_min,
        max_value=data_max,
    )

    categorias_df = carregar_categorias()
    cats_sel = st.multiselect(
        "Categorias",
        options=categorias_df["nome_categoria"].tolist(),
        default=categorias_df["nome_categoria"].tolist(),
        placeholder="Selecione categorias...",
    )
    if not cats_sel:
        cats_sel = categorias_df["nome_categoria"].tolist()

    ids_categoria = categorias_df[
        categorias_df["nome_categoria"].isin(cats_sel)
    ]["id_categoria"].tolist()

    st.divider()
    top_n = st.slider("Top N estabelecimentos", min_value=5, max_value=30, value=10)

# Parâmetros base para todas as queries
params_base = {
    "ids_titular":   tuple(ids_titular),
    "ids_categoria": tuple(ids_categoria),
    "data_inicio":   str(data_inicio),
    "data_fim":      str(data_fim),
}

# ─────────────────────────────────────────────
# CABEÇALHO
# ─────────────────────────────────────────────
st.title("💳 Dashboard — Transações de Cartão de Crédito")
st.caption(f"Período selecionado: **{data_inicio.strftime('%d/%m/%Y')}** a **{data_fim.strftime('%d/%m/%Y')}**")
st.divider()


# ─────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────
kpi_sql = """
    SELECT
        COUNT(*)                                            AS qtd_transacoes,
        COUNT(*) FILTER (WHERE f.valor_brl > 0)            AS qtd_compras,
        COUNT(*) FILTER (WHERE f.valor_brl < 0)            AS qtd_estornos,
        COALESCE(SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 0) AS total_compras,
        COALESCE(SUM(f.valor_brl) FILTER (WHERE f.valor_brl < 0), 0) AS total_estornos,
        COALESCE(SUM(f.valor_brl), 0)                      AS saldo_liquido,
        COALESCE(AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0), 0) AS ticket_medio
    FROM dw.fato_transacao f
    JOIN dw.dim_data d ON f.id_data = d.id_data
    WHERE f.id_titular   IN %(ids_titular)s
      AND f.id_categoria IN %(ids_categoria)s
      AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
"""
kpi = query(kpi_sql, params_base).iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📊 Transações",       f"{int(kpi['qtd_transacoes']):,}".replace(",", "."))
col2.metric("💰 Total Compras",    f"R$ {kpi['total_compras']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
col3.metric("↩️ Estornos",         f"R$ {abs(kpi['total_estornos']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
col4.metric("💵 Saldo Líquido",    f"R$ {kpi['saldo_liquido']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
col5.metric("🎯 Ticket Médio",     f"R$ {kpi['ticket_medio']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.divider()


# ─────────────────────────────────────────────
# LINHA 1 — Série temporal + Pizza de categorias
# ─────────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("📈 Evolução Mensal de Gastos")
    mensal_sql = """
        SELECT
            d.ano, d.mes, d.nome_mes,
            TO_CHAR(DATE_TRUNC('month', d.data), 'YYYY-MM') AS ano_mes,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0) AS total_compras,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl < 0) AS total_estornos
        FROM dw.fato_transacao f
        JOIN dw.dim_data d ON f.id_data = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
        GROUP BY d.ano, d.mes, d.nome_mes, DATE_TRUNC('month', d.data)
        ORDER BY d.ano, d.mes
    """
    mensal = query(mensal_sql, params_base)
    if not mensal.empty:
        fig_line = go.Figure()
        fig_line.add_trace(go.Bar(
            x=mensal["ano_mes"],
            y=mensal["total_compras"],
            name="Compras",
            marker_color="#4F8EF7",
        ))
        fig_line.add_trace(go.Scatter(
            x=mensal["ano_mes"],
            y=mensal["total_compras"],
            name="Tendência",
            mode="lines+markers",
            line=dict(color="#FF6B35", width=2),
            marker=dict(size=6),
        ))
        fig_line.update_layout(
            xaxis_title="Mês",
            yaxis_title="R$",
            legend=dict(orientation="h", y=1.1),
            margin=dict(l=0, r=0, t=30, b=0),
            height=320,
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Sem dados para o período selecionado.")

with col_right:
    st.subheader("🗂️ Top Categorias")
    cat_sql = """
        SELECT
            c.nome_categoria,
            SUM(f.valor_brl) AS total_brl
        FROM dw.fato_transacao f
        JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dw.dim_data d      ON f.id_data      = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl > 0
        GROUP BY c.nome_categoria
        ORDER BY total_brl DESC
        LIMIT 8
    """
    cat_df = query(cat_sql, params_base)
    if not cat_df.empty:
        fig_pie = px.pie(
            cat_df,
            names="nome_categoria",
            values="total_brl",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
            height=320,
        )
        st.plotly_chart(fig_pie, use_container_width=True)


# ─────────────────────────────────────────────
# LINHA 2 — Comparativo por titular + Parcelamento
# ─────────────────────────────────────────────
st.divider()
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("👤 Gasto por Titular")
    titular_sql = """
        SELECT
            t.nome_titular || ' (****' || t.final_cartao || ')' AS titular,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)     AS total_compras,
            COUNT(*) FILTER (WHERE f.valor_brl > 0)             AS qtd_compras
        FROM dw.fato_transacao f
        JOIN dw.dim_titular t ON f.id_titular = t.id_titular
        JOIN dw.dim_data d    ON f.id_data    = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
        GROUP BY t.nome_titular, t.final_cartao
        ORDER BY total_compras DESC
    """
    tit_df = query(titular_sql, params_base)
    if not tit_df.empty:
        fig_tit = px.bar(
            tit_df,
            x="total_compras",
            y="titular",
            orientation="h",
            text=tit_df["total_compras"].apply(lambda v: f"R$ {v:,.0f}"),
            color="total_compras",
            color_continuous_scale="Blues",
        )
        fig_tit.update_traces(textposition="outside")
        fig_tit.update_layout(
            xaxis_title="R$",
            yaxis_title="",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
        )
        st.plotly_chart(fig_tit, use_container_width=True)

with col_b:
    st.subheader("💳 À Vista vs Parcelado")
    parcela_sql = """
        SELECT
            CASE WHEN num_parcela IS NULL THEN 'À vista' ELSE 'Parcelado' END AS tipo,
            COUNT(*)         AS qtd,
            SUM(f.valor_brl) AS total_brl
        FROM dw.fato_transacao f
        JOIN dw.dim_data d ON f.id_data = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl > 0
        GROUP BY tipo
    """
    parc_df = query(parcela_sql, params_base)
    if not parc_df.empty:
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            fig_parc_qtd = px.pie(
                parc_df, names="tipo", values="qtd",
                title="Por Quantidade",
                hole=0.5,
                color_discrete_map={"À vista": "#4F8EF7", "Parcelado": "#FF6B35"},
            )
            fig_parc_qtd.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=260, showlegend=True)
            st.plotly_chart(fig_parc_qtd, use_container_width=True)
        with col_p2:
            fig_parc_val = px.pie(
                parc_df, names="tipo", values="total_brl",
                title="Por Valor (R$)",
                hole=0.5,
                color_discrete_map={"À vista": "#4F8EF7", "Parcelado": "#FF6B35"},
            )
            fig_parc_val.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=260, showlegend=True)
            st.plotly_chart(fig_parc_val, use_container_width=True)


# ─────────────────────────────────────────────
# LINHA 3 — Top estabelecimentos + Dia da semana
# ─────────────────────────────────────────────
st.divider()
col_c, col_d = st.columns([3, 2])

with col_c:
    st.subheader(f"🏪 Top {top_n} Estabelecimentos")
    estab_sql = """
        SELECT
            e.nome_estabelecimento,
            SUM(f.valor_brl)  AS total_brl,
            COUNT(*)          AS qtd
        FROM dw.fato_transacao f
        JOIN dw.dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
        JOIN dw.dim_data d            ON f.id_data            = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl > 0
        GROUP BY e.nome_estabelecimento
        ORDER BY total_brl DESC
        LIMIT %(top_n)s
    """
    estab_df = query(estab_sql, {**params_base, "top_n": top_n})
    if not estab_df.empty:
        fig_estab = px.bar(
            estab_df.sort_values("total_brl"),
            x="total_brl",
            y="nome_estabelecimento",
            orientation="h",
            text=estab_df.sort_values("total_brl")["total_brl"].apply(lambda v: f"R$ {v:,.0f}"),
            color="total_brl",
            color_continuous_scale="Teal",
        )
        fig_estab.update_traces(textposition="outside")
        fig_estab.update_layout(
            xaxis_title="R$",
            yaxis_title="",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=400,
        )
        st.plotly_chart(fig_estab, use_container_width=True)

with col_d:
    st.subheader("📅 Por Dia da Semana")
    semana_sql = """
        SELECT
            d.dia_semana,
            d.nome_dia,
            COUNT(*)         AS qtd,
            SUM(f.valor_brl) AS total_brl
        FROM dw.fato_transacao f
        JOIN dw.dim_data d ON f.id_data = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl > 0
        GROUP BY d.dia_semana, d.nome_dia
        ORDER BY d.dia_semana
    """
    sem_df = query(semana_sql, params_base)
    if not sem_df.empty:
        fig_sem = px.bar(
            sem_df,
            x="nome_dia",
            y="total_brl",
            text=sem_df["total_brl"].apply(lambda v: f"R$ {v:,.0f}"),
            color="total_brl",
            color_continuous_scale="Purples",
        )
        fig_sem.update_traces(textposition="outside", textfont_size=10)
        fig_sem.update_layout(
            xaxis_title="",
            yaxis_title="R$",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=400,
        )
        st.plotly_chart(fig_sem, use_container_width=True)


# ─────────────────────────────────────────────
# LINHA 4 — Gasto mensal por titular (heatmap)
# ─────────────────────────────────────────────
st.divider()
st.subheader("🗓️ Gasto Mensal por Titular")

heatmap_sql = """
    SELECT
        t.nome_titular || ' (****' || t.final_cartao || ')' AS titular,
        TO_CHAR(DATE_TRUNC('month', d.data), 'YYYY-MM')     AS ano_mes,
        SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)     AS total_compras
    FROM dw.fato_transacao f
    JOIN dw.dim_titular t ON f.id_titular = t.id_titular
    JOIN dw.dim_data d    ON f.id_data    = d.id_data
    WHERE f.id_titular   IN %(ids_titular)s
      AND f.id_categoria IN %(ids_categoria)s
      AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
    GROUP BY t.nome_titular, t.final_cartao, DATE_TRUNC('month', d.data)
    ORDER BY ano_mes
"""
heat_df = query(heatmap_sql, params_base)
if not heat_df.empty:
    pivot = heat_df.pivot(index="titular", columns="ano_mes", values="total_compras").fillna(0)
    fig_heat = px.imshow(
        pivot,
        color_continuous_scale="Blues",
        aspect="auto",
        text_auto=".0f",
    )
    fig_heat.update_layout(
        xaxis_title="Mês",
        yaxis_title="",
        margin=dict(l=0, r=0, t=10, b=0),
        height=220,
        coloraxis_colorbar=dict(title="R$"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)


# ─────────────────────────────────────────────
# LINHA 5 — Estornos
# ─────────────────────────────────────────────
st.divider()
st.subheader("↩️ Estornos e Créditos")

col_e1, col_e2 = st.columns(2)

with col_e1:
    estorno_tit_sql = """
        SELECT
            t.nome_titular || ' (****' || t.final_cartao || ')' AS titular,
            COUNT(*)          AS qtd,
            SUM(f.valor_brl)  AS total_estorno
        FROM dw.fato_transacao f
        JOIN dw.dim_titular t ON f.id_titular = t.id_titular
        JOIN dw.dim_data d    ON f.id_data    = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl < 0
        GROUP BY t.nome_titular, t.final_cartao
        ORDER BY total_estorno ASC
    """
    est_tit = query(estorno_tit_sql, params_base)
    if not est_tit.empty:
        est_tit["total_estorno_abs"] = est_tit["total_estorno"].abs()
        fig_est = px.bar(
            est_tit,
            x="titular", y="total_estorno_abs",
            text=est_tit["total_estorno_abs"].apply(lambda v: f"R$ {v:,.2f}"),
            color="total_estorno_abs",
            color_continuous_scale="Reds",
            title="Valor de Estornos por Titular",
        )
        fig_est.update_layout(coloraxis_showscale=False, height=280, margin=dict(l=0,r=0,t=40,b=0))
        st.plotly_chart(fig_est, use_container_width=True)
    else:
        st.info("Nenhum estorno no período/filtro selecionado.")

with col_e2:
    estorno_cat_sql = """
        SELECT
            c.nome_categoria,
            COUNT(*)          AS qtd,
            SUM(f.valor_brl)  AS total_estorno
        FROM dw.fato_transacao f
        JOIN dw.dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dw.dim_data d      ON f.id_data      = d.id_data
        WHERE f.id_titular   IN %(ids_titular)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
          AND f.valor_brl < 0
        GROUP BY c.nome_categoria
        ORDER BY total_estorno ASC
    """
    est_cat = query(estorno_cat_sql, params_base)
    if not est_cat.empty:
        est_cat["total_estorno_abs"] = est_cat["total_estorno"].abs()
        fig_est_cat = px.bar(
            est_cat,
            x="total_estorno_abs", y="nome_categoria",
            orientation="h",
            text=est_cat["total_estorno_abs"].apply(lambda v: f"R$ {v:,.2f}"),
            color="total_estorno_abs",
            color_continuous_scale="Oranges",
            title="Valor de Estornos por Categoria",
        )
        fig_est_cat.update_layout(coloraxis_showscale=False, height=280, margin=dict(l=0,r=0,t=40,b=0))
        st.plotly_chart(fig_est_cat, use_container_width=True)
    else:
        st.info("Nenhum estorno no período/filtro selecionado.")


# ─────────────────────────────────────────────
# TABELA DE DADOS BRUTOS (expansível)
# ─────────────────────────────────────────────
st.divider()
with st.expander("🔍 Ver transações detalhadas"):
    detalhe_sql = """
        SELECT
            d.data                   AS "Data",
            t.nome_titular           AS "Titular",
            t.final_cartao           AS "Cartão",
            c.nome_categoria         AS "Categoria",
            e.nome_estabelecimento   AS "Estabelecimento",
            f.parcela_texto          AS "Parcela",
            f.valor_brl              AS "Valor (R$)",
            f.valor_usd              AS "Valor (US$)",
            f.cotacao                AS "Cotação"
        FROM dw.fato_transacao f
        JOIN dw.dim_data d            ON f.id_data            = d.id_data
        JOIN dw.dim_titular t         ON f.id_titular          = t.id_titular
        JOIN dw.dim_categoria c       ON f.id_categoria        = c.id_categoria
        JOIN dw.dim_estabelecimento e ON f.id_estabelecimento  = e.id_estabelecimento
        WHERE f.id_titular   IN %(ids_titular)s
          AND f.id_categoria IN %(ids_categoria)s
          AND d.data BETWEEN %(data_inicio)s AND %(data_fim)s
        ORDER BY d.data DESC
        LIMIT 500
    """
    detalhe_df = query(detalhe_sql, params_base)
    st.dataframe(detalhe_df, use_container_width=True, height=400)
    st.caption(f"Mostrando até 500 registros. Total na seleção: {len(detalhe_df)} exibidos.")


# ─────────────────────────────────────────────
# RODAPÉ
# ─────────────────────────────────────────────
st.divider()
st.caption("💳 DW Transações de Cartão · Análise e Desenvolvimento de Sistemas · Dados: Mar/2025 – Fev/2026")
