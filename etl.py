import os
import re
import glob
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "dw_cartao",
    "user":     "postgres",
    "password": "masterkey",
    "options":  "-c client_encoding=UTF8",
}

# Diretório onde estão os arquivos Fatura_*.csv
CSV_DIR = "./dados"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def extract(csv_dir: str) -> pd.DataFrame:
    """
    Lê todos os arquivos Fatura_*.csv do diretório informado.
    Retorna um DataFrame com todas as linhas concatenadas e
    a coluna extra 'arquivo_origem' com o nome do arquivo de origem.
    """
    arquivos = sorted(glob.glob(os.path.join(csv_dir, "Fatura_*.csv")))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo Fatura_*.csv encontrado em: {csv_dir}")

    log.info(f"Encontrados {len(arquivos)} arquivo(s) CSV.")
    frames = []
    for caminho in arquivos:
        nome = os.path.basename(caminho)
        df = pd.read_csv(
            caminho,
            sep=";",
            encoding="utf-8",
            dtype=str,          # Ler tudo como texto para controlar a conversão
        )
        df["arquivo_origem"] = nome
        log.info(f"  {nome}: {len(df)} linhas lidas.")
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    log.info(f"Total bruto: {len(raw)} linhas.")
    return raw


MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março",    4: "Abril",
    5: "Maio",    6: "Junho",     7: "Julho",     8: "Agosto",
    9: "Setembro",10: "Outubro",  11: "Novembro", 12: "Dezembro",
}

DIAS_PT = {
    1: "Segunda-feira", 2: "Terça-feira",  3: "Quarta-feira",
    4: "Quinta-feira",  5: "Sexta-feira",  6: "Sábado",
    7: "Domingo",
}


def _parsear_parcela(texto: str):
    """
    Converte o texto de parcelamento em (num_parcela, total_parcelas).

    'Única'  → (None, None)
    '1/3'    → (1, 3)
    '10/12'  → (10, 12)
    Outro    → (None, None)
    """
    if pd.isna(texto) or str(texto).strip().upper() in ("ÚNICA", "UNICA", ""):
        return None, None
    m = re.match(r"^(\d+)/(\d+)$", str(texto).strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parsear_valor(valor_str: str) -> float | None:
    """
    Converte string de valor para float.
    Aceita ponto ou vírgula como separador decimal.
    Retorna None se vazio ou inválido.
    """
    if pd.isna(valor_str) or str(valor_str).strip() in ("", "-"):
        return None
    try:
        return float(str(valor_str).replace(",", ".").strip())
    except ValueError:
        return None


def transform(raw: pd.DataFrame) -> dict:
    """
    Aplica todas as transformações e devolve um dicionário com
    os DataFrames limpos para cada tabela do DW:
        {
            'dim_data':            DataFrame,
            'dim_titular':         DataFrame,
            'dim_categoria':       DataFrame,
            'dim_estabelecimento': DataFrame,
            'fato':                DataFrame,
        }
    """
    df = raw.copy()

    df.columns = [
        "data_compra", "nome_titular", "final_cartao",
        "categoria", "descricao", "parcela_texto",
        "valor_usd_str", "cotacao_str", "valor_brl_str", "arquivo_origem",
    ]

    df["data_compra"] = pd.to_datetime(
        df["data_compra"].str.strip(),
        format="%d/%m/%Y",
        errors="coerce",
    )
    linhas_data_invalida = df["data_compra"].isna().sum()
    if linhas_data_invalida > 0:
        log.warning(f"  {linhas_data_invalida} linha(s) com data inválida — serão descartadas.")
    df = df[df["data_compra"].notna()].copy()

    df["nome_titular"]  = df["nome_titular"].str.strip().str.title()
    df["final_cartao"]  = df["final_cartao"].str.strip().str.zfill(4)
    df["categoria"]     = df["categoria"].str.strip()
    df["descricao"]     = df["descricao"].str.strip()
    df["parcela_texto"] = df["parcela_texto"].str.strip()

    df["categoria"] = df["categoria"].replace({"-": "Não Categorizado", "": "Não Categorizado"})
    df["categoria"] = df["categoria"].fillna("Não Categorizado")
    df["descricao"] = df["descricao"].replace({"-": "Não Informado", "": "Não Informado"})
    df["descricao"] = df["descricao"].fillna("Não Informado")

    df["valor_brl"] = df["valor_brl_str"].apply(_parsear_valor)
    df["valor_usd"] = df["valor_usd_str"].apply(_parsear_valor)
    df["cotacao"]   = df["cotacao_str"].apply(_parsear_valor)

    df.loc[df["valor_usd"] == 0.0,  "valor_usd"] = None
    df.loc[df["cotacao"]   == 0.0,  "cotacao"]   = None

    sem_valor = df["valor_brl"].isna().sum()
    if sem_valor > 0:
        log.warning(f"  {sem_valor} linha(s) sem valor_brl — serão descartadas.")
    df = df[df["valor_brl"].notna()].copy()

    parcelas = df["parcela_texto"].apply(_parsear_parcela)
    df["num_parcela"]    = parcelas.apply(lambda x: x[0])
    df["total_parcelas"] = parcelas.apply(lambda x: x[1])

    log.info(f"  Após transformações: {len(df)} linhas válidas.")

    datas_unicas = df["data_compra"].dt.date.unique()
    rows_data = []
    for d in datas_unicas:
        dt = pd.Timestamp(d)
        rows_data.append({
            "data":         d,
            "dia":          dt.day,
            "mes":          dt.month,
            "nome_mes":     MESES_PT[dt.month],
            "trimestre":    (dt.month - 1) // 3 + 1,
            "ano":          dt.year,
            "dia_semana":   dt.isoweekday(),        # 1=Seg, 7=Dom (ISO)
            "nome_dia":     DIAS_PT[dt.isoweekday()],
            "eh_fim_semana": dt.isoweekday() >= 6,
        })
    dim_data = pd.DataFrame(rows_data).sort_values("data").reset_index(drop=True)

    dim_titular = (
        df[["nome_titular", "final_cartao"]]
        .drop_duplicates()
        .sort_values(["nome_titular", "final_cartao"])
        .reset_index(drop=True)
    )

    dim_categoria = (
        df[["categoria"]]
        .rename(columns={"categoria": "nome_categoria"})
        .drop_duplicates()
        .sort_values("nome_categoria")
        .reset_index(drop=True)
    )

    dim_estabelecimento = (
        df[["descricao"]]
        .rename(columns={"descricao": "nome_estabelecimento"})
        .drop_duplicates()
        .sort_values("nome_estabelecimento")
        .reset_index(drop=True)
    )

    fato = df[[
        "data_compra", "nome_titular", "final_cartao",
        "categoria", "descricao",
        "valor_brl", "valor_usd", "cotacao",
        "parcela_texto", "num_parcela", "total_parcelas",
        "arquivo_origem",
    ]].copy()
    fato = fato.rename(columns={"arquivo_origem": "arquivo_origem"}) 
    fato["data_compra"] = fato["data_compra"].dt.date

    log.info(f"  dim_data:             {len(dim_data)} linhas")
    log.info(f"  dim_titular:          {len(dim_titular)} linhas")
    log.info(f"  dim_categoria:        {len(dim_categoria)} linhas")
    log.info(f"  dim_estabelecimento:  {len(dim_estabelecimento)} linhas")
    log.info(f"  fato_transacao:       {len(fato)} linhas")

    return {
        "dim_data":            dim_data,
        "dim_titular":         dim_titular,
        "dim_categoria":       dim_categoria,
        "dim_estabelecimento": dim_estabelecimento,
        "fato":                fato,
    }

def _get_conn(config: dict):
    return psycopg2.connect(**config)


def load(tabelas: dict, db_config: dict) -> None:
    """
    Carrega os dados transformados no PostgreSQL na ordem correta:
    Dimensões → Fato.
    Usa INSERT ... ON CONFLICT DO NOTHING para idempotência (full reload seguro).
    """
    conn = _get_conn(db_config)
    conn.autocommit = False
    cur = conn.cursor()

    inicio_total = datetime.now()

    try:
        log.info("Carregando dw.dim_data ...")
        rows = [
            (r.data, r.dia, r.mes, r.nome_mes, r.trimestre,
             r.ano, r.dia_semana, r.nome_dia, bool(r.eh_fim_semana))
            for r in tabelas["dim_data"].itertuples()
        ]
        execute_values(cur, """
            INSERT INTO dw.dim_data
                (data, dia, mes, nome_mes, trimestre, ano, dia_semana, nome_dia, eh_fim_semana)
            VALUES %s
            ON CONFLICT (data) DO NOTHING
        """, rows)
        log.info(f"  {cur.rowcount} linhas inseridas em dim_data.")

        # Carregar mapa data → id_data para uso na fato
        cur.execute("SELECT id_data, data FROM dw.dim_data")
        map_data = {row[1]: row[0] for row in cur.fetchall()}

        log.info("Carregando dw.dim_titular ...")
        rows = [
            (r.nome_titular, r.final_cartao)
            for r in tabelas["dim_titular"].itertuples()
        ]
        execute_values(cur, """
            INSERT INTO dw.dim_titular (nome_titular, final_cartao)
            VALUES %s
            ON CONFLICT (nome_titular, final_cartao) DO NOTHING
        """, rows)
        log.info(f"  {cur.rowcount} linhas inseridas em dim_titular.")

        cur.execute("SELECT id_titular, nome_titular, final_cartao FROM dw.dim_titular")
        map_titular = {(r[1], r[2]): r[0] for r in cur.fetchall()}

        log.info("Carregando dw.dim_categoria ...")
        rows = [(r.nome_categoria,) for r in tabelas["dim_categoria"].itertuples()]
        execute_values(cur, """
            INSERT INTO dw.dim_categoria (nome_categoria)
            VALUES %s
            ON CONFLICT (nome_categoria) DO NOTHING
        """, rows)
        log.info(f"  {cur.rowcount} linhas inseridas em dim_categoria.")

        cur.execute("SELECT id_categoria, nome_categoria FROM dw.dim_categoria")
        map_categoria = {r[1]: r[0] for r in cur.fetchall()}

        log.info("Carregando dw.dim_estabelecimento ...")
        rows = [(r.nome_estabelecimento,) for r in tabelas["dim_estabelecimento"].itertuples()]
        execute_values(cur, """
            INSERT INTO dw.dim_estabelecimento (nome_estabelecimento)
            VALUES %s
            ON CONFLICT (nome_estabelecimento) DO NOTHING
        """, rows)
        log.info(f"  {cur.rowcount} linhas inseridas em dim_estabelecimento.")

        cur.execute("SELECT id_estabelecimento, nome_estabelecimento FROM dw.dim_estabelecimento")
        map_estab = {r[1]: r[0] for r in cur.fetchall()}

        log.info("Carregando dw.fato_transacao ...")
        fato = tabelas["fato"]
        rows_fato = []
        rejeitados = 0
        for r in fato.itertuples(index=False):
            id_data  = map_data.get(r.data_compra)
            id_tit   = map_titular.get((r.nome_titular, r.final_cartao))
            id_cat   = map_categoria.get(r.categoria)
            id_estab = map_estab.get(r.descricao)

            if None in (id_data, id_tit, id_cat, id_estab):
                rejeitados += 1
                log.warning(f"  FK não encontrada para linha: {r}")
                continue

            rows_fato.append((
                id_data, id_tit, id_cat, id_estab,
                float(r.valor_brl),
                float(r.valor_usd) if r.valor_usd is not None and str(r.valor_usd) != 'nan' else None,
                float(r.cotacao)   if r.cotacao   is not None and str(r.cotacao)   != 'nan' else None,
                r.parcela_texto if r.parcela_texto else None,
                int(r.num_parcela)    if r.num_parcela    is not None and str(r.num_parcela)    != 'nan' else None,
                int(r.total_parcelas) if r.total_parcelas is not None and str(r.total_parcelas) != 'nan' else None,
                r.arquivo_origem,
            ))

        execute_values(cur, """
            INSERT INTO dw.fato_transacao
                (id_data, id_titular, id_categoria, id_estabelecimento,
                 valor_brl, valor_usd, cotacao,
                 parcela_texto, num_parcela, total_parcelas,
                 arquivo_origem)
            VALUES %s
        """, rows_fato)

        fim_total = datetime.now()
        log.info(f"  {len(rows_fato)} linhas inseridas em fato_transacao.")
        if rejeitados:
            log.warning(f"  {rejeitados} linhas rejeitadas por FK inválida.")

        cur.execute("""
            INSERT INTO dw.meta_carga
                (arquivo, registros_lidos, registros_cargados, registros_rejeitados,
                 inicio_carga, fim_carga, status, observacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "ETL_FULL",
            len(fato),
            len(rows_fato),
            rejeitados,
            inicio_total,
            fim_total,
            "SUCESSO" if rejeitados == 0 else "PARCIAL",
            None,
        ))

        conn.commit()
        log.info("Carga finalizada com sucesso!")
        log.info(f"Duração total: {fim_total - inicio_total}")

    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante a carga: {e}")
        cur.execute("""
            INSERT INTO dw.meta_carga
                (arquivo, registros_lidos, registros_cargados, registros_rejeitados,
                 inicio_carga, fim_carga, status, observacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, ("ETL_FULL", 0, 0, 0, inicio_total, datetime.now(), "ERRO", str(e)))
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    log.info("=" * 60)
    log.info("INICIANDO PIPELINE ETL")
    log.info("=" * 60)

    # EXTRACT
    log.info("── FASE EXTRACT ──────────────────────────────────────────")
    raw = extract(CSV_DIR)

    # TRANSFORM
    log.info("── FASE TRANSFORM ────────────────────────────────────────")
    tabelas = transform(raw)

    # LOAD
    log.info("── FASE LOAD ─────────────────────────────────────────────")
    load(tabelas, DB_CONFIG)

    log.info("=" * 60)
    log.info("PIPELINE CONCLUÍDO")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
