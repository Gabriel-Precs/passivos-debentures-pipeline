import pandas as pd
import requests
import logging
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv


# ==============================
# LOGGING PROFISSIONAL
# ==============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# ENV
# ==============================

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# ==============================
# SUA TABELA — INTACTA
# ==============================

TABELA_RENTABILIDADE = {
    "Serie1":  {"tipo": "pos-fixado", "valor": 1.00},
    "Serie2":  {"tipo": "pos-fixado", "valor": 1.10},
    "Serie3":  {"tipo": "pos-fixado", "valor": 1.20},
    "Serie4":  {"tipo": "pos-fixado", "valor": 1.30},
    "Serie5":  {"tipo": "pos-fixado", "valor": 1.40},
    "Serie6":  {"tipo": "pos-fixado", "valor": 1.50},
    "Serie7":  {"tipo": "pos-fixado", "valor": 1.60},
    "Serie8":  {"tipo": "pos-fixado", "valor": 1.70},

    "Serie9":  {"tipo": "hibrido", "valor": 0.03},
    "Serie10": {"tipo": "hibrido", "valor": 0.04},
    "Serie11": {"tipo": "hibrido", "valor": 0.05},
    "Serie12": {"tipo": "hibrido", "valor": 0.06},

    "Serie13": {"tipo": "pre-fixado", "valor": 0.013},
    "Serie14": {"tipo": "pre-fixado", "valor": 0.014},
    "Serie15": {"tipo": "pre-fixado", "valor": 0.015},
    "Serie16": {"tipo": "pre-fixado", "valor": 0.020},
    "Serie17": {"tipo": "pre-fixado", "valor": 0.025}
}


# ==============================
# CDI BACEN
# ==============================

def baixar_cdi():
    logging.info("Baixando CDI do BACEN...")

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=01/01/2023"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Erro ao coletar CDI!")

    df = pd.DataFrame(response.json())

    df["valor"] = df["valor"].astype(float)
    df["data"] = pd.to_datetime(df["data"], dayfirst=True)

    return df


# ==============================
# BUSCAR TITULOS
# ==============================

def buscar_titulos():
    logging.info("Buscando títulos...")

    query = "SELECT * FROM titulos"

    df = pd.read_sql(query, engine)

    df["data_compra"] = pd.to_datetime(df["data_compra"])

    logging.info(f"{len(df)} títulos carregados")

    return df


# ==============================
# SUA LÓGICA — INTACTA
# ==============================

def calcular_pu(df_titulos, df_cdi):

    pu_atual = []

    for _, row in df_titulos.iterrows():

        data_compra = row["data_compra"]
        pu_compra = float(row["pu_compra"])
        serie = row["serie"].strip()

        regra = TABELA_RENTABILIDADE.get(serie)

        if regra is None:
            raise Exception(f"Serie não encontrada: {serie}")

        cdi_periodo = df_cdi[df_cdi["data"] >= data_compra].copy()
        cdi_periodo = cdi_periodo.sort_values("data")

        pu_evoluindo = pu_compra

        # POS FIXADO
        if regra["tipo"] == "pos-fixado":
            cdi_periodo["fator"] = 1 + (regra["valor"] * (cdi_periodo["valor"]) / 100)

        # HIBRIDO
        elif regra["tipo"] == "hibrido":
            fator_h = (1 + regra["valor"])**(1/252) - 1
            cdi_periodo["fator"] = (1 + (cdi_periodo["valor"]) / 100) * (1 + fator_h)

        # PRE FIXADO
        elif regra["tipo"] == "pre-fixado":
            prefixado_aa = (1 + regra["valor"])**12 - 1
            cdi_periodo["fator"] = (1 + prefixado_aa)**(1/252)

        else:
            raise Exception(f"Tipo desconhecido: {regra['tipo']}")

        for fator in cdi_periodo["fator"]:
            pu_evoluindo *= fator

        pu_atual.append(pu_evoluindo)

    df_titulos["pu_atualizado"] = pu_atual
    df_titulos["valor_atualizado"] = df_titulos["pu_atualizado"] * df_titulos["qtde"]

    return df_titulos


# ==============================
# INSERT SNAPSHOT
# ==============================

def salvar_snapshot(df,cdi):



    df_insert = df[["id", "pu_atualizado", "valor_atualizado"]].copy()
    df_insert["data_snapshot"] = cdi.loc[cdi["data"].idxmax(), "data"]

    records = df_insert.to_dict("records")

    with engine.begin() as conn:

        for r in records:
            conn.execute(text("""
                INSERT INTO snapshot_divida
                (titulo_id, data_snapshot, pu_atual, valor_atual)
                VALUES (:id, :data_snapshot, :pu, :valor)
                ON CONFLICT (titulo_id, data_snapshot)
                DO NOTHING;
            """), {
                "id": r["id"],
                "data_snapshot": r["data_snapshot"],
                "pu": float(r["pu_atualizado"]),
                "valor": float(r["valor_atualizado"])
            })

    logging.info("Snapshot salvo com sucesso!")


# ==============================
# PIPELINE
# ==============================

def rodar_pipeline():

    try:

        logging.info("Iniciando snapshot diário...")

        titulos = buscar_titulos()
        cdi = baixar_cdi()

        df_calculado = calcular_pu(titulos, cdi)

        salvar_snapshot(df_calculado,cdi)

        logging.info("PIPELINE FINALIZADO!")

    except Exception as e:

        logging.exception("🚨 ERRO NO SNAPSHOT")
        raise e


if __name__ == "__main__":
    rodar_pipeline()
