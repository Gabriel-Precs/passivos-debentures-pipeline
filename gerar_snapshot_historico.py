import pandas as pd
import requests
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

############################################
# TABELA DE RENTABILIDADE (SUA)
############################################

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

############################################
# CDI
############################################

def carregar_cdi():

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=01/01/2023"

    r = requests.get(url)

    if r.status_code != 200:
        raise Exception("Erro ao buscar CDI")

    df = pd.DataFrame(r.json())

    df["valor"] = df["valor"].astype(float)
    df["data"] = pd.to_datetime(df["data"], dayfirst=True)

    return df.sort_values("data")


############################################
# CONEXÃO
############################################

def conectar():

    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise Exception("DB_URL não encontrada no .env")

    return psycopg2.connect(
        db_url,
        sslmode="require"
    )


############################################
# GERAR SNAPSHOT HISTÓRICO
############################################

def gerar_snapshot():

    conn = conectar()
    cursor = conn.cursor()

    print("✅ Conectado ao Postgres")

    df_cdi = carregar_cdi()

    print("📈 CDI carregado")

    cursor.execute("""
        SELECT id, pu_compra, qtde, data_compra, serie
        FROM titulos
    """)

    titulos = cursor.fetchall()

    print(f"📊 {len(titulos)} títulos encontrados")

    inserts = []

    hoje = pd.Timestamp.today()

    for titulo in titulos:

        titulo_id, pu_compra, qtde, data_compra, serie = titulo

        regra = TABELA_RENTABILIDADE.get(serie.strip())

        if not regra:
            raise Exception(f"Serie não encontrada: {serie}")

        cdi_periodo = df_cdi[df_cdi["data"] >= pd.Timestamp(data_compra)].copy()

        pu = float(pu_compra)

        for _, row in cdi_periodo.iterrows():

            if row["data"] > hoje:
                break

            if regra["tipo"] == "pos-fixado":

                fator = 1 + (regra["valor"] * row["valor"]) / 100

            elif regra["tipo"] == "hibrido":

                fator_h = (1 + regra["valor"])**(1/252) - 1
                fator = (1 + row["valor"]/100) * (1 + fator_h)

            elif regra["tipo"] == "pre-fixado":

                prefixado_aa = (1 + regra["valor"])**12 - 1
                fator = (1 + prefixado_aa)**(1/252)

            pu *= fator

            valor = pu * qtde

            inserts.append((
                titulo_id,
                row["data"],
                pu,
                valor
            ))

        # 🔥 envia em lotes para não estourar memória
        if len(inserts) > 5000:

            execute_values(
                cursor,
                """
                INSERT INTO snapshot_divida
                (titulo_id, data_snapshot, pu_atual, valor_atual)
                VALUES %s
                ON CONFLICT (titulo_id, data_snapshot) DO NOTHING
                """,
                inserts
            )

            conn.commit()
            inserts.clear()

            print("🚀 lote inserido")

    # último lote
    if inserts:

        execute_values(
            cursor,
            """
            INSERT INTO snapshot_divida
            (titulo_id, data_snapshot, pu_atual, valor_atual)
            VALUES %s
            ON CONFLICT (titulo_id, data_snapshot) DO NOTHING
            """,
            inserts
        )

        conn.commit()

    cursor.close()
    conn.close()

    print("🔥 Snapshot histórico concluído!")


if __name__ == "__main__":
    gerar_snapshot()
