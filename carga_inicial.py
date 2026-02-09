import os
import csv
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# -----------------------------
# CONFIG
# -----------------------------

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")  # recomendo usar a string completa

CSV_PATH = "data/Debentures_Base.csv"
BATCH_SIZE = 1000

# -----------------------------
# LOGGING PROFISSIONAL
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

# -----------------------------
# HELPERS
# -----------------------------

def limpar_texto(valor):
    if valor is None:
        return None
    return valor.strip()


def converter_valor_br(valor):
    """
    "10.000,000000" -> 10000.000000
    """
    if not valor:
        return None

    valor = valor.replace(".", "").replace(",", ".")
    return float(valor)


def parse_data(data_str):
    """
    Aceita:
    28/01/2025
    2025-01-28
    """
    if not data_str:
        return None

    formatos = ("%d/%m/%Y", "%Y-%m-%d")

    for fmt in formatos:
        try:
            return datetime.strptime(data_str.strip(), fmt).date()
        except ValueError:
            pass

    raise ValueError(f"Formato de data inválido: {data_str}")


# -----------------------------
# CONEXÃO
# -----------------------------

def get_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        logger.info("✅ Conectado ao Postgres")
        return conn

    except Exception as e:
        logger.exception("Erro ao conectar no banco")
        raise e


# -----------------------------
# CARGA INICIAL
# -----------------------------

def carregar_titulos():

    conn = get_connection()
    cursor = conn.cursor()

    inserts = []
    erros = 0
    total = 0

    start_time = datetime.now()

    try:

        with open(CSV_PATH, encoding="utf-8-sig") as file:

            reader = csv.DictReader(file)

            for linha_num, row in enumerate(reader, start=2):

                try:

                    inserts.append((
                        limpar_texto(row["Cliente"]),
                        parse_data(row["Data Compra"]),
                        limpar_texto(row["Emissao"]),
                        limpar_texto(row["Série"]),
                        converter_valor_br(row["PU Compra"]),
                        int(row["Qtde"]),
                        converter_valor_br(row["Valor Compra"])
                    ))

                    total += 1

                    # BULK INSERT
                    if len(inserts) >= BATCH_SIZE:
                        execute_values(
                            cursor,
                            """
                            INSERT INTO titulos (
                                cliente,
                                data_compra,
                                emissao,
                                serie,
                                pu_compra,
                                qtde,
                                valor_compra
                            )
                            VALUES %s
                            """,
                            inserts
                        )

                        inserts.clear()

                except Exception as e:
                    erros += 1
                    logger.error(f"Linha {linha_num} ignorada -> {e}")

        # INSERT FINAL
        if inserts:
            execute_values(
                cursor,
                """
                INSERT INTO titulos (
                    cliente,
                    data_compra,
                    emissao,
                    serie,
                    pu_compra,
                    qtde,
                    valor_compra
                )
                VALUES %s
                """,
                inserts
            )

        conn.commit()

        elapsed = datetime.now() - start_time

        logger.info("====================================")
        logger.info("🎯 CARGA FINALIZADA")
        logger.info(f"Linhas inseridas: {total}")
        logger.info(f"Erros: {erros}")
        logger.info(f"Tempo: {elapsed}")
        logger.info("====================================")

    except Exception as e:

        conn.rollback()
        logger.exception("🚨 ERRO GRAVE — rollback realizado")
        raise e

    finally:
        cursor.close()
        conn.close()
        logger.info("Conexão encerrada.")


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    carregar_titulos()
