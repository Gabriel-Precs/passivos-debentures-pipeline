import numpy as np
import pandas as pd 
import requests 
from datetime import datetime

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


def cdi_atualizado():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=05/01/2023"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Erro ao coletar o CDI!")
    
    cdi = pd.DataFrame(response.json())
    cdi["valor"] = cdi["valor"].astype(float)
    cdi["data"] = pd.to_datetime(cdi["data"], dayfirst= True)
    return cdi



def criacao_df():
    
    df_debentures = pd.read_csv("data/Debentures_Base.csv")
    
    # Tratar a coluna da PU
    df_debentures["PU Compra"] = (df_debentures["PU Compra"].astype(str).str.replace(".",""))
    df_debentures["PU Compra"] = (df_debentures["PU Compra"].astype(str).str.replace(",",".").astype(float))
    
    #Tratar a coluna de Valor
    df_debentures["Valor Compra"] = (df_debentures["Valor Compra"].astype(str).str.replace(".",""))
    df_debentures["Valor Compra"] = (df_debentures["Valor Compra"].astype(str).str.replace(",",".").astype(float))

    df_debentures["Data Compra"] = df_debentures["Data Compra"].str.strip()
    df_debentures["Data Compra"] = pd.to_datetime(df_debentures["Data Compra"], dayfirst = True)
    #print(df_debentures)
    
    return df_debentures

def calculo_PU_atulizado(df_debentures,df_cdi):
    pu_atual = []
    
    #print(df_cdi["data"].max())

    for index ,row in df_debentures.iterrows():
        data_compra = row["Data Compra"]
        pu_compra = row["PU Compra"]
        serie = row["Série"].strip()
        regra = TABELA_RENTABILIDADE.get(serie)

        if regra is None:
            raise Exception(f"Serie nao encontrada na Tabela: {serie}")
        

        cdi_periodo = df_cdi[df_cdi["data"] >= data_compra].copy()
        cdi_periodo = cdi_periodo.sort_values("data")
        
        pu_evoluindo = pu_compra

        #-------------------------
        #PRIMEIRA REGRA POS-FIXADO
        #-------------------------

        if regra["tipo"] == "pos-fixado":
            cdi_periodo["fator"] = 1+ (regra["valor"] * (cdi_periodo["valor"])/100)
            
        #-------------------------
        #SEGUNDA REGRA HIBRIDO
        #-------------------------

        elif regra["tipo"] == "hibrido":
            fator_h = (1 + regra["valor"])**(1/252)-1
            cdi_periodo["fator"] = (1+(cdi_periodo["valor"])/100)*(1+fator_h)

        #-------------------------
        #TERCEIRA REGRA PRE-FIXADO
        #-------------------------

        elif regra["tipo"] == "pre-fixado":
            prefixado_aa = (1+regra["valor"])**12-1
            cdi_periodo["fator"] = (1+prefixado_aa)**(1/252)
        
        else:
            raise Exception(f"Tipo de rentabilidade desconhcida: {regra['tipo']}")
        
        
        for fator_diario in cdi_periodo["fator"]:
            pu_evoluindo = pu_evoluindo * fator_diario
        
        pu_atual.append(pu_evoluindo)

    df_debentures["PU Atualizado"] = pu_atual
    df_debentures["Valor Atualizado"] = df_debentures["PU Atualizado"] * df_debentures["Qtde"]
            
    return df_debentures

def gerar_relatorio():
    
    df = criacao_df()
    df_cdi = cdi_atualizado()

    df = calculo_PU_atulizado(df,df_cdi)
    df.to_csv("Relatorio_Debentures_Atualizado.csv", index= False)

    print("\nRelatorio gerado com sucesso!\n")

if __name__ == "__main__":
    gerar_relatorio()