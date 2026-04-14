import os
import json
from pathlib import Path
import requests
import pandas as pd
import gspread
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO
import urllib3
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuracoes globais ---
AUTH_TOKEN = os.getenv("AUTH_TOKEN")


SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

HEADERS = {
    "x-api-key": AUTH_TOKEN,
    "Content-Type": "application/json"
}

# --- Mapeamento Plano Operacional ---
MAPEAMENTO_PLANO_OPERACIONAL = {
    4:  {"coluna": "detr_opera_avg",       "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    5:  {"coluna": "frota_BR_avg",         "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    7:  {"coluna": "bookings_avg",         "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    9:  {"coluna": "detractor_fixes_avg",  "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    10: {"coluna": "detractor_issue_avg",  "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    11: {"coluna": "detractor_recurrent_avg", "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    12: {"coluna": "detractor_inspec_avg", "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    13: {"coluna": "detractor_prep_avg",   "estado": None, "card_id": 88627, "param_name": "data_inicial"},
    15: {"coluna": "detractor_maint_avg",  "estado": "RS", "card_id": 88627, "param_name": "data_inicial"},
    17: {"coluna": "detractor_maint_avg",  "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    18: {"coluna": "detractor_fun_avg",    "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    19: {"coluna": "detractor_mec_avg",    "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    20: {"coluna": "detractor_kts_avg",    "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    21: {"coluna": "detractor_chave_avg",  "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    22: {"coluna": "detractor_vidro_avg",  "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    24: {"coluna": "detractor_inspec_avg", "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    25: {"coluna": "detractor_inspec_mec_avg", "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    26: {"coluna": "detractor_inspec_fun_avg", "estado": "SP", "card_id": 88627, "param_name": "data_inicial"},
    28: {"coluna": "media_mensal_ag_peca", "estado": None, "card_id": 89637, "param_name_start": "data_inicial", "param_name_end": "data_final"},
    30: {"coluna": "total_ops", "estado": None, "card_id": 89636, "param_name": "data_backlog"},
    31: {"coluna": "total_desmob", "estado": None, "card_id": 89636, "param_name": "data_backlog"},
}

# --- Mapeamento Reunião de Produção ---
MAPEAMENTO_REUNIAO_PRODUCAO = {
    3:  {"url": 89657, "coluna": "flag_mec", "params": ["estado", "data_backlog"]},
    5:  {"url": 89657, "coluna": "total_kts", "params": ["estado", "data_backlog"]},
    6:  {"url": 89657, "coluna": "flag_fun", "params": ["estado", "data_backlog"]},
    7:  {"url": 89657, "coluna": "total_preparation", "params": ["estado", "data_backlog"]},
    23: {"url": 89657, "coluna": "total_diag_mec_pend", "params": ["estado", "data_backlog"]},
    24: {"url": 89657, "coluna": "total_diag_fun_pend", "params": ["estado", "data_backlog"]},
    25: {"url": 89657, "coluna": "inspection_sem_servico", "params": ["estado", "data_backlog"]},
    33: {"url": 89652, "coluna": "ag_cq_mec", "params": ["data"]},
    34: {"url": 89652, "coluna": "ag_cq_fun", "params": ["data"]},
    35: {"url": 89652, "coluna": "ag_cq_torre", "params": ["data"]},
    36: {"url": 89652, "coluna": "ag_cq_kts", "params": ["data"]},
    29: {"url": 89651, "coluna": "M1", "params": []},
    30: {"url": 89651, "coluna": "R1", "params": []},
    31: {"url": 89651, "coluna": "F1", "params": []},
    4:  {"url": 87449, "coluna": "total", "params": ["data"]},
    26: {"url": 89686, "coluna": "pendente_diagnostico", "params": []},
    27: {"url": 89687, "coluna": "total", "params": []},
}

# --- Mapeamento Comitê de Crise ---
comite_de_crise_mapping_I = {
    2: {"url": 89761, "coluna": "frota_sp", "coluna_alvo": "N"},
    3: {"url": 89761, "coluna": "detr_opera", "coluna_alvo": "I"},
    4: {"url": 89761, "coluna": "maint_fixes", "coluna_alvo": "I"},
    5: {"url": 89761, "coluna": "maint_issue", "coluna_alvo": "I"},
    6: {"url": 89761, "coluna": "maint_recurrent", "coluna_alvo": "I"},
    7: {"url": 89761, "coluna": "total_inspection", "coluna_alvo": "I"},
    8: {"url": 91684, "coluna": "total_BR", "coluna_alvo": "I"},
    9: {"url": 91684, "coluna": "Sao Paulo", "coluna_alvo": "I"},
    10: {"url": 91684, "coluna": "POA", "coluna_alvo": "I"},
    11: {"url": 91684, "coluna": "Novas cidades", "coluna_alvo": "I"},
    12: {"url": 93017, "coluna": "detr_opera", "coluna_alvo": "I"},
    13: {"url": 93017, "coluna": "maint_fixes", "coluna_alvo": "I"},
    14: {"url": 93017, "coluna": "maint_issue", "coluna_alvo": "I"},
    15: {"url": 93017, "coluna": "maint_recurrent", "coluna_alvo": "I"},
    16: {"url": 93017, "coluna": "total_inspection", "coluna_alvo": "I"},           
    17: {"url": 89765, "coluna": "rec_pend", "coluna_alvo": "I"},
    18: {"url": 89766, "coluna": "ag_diag", "coluna_alvo": "I"},
    19: {"url": 96394, "coluna": "pendente_diagnostico", "coluna_alvo": "I"},
    20: {"url": 89761, "coluna": "total_flag_mec", "coluna_alvo": "I"},
    21: {"url": 89761, "coluna": "total_flag_fun", "coluna_alvo": "I"},
    22: {"url": 93017, "coluna": "total_flag_mec", "coluna_alvo": "I"},
    23: {"url": 93017, "coluna": "total_flag_fun", "coluna_alvo": "I"},    
    24: {"url": 89771, "coluna": "total_recebidos", "coluna_alvo": "I", "filtro_data_chegada": True},
    25: {"url": 89773, "coluna": "diag_mec", "coluna_alvo": "I"},
    26: {"url": 89850, "coluna": "total_diag", "coluna_alvo": "I"},
    27: {"url": 89777, "coluna": "JURUBATUBA - MECANICA", "coluna_alvo": "I", "filtro_data_inicial": True},
    28: {"url": 89777, "coluna": "JURUBATUBA - FUNILARIA", "coluna_alvo": "I", "filtro_data_inicial": True},
    29: {"url": 89777, "coluna": "AMADOR BUENO - FUNILARIA", "coluna_alvo": "I", "filtro_data_inicial": True},
    30: {"url": 89777, "coluna": "OFICINA EXTERNA", "coluna_alvo": "I", "filtro_data_inicial": True},
    32: {"url": 97334, "coluna": "total", "coluna_alvo": "I", "filtro_data_inicial": True}
}


comite_de_crise_mapping_J = {
    3: {"url": 89761, "coluna": "detr_operacoes", "coluna_alvo": "J"},
    4: {"url": 89761, "coluna": "detr_fixes", "coluna_alvo": "J"},
    5: {"url": 89761, "coluna": "detr_issue", "coluna_alvo": "J"},
    6: {"url": 89761, "coluna": "detr_recurrent", "coluna_alvo": "J"},
    7: {"url": 89761, "coluna": "detr_inspection", "coluna_alvo": "J"},
    8: {"url": 89761, "coluna": "detr_preparation", "coluna_alvo": "J"},
    9: {"url": 89761, "coluna": "detr_prep_sp", "coluna_alvo": "J"},
    10: {"url": 89761, "coluna": "detr_prep_poa", "coluna_alvo": "J"},
    11: {"url": 89761, "coluna": "detr_prep_cidades", "coluna_alvo": "J"},
    12: {"url": 89761, "coluna": "detr_operacoes_sp", "coluna_alvo": "J"},
    13: {"url": 89761, "coluna": "detr_fixes_sp", "coluna_alvo": "J"},
    14: {"url": 89761, "coluna": "detr_issue_sp", "coluna_alvo": "J"},
    15: {"url": 89761, "coluna": "detr_recurrent_sp", "coluna_alvo": "J"},
    16: {"url": 89761, "coluna": "detr_inspection_sp", "coluna_alvo": "J"},
    20: {"url": 89761, "coluna": "detr_mec", "coluna_alvo": "J"},
    21: {"url": 89761, "coluna": "detr_fun", "coluna_alvo": "J"},
    22: {"url": 89761, "coluna": "detr_mec_sp", "coluna_alvo": "J"},
    23: {"url": 89761, "coluna": "detr_fun_sp", "coluna_alvo": "J"}
}

def configurar_google_sheets(json_keyfile=None):
    google_creds_env = os.getenv("GOOGLE_CREDENTIALS")

    if google_creds_env:
        creds_dict = json.loads(google_creds_env)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
    else:
        if json_keyfile is None:
            json_keyfile = Path(__file__).resolve().parent / "atualiza-sheets-15ac1cb4807d.json"

        creds = ServiceAccountCredentials.from_json_keyfile_name(str(json_keyfile), SCOPES)

    client = gspread.authorize(creds)
    return client.open("[OPS] Farol indicadores UR")

def escolher_aba(spreadsheet):
    abas = [ws.title for ws in spreadsheet.worksheets()]
    print("\nAbas disponíveis na planilha:")
    for idx, aba in enumerate(abas, 1):
        print(f"{idx}: {aba}")
    while True:
        escolha = input("Digite o número da aba que deseja atualizar: ")
        try:
            idx = int(escolha)
            if 1 <= idx <= len(abas):
                return abas[idx-1]
            else:
                print("Número inválido. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Digite apenas o número da aba.")

def get_data_producao():
    hoje = datetime.today()
    data_producao = hoje - timedelta(days=1)
    if data_producao.weekday() == 6:  # domingo
        data_producao = hoje - timedelta(days=2)
    return data_producao.strftime("%Y-%m-%d")

def get_data_sexta_anterior():
    hoje = datetime.today()
    ontem = hoje - timedelta(days=1)

    # Só entra se ontem foi domingo
    if ontem.weekday() == 6:
        sabado = hoje - timedelta(days=2)
        sexta = hoje - timedelta(days=3)
        return {
            "sexta": sexta.strftime("%Y-%m-%d"),
            "sabado": sabado.strftime("%Y-%m-%d")
        }
    return None

def parametros_json_safe(parametros):
    for p in parametros:
        if "value" in p:
            if hasattr(p["value"], "item"):
                p["value"] = p["value"].item()
            elif isinstance(p["value"], (pd.Timestamp, datetime)):
                p["value"] = str(p["value"].date())
            else:
                p["value"] = p["value"]
    return parametros

def obter_dados_metabase(card_id, parametros):
    url = f"https://metabase.kovi.us/api/card/{card_id}/query/json"
    parametros = parametros_json_safe(parametros)
    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json={"parameters": parametros},
            verify=False
        )
        response.raise_for_status()

        resultado = response.json()

        if isinstance(resultado, list):
            df = pd.DataFrame(resultado)
        elif isinstance(resultado, dict) and "data" in resultado:
            cols = [col["name"] for col in resultado["data"]["cols"]]
            rows = resultado["data"]["rows"]
            df = pd.DataFrame(rows, columns=cols)
        else:
            print(f"Formato inesperado no card {card_id}: {resultado}")
            return None

        df.columns = df.columns.str.strip()
        
        
        return df

    except Exception as e:
        print(f"Erro ao obter dados do card {card_id}: {str(e)}")
        try:
            print("Resposta:", response.text[:1000])
        except:
            pass
        return None
    
def get_data_reuniao_producao():
    hoje = datetime.today()
    data_ontem = hoje - timedelta(days=1)
    return data_ontem.strftime("%Y-%m-%d")


def processar_atualizacao(sheet, data_atual, mapeamento, coluna_alvo, tipo_aba=None):
    data_str = data_atual.strftime("%Y-%m-%d")
    primeiro_dia_mes = date(data_atual.year, data_atual.month, 1).strftime("%Y-%m-%d")
    
    if tipo_aba == "reuniao_producao":
        data_filtro = get_data_reuniao_producao()
    else:
        data_filtro = data_str


    for linha, info in mapeamento.items():
        try:
            parametros = []
            if "url" in info:
                if info["url"] == 89773:
                    parametros = []
                else:
                    if info.get("params"):
                        for param in info["params"]:
                            if param == "estado":
                                parametros.append({"type": "category", "target": ["variable", ["template-tag", param]], "value": "SP"})
                            elif param == "data_backlog":
                                parametros.append({"type": "date", "target": ["variable", ["template-tag", param]], "value": data_filtro})
                            elif param == "data":
                                parametros.append({"type": "date", "target": ["variable", ["template-tag", param]], "value": data_filtro})
                    if info.get("filtro_data_chegada"):
                        parametros.append({"type": "date", "target": ["variable", ["template-tag", "data_chegada"]], "value": data_filtro})
                    if info.get("filtro_data_inicial"):
                        parametros.append({"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_filtro})
                    if info.get("filtro_data"):
                        parametros.append({"type": "date", "target": ["variable", ["template-tag", "data"]], "value": data_filtro})
                df = obter_dados_metabase(info["url"], parametros)
                coluna = info["coluna"]
                coluna_destino = info.get("coluna_alvo", coluna_alvo)
            else:
                parametros = []
                if linha == 28:
                    parametros = [
                        {"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": primeiro_dia_mes},
                        {"type": "date", "target": ["variable", ["template-tag", "data_final"]], "value": primeiro_dia_mes}
                    ]
                else:
                    if info.get("param_name") == "data_inicial":
                        parametros.append({"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": primeiro_dia_mes})
                    elif info.get("param_name") == "data_backlog":
                        parametros.append({"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_str})
                if info.get("estado"):
                    parametros.append({"type": "category", "target": ["variable", ["template-tag", "estado"]], "value": info["estado"]})
                df = obter_dados_metabase(info["card_id"], parametros)
                coluna = info["coluna"]
                coluna_destino = coluna_alvo
            if df is not None and coluna in df.columns:
                raw_valor = df[coluna].values[0]
                if coluna_destino == "H":
                    valor = float(raw_valor)
                else:
                    valor = float(raw_valor)
                cell = f"{coluna_destino}{linha}"
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} atualizada: {valor}")
            else:
                print(f"Dados não encontrados para linha {linha} coluna {coluna}")
        except Exception as e:
            print(f"Erro na linha {linha}: {str(e)}")

def atualizar_comite_de_crise(sheet, data_atual):
    # Atualização padrão (colunas I e J)
    processar_atualizacao(sheet, data_atual, comite_de_crise_mapping_I, "I")
    processar_atualizacao(sheet, data_atual, comite_de_crise_mapping_J, "J")

    # Atualizações detalhadas para G e H colunas e linhas específicas
    data_filtro = get_data_reuniao_producao()

    card_id_89830 = 89830
    colunas_89830 = [ 
        ("M2", "frota_sp"),
        ("G3", "detr_opera"),
        ("G4", "maint_fixes"),
        ("G5", "maint_issue"),
        ("G6", "maint_recurrent"),
        ("G7", "total_inspection"),
        ("G12", "detr_opera_sp"),
        ("G13", "maint_fixes_sp"),
        ("G14", "maint_issue_sp"),
        ("G15", "maint_recurrent_sp"),
        ("G16", "total_inspection_sp"),
             
    ]
    card_id_91686 = 91686
    colunas_91686 = [
        ("G8", "total_preparation"),
        ("G9", "prep_sao_paulo"),
        ("G10", "prep_poa"),
        ("G11", "prep_novas_cidades"),
        ("H9", "detr_prep_sp"),
        ("H10", "detr_prep_poa"),
        ("H11", "detr_prep_cidades"),
               
        
    ]
    colunas_H = [
        ("H3", "detr_operacoes"),
        ("H4", "detr_fixes"),
        ("H5", "detr_issue"),
        ("H6", "detr_recurrent"),
        ("H7", "detr_inspection"),
        ("H8", "detr_preparation"),
        ("H12", "detr_operacoes_sp"),
        ("H13", "detr_fixes_sp"),
        ("H14", "detr_issue_sp"),
        ("H15", "detr_recurrent_sp"),
        ("H16", "detr_inspection_sp"),
    ]
    for cell, coluna in colunas_89830:
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_89830, parametros)
        if df is not None and coluna in df.columns:
            valor = float(df[coluna].values[0])
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")
    for cell, coluna in colunas_91686:
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_91686, parametros)
        if df is not None and coluna in df.columns:
            valor = float(df[coluna].values[0])
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")
    for cell, coluna in colunas_H:
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_89830, parametros)  
        if df is not None and coluna in df.columns:
            valor = float(df[coluna].values[0])
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")
            
    card_id_92088 = 92088
    parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_filtro}]
    df = obter_dados_metabase(card_id_92088, parametros)
    if df is not None and "ag_diag" in df.columns:
        valor = round(float(df["ag_diag"].values[0]), 2)
        sheet.update_acell("G18", valor)
        print("Célula G18 atualizada:", valor)


    card_id_3 = 89847
    for cell, coluna in [("G20", "total_flag_mec"), ("G21", "total_flag_fun"), ("G22", "total_flag_mec_sp"), ("G23", "total_flag_fun_sp"), ("H20", "detr_mec"), ("H21", "detr_fun"), ("H22", "detr_mec_sp"), ("H23", "detr_fun_sp")]:
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_3, parametros)
        if df is not None and coluna in df.columns:
            valor = float(df[coluna].values[0])
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")

    card_id_4 = 89771
    parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_chegada"]], "value": data_filtro}]
    df = obter_dados_metabase(card_id_4, parametros)
    if df is not None and "total_recebidos" in df.columns:
        valor = round(float(df["total_recebidos"].values[0]), 2)
        sheet.update_acell("G24", valor)
        print("Célula G24 atualizada:", valor)

    card_id_5 = 89773
    parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_filtro}]
    df = obter_dados_metabase(card_id_5, parametros)
    if df is not None and "diag_mec" in df.columns:
        valor = round(float(df["diag_mec"].values[0]), 2)
        sheet.update_acell("G25", valor)
        print("Célula G25 atualizada:", valor)

    card_id_6 = 89850
    parametros = [{"type": "date", "target": ["variable", ["template-tag", "data"]], "value": data_filtro}]
    df = obter_dados_metabase(card_id_6, parametros)
    if df is not None and "total_diag" in df.columns:
        valor = round(float(df["total_diag"].values[0]), 2)
        sheet.update_acell("G26", valor)
        print("Célula G26 atualizada:", valor)
    
    card_id_97334 = 97334
    parametros = [{"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_filtro}]
    df = obter_dados_metabase(card_id_97334, parametros)
    if df is not None and "total" in df.columns:
        valor = round(float(df["total"].values[0]), 2)
        sheet.update_acell("G32", valor)
        print("Célula G32 atualizada:", valor)

    card_id_7 = 89777
    colunas_e18_e21 = [
        ("G27", "JURUBATUBA - MECANICA"),
        ("G28", "JURUBATUBA - FUNILARIA"),
        ("G29", "AMADOR BUENO - FUNILARIA"),
        ("G30", "OFICINA EXTERNA"),
    ]
    for cell, coluna in colunas_e18_e21:
        parametros = [
            {"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_filtro},
            {"type": "date", "target": ["variable", ["template-tag", "Date_final"]], "value": data_filtro}
        ]
        df = obter_dados_metabase(card_id_7, parametros)
        if df is not None and coluna in df.columns:
            valor = float(df[coluna].values[0])
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")

    # Atualização para sexta-feira se ontem foi domingo (colunas C e D)
    datas = get_data_sexta_anterior()
    data_sexta = datas["sexta"] if datas else None
    if data_sexta:
        card_id_89830 = 89830
        colunas_89830 = [
        ("M4", "frota_sp"),
        ("C3", "detr_opera"),
        ("C4", "maint_fixes"),
        ("C5", "maint_issue"),
        ("C6", "maint_recurrent"),
        ("C7", "total_inspection"),
        ("C12", "detr_opera_sp"),
        ("C13", "maint_fixes_sp"),
        ("C14", "maint_issue_sp"),
        ("C15", "maint_recurrent_sp"),
        ("C16", "total_inspection_sp"),
        
        ]
    
        card_id_91686 = 91686
        colunas_91686 = [
        ("C8", "total_preparation"),
        ("C9", "prep_sao_paulo"),
        ("C10", "prep_poa"),
        ("C11", "prep_novas_cidades"),
        ("D9", "detr_prep_sp"),
        ("D10", "detr_prep_poa"),
        ("D11", "detr_prep_cidades"),
    ]
        
        card_id_colunas_d = 89830  
        colunas_d = [
        ("D3", "detr_operacoes"),
        ("D4", "detr_fixes"),
        ("D5", "detr_issue"),
        ("D6", "detr_recurrent"),
        ("D7", "detr_inspection"),
        ("D8", "detr_preparation"),
        ("D12", "detr_operacoes_sp"),
        ("D13", "detr_fixes_sp"),
        ("D14", "detr_issue_sp"),
        ("D15", "detr_recurrent_sp"),
        ("D16", "detr_inspection_sp"),
    ]
        for cell, coluna in colunas_89830:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sexta}]
            df = obter_dados_metabase(card_id_89830, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")
        for cell, coluna in colunas_91686:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sexta}]
            df = obter_dados_metabase(card_id_91686, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")
        for cell, coluna in colunas_d:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sexta}]
            df = obter_dados_metabase(card_id_colunas_d, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")
                
            
        card_id_92088 = 92088
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_92088, parametros)
        if df is not None and "ag_diag" in df.columns:
            valor = round(float(df["ag_diag"].values[0]), 2)
            sheet.update_acell("C18", valor)
            print("Célula C18 atualizada:", valor)

        card_id_3 = 89847
        for cell, coluna in [("C20", "total_flag_mec"), ("C21", "total_flag_fun"), ("C22", "total_flag_mec_sp"), ("C23", "total_flag_fun_sp"), ("D20", "detr_mec"), ("D21", "detr_fun"), ("D22", "detr_mec_sp"), ("D23", "detr_fun_sp")]:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sexta}]
            df = obter_dados_metabase(card_id_3, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")

        card_id_4 = 89771
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_chegada"]], "value": data_sexta}]
        df = obter_dados_metabase(card_id_4, parametros)
        if df is not None and "total_recebidos" in df.columns:
            valor = round(float(df["total_recebidos"].values[0]), 2)
            sheet.update_acell("C24", valor)
            print("Célula C24 (sexta) atualizada:", valor)

        card_id_5 = 89773
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_sexta}]
        df = obter_dados_metabase(card_id_5, parametros)
        if df is not None and "diag_mec" in df.columns:
            valor = round(float(df["diag_mec"].values[0]), 2)
            sheet.update_acell("C25", valor)
            print("Célula C25 (sexta) atualizada:", valor)

        card_id_6 = 89850
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data"]], "value": data_sexta}]
        df = obter_dados_metabase(card_id_6, parametros)
        if df is not None and "total_diag" in df.columns:
            valor = round(float(df["total_diag"].values[0]), 2)
            sheet.update_acell("C26", valor)
            print("Célula C26 (sexta) atualizada:", valor)
            
        card_id_97334 = 97334
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_sexta}]
        df = obter_dados_metabase(card_id_97334, parametros)
        if df is not None and "total" in df.columns:
            valor = round(float(df["total"].values[0]), 2)
            sheet.update_acell("C32", valor)
            print("Célula C32 (sexta) atualizada:", valor)

        card_id_7 = 89777
        colunas_c18_c21 = [
            ("C27", "JURUBATUBA - MECANICA"),
            ("C28", "JURUBATUBA - FUNILARIA"),
            ("C29", "AMADOR BUENO - FUNILARIA"),
            ("C30", "OFICINA EXTERNA"),
        ]
        for cell, coluna in colunas_c18_c21:
            parametros = [
                {"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_sexta},
                {"type": "date", "target": ["variable", ["template-tag", "Date_final"]], "value": data_sexta}
            ]
            df = obter_dados_metabase(card_id_7, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")
    
    # Atualização para sábado se ontem foi domingo (colunas E e F)
    if data_sexta:
        data_sabado = (datetime.strptime(data_sexta, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        card_id_89830 = 89830
        colunas_89830 = [
            ("M3", "frota_sp"),
            ("E3", "detr_opera"),
            ("E4", "maint_fixes"),
            ("E5", "maint_issue"),
            ("E6", "maint_recurrent"),
            ("E7", "total_inspection"),
            ("E12", "detr_opera_sp"),
            ("E13", "maint_fixes_sp"),
            ("E14", "maint_issue_sp"),
            ("E15", "maint_recurrent_sp"),
            ("E16", "total_inspection_sp"),
        ]
        card_id_91686 = 91686
        colunas_91686 = [
            ("E8", "total_preparation"),
            ("E9", "prep_sao_paulo"),
            ("E10", "prep_poa"),
            ("E11", "prep_novas_cidades"),
            ("F9", "detr_prep_sp"),
            ("F10", "detr_prep_poa"),
            ("F11", "detr_prep_cidades"),
        ]
        colunas_f = [
            ("F3", "detr_operacoes"),
            ("F4", "detr_fixes"),
            ("F5", "detr_issue"),
            ("F6", "detr_recurrent"),
            ("F7", "detr_inspection"),
            ("F8", "detr_preparation"),
            ("F12", "detr_operacoes_sp"),
            ("F13", "detr_fixes_sp"),
            ("F14", "detr_issue_sp"),
            ("F15", "detr_recurrent_sp"),
            ("F16", "detr_inspection_sp"),
        ]

        # Atualiza colunas E (dados principais)
        for cell, coluna in colunas_89830:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sabado}]
            df = obter_dados_metabase(card_id_89830, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sábado) atualizada: {valor}")

        for cell, coluna in colunas_91686:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sabado}]
            df = obter_dados_metabase(card_id_91686, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sábado) atualizada: {valor}")

        # Atualiza colunas F (detratores)
        for cell, coluna in colunas_f:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sabado}]
            df = obter_dados_metabase(card_id_89830, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sábado) atualizada: {valor}")
                
                
        card_id_92088 = 92088
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_filtro}]
        df = obter_dados_metabase(card_id_92088, parametros)
        if df is not None and "ag_diag" in df.columns:
            valor = round(float(df["ag_diag"].values[0]), 2)
            sheet.update_acell("E18", valor)
            print("Célula E18 atualizada:", valor)

        card_id_3 = 89847
        for cell, coluna in [("E20", "total_flag_mec"), ("E21", "total_flag_fun"), ("E22", "total_flag_mec_sp"), ("E23", "total_flag_fun_sp"), ("F20", "detr_mec"), ("F21", "detr_fun"), ("F22", "detr_mec_sp"), ("F23", "detr_fun_sp")]:
            parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_backlog"]], "value": data_sabado}]
            df = obter_dados_metabase(card_id_3, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sabado) atualizada: {valor}")

        card_id_4 = 89771
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_chegada"]], "value": data_sabado}]
        df = obter_dados_metabase(card_id_4, parametros)
        if df is not None and "total_recebidos" in df.columns:
            valor = round(float(df["total_recebidos"].values[0]), 2)
            sheet.update_acell("E24", valor)
            print("Célula E24 (sabado) atualizada:", valor)

        card_id_5 = 89773
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data_inicial"]], "value": data_sabado}]
        df = obter_dados_metabase(card_id_5, parametros)
        if df is not None and "diag_mec" in df.columns:
            valor = round(float(df["diag_mec"].values[0]), 2)
            sheet.update_acell("E25", valor)
            print("Célula E25 (sabado) atualizada:", valor)

        card_id_6 = 89850
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "data"]], "value": data_sabado}]
        df = obter_dados_metabase(card_id_6, parametros)
        if df is not None and "total_diag" in df.columns:
            valor = round(float(df["total_diag"].values[0]), 2)
            sheet.update_acell("E26", valor)
            print("Célula E26 (sabado) atualizada:", valor)
            
        card_id_97334 = 97334
        parametros = [{"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_sabado}]
        df = obter_dados_metabase(card_id_97334, parametros)
        if df is not None and "total" in df.columns:
            valor = round(float(df["total"].values[0]), 2)
            sheet.update_acell("E32", valor)
            print("Célula E32 (sabado) atualizada:", valor)

        card_id_7 = 89777
        colunas_c18_c21 = [
            ("E27", "JURUBATUBA - MECANICA"),
            ("E28", "JURUBATUBA - FUNILARIA"),
            ("E29", "AMADOR BUENO - FUNILARIA"),
            ("E30", "OFICINA EXTERNA"),
        ]
        for cell, coluna in colunas_c18_c21:
            parametros = [
                {"type": "date", "target": ["variable", ["template-tag", "Date_inicial"]], "value": data_sabado},
                {"type": "date", "target": ["variable", ["template-tag", "Date_final"]], "value": data_sabado}
            ]
            df = obter_dados_metabase(card_id_7, parametros)
            if df is not None and coluna in df.columns:
                valor = float(df[coluna].values[0])
                sheet.update_acell(cell, valor)
                print(f"Célula {cell} (sexta) atualizada: {valor}")

def atualizar_blockers_mec_jurubatuba(sheet):
    card_id = 88935
    hoje = datetime.today()
    data_atual = hoje.strftime("%Y-%m-%d")

    url = f"https://metabase.kovi.us/api/card/{card_id}/query/json"

    # Coluna C (C2 a C13): data atual
    parametros_c = [{"type": "date", "target": ["variable", ["template-tag", "data"]], "value": data_atual}]
    try:
        response = requests.post(url, headers=HEADERS, json={"parameters": parametros_c}, verify=False)
        response.raise_for_status()

        resultado = response.json()
        cols = [col["name"] for col in resultado["data"]["cols"]]
        rows = resultado["data"]["rows"]

        df = pd.DataFrame(rows, columns=cols)
        df.columns = df.columns.str.strip()

        valores_c = df['total_dia'].head(12).tolist()

    except Exception as e:
        print(f"Erro ao obter dados do card {card_id} para data atual: {str(e)}")
        valores_c = [None]*12

    for i, valor in enumerate(valores_c, start=2):
        cell = f"C{i}"
        if valor is not None:
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")

    # Coluna B (B2 a B13): ontem ou anteontem se domingo
    data_ontem = hoje - timedelta(days=1)
    if data_ontem.weekday() == 6:
        data_ontem = hoje - timedelta(days=2)

    data_ontem_str = data_ontem.strftime("%Y-%m-%d")

    parametros_b = [{
        "type": "date",
        "target": ["variable", ["template-tag", "data"]],
        "value": data_ontem_str
    }]

    try:
        response = requests.post(url, headers=HEADERS, json={"parameters": parametros_b}, verify=False)
        response.raise_for_status()

        resultado = response.json()
        cols = [col["name"] for col in resultado["data"]["cols"]]
        rows = resultado["data"]["rows"]

        df = pd.DataFrame(rows, columns=cols)
        df.columns = df.columns.str.strip()

        valores_b = df['total_dia'].head(12).tolist()

    except Exception as e:
        print(f"Erro ao obter dados do card {card_id} para data {data_ontem_str}: {str(e)}")
        valores_b = [None]*12

    for i, valor in enumerate(valores_b, start=2):
        cell = f"B{i}"
        if valor is not None:
            sheet.update_acell(cell, valor)
            print(f"Célula {cell} atualizada: {valor}")

def main():
    data_atual = datetime.today()
    client = configurar_google_sheets("atualiza-sheets-15ac1cb4807d.json")
    spreadsheet = client

    nome_aba = "comite de crise"
    print(f"\nExecutando automaticamente a aba: {nome_aba}")

    sheet = spreadsheet.worksheet(nome_aba)
    if nome_aba.lower() == "plano operacional":
        data_atual = datetime.today() - timedelta(days=1)
        processar_atualizacao(sheet, data_atual, MAPEAMENTO_PLANO_OPERACIONAL, "F")
    elif nome_aba.lower() in ["reunião de produção", "reuniao de producao"]:
        processar_atualizacao(sheet, data_atual, MAPEAMENTO_REUNIAO_PRODUCAO, "E", tipo_aba="reuniao_producao")
    elif nome_aba.lower() == "comite de crise":
        atualizar_comite_de_crise(sheet, data_atual)
    elif nome_aba.lower() == "blockers-mec-jurubatuba":
        atualizar_blockers_mec_jurubatuba(sheet)
    else:
        print("Aba não mapeada para atualização automática.")

if __name__ == "__main__":
    main()
