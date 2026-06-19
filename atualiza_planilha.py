import os
import json
from pathlib import Path
from datetime import datetime, timedelta

import requests
import pandas as pd
import gspread
import urllib3
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

SPREADSHEET_NAME = "[OPS] Farol indicadores UR"
SHEET_NAME = "comite de crise"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "x-api-key": AUTH_TOKEN,
    "Content-Type": "application/json",
}


# =========================================================
# GOOGLE SHEETS
# =========================================================
def configurar_google_sheets(json_keyfile=None):
    if GOOGLE_CREDENTIALS:
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
    else:
        if json_keyfile is None:
            json_keyfile = Path(__file__).resolve().parent / "atualiza-sheets-15ac1cb4807d.json"

        creds = ServiceAccountCredentials.from_json_keyfile_name(str(json_keyfile), SCOPES)

    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)


# =========================================================
# DATAS
# =========================================================
def get_data_atual():
    return datetime.today().strftime("%Y-%m-%d")


def get_data_ontem():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_datas_fim_de_semana_se_segunda():
    hoje = datetime.today()
    ontem = hoje - timedelta(days=1)

    # Só entra quando ontem foi domingo, ou seja, execução na segunda-feira
    if ontem.weekday() == 6:
        sexta = hoje - timedelta(days=3)
        sabado = hoje - timedelta(days=2)

        return {
            "sexta": sexta.strftime("%Y-%m-%d"),
            "sabado": sabado.strftime("%Y-%m-%d"),
        }

    return None


# =========================================================
# METABASE
# =========================================================
def parametros_json_safe(parametros):
    for p in parametros:
        if "value" in p:
            if hasattr(p["value"], "item"):
                p["value"] = p["value"].item()
            elif isinstance(p["value"], pd.Timestamp):
                p["value"] = str(p["value"].date())
            elif isinstance(p["value"], datetime):
                p["value"] = str(p["value"].date())

    return parametros


def parametro_data(nome_parametro, valor):
    return {
        "type": "date",
        "target": ["variable", ["template-tag", nome_parametro]],
        "value": valor,
    }


def obter_dados_metabase(card_id, parametros=None):
    url = f"https://metabase.kovi.us/api/card/{card_id}/query/json"
    parametros = parametros_json_safe(parametros or [])

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json={"parameters": parametros},
            verify=False,
            timeout=120,
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
        print(f"Erro ao obter dados do card {card_id}: {e}")
        try:
            print("Resposta:", response.text[:1000])
        except Exception:
            pass
        return None


def atualizar_celula_metabase(sheet, cell, card_id, coluna, parametros=None, casas_decimais=None):
    df = obter_dados_metabase(card_id, parametros)

    if df is None:
        print(f"{cell}: sem retorno do card {card_id}")
        return

    if df.empty:
        print(f"{cell}: card {card_id} retornou vazio")
        return

    if coluna not in df.columns:
        print(f"{cell}: coluna '{coluna}' não encontrada no card {card_id}")
        print("Colunas retornadas:", df.columns.tolist())
        return

    raw_valor = df[coluna].values[0]

    if pd.isna(raw_valor):
        print(f"{cell}: valor nulo no card {card_id}, coluna {coluna}")
        return

    valor = float(raw_valor)

    if casas_decimais is not None:
        valor = round(valor, casas_decimais)

    sheet.update_acell(cell, valor)
    print(f"Célula {cell} atualizada: {valor}")


# =========================================================
# ATUALIZAÇÃO HOJE - COLUNAS I/J
# Mantém a lógica original do processar_atualizacao para comitê
# =========================================================
def atualizar_hoje_I_J(sheet, data_atual):
    print(f"\n===== ATUALIZANDO HOJE - I/J - {data_atual} =====")

    indicadores_I = [
        ("N2", 89761, "frota_sp", []),
        ("I3", 89761, "detr_opera", []),
        ("I4", 89761, "maint_fixes", []),
        ("I5", 89761, "maint_issue", []),
        ("I6", 89761, "maint_recurrent", []),
        ("I7", 89761, "total_inspection", []),
        ("I8", 89761, "total_inspection_mec", []),
        ("I9", 89761, "total_inspection_fun", []),
        ("I10", 91684, "total_BR", []),
        ("I11", 91684, "Sao Paulo", []),
        ("I12", 91684, "POA", []),
        ("I13", 91684, "Novas cidades", []),
        ("I14", 93017, "detr_opera", []),
        ("I15", 93017, "maint_fixes", []),
        ("I16", 93017, "maint_issue", []),
        ("I17", 93017, "maint_recurrent", []),
        ("I18", 93017, "total_inspection", []),
        ("I19", 89765, "rec_pend", []),
        ("I20", 89662, "backlog", []),
        ("I21", 96394, "pendente_diagnostico", []),
        ("I22", 89761, "total_flag_mec", []),
        ("I23", 89761, "total_flag_fun", []),
        ("I24", 93017, "total_flag_mec", []),
        ("I25", 93017, "total_flag_fun", []),
        ("I26", 89771, "total_recebidos", [parametro_data("data_chegada", data_atual)]),
        # No original, card 89773 era chamado sem parâmetro dentro do processar_atualizacao.
        ("I27", 89773, "diag_mec", []),
        ("I28", 89850, "total_diag", []),
        ("I29", 89777, "JURUBATUBA - MECANICA", [parametro_data("Date_inicial", data_atual)]),
        ("I30", 89777, "JURUBATUBA - FUNILARIA", [parametro_data("Date_inicial", data_atual)]),
        ("I31", 89777, "AMADOR BUENO - FUNILARIA", [parametro_data("Date_inicial", data_atual)]),
        ("I32", 98982, "funilaria", [parametro_data("DATA_INICIAL", data_atual)]),
        ("I33", 98982, "mecanica", [parametro_data("DATA_INICIAL", data_atual)]),
        ("I35", 97334, "total", [parametro_data("Date_inicial", data_atual)]),
    ]

    indicadores_J = [
        ("J3", 89761, "detr_operacoes", []),
        ("J4", 89761, "detr_fixes", []),
        ("J5", 89761, "detr_issue", []),
        ("J6", 89761, "detr_recurrent", []),
        ("J7", 89761, "detr_inspection", []),
        ("J8", 89761, "detr_inspection_mec", []),
        ("J9", 89761, "detr_inspection_fun", []),
        ("J10", 89761, "detr_preparation", []),
        ("J11", 89761, "detr_prep_sp", []),
        ("J12", 89761, "detr_prep_poa", []),
        ("J13", 89761, "detr_prep_cidades", []),
        ("J14", 89761, "detr_operacoes_sp", []),
        ("J15", 89761, "detr_fixes_sp", []),
        ("J16", 89761, "detr_issue_sp", []),
        ("J17", 89761, "detr_recurrent_sp", []),
        ("J18", 89761, "detr_inspection_sp", []),
        ("J22", 89761, "detr_mec", []),
        ("J23", 89761, "detr_fun", []),
        ("J24", 89761, "detr_mec_sp", []),
        ("J25", 89761, "detr_fun_sp", []),
    ]

    for cell, card_id, coluna, parametros in indicadores_I + indicadores_J:
        atualizar_celula_metabase(sheet, cell, card_id, coluna, parametros)


# =========================================================
# BLOCOS DETALHADOS - G/H, C/D, E/F
# =========================================================
def atualizar_bloco_principal(sheet, data_filtro, coluna_base, coluna_aux_m=None):
    mapa = [
        (f"{coluna_base}3", 89830, "detr_opera", "data_backlog"),
        (f"{coluna_base}4", 89830, "maint_fixes", "data_backlog"),
        (f"{coluna_base}5", 89830, "maint_issue", "data_backlog"),
        (f"{coluna_base}6", 89830, "maint_recurrent", "data_backlog"),
        (f"{coluna_base}7", 89830, "total_inspection", "data_backlog"),
        (f"{coluna_base}8", 89830, "total_inspection_mec", "data_backlog"),
        (f"{coluna_base}9", 89830, "total_inspection_fun", "data_backlog"),
        (f"{coluna_base}10", 91686, "total_preparation", "data_backlog"),
        (f"{coluna_base}11", 91686, "prep_sao_paulo", "data_backlog"),
        (f"{coluna_base}12", 91686, "prep_poa", "data_backlog"),
        (f"{coluna_base}13", 91686, "prep_novas_cidades", "data_backlog"),
        (f"{coluna_base}14", 89830, "detr_opera_sp", "data_backlog"),
        (f"{coluna_base}15", 89830, "maint_fixes_sp", "data_backlog"),
        (f"{coluna_base}16", 89830, "maint_issue_sp", "data_backlog"),
        (f"{coluna_base}17", 89830, "maint_recurrent_sp", "data_backlog"),
        (f"{coluna_base}18", 89830, "total_inspection_sp", "data_backlog"),
        (f"{coluna_base}20", 92088, "ag_diag", "data_inicial"),
        (f"{coluna_base}22", 89847, "total_flag_mec", "data_backlog"),
        (f"{coluna_base}23", 89847, "total_flag_fun", "data_backlog"),
        (f"{coluna_base}24", 89847, "total_flag_mec_sp", "data_backlog"),
        (f"{coluna_base}25", 89847, "total_flag_fun_sp", "data_backlog"),
        (f"{coluna_base}26", 89771, "total_recebidos", "data_chegada"),
        (f"{coluna_base}27", 89773, "diag_mec", "data_inicial"),
        (f"{coluna_base}28", 89850, "total_diag", "data"),
        (f"{coluna_base}35", 97334, "total", "Date_inicial"),
    ]

    for cell, card_id, coluna, param_name in mapa:
        parametros = [parametro_data(param_name, data_filtro)]
        atualizar_celula_metabase(sheet, cell, card_id, coluna, parametros, casas_decimais=2)

    if coluna_aux_m:
        atualizar_celula_metabase(
            sheet,
            coluna_aux_m,
            89830,
            "frota_sp",
            [parametro_data("data_backlog", data_filtro)],
        )


def atualizar_detratores(sheet, data_filtro, coluna_destino):
    mapa = [
        (f"{coluna_destino}3", 89830, "detr_operacoes"),
        (f"{coluna_destino}4", 89830, "detr_fixes"),
        (f"{coluna_destino}5", 89830, "detr_issue"),
        (f"{coluna_destino}6", 89830, "detr_recurrent"),
        (f"{coluna_destino}7", 89830, "detr_inspection"),
        (f"{coluna_destino}8", 89830, "detr_inspection_mec"),
        (f"{coluna_destino}9", 89830, "detr_inspection_fun"),
        (f"{coluna_destino}10", 89830, "detr_preparation"),
        (f"{coluna_destino}11", 91686, "detr_prep_sp"),
        (f"{coluna_destino}12", 91686, "detr_prep_poa"),
        (f"{coluna_destino}13", 91686, "detr_prep_cidades"),
        (f"{coluna_destino}14", 89830, "detr_operacoes_sp"),
        (f"{coluna_destino}15", 89830, "detr_fixes_sp"),
        (f"{coluna_destino}16", 89830, "detr_issue_sp"),
        (f"{coluna_destino}17", 89830, "detr_recurrent_sp"),
        (f"{coluna_destino}18", 89830, "detr_inspection_sp"),
        (f"{coluna_destino}22", 89847, "detr_mec"),
        (f"{coluna_destino}23", 89847, "detr_fun"),
        (f"{coluna_destino}24", 89847, "detr_mec_sp"),
        (f"{coluna_destino}25", 89847, "detr_fun_sp"),
    ]

    for cell, card_id, coluna in mapa:
        parametros = [parametro_data("data_backlog", data_filtro)]
        atualizar_celula_metabase(sheet, cell, card_id, coluna, parametros)


def atualizar_oficinas(sheet, data_filtro, coluna_base):
    mapa = [
        (f"{coluna_base}29", 89777, "JURUBATUBA - MECANICA", [
            parametro_data("Date_inicial", data_filtro),
            parametro_data("Date_final", data_filtro),
        ]),
        (f"{coluna_base}30", 89777, "JURUBATUBA - FUNILARIA", [
            parametro_data("Date_inicial", data_filtro),
            parametro_data("Date_final", data_filtro),
        ]),
        (f"{coluna_base}31", 89777, "AMADOR BUENO - FUNILARIA", [
            parametro_data("Date_inicial", data_filtro),
            parametro_data("Date_final", data_filtro),
        ]),
        (f"{coluna_base}32", 98982, "funilaria", [
            parametro_data("DATA_INICIAL", data_filtro),
        ]),
        (f"{coluna_base}33", 98982, "mecanica", [
            parametro_data("DATA_INICIAL", data_filtro),
        ]),
    ]

    for cell, card_id, coluna, parametros in mapa:
        atualizar_celula_metabase(sheet, cell, card_id, coluna, parametros)


# =========================================================
# ORQUESTRADOR COMITÊ DE CRISE
# =========================================================
def atualizar_comite_de_crise(sheet):
    data_atual = get_data_atual()
    data_ontem = get_data_ontem()

    # 1) Hoje: colunas I/J, como no código original
    atualizar_hoje_I_J(sheet, data_atual)

    # 2) Ontem: colunas G/H, como no bloco detalhado original
    print(f"\n===== ATUALIZANDO ONTEM - G/H - {data_ontem} =====")
    atualizar_bloco_principal(sheet, data_ontem, "G", coluna_aux_m="M2")
    atualizar_detratores(sheet, data_ontem, "H")
    atualizar_oficinas(sheet, data_ontem, "G")

    # 3) Segunda-feira: atualiza sexta C/D e sábado E/F
    datas = get_datas_fim_de_semana_se_segunda()

    if datas:
        data_sexta = datas["sexta"]
        data_sabado = datas["sabado"]

        print(f"\n===== ATUALIZANDO SEXTA - C/D - {data_sexta} =====")
        atualizar_bloco_principal(sheet, data_sexta, "C", coluna_aux_m="M4")
        atualizar_detratores(sheet, data_sexta, "D")
        atualizar_oficinas(sheet, data_sexta, "C")

        print(f"\n===== ATUALIZANDO SÁBADO - E/F - {data_sabado} =====")
        atualizar_bloco_principal(sheet, data_sabado, "E", coluna_aux_m="M3")
        atualizar_detratores(sheet, data_sabado, "F")
        atualizar_oficinas(sheet, data_sabado, "E")


# =========================================================
# MAIN
# =========================================================
def main():
    spreadsheet = configurar_google_sheets()
    sheet = spreadsheet.worksheet(SHEET_NAME)

    print(f"\nExecutando automaticamente a aba: {SHEET_NAME}")
    atualizar_comite_de_crise(sheet)


if __name__ == "__main__":
    main()
