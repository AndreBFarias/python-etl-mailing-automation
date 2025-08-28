import pandas as pd
from pathlib import Path
import glob
import chardet

# --- CONFIGURAÇÕES DO RITUAL DE DIAGNÓSTICO ---
# O diretório onde seus tesouros de dados estão guardados.
INPUT_DIR = Path("./data_input")
# Quantas linhas de amostra queremos ver de cada arquivo/aba.
HEAD_SAMPLE_SIZE = 5

def divider(title: str, char: str = "="):
    """Função para imprimir um separador bonito e legível no console."""
    print("\n" + char * 35)
    print(f" {title.upper()} ")
    print(char * 35 + "\n")

def analyze_csv(file_path: Path):
    """
    Analisa um único arquivo CSV, tentando diferentes encodings e mostrando um head.
    """
    print(f"--- INSPECIONANDO ARQUIVO CSV: {file_path.name} ---")
    
    # Tentativa de adivinhar o encoding, mas com nossas próprias prioridades.
    encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1']
    df = None
    successful_encoding = None

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(file_path, sep=';', encoding=enc, low_memory=False, nrows=HEAD_SAMPLE_SIZE * 2)
            successful_encoding = enc
            print(f"[SUCESSO] Arquivo lido com o encoding: '{enc}'")
            break
        except (UnicodeDecodeError, AttributeError):
            # AttributeError pode acontecer se o separador estiver errado e o pandas falhar.
            print(f"[INFO] Falha ao tentar ler com encoding: '{enc}'")
            continue
    
    if df is None:
        print(f"\n[FALHA] Não foi possível ler o arquivo CSV '{file_path.name}' com os encodings testados.")
        return

    print("\n[COLUNAS ENCONTRADAS]")
    print(df.columns.to_list())
    
    print(f"\n[AMOSTRA DE DADOS (as primeiras {HEAD_SAMPLE_SIZE} linhas)]")
    print(df.head(HEAD_SAMPLE_SIZE))
    print("-" * 70)

def analyze_excel(file_path: Path):
    """
    Analisa um arquivo Excel, mostrando um head de cada uma de suas abas.
    """
    print(f"--- INSPECIONANDO ARQUIVO EXCEL: {file_path.name} ---")
    try:
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        sheet_names = xls.sheet_names
        print(f"[INFO] Encontradas {len(sheet_names)} abas: {sheet_names}")

        for i, sheet_name in enumerate(sheet_names):
            divider(f"Aba #{i+1}: '{sheet_name}'", char="-")
            try:
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                
                print("[COLUNAS ENCONTRADAS]")
                print(df_sheet.columns.to_list())
    
                print(f"\n[AMOSTRA DE DADOS (as primeiras {HEAD_SAMPLE_SIZE} linhas)]")
                print(df_sheet.head(HEAD_SAMPLE_SIZE))

            except Exception as e_sheet:
                print(f"\n[FALHA] Não foi possível ler a aba '{sheet_name}'. Erro: {e_sheet}")
        
        print("-" * 70)

    except Exception as e_file:
        print(f"\n[FALHA] Não foi possível abrir o arquivo Excel '{file_path.name}'. Erro: {e_file}")

def main():
    """
    Função principal que orquestra a análise de todos os arquivos no diretório de entrada.
    """
    divider("INÍCIO DO DIAGNÓSTICO COMPLETO DE DADOS")
    
    if not INPUT_DIR.exists() or not INPUT_DIR.is_dir():
        print(f"[ERRO CRÍTICO] O diretório de entrada '{INPUT_DIR}' não foi encontrado.")
        return

    # Lista todos os arquivos CSV e Excel no diretório
    files_to_analyze = list(INPUT_DIR.glob('*.csv')) + list(INPUT_DIR.glob('*.xlsx'))
    
    if not files_to_analyze:
        print("[INFO] Nenhum arquivo .csv ou .xlsx encontrado para análise.")
        return
        
    for file_path in sorted(files_to_analyze):
        if file_path.suffix.lower() == '.csv':
            analyze_csv(file_path)
        elif file_path.suffix.lower() == '.xlsx':
            analyze_excel(file_path)

    divider("DIAGNÓSTICO CONCLUÍDO")

if __name__ == "__main__":
    # Salve este script na pasta raiz do seu projeto (a mesma do main.py)
    # e execute-o com: python3 diagnostico_completo.py
    main()

