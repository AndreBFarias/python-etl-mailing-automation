import pandas as pd
from pathlib import Path
import glob

# --- CONFIGURAÇÕES ---
# Ajuste se seus dados de teste estiverem em outro lugar.
INPUT_DIR = Path("./data_input")

def find_latest_file(directory: Path, pattern: str) -> Path | None:
    """Função auxiliar para encontrar o arquivo mais recente."""
    if not directory.exists():
        return None
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)

def divider(title: str):
    """Função para imprimir um separador bonito."""
    print("\n" + "="*25 + f" {title.upper()} " + "="*25 + "\n")


print("--- INICIANDO RITUAL DE DIAGNÓSTICO ---")

# 1. Análise do Arquivo de Mailing
divider("Analisando MAILING_NUCLEO")
mailing_pattern = "MAILING_NUCLEO_*.xlsx"
mailing_path = find_latest_file(INPUT_DIR, mailing_pattern)

if mailing_path:
    print(f"Arquivo encontrado: {mailing_path.name}\n")
    try:
        df_mailing = pd.read_excel(mailing_path)
        print("--- Colunas Encontradas no Mailing ---")
        print(df_mailing.columns.to_list())
        print("\n--- Primeiras 5 Linhas do Mailing (head) ---")
        print(df_mailing.head())
        
        # Foco nas colunas de telefone do mailing
        print("\n--- Amostra de Dados das Colunas de Telefone do Mailing ---")
        for i in range(1, 5):
            col_name = f'IND_TELEFONE_{i}_VALIDO'
            if col_name in df_mailing.columns:
                # Mostra os 5 valores mais comuns e quantos nulos existem
                print(f"\n- Análise da coluna: '{col_name}'")
                print(df_mailing[col_name].value_counts(dropna=False).head(5))
            else:
                 print(f"\n- Coluna '{col_name}' NÃO ENCONTRADA no mailing.")

    except Exception as e:
        print(f"!!! Erro ao ler o arquivo de mailing: {e}")
else:
    print("!!! ARQUIVO DE MAILING NÃO ENCONTRADO.")


# 2. Análise do Arquivo de Enriquecimento (Pontuação)
divider("Analisando Pontuação.xlsx")
pontuacao_path = INPUT_DIR / "Pontuação.xlsx"

if pontuacao_path.exists():
    print(f"Arquivo encontrado: {pontuacao_path.name}\n")
    try:
        xls = pd.ExcelFile(pontuacao_path)
        print("--- Abas (Sheets) Encontradas no Arquivo 'Pontuação.xlsx' ---")
        print(xls.sheet_names)

        if xls.sheet_names:
            # Analisa a primeira aba por padrão
            sheet_to_read = xls.sheet_names[0]
            print(f"\n--- Analisando a PRIMEIRA aba: '{sheet_to_read}' ---")
            df_pontuacao = pd.read_excel(pontuacao_path, sheet_name=sheet_to_read)
            
            print("\n--- Colunas Encontradas na Aba de Pontuação ---")
            print(df_pontuacao.columns.to_list())
            
            print("\n--- Primeiras 5 Linhas da Aba de Pontuação (head) ---")
            print(df_pontuacao.head())

    except Exception as e:
        print(f"!!! Erro ao ler o arquivo de pontuação: {e}")
else:
    print("!!! ARQUIVO 'Pontuação.xlsx' NÃO ENCONTRADO.")

divider("FIM DO DIAGNÓSTICO")


