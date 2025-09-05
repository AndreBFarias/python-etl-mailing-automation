import pandas as pd
from pathlib import Path

# --- CONFIGURAÇÕES ---
INPUT_DIR = Path("./data_input")
MAILING_PATTERN = "MAILING_NUCLEO_*.xlsx"
# O nome da coluna que descobrimos ser a correta
STATUS_COLUMN = "BLOQ" 

def find_latest_file(directory: Path, pattern: str) -> Path | None:
    files = list(directory.glob(pattern))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None

def main():
    print("--- Iniciando Inspetor de Status da Coluna 'BLOQ' ---")
    mailing_path = find_latest_file(INPUT_DIR, MAILING_PATTERN)

    if not mailing_path:
        print(f"[FALHA] Nenhum arquivo de mailing encontrado com o padrão '{MAILING_PATTERN}'.")
        return

    print(f"Analisando o arquivo: {mailing_path.name}")
    
    try:
        df = pd.read_excel(mailing_path, usecols=[STATUS_COLUMN])
        unique_statuses = df[STATUS_COLUMN].dropna().unique()

        print("\n" + "="*50)
        print(" VALORES DE STATUS ÚNICOS ENCONTRADOS NA COLUNA 'BLOQ'")
        print("="*50)
        
        if len(unique_statuses) > 0:
            for status in sorted(unique_statuses):
                print(f"  - {status}")
            print("\nCOPIE os valores acima que indicam remoção e COLE na seção")
            print("[FILTROS_FINAIS] do seu arquivo 'config.ini'.")
        else:
            print("Nenhum valor encontrado na coluna 'BLOQ'.")

    except Exception as e:
        print(f"\n[FALHA] Não foi possível ler a coluna '{STATUS_COLUMN}'. Erro: {e}")

if __name__ == "__main__":
    main()
