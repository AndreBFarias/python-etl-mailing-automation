#1
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def find_latest_file(directory: Path, pattern: str) -> Path | None:
    files = list(directory.glob(pattern))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None

def main():
    INPUT_DIR = Path("./data_input")
    MAILING_PATTERN = "MAILING_NUCLEO_*.xlsx"
    BLOQUEIO_FILE = "Tabulações para Retirar.xlsx"

    logging.info("--- Iniciando Diagnóstico de Chaves de Bloqueio ---")

    mailing_path = find_latest_file(INPUT_DIR, MAILING_PATTERN)
    bloqueio_path = INPUT_DIR / BLOQUEIO_FILE

    if not mailing_path or not bloqueio_path.exists():
        logging.error("Arquivo de mailing ou de bloqueio não encontrado. Abortando.")
        return

    #2
    logging.info(f"Analisando Mailing: {mailing_path.name}")
    df_mailing = pd.read_excel(mailing_path, usecols=['NDOC'], dtype=str)
    df_mailing.rename(columns=lambda c: c.strip().lower(), inplace=True)
    mailing_keys = df_mailing['ndoc'].str.strip().dropna().unique()
    
    logging.info(f"Analisando Bloqueio: {bloqueio_path.name}")
    df_bloqueio = pd.read_excel(bloqueio_path, usecols=['IdCliente'], dtype=str)
    df_bloqueio.rename(columns=lambda c: c.strip().lower(), inplace=True)
    bloqueio_keys = df_bloqueio['idcliente'].str.strip().dropna().unique()

    logging.info(f"Total de chaves únicas no Mailing ('ndoc'): {len(mailing_keys)}")
    logging.info(f"Total de chaves únicas no Bloqueio ('idcliente'): {len(bloqueio_keys)}")

    #3
    matches = pd.Series(bloqueio_keys).isin(mailing_keys)
    match_count = matches.sum()

    logging.info("--- RESULTADO DA ANÁLISE ---")
    logging.info(f"Correspondências encontradas: {match_count} de {len(bloqueio_keys)} (Exatamente o que o log mostrou: {match_count == 177})")
    
    #4
    if match_count < len(bloqueio_keys):
        logging.warning("Exibindo 10 exemplos de chaves de bloqueio que NÃO foram encontradas no mailing:")
        unmatched_keys = pd.Series(bloqueio_keys)[~matches].head(10)
        
        print("\n" + "="*40)
        print("  Chaves de Bloqueio Não Encontradas")
        print("="*40)
        for key in unmatched_keys:
            print(f"  - '{key}'")
        print("\nCompare o formato destas chaves com os valores na coluna 'NDOC' do seu mailing.")
        print("A causa provável é a presença de '.0' ou outros caracteres.")

if __name__ == "__main__":
    main()
