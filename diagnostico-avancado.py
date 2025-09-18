# 1. Importações
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Configuração básica de logging para o console
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# 2. Constantes e Configurações
INPUT_DIR = Path('./data_input')
OUTPUT_FILENAME_TEMPLATE = "diagnostico_de_entrada_{timestamp}.txt"
TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%Hh%Mm%Ss")
OUTPUT_FILE = Path(OUTPUT_FILENAME_TEMPLATE.format(timestamp=TIMESTAMP))

# 3. Função Principal
def gerar_diagnostico_avancado():
    """
    Varre a pasta de entrada, extrai o cabeçalho e amostras de cada arquivo/planilha
    e consolida tudo em um único relatório de texto para análise.
    """
    logging.info(f"--- Iniciando Diagnóstico Avançado da pasta '{INPUT_DIR}' ---")
    
    if not INPUT_DIR.is_dir():
        logging.error(f"O diretório de entrada '{INPUT_DIR}' não foi encontrado.")
        return

    # 4. Abre o arquivo de saída para escrita
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write(f" RELATÓRIO DE DIAGNÓSTICO AVANÇADO DE ENTRADA\n")
            f.write(f" Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("="*80 + "\n\n")

            # 5. Processa arquivos Excel
            excel_files = sorted(list(INPUT_DIR.glob('*.xlsx')))
            logging.info(f"Encontrados {len(excel_files)} arquivo(s) Excel para analisar.")
            for excel_path in excel_files:
                try:
                    logging.info(f"Analisando arquivo Excel: {excel_path.name}")
                    xls = pd.ExcelFile(excel_path)
                    for sheet_name in xls.sheet_names:
                        # Lê apenas o cabeçalho e as 10 primeiras linhas para otimizar
                        df = pd.read_excel(xls, sheet_name=sheet_name, nrows=10)
                        
                        f.write("-" * 80 + "\n")
                        f.write(f"Arquivo...: {excel_path.name}\n")
                        f.write(f"Planilha..: {sheet_name}\n")
                        f.write(f"Colunas...: {df.columns.to_list()}\n")
                        f.write("-" * 80 + "\n")
                        f.write("Amostra de Dados (10 primeiras linhas):\n")
                        f.write(df.to_string())
                        f.write("\n\n")
                except Exception as e:
                    logging.warning(f"Não foi possível ler '{excel_path.name}'. Erro: {e}")
                    f.write(f"!!! ERRO ao ler o arquivo: {excel_path.name} | Motivo: {e} !!!\n\n")

            # 6. Processa arquivos CSV
            csv_files = sorted(list(INPUT_DIR.glob('*.csv')))
            logging.info(f"Encontrados {len(csv_files)} arquivo(s) CSV para analisar.")
            for csv_path in csv_files:
                try:
                    logging.info(f"Analisando arquivo CSV: {csv_path.name}")
                    # Tenta detectar o separador, mas assume ';' como padrão
                    df = pd.read_csv(csv_path, sep=';', low_memory=False, nrows=10, encoding='utf-8')
                    # Se tiver apenas uma coluna, tenta com vírgula
                    if len(df.columns) == 1 and ',' in df.columns[0]:
                         df = pd.read_csv(csv_path, sep=',', low_memory=False, nrows=10, encoding='utf-8')

                    f.write("-" * 80 + "\n")
                    f.write(f"Arquivo...: {csv_path.name}\n")
                    f.write(f"Colunas...: {df.columns.to_list()}\n")
                    f.write("-" * 80 + "\n")
                    f.write("Amostra de Dados (10 primeiras linhas):\n")
                    f.write(df.to_string())
                    f.write("\n\n")
                except Exception as e:
                    logging.warning(f"Não foi possível ler '{csv_path.name}'. Erro: {e}")
                    f.write(f"!!! ERRO ao ler o arquivo: {csv_path.name} | Motivo: {e} !!!\n\n")

        logging.info(f"--- Diagnóstico Concluído ---")
        logging.info(f"Relatório salvo com sucesso em: '{OUTPUT_FILE}'")

    except Exception as e:
        logging.error(f"Ocorreu uma falha crítica ao gerar o relatório: {e}")


# 7. Ponto de Entrada
if __name__ == "__main__":
    gerar_diagnostico_avancado()
