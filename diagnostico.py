#1
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def find_latest_file(directory: Path, pattern: str) -> Path:
    """Encontra o arquivo mais recente que corresponde ao padrão."""
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo correspondente a '{pattern}' foi encontrado em '{directory}'.")
    return files[-1]

def safe_to_float(series: pd.Series) -> pd.Series:
    """
    Converte uma coluna para float de forma segura, tratando vírgulas como separadores decimais.
    """
    # Garante que a coluna seja do tipo string, substitui a vírgula pelo ponto.
    series_str = series.astype(str).str.replace(',', '.', regex=False)
    # Converte para numérico, e os erros se tornarão NaN (Not a Number)
    return pd.to_numeric(series_str, errors='coerce')

def main():
    """
    Função principal para investigar e converter colunas financeiras.
    """
    #2
    print("="*80)
    print("  INICIANDO INVESTIGADOR FINANCEIRO (ANÁLISE DE PRECISÃO FLOAT)")
    print("="*80)

    input_dir = Path("./data_input")
    if not input_dir.exists():
        print(f"\n[FALHA] O diretório de entrada '{input_dir}' não foi encontrado. Abortando.")
        return

    try:
        # Identificar colunas financeiras potenciais
        potential_value_columns = ['liquido', 'total_toi', 'valor']
        
        # Carregar arquivo de Mailing
        print("\n[FASE 1/2] Carregando arquivo de Mailing...")
        mailing_path = find_latest_file(input_dir, "MAILING_NUCLEO_*.xlsx")
        print(f"  - Lendo: '{mailing_path.name}'...")
        
        # Lê o arquivo completo de uma vez
        with tqdm(total=1, desc="Lendo arquivo Excel", unit="file") as pbar:
            df_mailing = pd.read_excel(mailing_path)
            df_mailing.columns = [str(c).lower().strip() for c in df_mailing.columns]
            pbar.update(1)

        # Identifica quais das colunas potenciais realmente existem no arquivo
        cols_to_process = [col for col in potential_value_columns if col in df_mailing.columns]
        
        if not cols_to_process:
            print("\n[FALHA] Nenhuma das colunas financeiras esperadas ('liquido', 'total_toi', 'valor') foi encontrada.")
            return

        print(f"\n[FASE 2/2] Processando {len(cols_to_process)} colunas financeiras...")
        
        # Cria um DataFrame de relatório com as colunas de identificação
        df_report = df_mailing[['ncpf', 'nomecad']].copy()

        for col_name in tqdm(cols_to_process, desc="Convertendo colunas", unit="col"):
            # Adiciona a coluna original ao relatório para comparação
            df_report[f'{col_name}_original'] = df_mailing[col_name]
            # Adiciona a coluna convertida e corrigida
            df_report[f'{col_name}_convertido_float'] = safe_to_float(df_mailing[col_name])

        # Filtra o relatório para mostrar apenas linhas onde a conversão fez diferença
        # ou onde havia algum valor original para começar.
        df_report.dropna(subset=[f'{c}_convertido_float' for c in cols_to_process], how='all', inplace=True)

        output_path = Path("./relatorio_financeiro.csv")
        df_report.to_csv(output_path, index=False, sep=';', decimal=',')
        
        print(f"\n  - SUCESSO! Relatório financeiro com {len(df_report)} registros relevantes salvo em '{output_path}'")
        print("    - Verifique as colunas '_convertido_float' para confirmar a precisão.")

    except (FileNotFoundError, KeyError) as e:
        print(f"\n[ERRO CRÍTICO] {e}. Verifique se os arquivos e nomes de colunas estão corretos.")
    except Exception as e:
        print(f"\n[ERRO INESPERADO] Ocorreu uma falha durante o processo: {e}")
        
    print("\n" + "="*80)
    print("  INVESTIGAÇÃO CONCLUÍDA")
    print("="*80)

#3
if __name__ == "__main__":
    main()
