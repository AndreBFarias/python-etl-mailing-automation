#1
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def normalize_series(series: pd.Series) -> pd.Series:
    """Aplica uma limpeza profunda em uma série para comparação."""
    # Converte para string, remove espaços, remove '.0' e descarta nulos
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True).dropna()

def main():
    INPUT_DIR = Path("./data_input")
    mailing_path = INPUT_DIR / "mailing_ruim.xlsx"
    tabulacoes_path = INPUT_DIR / "tabulacoes_ruins.xlsx"

    if not mailing_path.exists() or not tabulacoes_path.exists():
        logging.error("Certifique-se de que 'mailing_ruim.xlsx' e 'tabulacoes_ruins.xlsx' estão em 'data_input'.")
        return

    logging.info("Carregando arquivos (isso pode levar alguns minutos)...")
    #2
    df_mailing = pd.read_excel(mailing_path, dtype=str)
    df_tabulacoes = pd.read_excel(tabulacoes_path, dtype=str)

    logging.info("Iniciando análise de afinidade entre todas as colunas...")
    
    results = []

    #3
    # Cria uma barra de progresso com a biblioteca tqdm
    pbar = tqdm(total=len(df_mailing.columns) * len(df_tabulacoes.columns), desc="Testando Pares de Colunas")

    for mail_col in df_mailing.columns:
        mail_series_norm = normalize_series(df_mailing[mail_col])
        mail_set = set(mail_series_norm.unique())

        for tab_col in df_tabulacoes.columns:
            tab_series_norm = normalize_series(df_tabulacoes[tab_col])
            
            # Pula colunas vazias
            if tab_series_norm.empty:
                pbar.update(1)
                continue
            
            #4
            # Calcula a interseção (matches)
            match_count = len([val for val in tab_series_norm if val in mail_set])
            
            # Calcula a pontuação de afinidade baseada no menor conjunto de dados
            if match_count > 0:
                affinity_score = (match_count / len(tab_series_norm.unique())) * 100
                results.append((affinity_score, match_count, mail_col, tab_col))

            pbar.update(1)
            
    pbar.close()

    #5
    # Ordena os resultados pela pontuação de afinidade, do maior para o menor
    sorted_results = sorted(results, key=lambda item: item[0], reverse=True)

    print("\n" + "="*80)
    print(" " * 20 + "RELATÓRIO DE AFINIDADE DE COLUNAS")
    print("="*80)
    print("As 5 combinações de colunas com maior probabilidade de serem a chave de ligação são:")
    print(f"\n{'Afinidade (%)':<15} | {'Correspondências':<20} | {'Coluna do Mailing':<25} | {'Coluna das Tabulações':<25}")
    print("-"*80)

    for score, count, m_col, t_col in sorted_results[:5]:
        print(f"{score:<15.2f} | {str(count):<20} | {m_col:<25} | {t_col:<25}")
        
    print("="*80)
    
    if sorted_results:
        best_match = sorted_results[0]
        print("\n--- CONCLUSÃO ---")
        print(f"A análise sugere que a melhor chave de ligação é entre a coluna '{best_match[2]}' (do mailing)")
        print(f"e a coluna '{best_match[3]}' (das tabulações), com uma afinidade de {best_match[0]:.2f}%.")
        print("Use estes nomes de coluna para ajustar o `config.ini` e a lógica do pipeline.")
    else:
        print("\n--- CONCLUSÃO ---")
        print("Nenhuma correspondência significativa foi encontrada entre os arquivos.")

if __name__ == "__main__":
    main()
