# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import re
from typing import List, Dict

# 1. Configurações da Investigação
INPUT_DIR = Path("./data_input")
FILE_PATTERN = "MAILING_NUCLEO_*.xlsx"

# 2. Lista de Alvos: Palavras-chave que definem um "status" de remoção.
TARGET_STATUS_KEYWORDS = [
    "Cliente Falecido",
    "CLIENTE NAO DESEJA CONTATO",
    "COBRANCA - CLIENTE PAGOU - LOCALIZADO BAIXA NO SINED",
    "COBRANCA - NEGOCIACAO CONCRETIZADA",
    "CONTA NEGOCIADA",
    "CONTA PAGA CLIENTE",
    "CONTA PAGA SISTEMA",
    "CONTA REFATURADA",
    "CONTAS PRESCRITAS",
    "MARCACAO POR LIMINAR",
    "PROCESSO JUDICIAL",
    "NAO PERTENCE A UC"
]

def find_latest_file(directory: Path, pattern: str) -> Path | None:
    """Encontra o arquivo mais recente que corresponde ao padrão no diretório."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)

def main():
    """Função principal que orquestra a busca e análise da coluna de status."""
    print("="*80)
    print("  Detetive de Coluna de Status")
    print("  Analisando o arquivo de mailing para encontrar a coluna mais provável...")
    print("="*80)

    # 3. Localiza a evidência principal (o último arquivo de mailing)
    mailing_file = find_latest_file(INPUT_DIR, FILE_PATTERN)
    if not mailing_file:
        print(f"[FALHA] Nenhum arquivo de mailing encontrado no padrão '{FILE_PATTERN}' em '{INPUT_DIR}'.")
        return

    print(f"\nArquivo sob investigação: {mailing_file.name}\n")

    try:
        df = pd.read_excel(mailing_file)
    except Exception as e:
        print(f"[FALHA] Não foi possível ler o arquivo Excel. Erro: {e}")
        return

    # 4. Prepara a ferramenta de busca (regex)
    # Constrói um padrão regex que busca qualquer uma das palavras-chave, ignorando maiúsculas/minúsculas.
    search_pattern = '|'.join(re.escape(term) for term in TARGET_STATUS_KEYWORDS)
    
    column_scores: Dict[str, int] = {}

    print("Analisando o conteúdo de cada coluna...")
    # 5. Interroga cada coluna
    for col in df.columns:
        try:
            # Converte a coluna para string para poder fazer a busca
            series_str = df[col].astype(str)
            # Conta quantas células na coluna contêm um dos nossos alvos
            matches = series_str.str.contains(search_pattern, case=False, na=False).sum()
            if matches > 0:
                column_scores[col] = matches
        except Exception:
            # Ignora colunas que não podem ser convertidas para string
            continue
    
    if not column_scores:
        print("\n[RESULTADO] Nenhuma coluna parece conter os status procurados.")
        return

    # 6. Gera o Relatório de Inteligência
    print("\n" + "="*80)
    print("  Relatório de Análise Forense de Colunas")
    print("="*80)
    print("As colunas a seguir são as candidatas mais prováveis a conter o 'status':\n")

    # Ordena as colunas pela pontuação (número de matches)
    sorted_candidates = sorted(column_scores.items(), key=lambda item: item[1], reverse=True)

    for i, (col_name, score) in enumerate(sorted_candidates[:5]):
        print(f"--- Candidata #{i+1} ---")
        print(f"Nome da Coluna: '{col_name}'")
        print(f"Pontuação (correspondências encontradas): {score}")
        
        # Mostra uma amostra dos valores mais comuns para confirmação visual
        print("Amostra (Top 10 valores mais comuns):")
        try:
            top_values = df[col_name].value_counts().nlargest(10)
            for value, count in top_values.items():
                print(f"  - \"{str(value)[:70]}\" (ocorrências: {count})")
        except Exception:
            print("  - (Não foi possível exibir amostra de valores)")
        print("-"*(len(col_name) + 20))

    print("\n" + "="*80)
    print("  Análise Concluída.")
    print("  Use o nome da coluna com a maior pontuação no seu arquivo 'config.ini'.")
    print("="*80)


if __name__ == "__main__":
    main()

