# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple

# 1. Configuração do diretório de entrada
# O script espera ser executado da pasta raiz do projeto.
INPUT_DIR = Path("./data_input")
FILE_PATTERN = "MAILING_NUCLEO_*.xlsx"

def get_excel_columns(file_path: Path) -> List[str]:
    """
    Lê apenas o cabeçalho de um arquivo Excel para extrair os nomes das colunas.
    É uma forma otimizada de não carregar o arquivo inteiro na memória.
    """
    try:
        # nrows=0 lê apenas as colunas, sem nenhuma linha de dados.
        df_header = pd.read_excel(file_path, nrows=0)
        return [str(col).strip() for col in df_header.columns]
    except Exception as e:
        print(f"  [ERRO] Não foi possível ler o arquivo {file_path.name}: {e}")
        return []

def compare_schemas(base_schema: List[str], new_schema: List[str]) -> Tuple[List[str], List[str], bool]:
    """
    Compara duas listas de colunas (schemas) e retorna as diferenças.
    """
    base_set = set(base_schema)
    new_set = set(new_schema)
    
    added_columns = sorted(list(new_set - base_set))
    removed_columns = sorted(list(base_set - new_set))
    
    # 1. Lógica de Comparação Corrigida
    # Extrai a lista de colunas comuns de cada schema, mantendo a ordem original.
    common_cols_in_base = [col for col in base_schema if col in new_set]
    common_cols_in_new = [col for col in new_schema if col in base_set]
    
    # A ordem é diferente se as duas listas de colunas comuns não forem idênticas.
    order_is_different = common_cols_in_base != common_cols_in_new
    
    return added_columns, removed_columns, order_is_different

def main():
    """
    Função principal que orquestra a busca, leitura e comparação dos schemas.
    """
    print("="*80)
    print("  Analisador de Schema dos Arquivos de Mailing (v3 - Lógica Corrigida)")
    print("="*80)
    
    if not INPUT_DIR.exists():
        print(f"[FALHA] O diretório de entrada '{INPUT_DIR}' não foi encontrado.")
        return

    # Encontra e ordena os arquivos para garantir a comparação cronológica
    files = sorted(INPUT_DIR.glob(FILE_PATTERN))
    
    if len(files) < 2:
        print("[INFO] Menos de dois arquivos de mailing encontrados. Não há nada para comparar.")
        if files:
            print(f"\nArquivo encontrado: {files[0].name}")
            columns = get_excel_columns(files[0])
            print("Colunas:")
            for col in columns:
                print(f"  - {col}")
        return

    # Extrai o schema de todos os arquivos
    schemas: Dict[str, List[str]] = {}
    for file in files:
        print(f"\nLendo schema de: {file.name}...")
        columns = get_excel_columns(file)
        if columns:
            schemas[file.name] = columns
            print(f"  - {len(columns)} colunas encontradas.")

    if not schemas:
        print("\n[FALHA] Nenhum schema pôde ser lido. Verifique os arquivos.")
        return

    # Compara cada arquivo com o primeiro (base)
    base_file_name = list(schemas.keys())[0]
    base_schema = schemas[base_file_name]
    
    print("\n" + "="*80)
    print("  Relatório de Comparação Forense")
    print(f"  Arquivo Base para Comparação: {base_file_name}")
    print("="*80)

    for file_name, new_schema in list(schemas.items())[1:]:
        print(f"\nComparando com: {file_name}")
        
        # 2. Aprimoramento da Evidência Visual
        # Mostra as 15 primeiras colunas lado a lado para comparação visual imediata.
        print("\n  --- EVIDÊNCIA: Amostra da Ordem das Colunas (as 15 primeiras) ---")
        print(f"  {'Arquivo Base'.ljust(35)} | {'Arquivo Comparado'.ljust(35)}")
        print(f"  {'-'*35} | {'-'*35}")
        for i in range(15):
            base_col = base_schema[i] if i < len(base_schema) else ""
            new_col = new_schema[i] if i < len(new_schema) else ""
            marker = "  " if base_col == new_col else ">>"
            print(f"{marker} {base_col.ljust(33)} | {new_col.ljust(35)}")
        print("  ---------------------------------------------------------------------")
        
        added, removed, order_changed = compare_schemas(base_schema, new_schema)
        
        print("\n  --- DIAGNÓSTICO ---")
        # 3. Clareza no Diagnóstico
        has_no_diffs = not added and not removed and not order_changed
        if has_no_diffs:
            print("  [OK] A estrutura (conjunto e ordem das colunas) é IDÊNTICA à do arquivo base.")
            continue
            
        if added:
            print(f"  [ALERTA] Colunas Adicionadas ({len(added)}): {', '.join(added)}")
        
        if removed:
            print(f"  [ALERTA] Colunas Removidas ({len(removed)}): {', '.join(removed)}")
        
        if order_changed:
            print("  [ALERTA] A ORDEM das colunas foi alterada.")

    print("\n" + "="*80)
    print("  Análise Concluída.")
    print("="*80)


if __name__ == "__main__":
    main()

