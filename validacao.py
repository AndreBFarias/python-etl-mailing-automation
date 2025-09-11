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

def normalize_key(series: pd.Series) -> pd.Series:
    """Converte uma coluna para string limpa, removendo espaços e '.0' no final."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

def main():
    """
    Função principal para auditar o cruzamento de chaves e a precisão dos valores float.
    """
    #2
    print("="*80)
    print("  INICIANDO VALIDADOR FINAL DE DADOS (CRUZAMENTO DE ID E PRECISÃO FLOAT)")
    print("="*80)

    input_dir = Path("./data_input")
    if not input_dir.exists():
        print(f"\n[FALHA] O diretório de entrada '{input_dir}' não foi encontrado. Abortando.")
        return

    try:
        # Carregar arquivos
        print("\n[FASE 1/3] Carregando arquivos de entrada...")
        mailing_path = find_latest_file(input_dir, "MAILING_NUCLEO_*.xlsx")
        tabulacoes_path = find_latest_file(input_dir, "*abulaç*.xlsx")

        with tqdm(total=2, desc="Lendo arquivos Excel", unit="file") as pbar:
            df_mailing = pd.read_excel(mailing_path)
            df_mailing.columns = [str(c).lower().strip() for c in df_mailing.columns]
            pbar.update(1)
            
            df_tab = pd.read_excel(tabulacoes_path)
            df_tab.columns = [str(c).lower().strip() for c in df_tab.columns]
            pbar.update(1)
        
        # Auditoria de Cruzamento
        print("\n[FASE 2/3] Auditando o cruzamento de chaves...")
        ncpf_keys = normalize_key(df_mailing['ncpf']).dropna().unique()
        idcliente_keys = normalize_key(df_tab['idcliente']).dropna().unique()
        
        set_ncpf = set(ncpf_keys)
        set_idcliente = set(idcliente_keys)
        
        found_ids = set_idcliente.intersection(set_ncpf)
        missing_ids = set_idcliente - set_ncpf
        
        df_found = pd.DataFrame(list(found_ids), columns=['id_cliente'])
        df_found['status_cruzamento'] = 'ENCONTRADO NO MAILING'
        
        df_missing = pd.DataFrame(list(missing_ids), columns=['id_cliente'])
        df_missing['status_cruzamento'] = 'NAO ENCONTRADO NO MAILING'
        
        df_cruzamento = pd.concat([df_found, df_missing], ignore_index=True)
        
        output_cruzamento_path = Path("./relatorio_cruzamento.csv")
        df_cruzamento.to_csv(output_cruzamento_path, index=False, sep=';')
        print(f"  - SUCESSO! Relatório de cruzamento salvo em '{output_cruzamento_path}'")
        print(f"    - {len(found_ids)} IDs encontrados.")
        print(f"    - {len(missing_ids)} IDs NÃO encontrados.")

        # Auditoria de Valores Float
        print("\n[FASE 3/3] Auditando valores com precisão decimal...")
        if 'liquido' in df_mailing.columns:
            df_mailing['liquido_numeric'] = pd.to_numeric(df_mailing['liquido'], errors='coerce')
            
            # Filtra apenas valores que não são inteiros (ou seja, têm casas decimais)
            df_floats_reais = df_mailing[
                df_mailing['liquido_numeric'].notna() & (df_mailing['liquido_numeric'] % 1 != 0)
            ]
            
            output_floats_path = Path("./relatorio_valores_reais.csv")
            df_floats_reais[['ncpf', 'liquido']].to_csv(output_floats_path, index=False, sep=';')
            print(f"  - SUCESSO! Relatório de valores com centavos salvo em '{output_floats_path}'")
            print(f"    - Encontrados {len(df_floats_reais)} registros com valores decimais não nulos.")
        else:
            print("  - [AVISO] Coluna 'liquido' não encontrada no mailing. Auditoria de float pulada.")

    except (FileNotFoundError, KeyError) as e:
        print(f"\n[ERRO CRÍTICO] {e}. Verifique se os arquivos e nomes de colunas ('ncpf', 'idcliente', 'liquido') estão corretos.")
    except Exception as e:
        print(f"\n[ERRO INESPERADO] Ocorreu uma falha durante o processo: {e}")
        
    print("\n" + "="*80)
    print("  VALIDAÇÃO FINAL CONCLUÍDA")
    print("="*80)

if __name__ == "__main__":
    #3
    main()
