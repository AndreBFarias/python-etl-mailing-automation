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

def main():
    """
    Função principal para extrair e exportar as chaves de cruzamento.
    """
    #2
    print("="*60)
    print("  INICIANDO EXTRATOR DE CHAVES PARA ANÁLISE DE CRUZAMENTO")
    print("="*60)

    input_dir = Path("./data_input")
    if not input_dir.exists():
        print(f"\n[FALHA] O diretório de entrada '{input_dir}' não foi encontrado. Abortando.")
        return

    try:
        #3
        # Processar arquivo de Mailing
        print("\n[FASE 1/2] Processando arquivo de Mailing...")
        mailing_path = find_latest_file(input_dir, "MAILING_NUCLEO_*.xlsx")
        print(f"  - Lendo: '{mailing_path.name}'...")
        df_mailing = pd.read_excel(mailing_path, usecols=['NCPF'])
        
        print("  - Normalizando e extraindo chaves 'NCPF'...")
        s_mailing = df_mailing['NCPF'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).dropna()
        
        output_mailing_path = Path("./chaves_ncpf.csv")
        s_mailing.to_csv(output_mailing_path, index=False, header=['ncpf'])
        print(f"  - SUCESSO! {len(s_mailing)} chaves salvas em '{output_mailing_path}'")

        # Processar arquivo de Tabulações
        print("\n[FASE 2/2] Processando arquivo de Tabulações...")
        tabulacoes_path = find_latest_file(input_dir, "*abulaç*.xlsx")
        print(f"  - Lendo: '{tabulacoes_path.name}'...")
        df_tab = pd.read_excel(tabulacoes_path, usecols=['IdCLiente'])
        
        print("  - Normalizando e extraindo chaves 'IdCLiente'...")
        s_tab = df_tab['IdCLiente'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).dropna()

        output_tab_path = Path("./chaves_idcliente.csv")
        s_tab.to_csv(output_tab_path, index=False, header=['idcliente'])
        print(f"  - SUCESSO! {len(s_tab)} chaves salvas em '{output_tab_path}'")

    except FileNotFoundError as e:
        print(f"\n[ERRO CRÍTICO] {e}")
    except Exception as e:
        print(f"\n[ERRO INESPERADO] Ocorreu uma falha durante o processo: {e}")
        
    print("\n" + "="*60)
    print("  EXTRAÇÃO CONCLUÍDA")
    print("="*60)


if __name__ == "__main__":
    main()
