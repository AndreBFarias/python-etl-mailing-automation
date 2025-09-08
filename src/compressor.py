#1
import shutil
from pathlib import Path
from datetime import datetime

def organize_and_compress_output():
    """
    Organiza os arquivos de saída do dia em uma pasta datada,
    copia o log correspondente e comprime tudo em um arquivo .zip.
    """
    print("\n--- INICIANDO ROTINA DE ORGANIZAÇÃO E COMPRESSÃO ---")
    
    # 1
    today = datetime.now()
    date_str_files = today.strftime('%d_%m_%Y')
    date_str_log = today.strftime('%Y-%m-%d')
    date_str_zip = today.strftime('%d-%m-%Y')

    output_dir = Path('./data_output')
    log_dir = Path('./logs')

    # 2
    dated_folder = output_dir / date_str_files
    try:
        dated_folder.mkdir(exist_ok=True)
        print(f"INFO: Pasta do dia criada em: '{dated_folder}'")
    except OSError as e:
        print(f"ERRO: Não foi possível criar a pasta '{dated_folder}'. Erro: {e}")
        return

    # 3
    csv_pattern = f"Telecobranca_TOI_*_{date_str_files}.csv"
    files_to_move = list(output_dir.glob(csv_pattern))
    
    if not files_to_move:
        print("AVISO: Nenhum arquivo CSV encontrado para a data de hoje. Nenhum arquivo foi movido.")
    else:
        print(f"INFO: Movendo {len(files_to_move)} arquivos CSV para a pasta do dia...")
        for f in files_to_move:
            try:
                shutil.move(str(f), str(dated_folder / f.name))
            except Exception as e:
                print(f"ERRO: Falha ao mover o arquivo '{f.name}'. Erro: {e}")
        print("INFO: Arquivos CSV movidos com sucesso.")

    # 4
    log_file_name = f"automacao_{date_str_log}.log"
    log_file_source = log_dir / log_file_name
    
    if log_file_source.is_file():
        try:
            shutil.copy2(str(log_file_source), str(dated_folder / log_file_name))
            print(f"INFO: Log do dia '{log_file_name}' copiado para a pasta.")
        except Exception as e:
            print(f"ERRO: Falha ao copiar o arquivo de log. Erro: {e}")
    else:
        print(f"AVISO: Arquivo de log '{log_file_name}' não encontrado na pasta de logs.")

    # 5
    zip_filename_base = output_dir / f"mailing-{date_str_zip}"
    try:
        shutil.make_archive(
            base_name=str(zip_filename_base),
            format='zip',
            root_dir=str(dated_folder)
        )
        print(f"INFO: Pasta do dia comprimida com sucesso em '{zip_filename_base}.zip'")
    except Exception as e:
        print(f"ERRO: Falha ao criar o arquivo .zip. Erro: {e}")

    print("--- ROTINA DE ORGANIZAÇÃO CONCLUÍDA ---")


if __name__ == '__main__':
    # Bloco para teste manual do script
    organize_and_compress_output()
