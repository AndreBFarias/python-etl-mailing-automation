# -*- coding: utf-8 -*-
import shutil
from pathlib import Path
from datetime import datetime
import os
import glob
from configparser import ConfigParser

# def organize_and_compress_output(config: ConfigParser):
#     # HOMOLOGAÇÃO: A lógica antiga era falha. Ela tentava encontrar os arquivos na pasta
#     # raiz do output e movê-los para a pasta datada. Isso falhava porque os padrões
#     # de busca não correspondiam e criava uma condição de corrida com o formatador.
#     """
#     Organiza os arquivos de saída do dia em uma pasta datada,
#     copia o log correspondente e comprime tudo em um arquivo .zip.
#     """
#     print("\n--- INICIANDO ROTINA DE ORGANIZAÇÃO E COMPRESSÃO ---")
#     
#     today = datetime.now()
#     date_str_files = today.strftime('%d_%m_%Y')
#     date_str_log = today.strftime('%Y-%m-%d')
#     date_str_zip = today.strftime('%d-%m-%Y')
# 
#     output_dir = Path(config.get('PATHS', 'output_dir', fallback='data_output'))
#     log_dir = Path(config.get('PATHS', 'log_dir', fallback='logs'))
# 
#     dated_folder = output_dir / date_str_files
#     try:
#         dated_folder.mkdir(exist_ok=True)
#         print(f"INFO: Pasta do dia criada em: '{dated_folder}'")
#     except OSError as e:
#         print(f"ERRO: Não foi possível criar a pasta '{dated_folder}'. Erro: {e}")
#         return
# 
#     humano_prefix = config.get('EXPORT_FILENAMES', 'humano_prefix', fallback='mailing_humano')
#     robo_prefix = config.get('EXPORT_FILENAMES', 'robo_prefix', fallback='mailing_robo')
#     date_format = config.get('EXPORT_FILENAMES', 'date_format', fallback='%d_%m_%Y')
#     date_str_for_files = today.strftime(date_format)
# 
#     patterns = [
#         f"{humano_prefix}*_{date_str_for_files}.csv",
#         f"{robo_prefix}*_{date_str_for_files}.csv"
#     ]
#     
#     files_to_move = []
#     for pattern in patterns:
#         files_to_move.extend(output_dir.glob(pattern))
#     
#     if not files_to_move:
#         print("AVISO: Nenhum arquivo CSV encontrado para a data de hoje. Nenhum arquivo foi movido.")
#     else:
#         print(f"INFO: Movendo {len(files_to_move)} arquivos CSV para a pasta do dia...")
#         for f in files_to_move:
#             try:
#                 shutil.move(str(f), str(dated_folder / f.name))
#             except Exception as e:
#                 print(f"ERRO: Falha ao mover o arquivo '{f.name}'. Erro: {e}")
#         print("INFO: Arquivos CSV movidos com sucesso.")
# 
#     log_file_name = f"automacao_{date_str_log}.log"
#     log_file_source = log_dir / log_file_name
#     
#     if log_file_source.is_file():
#         try:
#             shutil.copy2(str(log_file_source), str(dated_folder / log_file_name))
#             print(f"INFO: Log do dia '{log_file_name}' copiado para a pasta.")
#         except Exception as e:
#             print(f"ERRO: Falha ao copiar o arquivo de log. Erro: {e}")
#     else:
#         print(f"AVISO: Arquivo de log '{log_file_name}' não encontrado na pasta de logs.")
# 
#     zip_filename_base = output_dir / f"mailing-{date_str_zip}"
#     try:
#         shutil.make_archive(
#             base_name=str(zip_filename_base),
#             format='zip',
#             root_dir=str(dated_folder)
#         )
#         print(f"INFO: Pasta do dia comprimida com sucesso em '{zip_filename_base}.zip'")
#     except Exception as e:
#         print(f"ERRO: Falha ao compactar a pasta. Erro: {e}")

def organize_and_compress_output(config: ConfigParser):
    """
    NOVA LÓGICA: Localiza a pasta do dia (que já deve conter todos os arquivos),
    copia o log para dentro dela e, em seguida, compacta a pasta inteira.
    """
    print("\n--- INICIANDO ROTINA DE ORGANIZAÇÃO E COMPRESSÃO ---")
    
    today = datetime.now()
    date_str_folder = today.strftime('%d_%m_%Y')
    date_str_log = today.strftime('%Y-%m-%d')
    date_str_zip = today.strftime('%d-%m-%Y')

    output_dir = Path(config.get('PATHS', 'output_dir', fallback='data_output'))
    log_dir = Path(config.get('PATHS', 'log_dir', fallback='logs'))

    # Passo 1: Localizar a pasta do dia. Os arquivos já devem estar lá.
    dated_folder = output_dir / date_str_folder
    if not dated_folder.is_dir():
        print(f"ERRO CRÍTICO: A pasta do dia '{dated_folder}' não foi encontrada. A compressão não pode continuar.")
        return

    # Passo 2: Copiar o arquivo de log para dentro da pasta do dia.
    log_file_name = f"automacao_{date_str_log}.log"
    log_file_source = log_dir / log_file_name
    
    if log_file_source.is_file():
        try:
            shutil.copy2(str(log_file_source), str(dated_folder / log_file_name))
            print(f"INFO: Log do dia '{log_file_name}' copiado para a pasta de arquivamento.")
        except Exception as e:
            print(f"ERRO: Falha ao copiar o arquivo de log. Erro: {e}")
    else:
        print(f"AVISO: Arquivo de log '{log_file_name}' não encontrado na pasta de logs.")

    # Passo 3: Compactar a pasta do dia.
    # O nome do arquivo zip será salvo na pasta de output principal.
    zip_filename_base = output_dir / f"mailing-{date_str_zip}"
    try:
        shutil.make_archive(
            base_name=str(zip_filename_base),
            format='zip',
            root_dir=str(dated_folder) # O conteúdo desta pasta será o conteúdo do zip.
        )
        print(f"INFO: Pasta do dia comprimida com sucesso em '{zip_filename_base}.zip'")
    except Exception as e:
        print(f"ERRO: Falha ao compactar a pasta. Erro: {e}")
