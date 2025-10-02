# -*- coding: utf-8 -*-
import shutil
from pathlib import Path
from datetime import datetime
import os
import logging
from configparser import ConfigParser
import pandas as pd

logger = logging.getLogger(__name__)

def _exorcizar_arquivos_fantasmas(diretorio_alvo: Path):
    logger.info("Iniciando ritual de exorcismo de arquivos fantasmas (BOM)...")
    fantasmas_encontrados = [f for f in diretorio_alvo.glob('*.csv') if 'ï»¿' in f.name]
    if not fantasmas_encontrados:
        logger.info("Nenhum fantasma (BOM) encontrado nos nomes dos arquivos.")
        return
    for fantasma in fantasmas_encontrados:
        try:
            os.remove(fantasma)
            logger.warning(f"FANTASMA BANIDO: O arquivo '{fantasma.name}' foi removido.")
        except OSError as e:
            logger.error(f"Falha ao banir o fantasma '{fantasma.name}': {e}")

def _substituir_nan_por_nulo(diretorio_alvo: Path):
    logger.info("--- Iniciando substituição final de 'nan' por nulo ---")
    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name or 'TOI_AD_FF_ENERGISA' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig', keep_default_na=False)
            df.replace(['nan', 'NaT', 'None', 'NAN'], '', inplace=True)
            df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
        except Exception as e:
            logger.error(f"Falha ao substituir 'nan' no arquivo '{file_path.name}': {e}")

# 1
def _deduplicar_arquivos_finais(diretorio: Path):
    logger.info("--- Iniciando purga final de duplicatas nos arquivos de saída ---")
    for file_path in diretorio.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name or 'TOI_AD_FF_ENERGISA' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            chave_deduplicacao = 'CPF'
            if chave_deduplicacao in df.columns and df.duplicated(subset=[chave_deduplicacao]).any():
                tamanho_inicial = len(df)
                df['completude'] = df.notna().sum(axis=1)
                df.sort_values(by=[chave_deduplicacao, 'completude'], ascending=[True, False], inplace=True)
                df.drop_duplicates(subset=[chave_deduplicacao], keep='last', inplace=True)
                df.drop(columns=['completude'], inplace=True)
                df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
                removidos = tamanho_inicial - len(df)
                logger.warning(f"  -> {removidos} duplicatas removidas de '{file_path.name}'.")
        except Exception as e:
            logger.error(f"Falha ao deduplicar o arquivo '{file_path.name}': {e}")

def _corrigir_encoding_geral(diretorio_alvo: Path):
    logger.info("--- Iniciando correção geral de encoding (Até, NÃO) ---")
    correcoes = {'AtÃ©': 'Até', 'NÃƒO': 'NÃO'}
    colunas_alvo = ['faixa', 'reav', 'corte_toi', 'cortepen', 'iu12m', 'Cliente_Regulariza']
    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name or 'TOI_AD_FF_ENERGISA' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            for coluna in colunas_alvo:
                if coluna in df.columns:
                    for texto_corrompido, texto_correto in correcoes.items():
                        df[coluna] = df[coluna].str.replace(texto_corrompido, texto_correto, regex=False)
            df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
        except Exception as e:
            logger.error(f"Falha ao corrigir encoding no arquivo '{file_path.name}': {e}")

# 2
def _limpar_cpf_numerico(diretorio_alvo: Path):
    logger.info("--- Iniciando purificação de CPFs não numéricos ---")
    coluna_cpf = 'CPF'
    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name or 'TOI_AD_FF_ENERGISA' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            if coluna_cpf in df.columns:
                df[coluna_cpf] = df[coluna_cpf].str.replace(r'\D', '', regex=True)
                df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
        except Exception as e:
            logger.error(f"Falha ao purificar CPFs no arquivo '{file_path.name}': {e}")

# 3
def organize_and_compress_output(config: ConfigParser, run_log_file: str):
    logger.info("--- INICIANDO ROTINA DE ORGANIZAÇÃO E COMPRESSÃO ---")
    
    output_dir = Path(config.get('PATHS', 'output_dir'))
    date_format_str = config.get('SETTINGS', 'output_date_format').replace('%%', '%')
    pasta_do_dia = output_dir / datetime.now().strftime(date_format_str)

    if not pasta_do_dia.is_dir():
        logger.error(f"Pasta do dia '{pasta_do_dia}' não encontrada. Abortando.")
        return

    if run_log_file and Path(run_log_file).exists():
        shutil.copy(run_log_file, pasta_do_dia)
        logger.info(f"Log da execução '{Path(run_log_file).name}' copiado para a pasta de arquivamento.")
    
    _exorcizar_arquivos_fantasmas(pasta_do_dia)
    _substituir_nan_por_nulo(pasta_do_dia)
    _deduplicar_arquivos_finais(pasta_do_dia)
    _corrigir_encoding_geral(pasta_do_dia)
    _limpar_cpf_numerico(pasta_do_dia)

    archive_name_prefix = config.get('COMPRESSOR', 'archive_name_prefix', fallback='mailing_')
    zip_name = f"{archive_name_prefix}{datetime.now().strftime('%d-%m-%Y')}.zip"
    zip_path = output_dir / zip_name

    try:
        shutil.make_archive(str(zip_path.with_suffix('')), 'zip', str(pasta_do_dia))
        logger.info(f"Pasta do dia comprimida com sucesso em '{zip_path}'")
        shutil.rmtree(pasta_do_dia)
        logger.info(f"Pasta de trabalho original '{pasta_do_dia}' removida com sucesso.")
    except Exception as e:
        logger.error(f"Falha na compressão ou remoção: {e}")
