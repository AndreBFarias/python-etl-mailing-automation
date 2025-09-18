# -*- coding: utf-8 -*-

import os
import pandas as pd
from pathlib import Path
import logging
import tempfile # Para escrita segura em arquivos

logger = logging.getLogger(__name__)

def exorcizar_fantasmas(diretorio_alvo: Path):
    """
    Varre o diretório alvo e remove quaisquer arquivos CSV com o caractere 'ï»¿' no nome.
    """
    logger.info("--- ESTÁGIO 5.1: Caça aos Fantasmas ---")
    try:
        fantasmas_encontrados = list(diretorio_alvo.glob('*ï»¿*.csv'))
        
        if not fantasmas_encontrados:
            logger.info("Nenhum fantasma encontrado. O reino está limpo.")
            return

        logger.warning(f"Encontrados {len(fantasmas_encontrados)} arquivos fantasmas. Iniciando exorcismo.")
        for fantasma in fantasmas_encontrados:
            try:
                os.remove(fantasma)
                logger.info(f"Fantasma '{fantasma.name}' banido com sucesso.")
            except OSError as e:
                logger.error(f"Falha ao banir o fantasma '{fantasma.name}': {e}")
    except Exception as e:
        logger.error(f"Ocorreu um erro durante o ritual de exorcismo: {e}", exc_info=True)

def polir_colunas_do_robo(diretorio_alvo: Path):
    """
    Encontra todos os arquivos do robô no diretório alvo e remove o '.0' do final de
    colunas específicas (CPF, Telefone1, telefone2).
    """
    logger.info("--- ESTÁGIO 5.2: Polimento de Colunas do Robô ---")
    try:
        arquivos_robo = list(diretorio_alvo.glob('*Robo_*.csv'))
        colunas_para_polir = ['CPF', 'Telefone1', 'telefone2']

        if not arquivos_robo:
            logger.warning("Nenhum arquivo do robô encontrado para polimento.")
            return

        for arquivo_path in arquivos_robo:
            try:
                logger.info(f"Processando arquivo do robô: '{arquivo_path.name}'")
                # Força todas as colunas a serem lidas como texto
                df = pd.read_csv(arquivo_path, sep='|', dtype=str, encoding='utf-8-sig')

                for coluna in colunas_para_polir:
                    if coluna in df.columns:
                        # Garante que a coluna é tratada como string antes da substituição
                        df[coluna] = df[coluna].astype(str).str.replace(r'\.0$', '', regex=True).fillna('')
                        logger.debug(f"  - Coluna '{coluna}' polida.")
                
                df.to_csv(arquivo_path, sep='|', index=False, encoding='utf-8-sig')
                logging.info(f"Arquivo '{arquivo_path.name}' polido e salvo com sucesso.")

            except Exception as e:
                logging.error(f"Ocorreu um erro ao processar o arquivo '{arquivo_path.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Ocorreu uma falha geral durante o polimento de colunas do robô: {e}", exc_info=True)

def polir_colunas_humanas(diretorio_alvo: Path):
    """
    Encontra todos os arquivos humanos e aplica limpezas específicas: remove '.0' de
    identificadores e corrige strings de 'NÃO' corrompidas.
    """
    logger.info("--- ESTÁGIO 5.3: Polimento de Colunas Humanas ---")
    try:
        arquivos_humanos = [f for f in diretorio_alvo.glob('*.csv') if 'Robo' not in f.name]
        
        colunas_ponto_zero = ['CPF', 'diasprot', 'ind_telefone_1_valido', 'ind_telefone_2_valido']
        colunas_nao_bugado = ['iu12m', 'reav', 'corte_toi', 'cortepen']

        if not arquivos_humanos:
            logger.warning("Nenhum arquivo humano encontrado para polimento.")
            return

        for arquivo_path in arquivos_humanos:
            try:
                logger.info(f"Processando arquivo humano: '{arquivo_path.name}'")
                # Lê o CSV forçando todos os dados a serem string para máxima segurança
                df = pd.read_csv(arquivo_path, sep=';', dtype=str, encoding='utf-8-sig')

                # 1. Polir colunas com '.0'
                for coluna in colunas_ponto_zero:
                    if coluna in df.columns:
                        df[coluna] = df[coluna].str.replace(r'\.0$', '', regex=True).fillna('')
                        logger.debug(f"  - Coluna '{coluna}' polida (removido .0).")

                # 2. Corrigir 'NÃƒO' para 'NÃO'
                for coluna in colunas_nao_bugado:
                    if coluna in df.columns:
                        df[coluna] = df[coluna].str.replace('NÃƒO', 'NÃO', regex=False)
                        logger.debug(f"  - Coluna '{coluna}' corrigida (NÃO).")
                
                # Implementa a escrita segura para evitar corrupção de arquivos
                temp_path = None
                with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=diretorio_alvo, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    df.to_csv(temp_file.name, sep=';', index=False, encoding='utf-8-sig')
                    temp_path = Path(temp_file.name)
                
                # Substitui o arquivo original pelo temporário já corrigido.
                if temp_path:
                    temp_path.replace(arquivo_path)
                
                logging.info(f"Arquivo '{arquivo_path.name}' polido e salvo com sucesso.")

            except Exception as e:
                logging.error(f"Ocorreu um erro ao processar o arquivo '{arquivo_path.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Ocorreu uma falha geral durante o polimento de colunas humanas: {e}", exc_info=True)
