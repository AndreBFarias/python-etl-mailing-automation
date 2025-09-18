# -*- coding: utf-8 -*-

import os
import pandas as pd
from pathlib import Path
import logging

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
                logger.info(f"Processando arquivo: '{arquivo_path.name}'")
                # Define o tipo de todas as colunas de polimento como string na leitura
                dtypes = {col: str for col in colunas_para_polir}
                df = pd.read_csv(arquivo_path, sep='|', dtype=dtypes, encoding='utf-8-sig')

                for coluna in colunas_para_polir:
                    if coluna in df.columns:
                        # Garante que a coluna seja string e remove o '.0'
                        df[coluna] = df[coluna].astype(str).str.replace(r'\.0$', '', regex=True).fillna('')
                        logger.debug(f"  - Coluna '{coluna}' polida.")
                    else:
                        logging.warning(f"Coluna '{coluna}' não encontrada em '{arquivo_path.name}'.")
                
                # Salva o arquivo de volta, sobrescrevendo o original
                df.to_csv(arquivo_path, sep='|', index=False, encoding='utf-8-sig')
                logging.info(f"Arquivo '{arquivo_path.name}' polido e salvo com sucesso.")

            except Exception as e:
                logging.error(f"Ocorreu um erro ao processar o arquivo '{arquivo_path.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Ocorreu uma falha geral durante o polimento de colunas: {e}", exc_info=True)
