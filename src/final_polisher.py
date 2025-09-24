# -*- coding: utf-8 -*-
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# 1. Adicionadas novas colunas para polimento
COLUNAS_ENCODING_NAO = ['reav', 'corte_toi', 'cortepen', 'iu12m', 'Cliente_Regulariza']
COLUNAS_TEXTO_INTEIRO = [
    'ind_telefone_1_valido', 
    'ind_telefone_2_valido',
    'ndoc',
    'consumo',
    'fone_consumidor',
    'diasprot',
    'CPF',
    'Quantidade_UC_por_CPF'
]

def polimento_final(diretorio_alvo: Path):
    """
    Executa a limpeza final em todos os arquivos CSV gerados.
    - Remove o sufixo '.0' de colunas que devem ser inteiras.
    - Corrige erros de encoding (NÃƒO -> NÃO).
    """
    logger.info("--- Iniciando polimento final nos arquivos ---")

    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            logger.info(f"Polindo o arquivo: '{file_path.name}'")
            sep = '|' if 'Robo' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            
            # 2. Corrige o encoding 'NÃƒO' -> 'NÃO'
            for coluna in COLUNAS_ENCODING_NAO:
                if coluna in df.columns:
                    df[coluna] = df[coluna].str.replace('NÃƒO', 'NÃO', regex=False)
            
            # 3. Remove '.0' de colunas que devem ser texto/inteiro
            for coluna in COLUNAS_TEXTO_INTEIRO:
                if coluna in df.columns:
                    df[coluna] = df[coluna].astype(str).str.replace(r'\.0$', '', regex=True)

            df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
            logger.info(f"Polimento do arquivo '{file_path.name}' concluído.")
        except Exception as e:
            logger.error(f"Falha ao polir o arquivo '{file_path.name}': {e}")

