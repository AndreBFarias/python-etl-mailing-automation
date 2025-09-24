# -*- coding: utf-8 -*-

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

COLUNAS_ALVO = ['liquido', 'total_toi', 'valor', 'valorDivida']

def _formatar_valor_para_duas_casas(valor_str: str) -> str:
    """
    Converte uma string para um float, arredonda para 2 casas decimais,
    e formata de volta para uma string no padrão brasileiro (com ',').
    """
    if not isinstance(valor_str, str) or valor_str.strip() == '':
        return valor_str
    try:
        # Lógica robusta que primeiro tenta converter direto, depois limpando
        try:
            valor_float = float(valor_str)
        except ValueError:
            cleaned_str = valor_str.strip().replace('.', '').replace(',', '.', 1)
            valor_float = float(cleaned_str)
        
        return f'{valor_float:.2f}'.replace('.', ',')
    except (ValueError, TypeError):
        return valor_str

def formatar_csvs_para_padrao_br(diretorio_alvo: Path):
    """
    Varre um diretório, lê cada CSV humano e aplica a formatação monetária
    padrão brasileiro (2 casas decimais) nas colunas financeiras.
    """
    logger.info(f"--- INICIANDO FORMATAÇÃO FINAL PARA PADRÃO BRASILEIRO EM '{diretorio_alvo}' ---")
    
    csv_files = list(diretorio_alvo.glob('*.csv'))
    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em '{diretorio_alvo}' para formatação.")
        return

    for file_path in csv_files:
        try:
            if "Robo" in file_path.name:
                logger.info(f"Pulando formatação para o arquivo de robô: '{file_path.name}'")
                continue

            logger.info(f"Formatando o arquivo humano: '{file_path.name}'")
            df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8-sig')

            for coluna in COLUNAS_ALVO:
                if coluna in df.columns:
                    df[coluna] = df[coluna].apply(_formatar_valor_para_duas_casas)
            
            # Remove o '.0' de colunas que deveriam ser inteiras
            if 'CPF' in df.columns:
                df['CPF'] = df['CPF'].str.replace(r'\.0$', '', regex=True)

            df.to_csv(file_path, sep=';', index=False, encoding='utf-8-sig')
            logger.info(f"Arquivo '{file_path.name}' formatado e salvo com sucesso.")

        except Exception as e:
            logger.error(f"Falha ao formatar o arquivo '{file_path.name}': {e}", exc_info=True)
