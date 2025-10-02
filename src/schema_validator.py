# -*- coding: utf-8 -*-
import pandas as pd
from configparser import ConfigParser
import logging
from typing import List

logger = logging.getLogger(__name__)

# 1
"""
# --- LÓGICA ANTIGA (PRESERVADA PARA HOMOLOGAÇÃO) ---
def get_required_columns(config: ConfigParser, schema_key: str) -> List[str]:
    \"\"\"Lê e parseia as colunas requeridas de uma seção do config.ini.\"\"\"
    columns_str = config.get(schema_key, 'required_columns', fallback='')
    # Esta lógica falhava ao não remover as vírgulas no final de cada linha.
    return [col.strip().lower() for col in columns_str.split('\\n') if col.strip()]
"""

# 2
# --- NOVA LÓGICA DE PARSING ROBUSTA ---
def get_required_columns(config: ConfigParser, schema_key: str) -> List[str]:
    """Lê e parseia as colunas requeridas de uma seção do config.ini, lidando com vírgulas e quebras de linha."""
    columns_str = config.get(schema_key, 'required_columns', fallback='')
    # Substitui quebras de linha por vírgulas e depois divide pela vírgula
    cleaned_str = columns_str.replace('\n', ',')
    return [col.strip().lower() for col in cleaned_str.split(',') if col.strip()]

# 3
def validate_schema(df: pd.DataFrame, config: ConfigParser, schema_key: str, filename: str):
    """
    Valida o schema de um DataFrame contra as colunas requeridas no config.ini.
    """
    logger.info(f"Iniciando validação de schema para o arquivo: {filename}")
    
    required_columns = get_required_columns(config, schema_key)
    if not required_columns:
        logger.warning(f"Nenhuma coluna requerida definida para '{schema_key}' no config.ini. Validação pulada.")
        return

    df_columns = {col.lower() for col in df.columns}
    
    missing_columns = [col for col in required_columns if col not in df_columns]
    if missing_columns:
        error_msg = f"SCHEMA INVALIDO em '{filename}': Colunas obrigatórias não encontradas: {', '.join(missing_columns)}."
        logger.critical(error_msg)
        raise SchemaValidationError(error_msg)

    extra_columns = list(df_columns - set(required_columns))
    if extra_columns:
        logger.warning(
            f"ALERTA DE SCHEMA EM '{filename}': Novas colunas não esperadas foram encontradas e "
            f"serão mantidas: {extra_columns}. Considere adicioná-las ao 'config.ini' se forem permanentes."
        )
    
    logger.info(f"Validação de schema para '{filename}' concluída com sucesso.")

class SchemaValidationError(Exception):
    """Exceção customizada para erros de validação de schema."""
    pass
