# -*- coding: utf-8 -*-
import configparser
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def load_config(path: str) -> configparser.ConfigParser:
    """Carrega e parseia o arquivo de configuração."""
    config_path = Path(path)
    if not config_path.exists():
        msg = f"Arquivo de configuração '{path}' não encontrado."
        logger.critical(msg)
        raise FileNotFoundError(msg)
    
    config = configparser.ConfigParser()
    config.read(path, encoding='utf-8')
    logger.info("Arquivo de configuração carregado com sucesso.")
    return config

def validate_config(config: configparser.ConfigParser):
    """Valida se as seções e chaves essenciais existem no config.ini."""
    logger.info("Validando chaves de configuração essenciais...")
    
    required_sections = {
        'PATHS': ['input_dir', 'output_dir', 'log_dir'],
        'FILENAMES': [], # A validação de FILENAMES foi customizada abaixo
        'SETTINGS': ['log_level']
    }

    for section, keys in required_sections.items():
        if section not in config:
            raise ValueError(f"Seção obrigatória '{section}' não encontrada no config.ini")
        for key in keys:
            if key not in config[section]:
                raise ValueError(f"Chave obrigatória '{key}' não encontrada na seção '[{section}]' do config.ini")

    # 1
    """
    # --- LÓGICA ANTIGA (PRESERVADA PARA HOMOLOGAÇÃO) ---
    if 'mailing_nucleo_pattern' not in config['FILENAMES']:
        raise ValueError("Chave obrigatória 'mailing_nucleo_pattern' não encontrada na seção '[FILENAMES]' do config.ini")
    """

    # 2
    # --- NOVA LÓGICA DE VALIDAÇÃO PARA ARQUITETURA DE DOIS FUNIS ---
    required_filenames = ['mailing_regulariza_pattern', 'mailing_nao_regulariza_pattern']
    for filename_key in required_filenames:
        if filename_key not in config['FILENAMES']:
            raise ValueError(f"Chave obrigatória '{filename_key}' não encontrada na seção '[FILENAMES]' do config.ini")

    logger.info("Configuração validada com sucesso.")
