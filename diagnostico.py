# -*- coding: utf-8 -*-
import logging
from pathlib import Path
import sys

from src.config_manager import load_config
from src.compressor import organize_and_compress_output
from src.logger_setup import setup_logger

logger = logging.getLogger(__name__)

def run_compressor_only():
    """
    Função dedicada a executar apenas a etapa de compressão e organização final.
    """
    try:
        config = load_config('config.ini')
        log_dir = Path(config.get('PATHS', 'log_dir', fallback='./logs'))
        setup_logger(log_dir, config.get('SETTINGS', 'log_level', fallback='INFO'))

        logger.info("="*30)
        logger.info("--- MODO DE EXECUÇÃO: APENAS COMPRESSOR ---")
        logger.info("="*30)
        
        organize_and_compress_output(config)
        
        logger.info("="*30)
        logger.info("--- PROCESSO DO COMPRESSOR CONCLUÍDO ---")
        logger.info("="*30)

    except FileNotFoundError as e:
        print(f"ERRO CRÍTICO: {e}. Certifique-se de que o 'config.ini' existe.")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"UM ERRO INESPERADO OCORREU: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    run_compressor_only()
