# 1. Importações
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 2. Função Principal de Configuração
def setup_logger(log_dir: str, log_level: str = 'INFO'):
    """
    Configura um logger dual que escreve para um arquivo e para o console.

    Args:
        log_dir (str): O diretório onde os arquivos de log serão salvos.
        log_level (str): O nível mínimo de log a ser registrado (ex: 'INFO', 'DEBUG').
    """
    # 3. Formato do Log
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    )
    
    # Garante que o diretório de logs exista
    os.makedirs(log_dir, exist_ok=True)

    # Pega o logger raiz
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Evita adicionar múltiplos handlers se a função for chamada mais de uma vez
    if logger.hasHandlers():
        logger.handlers.clear()

    # 4. Configuração do Diário de Ecos (Arquivo)
    log_file_path = os.path.join(log_dir, f"automacao_{datetime.now().strftime('%Y-%m-%d')}.log")
    
    # Rotaciona o log quando ele atinge 5MB, mantendo 3 backups.
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(log_format)

    # 5. Configuração do Sussurro no Vento (Console)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # 6. União dos Sentidos
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("Logger configurado com sucesso.")

if __name__ == '__main__':
    # Exemplo de como usar o logger
    # Este bloco só será executado se você rodar este arquivo diretamente
    print("Executando teste do módulo de logging...")
    setup_logger('./logs_teste', 'DEBUG')
    
    logging.debug("Esta é uma mensagem de debug.")
    logging.info("O processo de teste foi iniciado.")
    logging.warning("Atenção: o café está acabando.")
    logging.error("Ocorreu um erro hipotético.")
    logging.critical("Falha crítica! O universo pode implodir.")
    print("Teste do módulo de logging concluído. Verifique a pasta './logs_teste'.")
