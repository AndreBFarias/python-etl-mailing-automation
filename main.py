import logging
from src.logger_setup import setup_logger
from src.config_manager import load_config, validate_config
from src.state_manager import read_state, write_state
# 5. Importação da nova exceção e do loader atualizado
from src.data_loader import load_all_data, SchemaValidationError
from src.processing_pipeline import processar_dados 
from src.data_exporter import exportar_dados
from datetime import datetime
import sys

def main():
    """Função principal que orquestra todo o processo de automação."""
    config = None
    try:
        config = load_config('config.ini')
        validate_config(config)
        setup_logger(config.get('PATHS', 'log_dir'), config.get('SETTINGS', 'log_level'))
    
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO CRÍTICO NA CONFIGURAÇÃO: {e}\nProcesso abortado.")
        sys.exit(1)

    try:
        logging.info("="*30 + " INÍCIO DO PROCESSO DE AUTOMAÇÃO " + "="*30)
        state_file_path = config.get('PATHS', 'state_file')
        state = read_state(state_file_path)

        logging.info("--- ESTÁGIO 1: Carregando e Validando arquivos de dados ---")
        all_dataframes = load_all_data(config)
        logging.info("--- ESTÁGIO 1 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 2: Processando dados ---")
        df_final = processar_dados(all_dataframes, config)
        logging.info("--- ESTÁGIO 2 CONCLUÍDO ---")

        if df_final.empty:
            logging.warning("O DataFrame final está vazio após o processamento. Nenhum arquivo será exportado.")
        else:
            logging.info("--- ESTÁGIO 3: Exportando arquivos finais ---")
            exportar_dados(df_final, config)
            logging.info("--- ESTÁGIO 3 CONCLUÍDO ---")
        
        success_state = {
            'last_successful_run': datetime.now().isoformat(),
            'status': 'COMPLETED'
        }
        write_state(state_file_path, success_state)
        
        logging.info("="*30 + " PROCESSO DE AUTOMAÇÃO CONCLUÍDO COM SUCESSO " + "="*30)

    # 6. Captura do Erro de Schema
    except (FileNotFoundError, SchemaValidationError) as e:
        # A mensagem de erro já vem formatada da exceção, então apenas a logamos.
        logging.critical(f"\n\n{e}\n")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"ERRO INESPERADO E NÃO TRATADO NO FLUXO PRINCIPAL: {e}", exc_info=True)
        if config:
            state_file_path = config.get('PATHS', 'state_file')
            error_state = {
                'last_failed_run': datetime.now().isoformat(),
                'status': 'FAILED',
                'error_message': str(e)
            }
            write_state(state_file_path, error_state)
        logging.critical("O processo foi interrompido devido a um erro crítico.")
        sys.exit(1)

if __name__ == '__main__':
    main()

