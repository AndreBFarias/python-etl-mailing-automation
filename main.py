import logging
from src.logger_setup import setup_logger
from src.config_manager import load_config, validate_config
from src.state_manager import read_state, write_state
from src.data_loader import load_all_data
from src.processing_pipeline import processar_dados 
from src.data_exporter import exportar_dados
from datetime import datetime

def main():
    """Função principal que orquestra todo o processo de automação."""
    config = None
    try:
        # Carrega e valida a configuração primeiro
        config = load_config('config.ini')
        validate_config(config)
        
        # Configura o logger com base no arquivo .ini
        setup_logger(config.get('PATHS', 'log_dir'), config.get('SETTINGS', 'log_level'))
    
    except (FileNotFoundError, ValueError) as e:
        # Se a configuração falhar, o log não estará disponível. Imprime no console.
        print(f"ERRO CRÍTICO NA CONFIGURAÇÃO: {e}\nProcesso abortado.")
        return # Encerra a execução

    try:
        logging.info("="*30 + " INÍCIO DO PROCESSO DE AUTOMAÇÃO " + "="*30)
        state_file_path = config.get('PATHS', 'state_file')
        state = read_state(state_file_path) # Lê o estado, mas não age sobre ele ainda

        logging.info("--- ESTÁGIO 1: Carregando arquivos de dados ---")
        all_dataframes = load_all_data(config)
        logging.info("--- ESTÁGIO 1 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 2: Processando dados (Alquimia) ---")
        df_final = processar_dados(all_dataframes, config)
        logging.info("--- ESTÁGIO 2 CONCLUÍDO ---")

        if df_final.empty:
            logging.warning("O DataFrame final está vazio após o processamento. Nenhum arquivo será exportado.")
        else:
            logging.info("--- ESTÁGIO 3: Exportando arquivos finais ---")
            exportar_dados(df_final, config)
            logging.info("--- ESTÁGIO 3 CONCLUÍDO ---")
        
        # Atualiza o estado para registrar uma execução bem-sucedida
        success_state = {
            'last_successful_run': datetime.now().isoformat(),
            'status': 'COMPLETED'
        }
        write_state(state_file_path, success_state)
        
        logging.info("="*30 + " PROCESSO DE AUTOMAÇÃO CONCLUÍDO COM SUCESSO " + "="*30)

    except FileNotFoundError as e:
        logging.critical(f"ERRO FATAL: Arquivo necessário não encontrado. {e}")
        logging.critical("O processo foi interrompido.")
    except Exception as e:
        logging.critical(f"ERRO INESPERADO E NÃO TRATADO NO FLUXO PRINCIPAL: {e}", exc_info=True)
        # Atualiza o estado para registrar a falha
        if config:
            state_file_path = config.get('PATHS', 'state_file')
            error_state = {
                'last_failed_run': datetime.now().isoformat(),
                'status': 'FAILED',
                'error_message': str(e)
            }
            write_state(state_file_path, error_state)
        logging.critical("O processo foi interrompido devido a um erro crítico.")

if __name__ == '__main__':
    main()


