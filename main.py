import logging
from src.logger_setup import setup_logger
from src.config_manager import load_config, validate_config
from src.state_manager import read_state, write_state
from src.data_loader import load_all_data
# Importa a nova versão do pipeline
from src.processing_pipeline import processar_dados 
from src.data_exporter import exportar_dados

def main():
    """Função principal que orquestra todo o processo de automação."""
    try:
        config = load_config('config.ini')
        validate_config(config)
        setup_logger(config.get('PATHS', 'log_dir'), config.get('SETTINGS', 'log_level'))
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO CRÍTICO NA CONFIGURAÇÃO: {e}\nProcesso abortado.")
        return

    try:
        logging.info("="*30 + " INÍCIO DO PROCESSO DE AUTOMAÇÃO " + "="*30)
        state_file_path = config.get('PATHS', 'state_file')
        state = read_state(state_file_path)

        logging.info("--- ESTÁGIO 1: Carregando arquivos de dados ---")
        all_dataframes = load_all_data(config)
        logging.info("--- ESTÁGIO 1 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 2: Processando dados (Alquimia) ---")
        df_final = processar_dados(all_dataframes, config)
        logging.info("--- ESTÁGIO 2 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 3: Exportando arquivos finais ---")
        exportar_dados(df_final, config)
        logging.info("--- ESTÁGIO 3 CONCLUÍDO ---")
        
        write_state(state_file_path, {'last_successful_run': 'all_stages_complete'})
        logging.info("Estado final salvo.")

        logging.info("="*32 + " PROCESSO CONCLUÍDO COM SUCESSO " + "="*32)

    except Exception as e:
        logging.critical("PROCESSO ABORTADO: Ocorreu um erro fatal.", exc_info=True)

if __name__ == "__main__":
    main()
