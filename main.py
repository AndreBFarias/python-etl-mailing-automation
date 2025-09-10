import logging
from src.logger_setup import setup_logger
from src.config_manager import load_config, validate_config
from src.state_manager import read_state, write_state
from src.data_loader import load_all_data, SchemaValidationError
from src.processing_pipeline import processar_dados
from src.data_exporter import exportar_dados
from src.compressor import organize_and_compress_output
# 1. IMPORTAÇÃO DO NOVO MÓDULO DE VALIDAÇÃO
from src.schema_validator import get_current_schema, save_snapshot, compare_and_report
from datetime import datetime
import sys

# 2. MENSAGEM DE COBRANÇA PRÉ-FORMATADA
MSG_COBRANCA_ERRO = """
==================================================
FALHA NA AUTOMAÇÃO: Mudança na estrutura dos dados de entrada.
==================================================

A automação foi interrompida porque a estrutura dos arquivos de entrada (nomes de arquivos, abas, colunas ou ordem) foi alterada sem comunicação prévia.

Um laudo técnico detalhado com todas as divergências encontradas foi gerado automaticamente. Por favor, verifiquem o arquivo anexo 'LAUDO_DE_ALTERACOES.txt' e alinhem o processo de extração para garantir a consistência dos dados.

A automação não pode prosseguir até que a estrutura seja corrigida ou as novas regras sejam formalizadas.
"""

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
        # --- CÓDIGO ANTIGO COMENTADO PARA HOMOLOGAÇÃO ---
        # df_final = processar_dados(all_dataframes, config)
        # --- FIM DO CÓDIGO ANTIGO ---

        # --- NOVO CÓDIGO CORRIGIDO ---
        # A função de processamento agora retorna dois DataFrames distintos.
        df_humano_final, df_robo_final = processar_dados(all_dataframes, config)
        # --- FIM DO NOVO CÓDIGO ---
        logging.info("--- ESTÁGIO 2 CONCLUÍDO ---")

        # A validação agora checa se ambos os dataframes estão vazios.
        if df_humano_final.empty and df_robo_final.empty:
            logging.warning("Ambos os DataFrames (humano e robô) estão vazios após o processamento. Nenhum arquivo será exportado.")
        else:
            logging.info("--- ESTÁGIO 3: Exportando arquivos finais ---")
            # --- CÓDIGO ANTIGO COMENTADO PARA HOMOLOGAÇÃO ---
            # exportar_dados(df_final, config)
            # --- FIM DO CÓDIGO ANTIGO ---
            
            # --- NOVO CÓDIGO CORRIGIDO ---
            # A função de exportação agora recebe ambos os DataFrames.
            exportar_dados(df_humano_final, df_robo_final, config)
            # --- FIM DO NOVO CÓDIGO ---
            logging.info("--- ESTÁGIO 3 CONCLUÍDO ---")
        
        success_state = {
            'last_successful_run': datetime.now().isoformat(),
            'status': 'COMPLETED'
        }
        write_state(state_file_path, success_state)
        
        logging.info("="*30 + " PROCESSO DE AUTOMAÇÃO CONCLUÍDO COM SUCESSO " + "="*30)
        
        organize_and_compress_output()

        # 3. CRIAÇÃO/ATUALIZAÇÃO DO SNAPSHOT EM CASO DE SUCESSO
        logging.info("Atualizando snapshot da estrutura de dados bem-sucedida...")
        current_schema = get_current_schema()
        save_snapshot(current_schema)

    except (FileNotFoundError, SchemaValidationError, Exception) as e:
        # 4. GATILHO DE AUTÓPSIA EM CASO DE QUALQUER ERRO
        logging.critical(f"ERRO CRÍTICO NO FLUXO PRINCIPAL: {e}", exc_info=True)
        
        logging.info("Iniciando diagnóstico automático da estrutura de dados...")
        laudo = compare_and_report()
        logging.critical(laudo)
        
        # Imprime a mensagem de cobrança para você copiar e colar
        print(MSG_COBRANCA_ERRO)

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
