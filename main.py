# -*- coding: utf-8 -*-
import logging
from pathlib import Path
from datetime import datetime
import sys

from src.logger_setup import setup_logger
from src.config_manager import load_config
from src.data_loader import load_all_data
from src.processing_pipeline import processar_dados
from src.data_exporter import exportar_dados_humanos
from src.gerador_robo_mestre import gerar_arquivo_robo_mestre
from src.formatador_dados import formatar_csvs_para_padrao_br
from src.final_polisher import polimento_final
from src.compressor import organize_and_compress_output

MSG_COBRANCA_ERRO = "FALHA NA AUTOMAÇÃO: Erro inesperado. Verifique o log para detalhes."

def main():
    config = None
    try:
        config = load_config('config.ini')
        setup_logger(config.get('PATHS', 'log_dir'), config.get('SETTINGS', 'log_level'))
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO CRÍTICO NA CONFIGURAÇÃO: {e}\nProcesso abortado.")
        sys.exit(1)

    try:
        logging.info("="*30 + " INÍCIO DO PROCESSO DE AUTOMAÇÃO (ARQUITETURA UNIFICADA) " + "="*30)

        logging.info("--- ESTÁGIO 1: Carregando e Validando dados ---")
        all_dataframes = load_all_data(config)
        logging.info("--- ESTÁGIO 1 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 2: Processando dados ---")
        df_humano, df_robo = processar_dados(all_dataframes, config)
        logging.info("--- ESTÁGIO 2 CONCLUÍDO ---")
        
        logging.info("--- SUMÁRIO PÓS-PROCESSAMENTO ---")
        logging.info(f"Registros para 'Acionamento Humano': {len(df_humano)}")
        logging.info(f"Registros para 'Acionamento Robô': {len(df_robo)}")
        logging.info("------------------------------------")

        output_dir = Path(config.get('PATHS', 'output_dir', fallback='./data_output'))
        date_format = config.get('SETTINGS', 'output_date_format').replace('%%', '%')
        pasta_do_dia = output_dir / datetime.now().strftime(date_format)
        pasta_do_dia.mkdir(exist_ok=True, parents=True)
        
        if df_humano.empty and df_robo.empty:
            logging.warning("Todos os DataFrames de saída estão vazios. Nenhum arquivo será exportado.")
        else:
            logging.info("--- ESTÁGIO 3: Exportando arquivos finais ---")
            exportar_dados_humanos(df_humano, config, pasta_do_dia)
            gerar_arquivo_robo_mestre(df_robo, config, pasta_do_dia)
            logging.info("--- ESTÁGIO 3 CONCLUÍDO ---")
        
        logging.info("--- ESTÁGIO 4: Formatando e Polindo Saídas ---")
        formatar_csvs_para_padrao_br(pasta_do_dia)
        polimento_final(pasta_do_dia) 
        logging.info("--- ESTÁGIO 4 CONCLUÍDO ---")

        logging.info("--- ESTÁGIO 5: Organizando e Comprimindo a saída ---")
        organize_and_compress_output(config)
        logging.info("--- ESTÁGIO 5 CONCLUÍDO ---")
        
        logging.info("="*30 + " PROCESSO DE AUTOMAÇÃO CONCLUÍDO COM SUCESSO " + "="*30)

    except Exception as e:
        logging.critical(f"ERRO CRÍTICO NO FLUXO PRINCIPAL: {e}", exc_info=True)
        print(MSG_COBRANCA_ERRO)
        sys.exit(1)

if __name__ == '__main__':
    main()

