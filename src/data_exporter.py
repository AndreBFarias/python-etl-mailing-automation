# -*- coding: utf-8 -*-
import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)

def _formatar_valor_para_duas_casas(valor) -> str:
    if pd.isna(valor): return ''
    try:
        valor_float = float(valor)
        return f'{valor_float:.2f}'.replace('.', ',')
    except (ValueError, TypeError):
        return str(valor)

def exportar_dados_humanos(df_humano: pd.DataFrame, config: ConfigParser, diretorio_alvo: Path):
    """
    Função refatorada para a arquitetura de fluxo único.
    Exporta o mailing humano, particionando por 'PRODUTO' e selecionando colunas específicas.
    """
    logger.info("="*20 + " INICIANDO EXPORTAÇÃO DE DADOS HUMANOS (FLUXO ÚNICO) " + "="*20)
    
    if df_humano.empty:
        logger.warning("DataFrame 'Humano' está vazio. Nenhuma exportação será realizada.")
        return

    formato_data_string = config.get('SETTINGS', 'output_date_format').replace('%%', '%')
    data_str_hoje = datetime.now().strftime(formato_data_string)
    
    colunas_data = ['dtvenc', 'dtreav', 'dtprot', 'dt_deslig', 'dtapr', 'data_encer_cont', 'min_datavcm', 'dt_aplicação']
    colunas_financeiras = ['liquido', 'total_toi', 'valor', 'valorDivida']
    
    df_export = df_humano.copy()

    # Aplica formatações
    for coluna in colunas_financeiras:
        if coluna in df_export.columns:
            df_export[coluna] = df_export[coluna].apply(_formatar_valor_para_duas_casas)
    for coluna_data in colunas_data:
        if coluna_data in df_export.columns:
            df_export[coluna_data] = pd.to_datetime(df_export[coluna_data], errors='coerce').dt.strftime('%d/%m/%Y')
    
    # 1
    # --- LÓGICA DE FILTRAGEM DE COLUNAS (NOVA) ---
    # 2
    try:
        colunas_human_str = config.get('EXPORT_COLUMNS', 'human_columns')
        colunas_finais_exportacao = [col.strip() for col in colunas_human_str.replace('\n', ',').split(',') if col.strip()]
        
        # 3
        colunas_presentes = [col for col in colunas_finais_exportacao if col in df_export.columns]
        df_export_final = df_export[colunas_presentes]
        logger.info(f"Aplicando filtro de exportação. {len(colunas_presentes)} colunas serão salvas nos arquivos humanos.")
    # 5
    except Exception as e:
        logger.warning(f"Não foi possível ler a configuração de colunas de exportação para arquivos humanos: {e}. Exportando todas as colunas.")
        df_export_final = df_export

    # 4
    # Exporta particionado por produto
    prefixo = config.get('SETTINGS', 'output_file_prefix', fallback='Telecobranca_TOI_')
    if 'PRODUTO' in df_export_final.columns:
        df_export_final['PRODUTO'] = df_export_final['PRODUTO'].astype(str).str.strip()
        for produto in df_export_final['PRODUTO'].unique():
            if pd.isna(produto) or not str(produto).strip(): continue
            
            df_produto = df_export_final[df_export_final['PRODUTO'] == produto]
            nome_arquivo = f"{prefixo}mailing_{produto}_{data_str_hoje}.csv"
            caminho_saida = diretorio_alvo / nome_arquivo
            
            logger.info(f"Exportando {len(df_produto)} linhas para '{caminho_saida}'")
            df_produto.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig', na_rep='')
    else:
        logger.error("Coluna 'PRODUTO' não encontrada. Não é possível particionar a exportação.")

    logger.info("="*20 + " EXPORTAÇÃO DE DADOS HUMANOS CONCLUÍDA " + "="*20)
