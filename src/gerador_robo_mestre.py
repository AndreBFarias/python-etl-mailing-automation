# -*- coding: utf-8 -*-
import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)

def _formatar_valor_para_robo(valor):
    if pd.isna(valor): return ''
    try:
        valor_float = float(valor)
        if valor_float == int(valor_float):
            return str(int(valor_float))
        else:
            return f'{valor_float:.2f}'.replace('.', ',')
    except (ValueError, TypeError):
        return str(valor)

def gerar_arquivo_robo_mestre(df_robo_consolidado: pd.DataFrame, config: ConfigParser, diretorio_alvo: Path):
    if df_robo_consolidado.empty:
        logger.warning("DataFrame consolidado do robô está vazio. Arquivos não serão gerados.")
        return
    
    logger.info("--- Iniciando Geração do Mailing Mestre do Robô (Consolidado) ---")
    
    col_cpf_padrao = 'CPF' 
    col_vencimento = config.get('SOURCE_COLUMNS', 'vencimento_fatura').lower()
    
    df_processado = df_robo_consolidado.copy()
    
    if col_vencimento not in df_processado.columns:
        logger.error(f"Coluna de vencimento '{col_vencimento}' definida no config.ini não foi encontrada. Abortando geração de arquivo robô.")
        return
    if col_cpf_padrao not in df_processado.columns:
        logger.error(f"Coluna de CPF padronizada '{col_cpf_padrao}' não foi encontrada apos o pipeline. Abortando geração de arquivo robô.")
        return

    # 1
    df_processado['dtvenc_dt'] = pd.to_datetime(df_processado[col_vencimento], errors='coerce', dayfirst=True)
    df_valid_dates = df_processado.dropna(subset=['dtvenc_dt']).copy()
    
    df_valid_dates = df_valid_dates.sort_values(by=[col_cpf_padrao, 'dtvenc_dt'], ascending=True)
    df_valid_dates['rank'] = df_valid_dates.groupby(col_cpf_padrao).cumcount() + 1
    
    df_faturas = df_valid_dates[df_valid_dates['rank'].isin([1, 2, 3])].copy()
    
    df_pivot = pd.DataFrame()
    if not df_faturas.empty:
        df_pivot = df_faturas.pivot_table(
            index=col_cpf_padrao, columns='rank',
            values=['dtvenc_dt', 'liquido', 'codbarra'], aggfunc='first'
        )
        df_pivot.columns = [f'{val}_{rank}' for val, rank in df_pivot.columns]
        df_pivot.reset_index(inplace=True)

    df_agregado = df_processado.groupby(col_cpf_padrao).first().reset_index()
    if not df_pivot.empty:
        df_agregado = pd.merge(df_agregado, df_pivot, on=col_cpf_padrao, how='left')

    colunas_finais_layout = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtrado', 'dtPrimeiraParcelaAtrasada',
        'dtSegundaParcelaAtrasada', 'dtTerceiraParcelaAtrasada', 'valorDivida', 'valorMinimo',
        'valorParcelaMaisAntiga', 'codigoBarrasConta1', 'codigoBarrasConta2', 'codigoBarrasConta3',
        'CodigoPixConta1', 'CodigoPixConta2', 'CodigoPixConta3', 'valorConta1', 'valorConta2',
        'valorConta3', 'PerfilPagamento', 'Telefone1', 'telefone2', 'RESP_NEG'
    ]
    df_final = pd.DataFrame()

    df_final['NOME_CLIENTE'] = df_agregado.get('NOME_CLIENTE')
    df_final['PRODUTO'] = df_agregado.get('PRODUTO')
    df_final['CPF'] = df_agregado.get('CPF')
    df_final['parcelasEmAtrado'] = df_agregado.get('parcelasEmAtrado')

    for i in range(1, 4):
        dt_col = f'dtvenc_dt_{i}'
        out_col = {1: 'dtPrimeiraParcelaAtrasada', 2: 'dtSegundaParcelaAtrasada', 3: 'dtTerceiraParcelaAtrasada'}[i]
        if dt_col in df_agregado.columns:
            date_series = pd.to_datetime(df_agregado[dt_col], errors='coerce')
            df_final[out_col] = date_series.dt.strftime('%d/%m/%Y')

    for i in range(1, 4):
        cb_col = f'codbarra_{i}'
        out_col = f'codigoBarrasConta{i}'
        if cb_col in df_agregado.columns:
            df_final[out_col] = df_agregado[cb_col]

    df_final['valorDivida'] = df_agregado['valorDivida'].apply(_formatar_valor_para_robo)
    df_final['valorParcelaMaisAntiga'] = df_agregado['liquido'].apply(_formatar_valor_para_robo)

    for i in range(1, 4):
        liq_col = f'liquido_{i}'
        out_col = f'valorConta{i}'
        if liq_col in df_agregado.columns:
            df_final[out_col] = df_agregado[liq_col].apply(_formatar_valor_para_robo)

    df_final['valorMinimo'] = df_agregado['liquido'].apply(_formatar_valor_para_robo)
    df_final['RESP_NEG'] = df_agregado.get('just')
    df_final['Telefone1'] = df_agregado.get('TELEFONE_01')
    df_final['telefone2'] = df_agregado.get('TELEFONE_02')
    df_final['PerfilPagamento'] = 'VISTA'

    for col in colunas_finais_layout:
        if col not in df_final.columns:
            df_final[col] = ''
    df_final = df_final.fillna('')
    
    # 2
    try:
        colunas_robo_str = config.get('EXPORT_COLUMNS', 'robo_columns')
        colunas_finais_exportacao = [col.strip() for col in colunas_robo_str.split(',') if col.strip()]
        
        # 3
        colunas_presentes = [col for col in colunas_finais_exportacao if col in df_final.columns]
        df_final_export = df_final[colunas_presentes]
        logger.info(f"Aplicando filtro de exportação para robô. {len(colunas_presentes)} colunas serão salvas.")
    # 4
    except Exception as e:
        logger.warning(f"Não foi possível ler a configuração de colunas de exportação para o robô: {e}. Exportando todas as colunas.")
        df_final_export = df_final[colunas_finais_layout]

    grupos_produto = {"08HRS": ['EPB', 'EFL', 'ESE'], "09HRS": ['EMT', 'EMS'], "10HRS": ['EAC', 'ERO', 'ETO']}
    now = datetime.now()
    prefixo_robo = config.get('ROBO', 'output_file_prefix', fallback='Telecobranca_TOI_Robo_')

    for horario, produtos in grupos_produto.items():
        df_grupo = df_final_export[df_final_export['PRODUTO'].isin(produtos)]
        if df_grupo.empty: continue
            
        nome_arquivo = f"{prefixo_robo}{horario}_{now.strftime('%H%M%S')}_{now.strftime('%d%m%Y')}.csv"
        caminho_saida = diretorio_alvo / nome_arquivo
        logger.info(f"Exportando {len(df_grupo)} registros do robô (consolidado) para: {caminho_saida}")
        df_grupo.to_csv(caminho_saida, sep='|', encoding='utf-8-sig', index=False, na_rep='')
        
    logger.info("Mailing Mestre do Robô (Consolidado) gerado com sucesso.")
