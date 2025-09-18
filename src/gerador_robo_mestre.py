import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
import numpy as np
import re

logger = logging.getLogger(__name__)

# def _formatar_valor_para_robo(series: pd.Series) -> pd.Series:
#     # HOMOLOGAÇÃO: Lógica anterior que foi substituída por uma abordagem mais simples,
#     # uma vez que o pipeline agora entrega os dados em um formato mais previsível.
#     """
#     Converte uma série numérica para string, usando vírgula como separador decimal,
#     preservando a precisão original.
#     """
#     # Converte para numérico primeiro para garantir que é um número.
#     # A lógica de conversão inicial já ocorreu no pipeline, aqui apenas garantimos o tipo.
#     numeric_series = pd.to_numeric(series, errors='coerce')
#     
#     # Converte para string e substitui o ponto pela vírgula.
#     return numeric_series.astype(str).str.replace('.', ',', regex=False).fillna('')

def _formatar_valor_para_robo(series: pd.Series) -> pd.Series:
    """
    NOVA LÓGICA: Recebe uma série de strings e garante que o ponto decimal seja
    substituído por vírgula. É a única transformação necessária, pois a precisão
    e o tipo de dado já foram tratados no pipeline.
    """
    return series.astype(str).str.replace('.', ',', regex=False).fillna('')


def gerar_arquivo_robo_mestre(df_robo_mestre: pd.DataFrame, config: ConfigParser, diretorio_alvo: Path):
    """
    Gera arquivos de mailing mestre particionados para o robô, aplicando
    enriquecimento de parcelas via pivotagem, mapeamento de layout e formatação robusta.
    """
    if df_robo_mestre.empty:
        logger.warning("DataFrame mestre do robô está vazio. Arquivos não serão gerados.")
        return

    logger.info("--- Iniciando Geração do Mailing Mestre do Robô ---")
    df_processado = df_robo_mestre.copy()

    logger.info("Iniciando enriquecimento de parcelas (pivotagem).")
    
    # A coluna 'dtvenc' vem como string, convertemos para data para poder ordenar.
    df_processado['dtvenc_dt'] = pd.to_datetime(df_processado['dtvenc'], errors='coerce', dayfirst=True)
    
    df_valid_dates = df_processado.dropna(subset=['dtvenc_dt']).copy()
    
    # CORREÇÃO CRÍTICA: O DataFrame já vem com a coluna 'CPF', não 'ncpf'.
    df_valid_dates = df_valid_dates.sort_values(by=['CPF', 'dtvenc_dt'], ascending=[True, True])
    df_valid_dates['rank'] = df_valid_dates.groupby('CPF').cumcount() + 1
    
    df_faturas = df_valid_dates[df_valid_dates['rank'].isin([1, 2, 3])].copy()

    df_pivot = pd.DataFrame()
    if not df_faturas.empty:
        df_pivot = df_faturas.pivot_table(
            index='CPF',
            columns='rank',
            values=['dtvenc_dt', 'liquido', 'codbarra'],
            aggfunc='first'
        )
        df_pivot.columns = [f'{val}_{rank}' for val, rank in df_pivot.columns]
        df_pivot.reset_index(inplace=True)

    # O df_robo_mestre já vem com CPFs únicos e colunas renomeadas.
    df_agregado = df_processado.copy()
    if not df_pivot.empty:
        df_agregado = pd.merge(df_agregado, df_pivot, on='CPF', how='left')

    colunas_finais = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtrado', 'dtPrimeiraParcelaAtrasada',
        'dtSegundaParcelaAtrasada', 'dtTerceiraParcelaAtrasada', 'valorDivida', 'valorMinimo',
        'valorParcelaMaisAntiga', 'codigoBarrasConta1', 'codigoBarrasConta2', 'codigoBarrasConta3',
        'CodigoPixConta1', 'CodigoPixConta2', 'CodigoPixConta3', 'valorConta1', 'valorConta2',
        'valorConta3', 'PerfilPagamento', 'Telefone1', 'telefone2', 'RESP_NEG'
    ]
    df_final = pd.DataFrame()

    logger.info("Mapeando colunas para o layout final do robô.")
    # Mapeamento direto, pois os nomes já foram ajustados no pipeline
    df_final['NOME_CLIENTE'] = df_agregado['NOME_CLIENTE']
    df_final['PRODUTO'] = df_agregado['PRODUTO']
    df_final['parcelasEmAtrado'] = df_agregado['parcelasEmAtrado']
    df_final['CPF'] = df_agregado['CPF']

    if 'dtvenc_dt_1' in df_agregado.columns:
        df_final['dtPrimeiraParcelaAtrasada'] = pd.to_datetime(df_agregado['dtvenc_dt_1'], errors='coerce').dt.strftime('%d/%m/%Y')
    if 'dtvenc_dt_2' in df_agregado.columns:
        df_final['dtSegundaParcelaAtrasada'] = pd.to_datetime(df_agregado['dtvenc_dt_2'], errors='coerce').dt.strftime('%d/%m/%Y')
    if 'dtvenc_dt_3' in df_agregado.columns:
        df_final['dtTerceiraParcelaAtrasada'] = pd.to_datetime(df_agregado['dtvenc_dt_3'], errors='coerce').dt.strftime('%d/%m/%Y')

    if 'codbarra_1' in df_agregado.columns:
        df_final['codigoBarrasConta1'] = df_agregado['codbarra_1']
    if 'codbarra_2' in df_agregado.columns:
        df_final['codigoBarrasConta2'] = df_agregado['codbarra_2']
    if 'codbarra_3' in df_agregado.columns:
        df_final['codigoBarrasConta3'] = df_agregado['codbarra_3']
    
    # CORREÇÃO: A coluna já se chama 'valorDivida' (D maiúsculo) vinda do pipeline.
    df_final['valorDivida'] = _formatar_valor_para_robo(df_agregado['valorDivida'])
    df_final['valorParcelaMaisAntiga'] = _formatar_valor_para_robo(df_agregado['liquido'])
    if 'liquido_1' in df_agregado.columns:
        df_final['valorConta1'] = _formatar_valor_para_robo(df_agregado['liquido_1'])
    if 'liquido_2' in df_agregado.columns:
        df_final['valorConta2'] = _formatar_valor_para_robo(df_agregado['liquido_2'])
    if 'liquido_3' in df_agregado.columns:
        df_final['valorConta3'] = _formatar_valor_para_robo(df_agregado['liquido_3'])

    df_final['Telefone1'] = df_agregado.get('TELEFONE_01', df_agregado.get('telefone_01'))
    df_final['telefone2'] = df_agregado.get('TELEFONE_02', df_agregado.get('telefone_02'))
    
    logger.debug("Preenchendo colunas fixas e vazias.")
    df_final['PerfilPagamento'] = 'VISTA'
    
    for col in colunas_finais:
        if col not in df_final.columns:
            df_final[col] = ''
    df_final = df_final[colunas_finais].fillna('')
    
    logger.info("Iniciando particionamento e exportação por grupo de produto.")
    grupos_produto = {
        "08HRS": ['EPB', 'EMR', 'ESS', 'ESE', 'ETO'],
        "09HRS": ['ERO', 'EMT', 'EMS'],
        "10HRS": ['EAC']
    }
    
    now = datetime.now()
    prefixo_robo = config.get('ROBO', 'output_file_prefix', fallback='Telecobranca_TOI_Robo_')
    
    for horario, produtos in grupos_produto.items():
        df_grupo = df_final[df_final['PRODUTO'].isin(produtos)]
        
        if df_grupo.empty:
            logger.warning(f"Nenhum registro encontrado para o grupo de produtos '{horario}'. Arquivo não será gerado.")
            continue

        nome_arquivo = f"{prefixo_robo}{horario}_{now.strftime('%H%M%S')}_{now.strftime('%d%m%Y')}.csv"
        caminho_saida = diretorio_alvo / nome_arquivo
        
        logger.info(f"Exportando {len(df_grupo)} registros do grupo '{horario}' para: {caminho_saida}")
        df_grupo.to_csv(
            caminho_saida,
            sep='|',
            encoding='utf-8-sig',
            index=False
        )

    logger.info("Mailing Mestre do Robô particionado e gerado com sucesso.")
