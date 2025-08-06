import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime
import re

logger = logging.getLogger(__name__)
tqdm.pandas(desc="Processando mailing")

def _clean_phone_number(phone_val):
    if pd.isna(phone_val):
        return None
    phone_str = str(phone_val).split('.')[0]
    cleaned = re.sub(r'\D', '', phone_str)
    return cleaned if cleaned else None

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def _limpar_cpf(cpf_series: pd.Series) -> pd.Series:
    if cpf_series.empty:
        return cpf_series
    return cpf_series.astype(str).str.replace(r'\D', '', regex=True)

def _remover_duplicatas(df: pd.DataFrame, chave_primaria: str) -> pd.DataFrame:
    if chave_primaria not in df.columns:
        logger.warning(f"Chave primária '{chave_primaria}' para deduplicação não encontrada. Pulando etapa.")
        return df
    
    tamanho_inicial = len(df)
    if df.duplicated(subset=[chave_primaria]).any():
        df_deduplicado = df.drop_duplicates(subset=[chave_primaria], keep='first')
        tamanho_final = len(df_deduplicado)
        removidos = tamanho_inicial - tamanho_final
        logger.info(f"Deduplicação: Removidos {removidos} registros duplicados com base na coluna '{chave_primaria}'.")
        return df_deduplicado
    else:
        logger.info("Deduplicação: Nenhum registro duplicado encontrado.")
        return df

def _remover_pagamentos(df_mailing: pd.DataFrame, df_pagamentos: pd.DataFrame) -> pd.DataFrame:
    if df_pagamentos.empty:
        logger.info("Nenhum dado de pagamento para processar. Pulando a remoção de pagos.")
        return df_mailing

    logger.info("Iniciando remoção de clientes com pagamentos registrados.")
    tamanho_inicial = len(df_mailing)

    chave_cols = ['empresa', 'ucv', 'ano', 'mes']
    for col in chave_cols:
        if col not in df_mailing.columns or col not in df_pagamentos.columns:
            logger.error(f"Coluna de chave '{col}' não encontrada em ambos os DataFrames. Abortando remoção de pagamentos.")
            return df_mailing
        
        df_mailing[col] = df_mailing[col].astype(str).str.strip()
        df_pagamentos[col] = df_pagamentos[col].astype(str).str.strip()

    df_mailing['chave_pagamento'] = df_mailing[chave_cols].agg(''.join, axis=1)
    df_pagamentos['chave_pagamento'] = df_pagamentos[chave_cols].agg(''.join, axis=1)
    
    df_pagamentos = df_pagamentos.drop_duplicates(subset=['chave_pagamento'])

    df_filtrado = df_mailing[~df_mailing['chave_pagamento'].isin(df_pagamentos['chave_pagamento'])]
    
    tamanho_final = len(df_filtrado)
    removidos = tamanho_inicial - tamanho_final
    logger.info(f"Remoção de Pagos: {removidos} registros removidos.")
    
    return df_filtrado.drop(columns=['chave_pagamento'])

def _adicionar_cliente_regulariza(df_mailing: pd.DataFrame, df_negociacao: pd.DataFrame) -> pd.DataFrame:
    if df_negociacao.empty:
        logger.warning("Arquivo de negociação não encontrado ou vazio. A coluna 'cliente_regulariza' será preenchida com 'NÃO'.")
        df_mailing['cliente_regulariza'] = 'NÃO'
        return df_mailing

    logger.info("Enriquecendo base com a flag 'cliente_regulariza'.")
    
    chave_mailing = 'uc'
    chave_negociacao = 'cdcdebito'
    
    if chave_mailing not in df_mailing.columns or chave_negociacao not in df_negociacao.columns:
        logger.error(f"Coluna de merge ('{chave_mailing}' ou '{chave_negociacao}') não encontrada. Pulando etapa.")
        df_mailing['cliente_regulariza'] = 'NÃO'
        return df_mailing
    
    df_mailing[chave_mailing] = df_mailing[chave_mailing].astype(str)
    df_negociacao[chave_negociacao] = df_negociacao[chave_negociacao].astype(str)

    df_negociacao['em_negociacao'] = 'SIM'
    
    df_merged = pd.merge(df_mailing, df_negociacao[[chave_negociacao, 'em_negociacao']].drop_duplicates(), left_on=chave_mailing, right_on=chave_negociacao, how='left')
    
    df_merged['cliente_regulariza'] = df_merged['em_negociacao'].fillna('NÃO')
    
    return df_merged.drop(columns=['em_negociacao', chave_negociacao])

def _enriquecer_telefones(df_mailing: pd.DataFrame, dataframes: dict) -> pd.DataFrame:
    logger.info("Iniciando enriquecimento de telefones.")
    
    if 'enriquecimento' not in dataframes or not isinstance(dataframes['enriquecimento'], dict):
        logger.warning("Dados de enriquecimento ('Pontuação.xlsx') não encontrados. Pulando etapa.")
        return df_mailing

    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    
    df_p100_raw = dataframes['enriquecimento'].get('100', pd.DataFrame())
    df_p50_raw = dataframes['enriquecimento'].get('50', pd.DataFrame())
    
    df_p100 = _standardize_columns(df_p100_raw)
    df_p50 = _standardize_columns(df_p50_raw)

    df_pontuacao = pd.concat([df_p100, df_p50], ignore_index=True)

    if df_pontuacao.empty or not all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        logger.warning(f"Abas de pontuação estão vazias ou não contêm as colunas necessárias: {colunas_pontuacao_necessarias}. Pulando.")
        return df_mailing

    df_pontuacao = df_pontuacao[colunas_pontuacao_necessarias].dropna(subset=['documento', 'telefone'])
    
    df_pontuacao['documento'] = df_pontuacao['documento'].astype(str).str.lower().str.strip()
    df_pontuacao['telefone'] = df_pontuacao['telefone'].apply(_clean_phone_number)
    df_pontuacao.dropna(subset=['documento', 'telefone'], inplace=True)
    
    logger.info(f"Encontrados {len(df_pontuacao)} telefones com pontuação em {df_pontuacao['documento'].nunique()} documentos únicos.")

    df_pontuacao = df_pontuacao.sort_values(by=['documento', 'pontuacao'], ascending=[True, False])
    
    telefones_agrupados = df_pontuacao.groupby('documento')['telefone'].apply(list).reset_index()
    telefones_agrupados.rename(columns={'telefone': 'telefones_enriquecidos'}, inplace=True)

    # 1
    if 'ndoc' not in df_mailing.columns:
        logger.error("Coluna 'ndoc' não encontrada no mailing. Não é possível enriquecer telefones.")
        return df_mailing
    
    df_mailing['chave_enriquecimento'] = df_mailing['ndoc'].astype(str).str.lower().str.strip()
    df_final = pd.merge(df_mailing, telefones_agrupados, left_on='chave_enriquecimento', right_on='documento', how='left')
    
    matches = df_final['telefones_enriquecidos'].notna().sum()
    logger.info(f"Junção com base de pontuação (usando NDOC) resultou em {matches} clientes com correspondência de telefone.")

    def popular_telefones(row):
        telefones_enriquecidos = row['telefones_enriquecidos'] if isinstance(row['telefones_enriquecidos'], list) else []
        
        telefones_mailing = []
        colunas_tel_mailing = ['ind_telefone_1_valido', 'ind_telefone_2_valido', 'fone_consumidor']
        for col_name in colunas_tel_mailing:
            if col_name in row and pd.notna(row[col_name]):
                cleaned_phone = _clean_phone_number(row[col_name])
                if cleaned_phone:
                    telefones_mailing.append(cleaned_phone)

        todos_telefones = list(dict.fromkeys(telefones_enriquecidos + telefones_mailing))
        
        return pd.Series([todos_telefones[i] if i < len(todos_telefones) else np.nan for i in range(4)])

    df_final[['telefone_01', 'telefone_02', 'telefone_03', 'telefone_04']] = df_final.progress_apply(popular_telefones, axis=1)
    
    telefones_preenchidos_4 = (df_final['telefone_04'].notna()).sum()
    logger.info(f"Total de {telefones_preenchidos_4} clientes tiveram a coluna TELEFONE_04 preenchida.")

    return df_final.drop(columns=['documento', 'telefones_enriquecidos', 'chave_enriquecimento'])

def _calcular_colunas_agregadas(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculando colunas agregadas.")
    
    if 'ncpf' not in df.columns or 'liquido' not in df.columns:
        logger.error("Colunas 'ncpf' ou 'liquido' não encontradas. Não é possível calcular valor da dívida.")
        df['valordivida'] = 0
    else:
        df['liquido'] = pd.to_numeric(df['liquido'], errors='coerce').fillna(0)
        df['valordivida'] = df.groupby('ncpf')['liquido'].transform('sum')

    if 'ncpf' not in df.columns or 'ucv' not in df.columns:
        logger.error("Colunas 'ncpf' ou 'ucv' não encontradas. Não é possível calcular quantidade de UCs.")
        df['quantidade_uc_por_cpf'] = 0
    else:
        df['quantidade_uc_por_cpf'] = df.groupby('ncpf')['ucv'].transform('nunique')

    return df

def _renomear_e_formatar_final(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Renomeando e formatando colunas para o layout final.")
    
    mapa_renomeacao_final = {
        'nomecad': 'NOME_CLIENTE',
        'empresa': 'PRODUTO',
        'ncpf': 'CPF',
        'mes': 'parcelasEmAtraso',
        'endereco': 'LOCALIDADE', # 2
        'valordivida': 'valorDivida',
        'quantidade_uc_por_cpf': 'Quantidade_UC_por_CPF',
        'cliente_regulariza': 'Cliente_Regulariza',
        'telefone_01': 'TELEFONE_01',
        'telefone_02': 'TELEFONE_02',
        'telefone_03': 'TELEFONE_03',
        'telefone_04': 'TELEFONE_04',
    }
    
    df.rename(columns=mapa_renomeacao_final, inplace=True)
    return df

def _reordenar_layout_final(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Reordenando colunas para o layout final.")
    
    colunas_principais = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtraso', 'valorDivida', 
        'Quantidade_UC_por_CPF', 'LOCALIDADE', 'Cliente_Regulariza', 
        'TELEFONE_01', 'TELEFONE_02', 'TELEFONE_03', 'TELEFONE_04'
    ]
    
    for col in colunas_principais:
        if col not in df.columns:
            df[col] = np.nan
            logger.warning(f"Coluna principal '{col}' não encontrada. Adicionada como vazia.")
            
    colunas_existentes = [col for col in colunas_principais if col in df.columns]
    outras_colunas = [col for col in df.columns if col not in colunas_existentes]
    
    ordem_final = colunas_existentes + outras_colunas
            
    return df[ordem_final]

def processar_dados(dataframes: dict[str, pd.DataFrame], config: ConfigParser) -> pd.DataFrame:
    logger.info(">>> INICIANDO PIPELINE DE PROCESSAMENTO DE DADOS <<<")
    
    if 'mailing' not in dataframes or dataframes['mailing'].empty:
        logger.critical("DataFrame de 'mailing' não encontrado ou vazio. Abortando o processamento.")
        return pd.DataFrame()

    df_processado = dataframes['mailing'].copy()
    
    df_processado = _standardize_columns(df_processado)
    for key in ['pagamentos', 'negociacao']:
        if key in dataframes and not dataframes[key].empty:
            dataframes[key] = _standardize_columns(dataframes[key])
    
    df_processado = _remover_duplicatas(df_processado, 'ncpf')
    df_processado = _remover_pagamentos(df_processado, dataframes.get('pagamentos', pd.DataFrame()))
    
    df_processado = _adicionar_cliente_regulariza(df_processado, dataframes.get('negociacao', pd.DataFrame()))
    df_processado = _enriquecer_telefones(df_processado, dataframes)
    
    df_processado = _calcular_colunas_agregadas(df_processado)
    df_processado = _renomear_e_formatar_final(df_processado)
    df_final = _reordenar_layout_final(df_processado)

    df_final['Data_de_Importacao'] = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(">>> PIPELINE DE PROCESSAMENTO DE DADOS CONCLUÍDO <<<")
    return df_final

