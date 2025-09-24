# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime
import re
from typing import Tuple, Dict

logger = logging.getLogger(__name__)
tqdm.pandas(desc="Processando mailing")

# --- FUNCOES AUXILIARES ---
def _clean_phone_number(phone_val):
    if pd.isna(phone_val): return None
    phone_str = str(phone_val).split('.')[0]
    cleaned = re.sub(r'\D', '', phone_str)
    return cleaned if cleaned else None

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None and not df.empty:
        df.columns = [str(col).strip().lower() for col in df.columns]
    return df

def _safe_to_float(series: pd.Series) -> pd.Series:
    series_str = series.astype(str).str.replace(',', '.', regex=False)
    return pd.to_numeric(series_str, errors='coerce')

# --- FUNCOES DE PROCESSAMENTO E LIMPEZA ---

def _tratar_datas(df: pd.DataFrame) -> tuple:
    colunas_data = ['dtvenc', 'dtreav', 'dtprot', 'dt_deslig', 'dtapr', 'data_encer_cont', 'min_datavcm', 'dt_aplicação']
    for coluna in colunas_data:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
    return df, "Tratamento de colunas de data concluído."

def _tratar_colunas_rebeldes(df: pd.DataFrame) -> tuple:
    financial_cols = ['liquido', 'total_toi', 'valor']
    for col in financial_cols:
        if col in df.columns:
            df[col] = _safe_to_float(df[col])
    if 'empresa' in df.columns:
        df['empresa'] = df['empresa'].astype(str).str.replace('\ufeff', '', regex=False).str.strip()
    if 'ndoc' in df.columns:
        df['ndoc'] = df['ndoc'].astype(str).str.replace(r'\.0$', '', regex=True)
    return df, "Tratamento inicial de colunas de valores e texto concluído."

def _remover_clientes_proibidos(df_mailing: pd.DataFrame, df_bloqueio_input: pd.DataFrame | None, config: ConfigParser) -> tuple:
    if df_bloqueio_input is None or df_bloqueio_input.empty:
        return df_mailing, "Remoção por Tabulação: Arquivo de regras não encontrado ou vazio. Etapa pulada."
    
    key_bloqueio = config.get('SOURCE_COLUMNS', 'id_cliente_tabulacao').lower()
    key_mailing = config.get('SOURCE_COLUMNS', 'cpf').lower()
    status_col = config.get('SOURCE_COLUMNS', 'status_tabulacao').lower()
    
    status_criticos_str = config.get('SCHEMA_TABULACOES', 'status_criticos_para_remocao', fallback='')
    status_criticos = [s.strip().lower() for s in status_criticos_str.split('\n') if s.strip()]
    limiar = config.getint('SCHEMA_TABULACOES', 'limiar_remocao_status_criticos', fallback=3)

    if not status_criticos: return df_mailing, "Remoção por Tabulação: Nenhum status crítico definido. Etapa pulada."
    if key_bloqueio not in df_bloqueio_input.columns or status_col not in df_bloqueio_input.columns or key_mailing not in df_mailing.columns:
        return df_mailing, f"AVISO: Colunas chave para remoção ('{key_bloqueio}', '{status_col}', '{key_mailing}') não encontradas. Etapa pulada."

    tamanho_inicial = len(df_mailing)
    df_bloqueio_input[key_bloqueio] = df_bloqueio_input[key_bloqueio].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_bloqueio_critico = df_bloqueio_input[df_bloqueio_input[status_col].astype(str).str.strip().str.lower().isin(status_criticos)]
    if df_bloqueio_critico.empty: return df_mailing, "Remoção por Tabulação: Nenhum registro com status crítico encontrado."
    
    contagem_status = df_bloqueio_critico.groupby(key_bloqueio).size()
    ids_para_remover = set(contagem_status[contagem_status >= limiar].index)
    if not ids_para_remover: return df_mailing, f"Remoção por Tabulação: Nenhum cliente atingiu o limiar de {limiar}."

    mailing_keys_cleaned = df_mailing[key_mailing].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_filtrado = df_mailing[~mailing_keys_cleaned.isin(ids_para_remover)]
    removidos = tamanho_inicial - len(df_filtrado)
    return df_filtrado, f"Remoção por Tabulação (Regra de Limiar): {removidos} registros removidos."

def _remover_duplicatas_inteligentemente(df: pd.DataFrame, config: ConfigParser) -> tuple:
    chave_primaria = config.get('SOURCE_COLUMNS', 'cpf').lower()
    if chave_primaria not in df.columns: return df, f"AVISO: Chave primária '{chave_primaria}' não encontrada para deduplicação."
    
    tamanho_inicial = len(df)
    if not df.duplicated(subset=[chave_primaria]).any(): return df, "Deduplicação: Nenhum registro duplicado encontrado."
    
    df_copy = df.copy()
    if 'nomecad' in df_copy.columns:
        df_copy['has_name'] = df_copy['nomecad'].notna()
        df_sorted = df_copy.sort_values(by=[chave_primaria, 'has_name'], ascending=[True, False])
        df_deduplicado = df_sorted.drop_duplicates(subset=[chave_primaria], keep='first')
        df_final = df_deduplicado.drop(columns=['has_name'])
    else:
        df_final = df.drop_duplicates(subset=[chave_primaria], keep='first')
    
    removidos = tamanho_inicial - len(df_final)
    return df_final, f"Deduplicação por '{chave_primaria}': Removidos {removidos} registros."

def _calcular_colunas_agregadas(df: pd.DataFrame, config: ConfigParser) -> tuple:
    col_cpf = config.get('SOURCE_COLUMNS', 'cpf').lower()
    col_valor = config.get('SOURCE_COLUMNS', 'valor_divida').lower()
    
    if col_cpf in df.columns and col_valor in df.columns:
        df['valorDivida'] = df.groupby(col_cpf)[col_valor].transform('sum')
    else: df['valorDivida'] = 0.0

    if col_cpf in df.columns and 'ucv' in df.columns:
        df['ucv'] = df['ucv'].astype(str)
        uc_por_cpf = df.groupby(col_cpf)['ucv'].apply(lambda x: ', '.join(x.unique()))
        df['Ucs_do_CPF'] = df[col_cpf].map(uc_por_cpf)
        df['Quantidade_UC_por_CPF'] = df[col_cpf].map(uc_por_cpf.apply(lambda x: len(x.split(', '))))
    else:
        df['Quantidade_UC_por_CPF'], df['Ucs_do_CPF'] = 0, ''
        
    return df, "Colunas agregadas (valorDivida, etc.) calculadas."

# 1
def _enriquecer_telefones(df_mailing: pd.DataFrame, dataframes: dict) -> tuple:
    """
    # --- LÓGICA ATUAL (PRESERVADA PARA HOMOLOGAÇÃO) ---
    for i in range(1, 5): df_mailing[f'telefone_0{i}'] = np.nan
    if 'enriquecimento' not in dataframes or not isinstance(dataframes['enriquecimento'], dict):
        return df_mailing, "Enriquecimento: Dados não encontrados. Etapa pulada."
    df_enriquecimento_dict = dataframes.get('enriquecimento', {})
    df_pontuacao = pd.concat(df_enriquecimento_dict.values(), ignore_index=True) if df_enriquecimento_dict else pd.DataFrame()
    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    if df_pontuacao.empty or not all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        return df_mailing, "Enriquecimento: Abas de pontuação válidas não encontradas. Etapa pulada."
    df_pontuacao = df_pontuacao[colunas_pontuacao_necessarias].dropna(subset=['documento', 'telefone'])
    df_pontuacao['join_key'] = df_pontuacao['documento'].astype(str).str.lower().str.strip()
    df_pontuacao['telefone'] = df_pontuacao['telefone'].apply(_clean_phone_number)
    df_pontuacao.dropna(subset=['join_key', 'telefone'], inplace=True)
    df_pontuacao = df_pontuacao.sort_values(by=['join_key', 'pontuacao'], ascending=[True, False])
    telefones_agrupados = df_pontuacao.groupby('join_key')['telefone'].apply(list).reset_index()
    telefones_agrupados.rename(columns={'telefone': 'telefones_enriquecidos'}, inplace=True)
    if 'ndoc' not in df_mailing.columns:
        return df_mailing, "Enriquecimento: ERRO - Coluna 'ndoc' não encontrada."
    df_mailing['join_key'] = df_mailing['ndoc'].astype(str).str.lower().str.strip()
    df_final = pd.merge(df_mailing, telefones_agrupados, on='join_key', how='left')
    matches = df_final['telefones_enriquecidos'].notna().sum()
    def popular_telefones(row):
        telefones = list(dict.fromkeys(
            (row['telefones_enriquecidos'] if isinstance(row['telefones_enriquecidos'], list) else []) +
            [_clean_phone_number(p) for p in [row.get('ind_telefone_1_valido'), row.get('ind_telefone_2_valido'), row.get('fone_consumidor')] if pd.notna(p) and p]
        ))
        for i in range(4): row[f'telefone_0{i+1}'] = telefones[i] if i < len(telefones) else np.nan
        return row
    df_final = df_final.progress_apply(popular_telefones, axis=1)
    df_final = df_final.drop(columns=['join_key', 'telefones_enriquecidos'], errors='ignore')
    return df_final, f"Enriquecimento de Telefones: {matches} clientes tiveram telefones encontrados."
    """
    # --- NOVA LÓGICA (RESTAURADA DA VERSÃO ESTÁVEL) ---
    logger.info("Iniciando enriquecimento de telefones.")
    
    for i in range(1, 5):
        df_mailing[f'telefone_0{i}'] = np.nan

    if 'enriquecimento' not in dataframes or not isinstance(dataframes['enriquecimento'], dict) or not dataframes['enriquecimento']:
        msg = "AVISO: Dados de enriquecimento ('Pontuação.xlsx') não encontrados ou vazios. Etapa pulada, telefones serão populados apenas com dados do mailing."
        logger.warning(msg)
        df_pontuacao = pd.DataFrame()
    else:
        df_enriquecimento_dict = dataframes.get('enriquecimento', {})
        df_pontuacao = pd.concat(df_enriquecimento_dict.values(), ignore_index=True)

    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    if not df_pontuacao.empty and all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        df_pontuacao = df_pontuacao[colunas_pontuacao_necessarias].dropna(subset=['documento', 'telefone'])
        df_pontuacao['join_key'] = df_pontuacao['documento'].astype(str).str.lower().str.strip()
        df_pontuacao['telefone'] = df_pontuacao['telefone'].apply(_clean_phone_number)
        df_pontuacao.dropna(subset=['join_key', 'telefone'], inplace=True)
        df_pontuacao = df_pontuacao.sort_values(by=['join_key', 'pontuacao'], ascending=[True, False])
        
        telefones_agrupados = df_pontuacao.groupby('join_key')['telefone'].apply(list).reset_index()
        telefones_agrupados.rename(columns={'telefone': 'telefones_enriquecidos'}, inplace=True)
        
        if 'ndoc' not in df_mailing.columns:
            msg = "ERRO: Coluna 'ndoc' não encontrada no mailing. Enriquecimento de telefones abortado."
            logger.error(msg)
            return df_mailing, msg
            
        df_mailing['join_key'] = df_mailing['ndoc'].astype(str).str.lower().str.strip()
        df_final = pd.merge(df_mailing, telefones_agrupados, on='join_key', how='left')
        matches = df_final['telefones_enriquecidos'].notna().sum()
        msg = f"Enriquecimento de Telefones: {matches} clientes tiveram telefones encontrados na base de pontuação."
    else:
        df_final = df_mailing.copy()
        df_final['telefones_enriquecidos'] = np.nan
        msg = "Enriquecimento de Telefones: Base de pontuação inválida ou ausente. Procedendo sem ela."

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
        for i in range(4):
            row[f'telefone_0{i+1}'] = todos_telefones[i] if i < len(todos_telefones) else np.nan
        return row

    df_final = df_final.progress_apply(popular_telefones, axis=1)
    df_final = df_final.drop(columns=['join_key', 'telefones_enriquecidos'], errors='ignore')
    
    logger.info(msg)
    return df_final, msg

def _criar_cliente_regulariza_from_mailing(df: pd.DataFrame) -> tuple:
    if 'venc_maior_1ano' in df.columns:
        df['Cliente_Regulariza'] = np.where(df['venc_maior_1ano'].notna() & (df['venc_maior_1ano'].astype(str).str.strip().str.upper() != 'N'), 'SIM', 'NÃO')
    else:
        df['Cliente_Regulariza'] = 'NÃO'
    return df, "'Cliente_Regulariza' criada."

def _remover_por_status_de_bloqueio(df: pd.DataFrame, config: ConfigParser) -> tuple:
    coluna_filtro = config.get('SOURCE_COLUMNS', 'bloqueio').lower()
    if coluna_filtro not in df.columns:
        return df, f"Filtro de Bloqueio: Coluna '{coluna_filtro}' não encontrada. Etapa pulada."
    status_para_remover_str = config.get('SCHEMA_MAILING', 'status_de_bloqueio_para_remover', fallback='')
    status_para_remover = [s.strip().lower() for s in status_para_remover_str.split('\n') if s.strip()]
    if not status_para_remover:
        return df, "Filtro de Bloqueio: Nenhum status de bloqueio para remover definido. Etapa pulada."
    tamanho_inicial = len(df)
    df_filtrado = df[~df[coluna_filtro].astype(str).str.strip().str.lower().isin(status_para_remover)]
    removidos = tamanho_inicial - len(df_filtrado)
    return df_filtrado, f"Filtro de Bloqueio ('{coluna_filtro}'): {removidos} registros removidos."

def _aplicar_ajustes_finais(df: pd.DataFrame, config: ConfigParser) -> tuple:
    col_cpf_original = config.get('SOURCE_COLUMNS', 'cpf').lower()
    mapa_renomeacao = {
        'nomecad': 'NOME_CLIENTE', 'empresa': 'PRODUTO', col_cpf_original: 'CPF',
        'totfat': 'parcelasEmAtrado', 'loc': 'LOCALIDADE', 'valordivida': 'valorDivida',
        'telefone_01': 'TELEFONE_01', 'telefone_02': 'TELEFONE_02',
        'telefone_03': 'TELEFONE_03', 'telefone_04': 'TELEFONE_04'
    }
    df.rename(columns=mapa_renomeacao, inplace=True)
    colunas_principais = ['NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtrado', 'Quantidade_UC_por_CPF', 'Ucs_do_CPF', 'LOCALIDADE', 'valorDivida', 'Cliente_Regulariza', 'TELEFONE_01', 'TELEFONE_02', 'TELEFONE_03', 'TELEFONE_04', 'Data_de_Importacao']
    for col in colunas_principais:
        if col not in df.columns: df[col] = ''
    outras_colunas = [col for col in df.columns if col not in colunas_principais]
    df = df[colunas_principais + outras_colunas]
    return df, "Ajustes finais de layout aplicados."

def _aplicar_ordenacao_final(df: pd.DataFrame, config: ConfigParser) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    priority_order = [p.strip().upper() for p in config.get('PRIORITIES', 'order').split('\n') if p.strip()]
    df['priority_level'] = len(priority_order)
    
    colunas_prioridade = [
        'faixa', 
        config.get('SOURCE_COLUMNS', 'status_instalacao', fallback='sit').lower(),
        config.get('SOURCE_COLUMNS', 'iu12m', fallback='iu12m').lower()
    ]

    for i, status in enumerate(priority_order):
        condicao_final = pd.Series([False] * len(df), index=df.index)
        for coluna in colunas_prioridade:
            if coluna in df.columns:
                condicao_parcial = (df[coluna].astype(str).str.upper() == status)
                condicao_final = condicao_final | condicao_parcial
        
        if condicao_final.any():
            df.loc[condicao_final, 'priority_level'] = i
            
    df_sorted = df.sort_values(by=['priority_level', 'valorDivida'], ascending=[True, False])
    return df_sorted.drop(columns=['priority_level'])

def _aplicar_filtros_estrategicos(df: pd.DataFrame, config: ConfigParser) -> Tuple[pd.DataFrame, pd.DataFrame]:
    secao_config = 'SEGMENTACAO'
    corte_humano = config.getfloat(secao_config, 'corte_humano_maior_igual')
    col_divida = config.get(secao_config, 'coluna_divida_filtro', fallback='valorDivida')
    if df.empty or col_divida not in df.columns: return pd.DataFrame(), pd.DataFrame()
    df_humano = df[df[col_divida] >= corte_humano].copy()
    df_robo = df[df[col_divida] < corte_humano].copy()
    logger.info(f"Segmentação final: {len(df_humano)} para humano, {len(df_robo)} para robô.")
    return df_humano, df_robo

# --- FUNCAO ORQUESTRADORA (ARQUITETURA UNIFICADA E ROBUSTA) ---
def processar_dados(dataframes: Dict, config: ConfigParser) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_mailing = dataframes.get('mailing', pd.DataFrame())
    if df_mailing.empty:
        logger.warning("Mailing de entrada vazio. Nenhum dado para processar.")
        return pd.DataFrame(), pd.DataFrame()

    logger.info("="*25 + " INICIANDO PROCESSAMENTO DE FLUXO ÚNICO " + "="*25)
    logger.info(f"Registros iniciais no mailing consolidado: {len(df_mailing)}")
    
    df_processado = df_mailing.copy()
    
    df_limpo, msg = _tratar_datas(df_processado); logger.info(msg)
    df_limpo, msg = _tratar_colunas_rebeldes(df_limpo); logger.info(msg)
    df_limpo, msg = _remover_clientes_proibidos(df_limpo, dataframes.get('regras_disposicao'), config); logger.info(msg)
    df_limpo, msg = _remover_duplicatas_inteligentemente(df_limpo, config); logger.info(msg)
    df_limpo, msg = _calcular_colunas_agregadas(df_limpo, config); logger.info(msg)
    
    # 2
    # --- LÓGICA ANTIGA (PRESERVADA PARA HOMOLOGAÇÃO) ---
    # df_limpo, msg = _enriquecer_telefones(df_limpo, dataframes, config); logger.info(msg)
    # --- NOVA LÓGICA (CHAMADA CORRIGIDA) ---
    df_limpo, msg = _enriquecer_telefones(df_limpo, dataframes); logger.info(msg)
    
    df_limpo, msg = _criar_cliente_regulariza_from_mailing(df_limpo); logger.info(msg)
    df_limpo, msg = _remover_por_status_de_bloqueio(df_limpo, config); logger.info(msg)
    df_limpo['Data_de_Importacao'] = datetime.now().strftime('%d/%m/%Y')
    df_limpo, msg = _aplicar_ajustes_finais(df_limpo, config); logger.info(msg)
    
    logger.info(f"Fim da limpeza: {len(df_limpo)} registros.")

    df_ordenado = _aplicar_ordenacao_final(df_limpo, config)
    logger.info("Ordenação estratégica final aplicada.")
    
    df_humano, df_robo = _aplicar_filtros_estrategicos(df_ordenado, config)

    return df_humano, df_robo
