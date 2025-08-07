import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime
import re

logger = logging.getLogger(__name__)
tqdm.pandas(desc="Processando mailing")

# --- FUNÇÕES UTILITÁRIAS (Sem alterações) ---

def _clean_phone_number(phone_val):
    if pd.isna(phone_val):
        return None
    phone_str = str(phone_val).split('.')[0]
    cleaned = re.sub(r'\D', '', phone_str)
    return cleaned if cleaned else None

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df

def _limpar_cpf(cpf_series: pd.Series) -> pd.Series:
    if cpf_series.empty:
        return cpf_series
    return cpf_series.astype(str).str.replace(r'\D', '', regex=True)

# --- FUNÇÕES DE PROCESSAMENTO (Com ajustes) ---

def _remover_duplicatas(df: pd.DataFrame, chave_primaria: str) -> tuple[pd.DataFrame, str]:
    if chave_primaria not in df.columns:
        msg = f"AVISO: Chave primária '{chave_primaria}' para deduplicação não encontrada. Etapa pulada."
        logger.warning(msg)
        return df, msg
    
    tamanho_inicial = len(df)
    if df.duplicated(subset=[chave_primaria]).any():
        df_deduplicado = df.drop_duplicates(subset=[chave_primaria], keep='first')
        tamanho_final = len(df_deduplicado)
        removidos = tamanho_inicial - tamanho_final
        msg = f"Deduplicação por '{chave_primaria}': Removidos {removidos} registros. Restantes: {tamanho_final}."
        logger.info(msg)
        return df_deduplicado, msg
    else:
        msg = "Deduplicação: Nenhum registro duplicado encontrado."
        logger.info(msg)
        return df, msg

def _remover_pagamentos(df_mailing: pd.DataFrame, df_pagamentos: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    if df_pagamentos.empty:
        msg = "Remoção de Pagos: Nenhum arquivo de pagamento encontrado. Etapa pulada."
        logger.info(msg)
        return df_mailing, msg

    logger.info("Iniciando remoção de clientes com pagamentos registrados.")
    tamanho_inicial = len(df_mailing)

    chave_cols = ['empresa', 'ucv', 'ano', 'mes']
    for col in chave_cols:
        if col not in df_mailing.columns or col not in df_pagamentos.columns:
            msg = f"ERRO: Coluna de chave '{col}' não encontrada para remoção de pagamentos. Etapa abortada."
            logger.error(msg)
            return df_mailing, msg
        
        df_mailing[col] = df_mailing[col].astype(str).str.strip()
        df_pagamentos[col] = df_pagamentos[col].astype(str).str.strip()

    df_mailing['chave_pagamento'] = df_mailing[chave_cols].agg(''.join, axis=1)
    df_pagamentos['chave_pagamento'] = df_pagamentos[chave_cols].agg(''.join, axis=1)
    
    df_pagamentos = df_pagamentos.drop_duplicates(subset=['chave_pagamento'])
    df_filtrado = df_mailing[~df_mailing['chave_pagamento'].isin(df_pagamentos['chave_pagamento'])]
    
    tamanho_final = len(df_filtrado)
    removidos = tamanho_inicial - tamanho_final
    msg = f"Remoção de Pagos: {removidos} registros removidos. Restantes: {tamanho_final}."
    logger.info(msg)
    
    return df_filtrado.drop(columns=['chave_pagamento']), msg

def _enriquecer_telefones(df_mailing: pd.DataFrame, dataframes: dict) -> tuple[pd.DataFrame, str]:
    logger.info("Iniciando enriquecimento de telefones.")
    
    for i in range(1, 5):
        df_mailing[f'telefone_0{i}'] = np.nan

    if 'enriquecimento' not in dataframes or not isinstance(dataframes['enriquecimento'], dict):
        msg = "AVISO: Dados de enriquecimento ('Pontuação.xlsx') não encontrados. Etapa pulada."
        logger.warning(msg)
        return df_mailing, msg

    df_p100_raw = dataframes['enriquecimento'].get('100', pd.DataFrame())
    df_p50_raw = dataframes['enriquecimento'].get('50', pd.DataFrame())
    df_p100 = _standardize_columns(df_p100_raw) if not df_p100_raw.empty else df_p100_raw
    df_p50 = _standardize_columns(df_p50_raw) if not df_p50_raw.empty else df_p50_raw
    df_pontuacao = pd.concat([df_p100, df_p50], ignore_index=True)
    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    if df_pontuacao.empty or not all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        msg = "AVISO: Abas de pontuação vazias ou sem colunas necessárias. Etapa pulada."
        logger.warning(msg)
        return df_mailing, msg
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
    
    msg = f"Enriquecimento de Telefones: {matches} clientes tiveram telefones encontrados na base de pontuação."
    logger.info(msg)
    return df_final, msg

def _calcular_colunas_agregadas(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculando colunas agregadas (valorDivida, Quantidade_UC_por_CPF).")
    
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

def _aplicar_ajustes_finais(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    logger.info("Aplicando ajustes finais de layout, mapeamento e formatação.")
    report_msgs = []
    df_ajustado = df.copy()

    # AJUSTE 1: Lógica de Coalesce para 'Cliente_Regulariza'
    fonte_regulariza = None
    if 'venc_maior_1ano' in df_ajustado.columns:
        fonte_regulariza = 'venc_maior_1ano'
    elif 'nc' in df_ajustado.columns:
        fonte_regulariza = 'nc'
    
    if fonte_regulariza:
        sim_count = df_ajustado[fonte_regulariza].apply(lambda x: pd.notna(x) and str(x).strip().upper() != 'N').sum()
        msg = f"Mapeamento 'Cliente_Regulariza': {sim_count} clientes marcados como 'SIM' a partir da coluna '{fonte_regulariza}'."
        logger.info(msg)
        report_msgs.append(msg)
        df_ajustado['Cliente_Regulariza'] = df_ajustado[fonte_regulariza].apply(lambda x: 'SIM' if pd.notna(x) and str(x).strip().upper() != 'N' else 'NÃO')
    else:
        msg = "AVISO: Nenhuma coluna de origem ('venc_maior_1ano' ou 'nc') encontrada. 'Cliente_Regulariza' será preenchida com 'NÃO'."
        logger.warning(msg)
        report_msgs.append(msg)
        df_ajustado['Cliente_Regulariza'] = 'NÃO'

    # AJUSTE 2: Mapear 'loc' para 'LOCALIDADE' e outras colunas
    mapa_renomeacao = {
        'nomecad': 'NOME_CLIENTE', 'empresa': 'PRODUTO', 'ncpf': 'CPF',
        'totfat': 'parcelasEmAtraso', 'valordivida': 'valorDivida',
        'quantidade_uc_por_cpf': 'Quantidade_UC_por_CPF', 'ucs_do_cpf': 'Ucs_do_CPF',
        'loc': 'LOCALIDADE', # Mapeamento direto de 'loc'
        'quantidades_de_acionamentos': 'Quantidades_de_Acionamentos',
        'telefone_01': 'TELEFONE_01', 'telefone_02': 'TELEFONE_02',
        'telefone_03': 'TELEFONE_03', 'telefone_04': 'TELEFONE_04',
    }
    df_ajustado.rename(columns=mapa_renomeacao, inplace=True)
    report_msgs.append("Renomeação de colunas (incluindo 'loc' -> 'LOCALIDADE') aplicada.")

    # AJUSTE 3: Reordenar colunas
    colunas_principais = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtraso', 'Quantidade_UC_por_CPF',
        'Ucs_do_CPF', 'LOCALIDADE', 'valorDivida', 'Cliente_Regulariza', 'TELEFONE_01',
        'TELEFONE_02', 'TELEFONE_03', 'TELEFONE_04', 'Quantidades_de_Acionamentos',
        'Data_de_Importacao'
    ]
    
    for col in colunas_principais:
        if col not in df_ajustado.columns:
            df_ajustado[col] = np.nan
    
    colunas_existentes = df_ajustado.columns.tolist()
    outras_colunas = [col for col in colunas_existentes if col not in colunas_principais]
    ordem_final = colunas_principais + outras_colunas
    df_ajustado = df_ajustado[ordem_final]
    report_msgs.append(f"Reordenação de colunas aplicada. Total de {len(ordem_final)} colunas no arquivo final.")

    # AJUSTE 4: Formatar números
    logger.info("Formatando colunas numéricas para remover '.0' de inteiros.")
    for col in df_ajustado.columns:
        if pd.api.types.is_numeric_dtype(df_ajustado[col]):
            df_ajustado[col] = df_ajustado[col].apply(
                lambda x: int(x) if pd.notna(x) and x == int(x) else x
            )
            df_ajustado[col] = df_ajustado[col].astype(str).replace('nan', '')
    report_msgs.append("Formatação de números para remover '.0' concluída.")

    return df_ajustado, report_msgs

# --- FUNÇÃO ORQUESTRADORA DO PIPELINE ---

def processar_dados(dataframes: dict[str, pd.DataFrame], config: ConfigParser) -> pd.DataFrame:
    relatorio_final = ["\n" + "="*25 + " RELATÓRIO FINAL DA ALQUIMIA " + "="*25]
    
    if 'mailing' not in dataframes or dataframes['mailing'].empty:
        logger.critical("DataFrame de 'mailing' não encontrado ou vazio. Abortando.")
        return pd.DataFrame()

    df_processado = dataframes['mailing'].copy()
    relatorio_final.append(f"1. Registros Iniciais no Mailing: {len(df_processado)}")
    
    df_processado = _standardize_columns(df_processado)
    
    for key in ['pagamentos', 'negociacao']:
        if key in dataframes and not dataframes[key].empty:
            dataframes[key] = _standardize_columns(dataframes[key])
    
    df_processado, msg_duplicatas = _remover_duplicatas(df_processado, 'ncpf')
    relatorio_final.append(f"2. {msg_duplicatas}")
    
    df_processado, msg_pagamentos = _remover_pagamentos(df_processado, dataframes.get('pagamentos', pd.DataFrame()))
    relatorio_final.append(f"3. {msg_pagamentos}")
    
    df_processado = _calcular_colunas_agregadas(df_processado)
    relatorio_final.append("4. Cálculo de Colunas Agregadas: Concluído.")
    
    df_processado, msg_enriquecimento = _enriquecer_telefones(df_processado, dataframes)
    relatorio_final.append(f"5. {msg_enriquecimento}")
    
    df_processado['Data_de_Importacao'] = datetime.now().strftime('%d/%m/%Y')

    df_final, msgs_ajustes = _aplicar_ajustes_finais(df_processado)
    relatorio_final.append("6. Ajustes Finais de Layout:")
    for msg in msgs_ajustes:
        relatorio_final.append(f"   - {msg}")
        
    relatorio_final.append(f"7. Registros Finais para Exportação: {len(df_final)}")
    relatorio_final.append("="*75 + "\n")
    
    for linha in relatorio_final:
        logger.info(linha)
    
    return df_final

