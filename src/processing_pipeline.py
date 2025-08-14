import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime
import re

logger = logging.getLogger(__name__)
tqdm.pandas(desc="Processando mailing")

# --- FUNÇÕES UTILITÁRIAS ---

def _clean_phone_number(phone_val):
    if pd.isna(phone_val):
        return None
    phone_str = str(phone_val).split('.')[0]
    cleaned = re.sub(r'\D', '', phone_str)
    return cleaned if cleaned else None

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None and not df.empty:
        df.columns = [str(col).strip().lower() for col in df.columns]
    return df

# --- FUNÇÕES DE PROCESSAMENTO ---

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
        msg = "Remoção de Pagos: Nenhum arquivo de pagamento encontrado ou processado. Etapa pulada."
        logger.warning(msg)
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

    df_enriquecimento_dict = dataframes.get('enriquecimento', {})
    df_p100 = pd.DataFrame()
    df_p50 = pd.DataFrame()

    for sheet_name, df_sheet in df_enriquecimento_dict.items():
        if '100' in str(sheet_name):
            df_p100 = df_sheet
            logger.info(f"Encontrada aba de pontuação 100: '{sheet_name}'")
        elif '50' in str(sheet_name):
            df_p50 = df_sheet
            logger.info(f"Encontrada aba de pontuação 50: '{sheet_name}'")

    df_pontuacao = pd.concat([df_p100, df_p50], ignore_index=True)
    
    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    if df_pontuacao.empty or not all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        msg = "AVISO: Abas de pontuação válidas não encontradas ou sem colunas necessárias. Etapa pulada."
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
    logger.info("Calculando colunas agregadas (valorDivida, Quantidade_UC_por_CPF, Ucs_do_CPF).")
    
    if 'ncpf' in df.columns and 'liquido' in df.columns:
        df['liquido'] = pd.to_numeric(df['liquido'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        df['valorDivida'] = df.groupby('ncpf')['liquido'].transform('sum')
    else:
        logger.error("Colunas 'ncpf' ou 'liquido' não encontradas. Não é possível calcular 'valorDivida'.")
        df['valorDivida'] = 0

    if 'ncpf' in df.columns and 'ucv' in df.columns:
        df['ucv'] = df['ucv'].astype(str)
        uc_por_cpf = df.groupby('ncpf')['ucv'].apply(lambda x: ', '.join(x.unique()))
        
        df['Ucs_do_CPF'] = df['ncpf'].map(uc_por_cpf)
        df['Quantidade_UC_por_CPF'] = df['ncpf'].map(uc_por_cpf.apply(lambda x: len(x.split(', '))))
    else:
        logger.error("Colunas 'ncpf' ou 'ucv' não encontradas. Não é possível calcular UCs.")
        df['Quantidade_UC_por_CPF'] = 0
        df['Ucs_do_CPF'] = ''

    return df

def _criar_cliente_regulariza_from_mailing(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    logger.info("Criando a coluna 'Cliente_Regulariza' a partir da base de mailing.")
    
    fonte_col = None
    if 'venc_maior_1ano' in df.columns:
        fonte_col = 'venc_maior_1ano'
    elif 'nc' in df.columns:
        fonte_col = 'nc'
    
    if fonte_col:
        df['Cliente_Regulariza'] = np.where(
            df[fonte_col].notna() & (df[fonte_col].astype(str).str.strip().str.upper() != 'N'), 
            'SIM', 
            'NÃO'
        )
        sim_count = (df['Cliente_Regulariza'] == 'SIM').sum()
        msg = f"Criação de 'Cliente_Regulariza' a partir da coluna '{fonte_col}': {sim_count} clientes marcados como 'SIM'."
        logger.info(msg)
    else:
        df['Cliente_Regulariza'] = 'NÃO'
        msg = "AVISO: Nenhuma coluna fonte ('venc_maior_1ano' ou 'nc') encontrada. 'Cliente_Regulariza' preenchida com 'NÃO'."
        logger.warning(msg)
        
    return df, msg

def _aplicar_ajustes_finais(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    logger.info("Aplicando ajustes finais de layout e mapeamento.")
    report_msgs = []
    df_ajustado = df.copy()
    
    mapa_renomeacao = {
        'nomecad': 'NOME_CLIENTE', 'empresa': 'PRODUTO', 'ncpf': 'CPF',
        'totfat': 'parcelasEmAtraso',
        'loc': 'LOCALIDADE',
        'quantidades_de_acionamentos': 'Quantidades_de_Acionamentos',
        'telefone_01': 'TELEFONE_01', 'telefone_02': 'TELEFONE_02',
        'telefone_03': 'TELEFONE_03', 'telefone_04': 'TELEFONE_04',
    }
    df_ajustado.rename(columns=mapa_renomeacao, inplace=True)
    report_msgs.append("Renomeação de colunas (incluindo 'loc' -> 'LOCALIDADE') aplicada.")

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
    report_msgs.append(f"Reordenação de colunas aplicada para o layout definitivo. Total de {len(ordem_final)} colunas.")
    
    return df_ajustado, report_msgs

def _formatar_e_limpar_para_exportacao(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    logger.info("Iniciando formatação e limpeza final para exportação.")
    df_formatado = df.copy()
    
    for col in df_formatado.columns:
        # Pula a coluna 'valorDivida' neste loop para tratá-la separadamente depois.
        if col == 'valorDivida':
            continue
        s = df_formatado[col].astype(str)
        s = s.str.replace(r'\.0$', '', regex=True)
        s = s.str.replace(r'^(nan|none|nat)$', '', case=False, regex=True)
        s = s.str.replace('NÃƒO', 'NÃO', regex=False)
        df_formatado[col] = s

    if 'valorDivida' in df_formatado.columns:
        # --- CÓDIGO ANTIGO COMENTADO ---
        # A linha abaixo falha se 'x' for uma string vazia (''), que é o que acontece
        # quando o loop anterior converte um NaN para string e depois o apaga.
        # df_formatado['valorDivida'] = df_formatado['valorDivida'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else '')

        # --- CÓDIGO CORRIGIDO ---
        # 1. Garante que a coluna seja numérica antes de formatar, tratando possíveis erros.
        df_formatado['valorDivida'] = pd.to_numeric(df_formatado['valorDivida'], errors='coerce')
        # 2. Aplica a formatação apenas em valores que são de fato numéricos.
        df_formatado['valorDivida'] = df_formatado['valorDivida'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
        # 3. Substitui o ponto decimal por vírgula.
        df_formatado['valorDivida'] = df_formatado['valorDivida'].astype(str).str.replace('.', ',', regex=False)

    msg = "Formatação final (remoção de '.0', substituição de 'nan' e correção de 'NÃO') concluída."
    logger.info(msg)
    return df_formatado, msg

# --- FUNÇÃO ORQUESTRADORA DO PIPELINE ---

def processar_dados(dataframes: dict[str, pd.DataFrame], config: ConfigParser) -> pd.DataFrame:
    relatorio_final = ["\n" + "="*25 + " RELATÓRIO FINAL DA ALQUIMIA " + "="*25]
    
    if 'mailing' not in dataframes or dataframes['mailing'].empty:
        logger.critical("DataFrame de 'mailing' não encontrado ou vazio. Abortando.")
        return pd.DataFrame()

    logger.info("Padronizando nomes de colunas para todos os DataFrames de entrada...")
    for key, df in dataframes.items():
        if isinstance(df, pd.DataFrame):
            dataframes[key] = _standardize_columns(df)
        elif isinstance(df, dict):
            for sheet_name, sheet_df in df.items():
                df[sheet_name] = _standardize_columns(sheet_df)
    
    df_processado = dataframes['mailing'].copy()

    relatorio_final.append(f"1. Registros Iniciais no Mailing: {len(df_processado)}")
    
    # --- LÓGICA CORRIGIDA ---
    df_processado, msg_pagamentos = _remover_pagamentos(df_processado, dataframes.get('pagamentos', pd.DataFrame()))
    relatorio_final.append(f"2. {msg_pagamentos}")

    df_processado = _calcular_colunas_agregadas(df_processado)
    relatorio_final.append("3. Cálculo de Colunas Agregadas (Ucs_do_CPF, valorDivida, etc): Concluído.")

    df_processado, msg_duplicatas = _remover_duplicatas(df_processado, 'ncpf')
    relatorio_final.append(f"4. {msg_duplicatas}")
    
    df_processado, msg_enriquecimento = _enriquecer_telefones(df_processado, dataframes)
    relatorio_final.append(f"5. {msg_enriquecimento}")
    
    df_processado, msg_regulariza = _criar_cliente_regulariza_from_mailing(df_processado)
    relatorio_final.append(f"6. {msg_regulariza}")
    
    df_processado['Data_de_Importacao'] = datetime.now().strftime('%d/%m/%Y')

    df_final, msgs_ajustes = _aplicar_ajustes_finais(df_processado)
    relatorio_final.append("7. Ajustes Finais de Layout:")
    for msg in msgs_ajustes:
        relatorio_final.append(f"   - {msg}")
        
    df_final, msg_formatacao = _formatar_e_limpar_para_exportacao(df_final)
    relatorio_final.append(f"8. {msg_formatacao}")

    relatorio_final.append(f"9. Registros Finais para Exportação: {len(df_final)}")
    relatorio_final.append("="*75 + "\n")
    
    for linha in relatorio_final:
        logger.info(linha)
    
    return df_final

