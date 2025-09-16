# -*- coding: utf-8 -*-
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

# CORREÇÃO: Nova função para tratar números com vírgula decimal
def _safe_to_float(series: pd.Series) -> pd.Series:
    """Converte uma coluna para float de forma segura, tratando vírgulas como separadores decimais."""
    series_str = series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(series_str, errors='coerce')

def _tratar_colunas_rebeldes(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando tratamento de colunas iniciais...")
    
    # CORREÇÃO: Aplica a conversão segura para float nas colunas financeiras logo no início.
    financial_cols = ['liquido', 'total_toi', 'valor']
    for col in financial_cols:
        if col in df.columns:
            df[col] = _safe_to_float(df[col])
            logger.info(f"Coluna '{col}' convertida para float com sucesso, preservando decimais.")

    if 'faixa' in df.columns:
        df['faixa'] = df['faixa'].astype(str).str.replace('Ã©', 'é', regex=False)
    if 'ndoc' in df.columns:
        numeric_ndoc = pd.to_numeric(df['ndoc'], errors='coerce')
        df['ndoc'] = numeric_ndoc.apply(lambda x: f'{x:.0f}' if pd.notna(x) else '')
    
    logger.info("Tratamento de colunas iniciais concluído.")
    return df

# --- FUNÇÕES DE PROCESSAMENTO ---

def _remover_clientes_proibidos(df_mailing: pd.DataFrame, df_bloqueio_input: pd.DataFrame | dict | None, config: ConfigParser) -> tuple[pd.DataFrame, str]:
    logger.info("Iniciando remoção unificada de clientes com base no arquivo de tabulações.")
    
    df_bloqueio = None
    if isinstance(df_bloqueio_input, dict) and df_bloqueio_input:
        first_sheet_name = next(iter(df_bloqueio_input))
        df_bloqueio = df_bloqueio_input[first_sheet_name]
    elif isinstance(df_bloqueio_input, pd.DataFrame):
        df_bloqueio = df_bloqueio_input

    if df_bloqueio is None or df_bloqueio.empty:
        msg = "Remoção por Tabulação: Arquivo de regras não encontrado ou vazio. Etapa pulada."
        logger.warning(msg)
        return df_mailing, msg

    key_bloqueio = config.get('SCHEMA_TABULACOES', 'primary_key', fallback='idcliente').lower()
    key_mailing = config.get('SCHEMA_MAILING', 'mailing_key_for_removals', fallback='ncpf').lower()
    status_col = config.get('SCHEMA_TABULACOES', 'coluna_status', fallback='status').lower()
    status_para_remover_str = config.get('SCHEMA_TABULACOES', 'status_para_remover', fallback='')
    status_para_remover = [s.strip().lower() for s in status_para_remover_str.split('\n') if s.strip()]

    if key_bloqueio not in df_bloqueio.columns or key_mailing not in df_mailing.columns:
        msg = f"AVISO: Chave para remoção não encontrada ('{key_mailing}' no mailing ou '{key_bloqueio}' nas tabulações). Etapa pulada."
        logger.warning(msg)
        return df_mailing, msg

    tamanho_inicial = len(df_mailing)
    
    ids_por_status = set()
    if status_col in df_bloqueio.columns and status_para_remover:
        df_filtrado_status = df_bloqueio[df_bloqueio[status_col].astype(str).str.strip().str.lower().isin(status_para_remover)]
        ids_por_status = set(df_filtrado_status[key_bloqueio].astype(str).str.strip().str.replace(r'\.0$', '', regex=True))
        logger.info(f"Encontrados {len(ids_por_status)} IDs para remoção com base em {len(status_para_remover)} status.")

    ids_totais_no_arquivo = set(df_bloqueio[key_bloqueio].astype(str).str.strip().str.replace(r'\.0$', '', regex=True))
    
    ids_para_remover = ids_totais_no_arquivo.union(ids_por_status)
    logger.info(f"Total de {len(ids_para_remover)} IDs únicos a serem removidos do mailing.")
    
    mailing_keys_cleaned = df_mailing[key_mailing].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_filtrado = df_mailing[~mailing_keys_cleaned.isin(ids_para_remover)]
    
    removidos = tamanho_inicial - len(df_filtrado)
    msg = f"Remoção Unificada por Tabulação: {removidos} registros removidos. Restantes: {len(df_filtrado)}."
    logger.info(msg)
    
    return df_filtrado, msg

def _remover_duplicatas_inteligentemente(df: pd.DataFrame, chave_primaria: str) -> tuple[pd.DataFrame, str]:
    if chave_primaria not in df.columns:
        msg = f"AVISO: Chave primária '{chave_primaria}' para deduplicação não encontrada. Etapa pulada."
        logger.warning(msg)
        return df, msg
    
    tamanho_inicial = len(df)
    if not df.duplicated(subset=[chave_primaria]).any():
        msg = "Deduplicação Inteligente: Nenhum registro duplicado encontrado."
        logger.info(msg)
        return df, msg

    logger.info("Iniciando deduplicação inteligente para preservar os registros mais completos.")
    df['has_name'] = df['nomecad'].notna()
    
    df_sorted = df.sort_values(by=[chave_primaria, 'has_name'], ascending=[True, False])
    
    df_deduplicado = df_sorted.drop_duplicates(subset=[chave_primaria], keep='first')
    
    df_final = df_deduplicado.drop(columns=['has_name'])
    
    tamanho_final = len(df_final)
    removidos = tamanho_inicial - tamanho_final
    msg = f"Deduplicação Inteligente por '{chave_primaria}': Removidos {removidos} registros. Restantes: {tamanho_final}."
    logger.info(msg)
    return df_final, msg

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
            msg = f"ERRO: Coluna de chave '{col}' não encontrada. Etapa abortada."
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
        msg = "AVISO: Dados de enriquecimento não encontrados. Etapa pulada."
        logger.warning(msg)
        return df_mailing, msg

    df_enriquecimento_dict = dataframes.get('enriquecimento', {})
    df_p100 = pd.DataFrame()
    df_p50 = pd.DataFrame()

    for sheet_name, df_sheet in df_enriquecimento_dict.items():
        if '100' in str(sheet_name):
            df_p100 = df_sheet
        elif '50' in str(sheet_name):
            df_p50 = df_sheet

    df_pontuacao = pd.concat([df_p100, df_p50], ignore_index=True)
    
    colunas_pontuacao_necessarias = ['documento', 'telefone', 'pontuacao']
    if df_pontuacao.empty or not all(col in df_pontuacao.columns for col in colunas_pontuacao_necessarias):
        msg = "AVISO: Abas de pontuação válidas não encontradas. Etapa pulada."
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
        msg = "ERRO: Coluna 'ndoc' não encontrada no mailing. Enriquecimento abortado."
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
    
    msg = f"Enriquecimento de Telefones: {matches} clientes tiveram telefones encontrados."
    logger.info(msg)
    return df_final, msg

def _calcular_colunas_agregadas(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculando colunas agregadas (valordivida, Quantidade_UC_por_CPF, Ucs_do_CPF).")
    
    if 'ncpf' in df.columns and 'liquido' in df.columns:
        # A coluna 'liquido' já é float, então apenas preenchemos nulos.
        df['valordivida'] = df.groupby('ncpf')['liquido'].transform('sum')
    else:
        logger.error("Colunas 'ncpf' ou 'liquido' não encontradas. Impossível calcular 'valordivida'.")
        df['valordivida'] = 0

    if 'ncpf' in df.columns and 'ucv' in df.columns:
        df['ucv'] = df['ucv'].astype(str)
        uc_por_cpf = df.groupby('ncpf')['ucv'].apply(lambda x: ', '.join(x.unique()))
        
        df['Ucs_do_CPF'] = df['ncpf'].map(uc_por_cpf)
        df['Quantidade_UC_por_CPF'] = df['ncpf'].map(uc_por_cpf.apply(lambda x: len(x.split(', '))))
    else:
        logger.error("Colunas 'ncpf' ou 'ucv' não encontradas. Impossível calcular UCs.")
        df['Quantidade_UC_por_CPF'] = 0
        df['Ucs_do_CPF'] = ''

    return df

def _criar_cliente_regulariza_from_mailing(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    logger.info("Criando a coluna 'Cliente_Regulariza'.")
    
    fonte_col = 'venc_maior_1ano'
    if fonte_col in df.columns:
        df['Cliente_Regulariza'] = np.where(
            df[fonte_col].notna() & (df[fonte_col].astype(str).str.strip().str.upper() != 'N'), 'SIM', 'NÃO'
        )
        sim_count = (df['Cliente_Regulariza'] == 'SIM').sum()
        msg = f"Criação de 'Cliente_Regulariza': {sim_count} clientes marcados como 'SIM'."
        logger.info(msg)
    else:
        df['Cliente_Regulariza'] = 'NÃO'
        msg = f"AVISO: Coluna '{fonte_col}' não encontrada. 'Cliente_Regulariza' preenchida com 'NÃO'."
        logger.warning(msg)
    return df, msg

def _manter_apenas_nao_bloqueados(df: pd.DataFrame, config: ConfigParser) -> tuple[pd.DataFrame, str]:
    logger.info("Iniciando filtro de registros bloqueados (mantendo apenas 'N').")
    coluna_filtro = config.get('SCHEMA_MAILING', 'coluna_filtro_status', fallback='bloq').lower()

    if coluna_filtro not in df.columns:
        msg = f"Filtro de Bloqueio: Coluna '{coluna_filtro}' não encontrada. Etapa pulada."
        logger.error(msg)
        return df, msg

    tamanho_inicial = len(df)
    df_filtrado = df[df[coluna_filtro].astype(str).str.strip().str.upper() == 'N']
    removidos = tamanho_inicial - len(df_filtrado)
    msg = f"Filtro de Bloqueio ('{coluna_filtro}'): {removidos} registros removidos. Restantes: {len(df_filtrado)}."
    logger.info(msg)
    return df_filtrado, msg

def _aplicar_filtros_valor_minimo(df: pd.DataFrame, config: ConfigParser) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    logger.info("Aplicando filtros de valor mínimo para mailing humano.")
    if not config.has_section('FILTROS_VALOR_MINIMO_HUMANO'):
        msg = "AVISO: Seção [FILTROS_VALOR_MINIMO_HUMANO] não encontrada. Nenhum filtro de valor aplicado."
        logger.warning(msg)
        return df, pd.DataFrame(), msg

    col_divida = config.get('FILTROS_VALOR_MINIMO_HUMANO', 'coluna_divida_filtro', fallback='valordivida').lower()
    empresas_especiais_str = config.get('FILTROS_VALOR_MINIMO_HUMANO', 'empresas_corte_especial', fallback='')
    corte_especial = config.getfloat('FILTROS_VALOR_MINIMO_HUMANO', 'valor_corte_especial', fallback=500.0)
    corte_geral = config.getfloat('FILTROS_VALOR_MINIMO_HUMANO', 'valor_corte_geral', fallback=200.0)
    empresas_especiais = [e.strip().upper() for e in empresas_especiais_str.split(',')]

    if col_divida not in df.columns:
        msg = f"ERRO: Coluna de dívida '{col_divida}' não encontrada. Filtro de valor mínimo abortado."
        logger.error(msg)
        return df, pd.DataFrame(), msg

    df[col_divida] = pd.to_numeric(df[col_divida], errors='coerce')
    cond_especial = (df['empresa'].str.upper().isin(empresas_especiais)) & (df[col_divida] >= corte_especial)
    cond_geral = (~df['empresa'].str.upper().isin(empresas_especiais)) & (df[col_divida] >= corte_geral)
    
    df_humano = df[cond_especial | cond_geral].copy()
    df_robo = df[~(cond_especial | cond_geral)].copy()
    
    msg = f"Filtro de Valor Mínimo: {len(df_humano)} para mailing humano, {len(df_robo)} para o robô."
    logger.info(msg)
    return df_humano, df_robo, msg

def _aplicar_ordenacao_humano(df: pd.DataFrame, config: ConfigParser) -> tuple[pd.DataFrame, str]:
    logger.info("Aplicando nova ordenação estratégica no mailing humano.")

    # HOMOLOGAÇÃO: Lógica de ordenação antiga foi removida para clareza.
    
    # CORREÇÃO: Nova lógica de ordenação multinível.
    conditions = [
        (df['faixa'].str.upper() == 'A VENCER'),
        (df['sit'].str.upper() == 'LIGADO'),
        (df['iu12m'].str.upper() == 'SIM'),
        (df['sit'].str.upper() == 'DESLIGADO'),
        (df['sit'].str.upper() == 'INATIVO')
    ]
    priorities = [1, 2, 3, 4, 5]
    df['priority_level'] = np.select(conditions, priorities, default=6)

    df_ordenado = df.sort_values(by=['priority_level', 'valordivida'], ascending=[True, False])
    df_ordenado = df_ordenado.drop(columns=['priority_level'])
    
    msg = "Nova ordenação estratégica (Situação/Faixa > Valor da Dívida) aplicada com sucesso."
    logger.info(msg)
    return df_ordenado, msg

def _aplicar_ajustes_finais(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    logger.info("Aplicando ajustes finais de layout e mapeamento.")
    report_msgs = []
    df_ajustado = df.copy()
    if 'empresa' in df_ajustado.columns:
        df_ajustado['empresa'] = df_ajustado['empresa'].astype(str).str.replace('\ufeff', '', regex=False)
    
    mapa_renomeacao = {
        'nomecad': 'NOME_CLIENTE', 'empresa': 'PRODUTO', 'ncpf': 'CPF',
        'totfat': 'parcelasEmAtraso', 'loc': 'LOCALIDADE',
        'quantidades_de_acionamentos': 'Quantidades_de_Acionamentos',
        'telefone_01': 'TELEFONE_01', 'telefone_02': 'TELEFONE_02',
        'telefone_03': 'TELEFONE_03', 'telefone_04': 'TELEFONE_04',
        'valordivida': 'valorDivida'
    }
    df_ajustado.rename(columns=mapa_renomeacao, inplace=True)
    report_msgs.append("Renomeação de colunas aplicada.")

    colunas_principais = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtraso', 'Quantidade_UC_por_CPF',
        'Ucs_do_CPF', 'LOCALIDADE', 'valorDivida', 'Cliente_Regulariza', 'TELEFONE_01',
        'TELEFONE_02', 'TELEFONE_03', 'TELEFONE_04', 'Quantidades_de_Acionamentos', 'Data_de_Importacao'
    ]
    for col in colunas_principais:
        if col not in df_ajustado.columns:
            df_ajustado[col] = np.nan
    
    outras_colunas = [col for col in df_ajustado.columns if col not in colunas_principais]
    df_ajustado = df_ajustado[colunas_principais + outras_colunas]
    report_msgs.append(f"Reordenação de colunas aplicada.")
    return df_ajustado, report_msgs

def _formatar_e_limpar_para_exportacao(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    logger.info("Iniciando formatação e limpeza final para exportação.")
    df_formatado = df.copy()
    
    coluna_divida_final = 'valorDivida'
    
    for col in df_formatado.columns:
        # CORREÇÃO: Não aplica formatação de string na coluna de dívida
        if col == coluna_divida_final:
            continue
        s = df_formatado[col].astype(str)
        s = s.str.replace(r'\.0$', '', regex=True)
        s = s.str.replace(r'^(nan|none|nat)$', '', case=False, regex=True)
        s = s.str.replace('NÃƒO', 'NÃO', regex=False)
        df_formatado[col] = s

    # HOMOLOGAÇÃO: A formatação da coluna de dívida foi comentada para preservar a precisão do float.
    # if coluna_divida_final in df_formatado.columns:
    #     valor_numerico = pd.to_numeric(df_formatado[coluna_divida_final], errors='coerce')
    #     df_formatado[coluna_divida_final] = valor_numerico.apply(
    #         lambda x: f'{x:.2f}'.replace('.', ',') if pd.notna(x) else ''
    #     )

    msg = "Formatação final concluída. A coluna 'valorDivida' foi mantida como float."
    logger.info(msg)
    return df_formatado, msg

# --- FUNÇÃO ORQUESTRADORA DO PIPELINE ---

def processar_dados(dataframes: dict[str, pd.DataFrame], config: ConfigParser) -> tuple[pd.DataFrame, pd.DataFrame]:
    relatorio_final = ["\n" + "="*25 + " RELATÓRIO FINAL DA ALQUIMIA " + "="*25]
    
    if 'mailing' not in dataframes or dataframes['mailing'].empty:
        logger.critical("DataFrame de 'mailing' não encontrado ou vazio. Abortando.")
        return pd.DataFrame(), pd.DataFrame()

    logger.info("Padronizando nomes de colunas...")
    for key, df_or_dict in dataframes.items():
        if isinstance(df_or_dict, pd.DataFrame): dataframes[key] = _standardize_columns(df_or_dict)
        elif isinstance(df_or_dict, dict):
            for sheet_name, sheet_df in df_or_dict.items():
                if isinstance(sheet_df, pd.DataFrame): df_or_dict[sheet_name] = _standardize_columns(sheet_df)
    
    df_processado = dataframes['mailing'].copy()
    df_processado = _tratar_colunas_rebeldes(df_processado)
    relatorio_final.append(f"1. Registros Iniciais: {len(df_processado)}")
    
    df_processado, msg_bloqueio = _remover_clientes_proibidos(df_processado, dataframes.get('regras_disposicao'), config)
    relatorio_final.append(f"2. {msg_bloqueio}")
    
    df_processado, msg_pagamentos = _remover_pagamentos(df_processado, dataframes.get('pagamentos', pd.DataFrame()))
    relatorio_final.append(f"3. {msg_pagamentos}")

    df_processado = _calcular_colunas_agregadas(df_processado)
    relatorio_final.append("4. Cálculo de Colunas Agregadas: Concluído.")

    df_processado, msg_duplicatas = _remover_duplicatas_inteligentemente(df_processado, 'ncpf')
    relatorio_final.append(f"5. {msg_duplicatas}")
    
    df_processado, msg_enriquecimento = _enriquecer_telefones(df_processado, dataframes)
    relatorio_final.append(f"6. {msg_enriquecimento}")
    
    df_processado, msg_regulariza = _criar_cliente_regulariza_from_mailing(df_processado)
    relatorio_final.append(f"7. {msg_regulariza}")
    
    df_processado['Data_de_Importacao'] = datetime.now().strftime('%d/%m/%Y')

    df_processado, msg_filtro_disp = _manter_apenas_nao_bloqueados(df_processado, config)
    relatorio_final.append(f"8. {msg_filtro_disp}")
    
    df_humano, df_robo, msg_filtro_valor = _aplicar_filtros_valor_minimo(df_processado, config)
    relatorio_final.append(f"9. {msg_filtro_valor}")
    
    df_humano, msg_ordenacao = _aplicar_ordenacao_humano(df_humano, config)
    relatorio_final.append(f"10. {msg_ordenacao}")

    df_humano_final, msgs_ajustes = _aplicar_ajustes_finais(df_humano)
    relatorio_final.append("11. Ajustes Finais de Layout (Humano):")
    for msg in msgs_ajustes: relatorio_final.append(f"   - {msg}")
        
    df_humano_final, msg_formatacao = _formatar_e_limpar_para_exportacao(df_humano_final)
    relatorio_final.append(f"12. {msg_formatacao}")

    df_robo_final, _ = _aplicar_ajustes_finais(df_robo)
    df_robo_final, _ = _formatar_e_limpar_para_exportacao(df_robo_final)

    relatorio_final.append(f"13. Registros Finais (Humano): {len(df_humano_final)}")
    
    relatorio_final.append(f"14. Registros Finais (Robô): {len(df_robo_final)}")
    relatorio_final.append("="*75 + "\n")
    
    for linha in relatorio_final: logger.info(linha)
    
    return df_humano_final, df_robo_final
