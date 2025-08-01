# 1. Comentários Explicativos
# O feitiço foi aprimorado com a sabedoria final, alinhada com Elivelton.
# - A função `_adicionar_cliente_regulariza` foi finalizada, usando a base de
#   negociações para criar a nova coluna 'Cliente_Regulariza'.
# - A função `_criar_colunas_finais` foi completamente reforjada para
#   executar a visão de Elivelton com precisão cirúrgica:
#   1. Ela preserva TODAS as colunas originais do mailing.
#   2. Ela renomeia as colunas conforme o combinado ('NOMECAD' -> 'NOME_CLIENTE',
#      'CEP_Endereco_da_UC' -> 'LOCALIDADE', etc.).
#   3. Ela calcula e adiciona as novas colunas.
#   4. Ela reordena o DataFrame final para que as 'colunas_principais'
#      apareçam primeiro, na ordem exata, seguidas por todas as outras
#      colunas da base bruta.

# 2. Código
import pandas as pd
import logging
from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)
tqdm.pandas(desc="Processando mailing")

def _remover_duplicatas(df: pd.DataFrame, chave_primaria: str) -> pd.DataFrame:
    """Remove registros duplicados com base em uma chave primária, mantendo o primeiro."""
    if chave_primaria not in df.columns:
        logger.warning(f"Chave primária '{chave_primaria}' para deduplicação não encontrada. Pulando etapa.")
        return df
        
    tamanho_inicial = len(df)
    if df.duplicated(subset=[chave_primaria]).any():
        df.drop_duplicates(subset=[chave_primaria], keep='first', inplace=True)
        tamanho_final = len(df)
        removidos = tamanho_inicial - tamanho_final
        logger.info(f"Deduplicação: Removidos {removidos} registros duplicados com base na coluna '{chave_primaria}'.")
    else:
        logger.info("Deduplicação: Nenhum registro duplicado encontrado.")
    return df

def _adicionar_cliente_regulariza(df_mailing: pd.DataFrame, dataframes: dict) -> pd.DataFrame:
    """Adiciona a coluna 'Cliente_Regulariza'."""
    df_negociacao = dataframes.get('negociacao')
    
    logger.info("Adicionando coluna 'Cliente_Regulariza'...")
    if df_negociacao is None or df_negociacao.empty:
        logger.warning("Base de negociações não foi carregada. Coluna 'Cliente_Regulariza' será preenchida com 'NÃO'.")
        df_mailing['Cliente_Regulariza'] = 'NÃO'
        return df_mailing

    chave_negociacao = 'cdcdebito'
    chave_mailing = 'UCV'
    if chave_negociacao not in df_negociacao.columns or chave_mailing not in df_mailing.columns:
        logger.error(f"Chaves para cruzamento ('{chave_mailing}', '{chave_negociacao}') não encontradas. Coluna 'Cliente_Regulariza' será 'NÃO'.")
        df_mailing['Cliente_Regulariza'] = 'NÃO'
        return df_mailing
        
    # Garante que as chaves sejam do mesmo tipo para o merge
    df_negociacao[chave_negociacao] = df_negociacao[chave_negociacao].astype(str)
    df_mailing[chave_mailing] = df_mailing[chave_mailing].astype(str)
    
    clientes_em_negociacao = df_negociacao[chave_negociacao].unique()
    df_mailing['Cliente_Regulariza'] = df_mailing[chave_mailing].isin(clientes_em_negociacao).map({True: 'SIM', False: 'NÃO'})
    logger.info("Coluna 'Cliente_Regulariza' adicionada com sucesso.")
    return df_mailing

def _higienizar_base(df_mailing: pd.DataFrame, dataframes: dict) -> pd.DataFrame:
    """Aplica todas as regras de higienização e filtragem."""
    logger.info("Iniciando higienização da base...")
    df_mailing = _remover_duplicatas(df_mailing, 'NCPF')
    # ... (Restante da lógica de higienização por pagamentos, etc. a ser adicionada) ...
    logger.info(f"Higienização concluída. Registros restantes: {len(df_mailing)}")
    return df_mailing

def _criar_colunas_finais(df_mailing: pd.DataFrame) -> pd.DataFrame:
    """Cria, formata e reordena as colunas, preservando todos os dados originais."""
    logger.info("Criando, formatando e reordenando colunas finais...")
    
    df_final = df_mailing.copy()

    # Calcula novas colunas
    if 'LIQUIDO' in df_final.columns and 'NCPF' in df_final.columns:
        df_final['valorDivida'] = df_final.groupby('NCPF')['LIQUIDO'].transform('sum')
    if 'UCV' in df_final.columns and 'NCPF' in df_final.columns:
        df_final['Quantidade_UC_por_CPF'] = df_final.groupby('NCPF')['UCV'].transform('nunique')
    
    df_final['Data_de_Importacao'] = datetime.now().strftime('%Y-%m-%d')
    
    # Renomeia colunas existentes para o padrão final, conforme alinhado com Elivelton
    rename_map = {
        'MES': 'parcelasEmAtraso', 
        'EMPRESA': 'PRODUTO', 
        'NCPF': 'CPF',
        'NOMECAD': 'NOME_CLIENTE',
        'CEP_Endereco_da_UC': 'LOCALIDADE'
    }
    df_final.rename(columns=rename_map, inplace=True)
    
    # Define a ordem das colunas principais que devem aparecer no início
    colunas_principais = [
        'NOME_CLIENTE', 'PRODUTO', 'CPF', 'parcelasEmAtraso', 'Quantidade_UC_por_CPF',
        'Ucs_do_CPF', 'LOCALIDADE', 'valorDivida', 'Cliente_Regulariza', 'TELEFONE_01', 'TELEFONE_02',
        'TELEFONE_03', 'TELEFONE_04', 'Quantidades_de_Acionamentos', 'Data_de_Importacao'
    ]
    
    # Garante que todas as colunas principais existam
    for col in colunas_principais:
        if col not in df_final.columns:
            df_final[col] = None
            logger.debug(f"Coluna principal '{col}' não encontrada, adicionada com valores nulos.")
    
    # Cria a ordem final: colunas principais primeiro, depois todas as outras
    colunas_existentes = df_final.columns.tolist()
    outras_colunas = [col for col in colunas_existentes if col not in colunas_principais]
    
    ordem_final = colunas_principais + outras_colunas
            
    return df_final[ordem_final]

def processar_dados(dataframes: dict[str, pd.DataFrame], config: ConfigParser) -> pd.DataFrame:
    """Orquestra todo o pipeline de processamento de dados."""
    logger.info(">>> INICIANDO PIPELINE DE PROCESSAMENTO DE DADOS <<<")
    
    df_processado = dataframes['mailing'].copy()

    df_processado = _higienizar_base(df_processado, dataframes)
    df_processado = _adicionar_cliente_regulariza(df_processado, dataframes)
    # ... (outras etapas como enriquecimento) ...
    df_final = _criar_colunas_finais(df_processado)

    logger.info(">>> PIPELINE DE PROCESSAMENTO DE DADOS CONCLUÍDO <<<")
    return df_final
