# 1. Comentários Explicativos
# O feitiço foi aprimorado com dois encantamentos de poder:
# - `load_pagamentos_csv`: Agora ignora o cabeçalho original dos arquivos PAGOS
#   e impõe os nomes corretos das colunas, resolvendo o `KeyError`.
# - `load_excel_data`: Ao carregar o mailing, ele agora limpa os nomes das colunas
#   e os dados da coluna 'EMPRESA' para remover o fantasma do BOM ('ï»¿'),
#   corrigindo o problema de encoding na saída.

# 2. Código
import pandas as pd
from pathlib import Path
import logging
import glob
from configparser import ConfigParser
from zipfile import BadZipFile

logger = logging.getLogger(__name__)

def _find_latest_file(directory: Path, pattern: str, optional: bool = False) -> Path | None:
    logger.debug(f"Procurando por '{pattern}' em '{directory}'")
    files = list(directory.glob(pattern))
    if not files:
        if optional:
            logger.warning(f"Arquivo opcional com padrão '{pattern}' não encontrado. Continuando.")
            return None
        raise FileNotFoundError(f"Nenhum arquivo CRÍTICO encontrado para o padrão '{pattern}' no diretório '{directory}'")
    
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Arquivo mais recente encontrado para '{pattern}': {latest_file.name}")
    return latest_file

def load_excel_data(path: Path | None, all_sheets: bool = False, clean_bom: bool = False) -> pd.DataFrame | dict[str, pd.DataFrame]:
    if path is None: return pd.DataFrame() if not all_sheets else {}
    
    logger.info(f"Carregando arquivo Excel: {path.name}...")
    try:
        if not all_sheets:
            df = pd.read_excel(path, engine='openpyxl')
            if clean_bom:
                df.columns = df.columns.str.strip()
                if 'EMPRESA' in df.columns:
                    df['EMPRESA'] = df['EMPRESA'].str.replace('ï»¿', '', regex=False)
            logger.info(f"Carregado com sucesso. {len(df)} linhas.")
            return df
        else:
            # Lógica para múltiplas abas (se necessário no futuro)
            xls = pd.ExcelFile(path)
            sheets = {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}
            logger.info(f"Carregado com sucesso. Encontradas {len(sheets)} abas.")
            return sheets
            
    except (BadZipFile, ValueError) as e:
        logger.error(f"O arquivo '{path.name}' parece estar corrompido ou vazio. Erro: {e}")
        return pd.DataFrame() if not all_sheets else {}

def load_pagamentos_csv(directory: Path, pattern: str) -> pd.DataFrame:
    logger.info(f"Carregando arquivos de PAGAMENTOS com o padrão '{pattern}'...")
    all_files = list(directory.glob(pattern))
    if not all_files:
        logger.warning(f"Nenhum arquivo de PAGAMENTOS encontrado.")
        return pd.DataFrame()

    col_names = ["DROP_1", "SIGLA","REGIONAL","UC_VINCULADO","UC","ANO","MES","UC_ANO_MES","SIGLA_UC_ANO_MES","VALOR","DT_PAGTO","DT_BAIXA"]
    
    df_list = []
    for f in all_files:
        try:
            # Lê o CSV pulando a primeira linha (header) e nomeando as colunas manualmente
            df = pd.read_csv(f, sep=';', encoding='utf-8-sig', header=None, skiprows=1, names=col_names, on_bad_lines='warn', low_memory=False)
            df_list.append(df)
        except pd.errors.EmptyDataError:
            logger.warning(f"O arquivo CSV '{f.name}' está vazio e será ignorado.")
            continue
    
    if not df_list: return pd.DataFrame()

    concatenated_df = pd.concat(df_list, ignore_index=True).drop(columns=['DROP_1'])
    logger.info(f"Carregados e concatenados {len(df_list)} arquivos de pagamento, totalizando {len(concatenated_df)} linhas.")
    return concatenated_df

def load_txt_data(path: Path | None) -> pd.DataFrame:
    if path is None: return pd.DataFrame()
    
    logger.info(f"Carregando arquivo de texto: {path.name}...")
    try:
        df = pd.read_csv(path, sep=';', encoding='utf-8-sig', on_bad_lines='warn', low_memory=False, header=None)
        logger.info(f"Carregado com sucesso. {len(df)} linhas.")
        return df
    except pd.errors.EmptyDataError:
        logger.warning(f"O arquivo de texto '{path.name}' está vazio. Retornando DataFrame vazio.")
        return pd.DataFrame()

def load_all_data(config: ConfigParser) -> dict[str, pd.DataFrame]:
    paths = config['PATHS']
    filenames = config['FILENAMES']
    input_dir = Path(paths['input_dir'])

    dataframes = {}
    try:
        mailing_path = _find_latest_file(input_dir, filenames['mailing_nucleo_pattern'])
        dataframes['mailing'] = load_excel_data(mailing_path, clean_bom=True)
        
        dataframes['pagamentos'] = load_pagamentos_csv(input_dir, filenames['pagamentos_pattern'])

        disposicoes_path = _find_latest_file(input_dir, filenames['disposicoes_pattern'], optional=True)
        dataframes['disposicoes'] = load_txt_data(disposicoes_path)
        
        enriquecimento_path = input_dir / filenames['enriquecimento_file']
        dataframes['enriquecimento'] = load_excel_data(enriquecimento_path, all_sheets=True)
        
        negociacao_path = input_dir / filenames['regras_negociacao_file']
        dataframes['negociacao'] = load_excel_data(negociacao_path)
        
        regras_disp_path = input_dir / filenames['regras_disposicao_file']
        dataframes['regras_disposicao'] = load_excel_data(regras_disp_path)

        logger.info("Todos os arquivos de dados foram carregados com sucesso.")
    except FileNotFoundError as e:
        logger.critical(f"Processo abortado: Arquivo de entrada crítico não encontrado. Detalhe: {e}")
        raise
    except Exception as e:
        logger.critical(f"Processo abortado: Erro inesperado durante o carregamento de dados. Detalhe: {e}")
        raise

    return dataframes
