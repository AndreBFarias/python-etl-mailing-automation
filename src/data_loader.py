import pandas as pd
from pathlib import Path
import logging
import glob
from configparser import ConfigParser
from zipfile import BadZipFile

logger = logging.getLogger(__name__)

def _find_latest_file(directory: Path, pattern: str, optional: bool = False) -> Path | None:
    """Encontra o arquivo mais recente que corresponde a um padrão em um diretório."""
    logger.debug(f"Procurando por '{pattern}' em '{directory}'")
    
    if not directory.exists():
        if optional:
            logger.warning(f"Diretório opcional '{directory}' não encontrado. Pulando a busca pelo padrão '{pattern}'.")
            return None
        raise FileNotFoundError(f"Diretório CRÍTICO '{directory}' não encontrado.")

    files = list(directory.glob(pattern))
    if not files:
        if optional:
            logger.warning(f"Arquivo opcional com padrão '{pattern}' não encontrado em '{directory}'. Continuando.")
            return None
        raise FileNotFoundError(f"Nenhum arquivo CRÍTICO encontrado para o padrão '{pattern}' no diretório '{directory}'")
    
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Arquivo mais recente encontrado para '{pattern}': {latest_file.name}")
    return latest_file

def _clean_bom_in_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove o BOM (Byte Order Mark) 'ï»¿' do início dos nomes das colunas."""
    df.columns = df.columns.str.replace('ï»¿', '', regex=False)
    return df

def load_excel_data(path: Path | None, all_sheets: bool = False) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    """Carrega dados de um arquivo Excel, com tratamento aprimorado para o BOM."""
    if path is None or not path.exists():
        logger.warning(f"Caminho do arquivo Excel não fornecido ou não existe: {path}. Retornando None.")
        return None
    try:
        logger.info(f"Carregando arquivo Excel: {path.name}")
        data = pd.read_excel(path, sheet_name=None if all_sheets else 0)
        
        # O tratamento agora é para um único DataFrame, que é o caso do mailing
        if not all_sheets and isinstance(data, pd.DataFrame):
            # 1. Limpa o BOM dos nomes das colunas
            data = _clean_bom_in_columns(data)
            
            # 2. EXORCISMO: Limpa o BOM dos DADOS da coluna 'EMPRESA'
            if 'EMPRESA' in data.columns:
                logger.debug("Exorcizando espectro do BOM da coluna 'EMPRESA'...")
                # Garante que a coluna seja do tipo string antes de usar .str
                data['EMPRESA'] = data['EMPRESA'].astype(str).str.replace('ï»¿', '', regex=False)
            
        elif isinstance(data, dict):
             for sheet_name, df in data.items():
                data[sheet_name] = _clean_bom_in_columns(df)

        return data
    except (BadZipFile, ValueError):
        logger.error(f"O arquivo {path.name} parece estar corrompido ou não é um arquivo Excel válido.")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar o arquivo Excel {path.name}: {e}")
        raise

def load_pagamentos_csv(directory: Path, pattern: str) -> pd.DataFrame:
    """Carrega e concatena todos os arquivos CSV de pagamentos, forçando um cabeçalho padrão."""
    logger.info(f"Procurando arquivos de pagamento com o padrão '{pattern}'")
    
    if not directory.exists():
        logger.warning(f"Diretório de pagamentos '{directory}' não encontrado. Retornando DataFrame vazio.")
        return pd.DataFrame()

    files = list(directory.glob(pattern))
    if not files:
        logger.warning(f"Nenhum arquivo de pagamento encontrado com o padrão '{pattern}'.")
        return pd.DataFrame()

    logger.info(f"Encontrados {len(files)} arquivos de pagamento. Concatenando...")
    
    # Cabeçalho correto conforme a necessidade de junção posterior
    colunas_pagamentos = ['EMPRESA', 'UCV', 'ANO', 'MES']
    
    all_dfs = []
    for f in files:
        try:
            # Carrega o CSV pulando a primeira linha (cabeçalho) e atribui os nomes corretos
            df = pd.read_csv(f, sep=';', header=None, names=colunas_pagamentos, encoding='utf-8', low_memory=False, skiprows=1)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Não foi possível ler o arquivo de pagamento {f.name}. Erro: {e}")
            
    if not all_dfs:
        logger.warning("Nenhum arquivo de pagamento pôde ser lido com sucesso.")
        return pd.DataFrame()

    df_final = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total de {len(df_final)} registros de pagamento carregados.")
    return df_final

def load_txt_data(path: Path | None) -> pd.DataFrame:
    """Carrega dados de um arquivo de texto (TXT), como o de disposições."""
    if path is None or not path.exists():
        logger.warning(f"Caminho do arquivo TXT não fornecido ou não existe: {path}. Retornando DataFrame vazio.")
        return pd.DataFrame()
    try:
        logger.info(f"Carregando arquivo de texto: {path.name}")
        return pd.read_csv(path, sep='\t', encoding='latin1', low_memory=False)
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo de texto {path.name}: {e}")
        return pd.DataFrame()

def load_all_data(config: ConfigParser) -> dict[str, pd.DataFrame]:
    """Orquestrador principal para carregar todos os arquivos de dados necessários."""
    paths = config['PATHS']
    filenames = config['FILENAMES']
    input_dir = Path(paths['input_dir'])

    dataframes = {}
    try:
        mailing_path = _find_latest_file(input_dir, filenames['mailing_nucleo_pattern'])
        dataframes['mailing'] = load_excel_data(mailing_path)
        
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
        logger.critical(f"Ocorreu um erro fatal durante o carregamento dos dados: {e}")
        raise
        
    return dataframes

