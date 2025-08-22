import pandas as pd
from pathlib import Path
import logging
from configparser import ConfigParser
from zipfile import BadZipFile

logger = logging.getLogger(__name__)

class SchemaValidationError(ValueError):
    """Erro customizado para falhas na validação do schema do arquivo."""
    pass

def _validate_dataframe_schema(df: pd.DataFrame, required_columns: list[str], file_name: str):
    logger.info(f"Iniciando validação de schema para o arquivo: {file_name}")
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    missing_columns = required_set - actual_columns
    if missing_columns:
        error_message = (
            f"ERRO DE SCHEMA NO ARQUIVO '{file_name}'.\n"
            f"O processo foi interrompido porque as seguintes colunas obrigatórias não foram encontradas: {sorted(list(missing_columns))}.\n"
            f"AÇÃO RECOMENDADA: Verifique se o arquivo está correto ou atualize as colunas obrigatórias no 'config.ini'."
        )
        raise SchemaValidationError(error_message)
    extra_columns = actual_columns - required_set
    if extra_columns:
        logger.warning(
            f"ALERTA DE SCHEMA EM '{file_name}': "
            f"Novas colunas não esperadas foram encontradas e serão mantidas: {sorted(list(extra_columns))}. "
            f"Considere adicioná-las ao 'config.ini' se forem permanentes."
        )
    logger.info(f"Validação de schema para '{file_name}' concluída com sucesso.")
    return True

def _find_latest_file(directory: Path, pattern: str, optional: bool = False) -> Path | None:
    logger.debug(f"Procurando por '{pattern}' em '{directory}'")
    if not directory.exists():
        if optional:
            return None
        raise FileNotFoundError(f"Diretório CRÍTICO '{directory}' não encontrado.")
    files = list(directory.glob(pattern))
    if not files:
        if optional:
            return None
        raise FileNotFoundError(f"Nenhum arquivo CRÍTICO encontrado para o padrão '{pattern}' no diretório '{directory}'")
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Arquivo mais recente encontrado para '{pattern}': {latest_file.name}")
    return latest_file

def load_excel_data(path: Path | None, config: ConfigParser, sheet_name_required: str | None = None, required_columns: list[str] | None = None) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    # 1. Contrato de Retorno Explícito
    if path is None or not path.exists():
        logger.warning(f"Caminho do arquivo Excel não fornecido ou não existe: {path}. Retornando None.")
        return None
    try:
        logger.info(f"Carregando arquivo Excel: {path.name}")
        
        all_sheets_data = pd.read_excel(path, sheet_name=None)
        
        data_to_return = None
        if sheet_name_required:
            if sheet_name_required not in all_sheets_data:
                raise SchemaValidationError(f"ERRO DE SCHEMA: A aba obrigatória '{sheet_name_required}' não foi encontrada no arquivo '{path.name}'.")
            data_to_return = all_sheets_data[sheet_name_required]
        elif len(all_sheets_data) == 1:
            # Se houver apenas uma aba, retorna o DataFrame diretamente.
            data_to_return = next(iter(all_sheets_data.values()))
        else:
            # Se houver múltiplas abas e nenhuma for exigida, retorna o dicionário.
            data_to_return = all_sheets_data

        if isinstance(data_to_return, pd.DataFrame):
            data_to_return.columns = [str(col).strip() for col in data_to_return.columns]
            if 'EMPRESA' in data_to_return.columns:
                data_to_return['EMPRESA'] = data_to_return['EMPRESA'].astype(str).str.replace('ï»¿', '', regex=False)
            if required_columns:
                _validate_dataframe_schema(data_to_return, required_columns, path.name)
        elif isinstance(data_to_return, dict):
             for sheet_name, df in data_to_return.items():
                df.columns = [str(col).strip() for col in df.columns]
                data_to_return[sheet_name] = df
        
        return data_to_return
    except (BadZipFile, ValueError) as e:
        error_message = (
            f"ERRO DE ARQUIVO CORROMPIDO em '{path.name}'.\n"
            f"O processo foi interrompido porque o arquivo não pôde ser lido."
        )
        raise SchemaValidationError(error_message) from e
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar o arquivo Excel {path.name}: {e}")
        raise

def load_pagamentos_csv(directory: Path, pattern: str) -> pd.DataFrame:
    logger.info(f"Procurando arquivos de pagamento com o padrão '{pattern}'")
    files = list(directory.glob(pattern))
    if not files:
        logger.warning(f"Nenhum arquivo de pagamento encontrado com o padrão '{pattern}'.")
        return pd.DataFrame()

    logger.info(f"Encontrados {len(files)} arquivos de pagamento. Concatenando...")
    all_dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, sep=',', encoding='utf-8', low_memory=False, header=0)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            col_map = {'SIGLA': 'EMPRESA', 'UC': 'UCV', 'ANO': 'ANO', 'MES': 'MES'}
            if not all(col in df.columns for col in col_map.keys()):
                logger.error(f"Arquivo de pagamento {f.name} não contém todas as colunas necessárias (SIGLA, UC, ANO, MES) após a limpeza. Pulando este arquivo.")
                continue
            df = df[list(col_map.keys())]
            df.rename(columns=col_map, inplace=True)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Não foi possível ler ou processar o arquivo de pagamento {f.name}. Erro: {e}")
            
    if not all_dfs:
        logger.warning("Nenhum arquivo de pagamento pôde ser processado com sucesso.")
        return pd.DataFrame()

    df_final = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total de {len(df_final)} registros de pagamento carregados com sucesso.")
    return df_final

def load_txt_data(path: Path | None) -> pd.DataFrame:
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
    paths = config['PATHS']
    filenames = config['FILENAMES']
    input_dir = Path(paths['input_dir'])
    dataframes = {}
    try:
        mailing_cols_str = config.get('SCHEMA_MAILING', 'required_columns', fallback='')
        mailing_required_cols = [col.strip() for col in mailing_cols_str.split(',') if col.strip()]
        mailing_path = _find_latest_file(input_dir, filenames['mailing_nucleo_pattern'])
        dataframes['mailing'] = load_excel_data(mailing_path, config, required_columns=mailing_required_cols)

        dataframes['pagamentos'] = load_pagamentos_csv(input_dir, filenames['pagamentos_pattern'])
        
        disposicoes_path = _find_latest_file(input_dir, filenames['disposicoes_pattern'], optional=True)
        dataframes['disposicoes'] = load_txt_data(disposicoes_path)
        
        enriquecimento_path = input_dir / filenames['enriquecimento_file']
        dataframes['enriquecimento'] = load_excel_data(enriquecimento_path, config)
        
        negociacao_path = input_dir / filenames['regras_negociacao_file']
        dataframes['negociacao'] = load_excel_data(negociacao_path, config)
        
        tab_cols_str = config.get('SCHEMA_TABULACOES', 'required_columns', fallback='')
        tab_required_cols = [col.strip() for col in tab_cols_str.split(',') if col.strip()]
        tab_sheet_name = config.get('SCHEMA_TABULACOES', 'required_sheet_name', fallback=None)
        regras_disp_path = input_dir / filenames['regras_disposicao_file']
        dataframes['regras_disposicao'] = load_excel_data(regras_disp_path, config, sheet_name_required=tab_sheet_name, required_columns=tab_required_cols)
        
        logger.info("Todos os arquivos de dados foram carregados e validados com sucesso.")
    except (FileNotFoundError, SchemaValidationError) as e:
        raise
    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal durante o carregamento dos dados: {e}")
        raise
    return dataframes

