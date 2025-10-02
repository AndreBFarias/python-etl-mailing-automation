# --- src/data_loader.py ---
import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from typing import Dict, Optional
from src.schema_validator import validate_schema, SchemaValidationError

logger = logging.getLogger(__name__)

def _find_latest_file(directory: Path, pattern: str, optional: bool = False) -> Optional[Path]:
    try:
        files = list(directory.glob(pattern))
        if not files:
            if optional:
                logger.warning(f"Arquivo opcional não encontrado para o padrão '{pattern}'.")
                return None
            raise FileNotFoundError(f"Nenhum arquivo CRÍTICO encontrado para o padrão '{pattern}'")
        latest = max(files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Arquivo mais recente encontrado para '{pattern}': {latest.name}")
        return latest
    except Exception as e:
        logger.error(f"Erro ao procurar por arquivos com o padrão '{pattern}': {e}")
        return None

def _load_excel_file(file_path: Path, config: ConfigParser, schema_key: str, all_sheets: bool = False) -> Optional[pd.DataFrame | Dict[str, pd.DataFrame]]:
    if not file_path: return None
    logger.info(f"Carregando arquivo Excel: {file_path.name}")
    try:
        data = pd.read_excel(file_path, engine='openpyxl', sheet_name=None if all_sheets else 0)
        
        if isinstance(data, dict):
            for sheet_name in data:
                data[sheet_name].columns = [str(col).strip().lower() for col in data[sheet_name].columns]
            return data

        df = data
        df.columns = [str(col).strip().lower() for col in df.columns]
        if 'empresa' in df.columns:
            df['empresa'] = df['empresa'].astype(str).str.replace('\ufeff', '', regex=False)
        validate_schema(df, config, schema_key, file_path.name)
        return df
    except Exception as e:
        logger.error(f"Falha ao carregar ou validar o arquivo {file_path.name}: {e}", exc_info=True)
        return None

def load_all_data(config: ConfigParser) -> Dict[str, pd.DataFrame]:
    input_dir = Path(config.get('PATHS', 'input_dir'))
    all_data = {}

    latest_mailing = _find_latest_file(input_dir, config.get('FILENAMES', 'mailing_nucleo_pattern'))
    all_data['mailing'] = _load_excel_file(latest_mailing, config, 'SCHEMA_MAILING') if latest_mailing else pd.DataFrame()

    logger.info("Etapa de carregamento de pagamentos pulada (obsoleta).")
    all_data['pagamentos'] = pd.DataFrame()

    latest_enriquecimento = _find_latest_file(input_dir, config.get('FILENAMES', 'enriquecimento_file'), optional=True)
    if latest_enriquecimento:
        all_data['enriquecimento'] = _load_excel_file(latest_enriquecimento, config, '', all_sheets=True)
    else:
        all_data['enriquecimento'] = {}

    latest_regras = _find_latest_file(input_dir, config.get('FILENAMES', 'regras_disposicao_file'), optional=True)
    if latest_regras:
        all_data['regras_disposicao'] = _load_excel_file(latest_regras, config, 'SCHEMA_TABULACOES')
    else:
        all_data['regras_disposicao'] = pd.DataFrame()

    logger.info("Todos os arquivos de dados foram carregados e validados com sucesso.")
    return all_data

# --- src/data_exporter.py ---
def exportar_dados_humanos(df_humano: pd.DataFrame, config: ConfigParser, diretorio_alvo: Path):
    if df_humano.empty:
        logger.warning("DataFrame 'Humano' está vazio. Nenhuma exportação será realizada.")
        return
        
    logger.info("="*20 + " INICIANDO EXPORTAÇÃO DE DADOS HUMANOS (FLUXO ÚNICO) " + "="*20)
    
    prefixo = config.get('SETTINGS', 'output_file_prefix', fallback='Telecobranca_TOI_')
    date_format = config.get('SETTINGS', 'output_date_format').replace('%%', '%')
    data_str_hoje = datetime.now().strftime(date_format)
    
    if 'PRODUTO' in df_humano.columns:
        for produto, data in df_humano.groupby('PRODUTO'):
            produto_seguro = "".join(c for c in str(produto) if c.isalnum() or c in (' ', '_')).rstrip()
            nome_arquivo = f"{prefixo}mailing_{produto_seguro}_{data_str_hoje}.csv"
            caminho_saida = diretorio_alvo / nome_arquivo
            logger.info(f"Exportando {len(data)} linhas para o produto '{produto}' em '{caminho_saida}'")
            data.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig', na_rep='')
    else:
        logger.warning("Coluna 'PRODUTO' não encontrada. Exportando arquivo consolidado.")
        nome_arquivo = f"{prefixo}Humano_Consolidado_{data_str_hoje}.csv"
        caminho_saida = diretorio_alvo / nome_arquivo
        df_humano.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig', na_rep='')
    logger.info("="*20 + " EXPORTAÇÃO DE DADOS HUMANOS CONCLUÍDA " + "="*20)


# --- src/final_polisher.py ---
def polimento_final(diretorio_alvo: Path):
    logger.info("--- Iniciando polimento final nos arquivos ---")
    
    colunas_texto_geral = [
        'ind_telefone_1_valido', 'ind_telefone_2_valido', 'ndoc',
        'consumo', 'fone_consumidor', 'diasprot', 'CPF', 'Quantidade_UC_por_CPF'
    ]

    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            logger.info(f"Polindo o arquivo: '{file_path.name}'")
            sep = '|' if 'Robo' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            
            for coluna in colunas_texto_geral:
                if coluna in df.columns:
                    df[coluna] = df[coluna].astype(str).str.replace(r'\.0$', '', regex=True)

            df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig')
            logger.info(f"Polimento do arquivo '{file_path.name}' concluído.")
        except Exception as e:
            logger.error(f"Falha ao polir o arquivo '{file_path.name}': {e}")

# --- src/compressor.py ---
def _exorcizar_arquivos_fantasmas(diretorio_alvo: Path):
    logger.info("Iniciando ritual de exorcismo de arquivos fantasmas (BOM)...")
    fantasmas_encontrados = [f for f in diretorio_alvo.glob('*.csv') if 'ï»¿' in f.name]
    if not fantasmas_encontrados:
        logger.info("Nenhum fantasma (BOM) encontrado nos nomes dos arquivos.")
        return
    for fantasma in fantasmas_encontrados:
        try:
            os.remove(fantasma)
            logger.warning(f"FANTASMA BANIDO: O arquivo '{fantasma.name}' foi removido.")
        except OSError as e:
            logger.error(f"Falha ao banir o fantasma '{fantasma.name}': {e}")

def _substituir_nan_por_nulo(diretorio_alvo: Path):
    logger.info("--- Iniciando substituição final de 'nan' por nulo ---")
    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig', keep_default_na=False)
            df.replace(['nan', 'NaT', 'None', 'NAN'], '', inplace=True)
            df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
        except Exception as e:
            logger.error(f"Falha ao substituir 'nan' no arquivo '{file_path.name}': {e}")

def _corrigir_encoding_faixa(diretorio_alvo: Path):
    logger.info("--- Iniciando correção de encoding na coluna 'faixa' ---")
    for file_path in diretorio_alvo.glob('*.csv'):
        try:
            sep = '|' if 'Robo' in file_path.name else ';'
            df = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig')
            if 'faixa' in df.columns:
                df['faixa'] = df['faixa'].str.replace('AtÃ©', 'Até', regex=False)
                df.to_csv(file_path, sep=sep, index=False, encoding='utf-8-sig', na_rep='')
        except Exception as e:
            logger.error(f"Falha ao corrigir encoding da 'faixa' no arquivo '{file_path.name}': {e}")
            
def organize_and_compress_output(config: ConfigParser):
    logger.info("--- INICIANDO ROTINA DE ORGANIZAÇÃO E COMPRESSÃO ---")
    output_dir = Path(config.get('PATHS', 'output_dir'))
    date_format_str = config.get('SETTINGS', 'output_date_format').replace('%%', '%')
    pasta_do_dia = output_dir / datetime.now().strftime(date_format_str)
    if not pasta_do_dia.is_dir():
        logger.error(f"Pasta do dia '{pasta_do_dia}' não encontrada. Abortando.")
        return

    _exorcizar_arquivos_fantasmas(pasta_do_dia)
    _substituir_nan_por_nulo(pasta_do_dia)
    _corrigir_encoding_faixa(pasta_do_dia)

    archive_name_prefix = config.get('COMPRESSOR', 'archive_name_prefix', fallback='mailing_')
    zip_name = f"{archive_name_prefix}{datetime.now().strftime('%d-%m-%Y')}.zip"
    zip_path = output_dir / zip_name
    try:
        shutil.make_archive(str(zip_path.with_suffix('')), 'zip', str(pasta_do_dia))
        logger.info(f"Pasta do dia comprimida com sucesso em '{zip_path}'")
        shutil.rmtree(pasta_do_dia)
        logger.info(f"Pasta de trabalho original '{pasta_do_dia}' removida com sucesso.")
    except Exception as e:
        logger.error(f"Falha na compressão ou remoção: {e}")


