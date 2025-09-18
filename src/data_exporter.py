import logging
from pathlib import Path
from configparser import ConfigParser
import pandas as pd
from datetime import datetime
import os

logger = logging.getLogger(__name__)

def _exorcizar_fantasmas(diretorio: Path):
    """
    Verifica o diretório de saída em busca de arquivos com o caractere 'ï'
    e os remove antes de iniciar a exportação.
    """
    logger.info("Iniciando ritual de limpeza de artefatos fantasmas...")
    try:
        fantasmas_encontrados = [f for f in diretorio.glob('*.csv') if 'ï' in f.name]
        if not fantasmas_encontrados:
            logger.info("Nenhum fantasma encontrado. O reino está limpo.")
            return

        logger.warning(f"Encontrados {len(fantasmas_encontrados)} arquivos fantasmas. Iniciando exorcismo.")
        for fantasma in fantasmas_encontrados:
            try:
                os.remove(fantasma)
                logger.info(f"Fantasma '{fantasma.name}' banido com sucesso.")
            except OSError as e:
                logger.error(f"Falha ao banir o fantasma '{fantasma.name}': {e}")
    except Exception as e:
        logger.error(f"Um erro ocorreu durante o ritual de exorcismo: {e}", exc_info=True)

def exportar_dados_humanos(df_humano: pd.DataFrame, config: ConfigParser, diretorio_alvo: Path):
    """
    Exporta o DataFrame de mailing humano, particionado por produto (empresa),
    com nomes de arquivo e codificação corretos.
    """
    logger.info("==================== INICIANDO EXPORTAÇÃO DE DADOS (HUMANO) ====================")
    
    _exorcizar_fantasmas(diretorio_alvo)

    if df_humano.empty:
        logger.warning("O DataFrame (humano) para exportação está vazio. Nenhum arquivo será gerado.")
        return

    try:
        prefixo = config.get('SETTINGS', 'output_file_prefix', fallback='mailing_')
        formato_data_string = config.get('SETTINGS', 'output_date_format', fallback='%%d_%%m_%%Y')
        data_str_hoje = datetime.now().strftime(formato_data_string)

        logger.info("\n--- Exportando Mailing Humano ---")
        
        df_export = df_humano.copy()

        # CORREÇÃO CRÍTICA: Limpa o caractere BOM (ï»¿) da coluna 'PRODUTO' ANTES de usá-la.
        # Isso impede a criação de novos arquivos fantasmas.
        if 'PRODUTO' in df_export.columns:
            logger.info("Realizando limpeza final na coluna 'PRODUTO' antes de criar a lista de arquivos.")
            df_export['PRODUTO'] = df_export['PRODUTO'].astype(str).str.replace('\ufeff', '', regex=False).str.strip()

        # A lista de produtos agora é criada a partir dos dados já limpos.
        produtos = df_export['PRODUTO'].unique()
        for produto in produtos:
            if pd.isna(produto) or not str(produto).strip():
                logger.warning("Produto com nome vazio ou nulo encontrado. Pulando.")
                continue

            df_produto = df_export[df_export['PRODUTO'] == produto]
            
            nome_arquivo = f"{prefixo}{produto}_{data_str_hoje}.csv"
            caminho_saida = diretorio_alvo / nome_arquivo
            
            logger.info(f"Exportando {len(df_produto)} linhas (humano) para o produto '{produto}' em '{caminho_saida}'...")
            
            df_produto.to_csv(
                caminho_saida,
                index=False,
                sep=';',
                encoding='utf-8' # O formatador cuidará do utf-8-sig depois
            )
            
        logger.info("Exportação do mailing humano concluída.")

    except Exception as e:
        logger.error(f"Ocorreu um erro catastrófico durante a exportação (humano): {e}", exc_info=True)
        raise
    
    finally:
        logger.info("==================== EXPORTAÇÃO DE DADOS (HUMANO) CONCLUÍDA ====================")
