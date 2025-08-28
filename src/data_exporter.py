import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
import os

logger = logging.getLogger(__name__)

def _limpar_artefatos_fantasmas(output_dir: Path):
    """
    Encontra e remove arquivos duplicados cujo nome contém caracteres de encoding quebrado, como 'ï'.
    Esta é a versão recalibrada da "gambiarra", focada apenas nos verdadeiros fantasmas.
    """
    logger.info("Iniciando ritual de limpeza de artefatos fantasmas...")
    
    arquivos_removidos = 0
    
    # Itera sobre todos os arquivos .csv no diretório de saída
    for f_path in output_dir.glob('*.csv'):
        # A lógica agora é simples e direta: se o nome do arquivo contém a marca do demônio,
        # ele é um fantasma e deve ser banido.
        if 'ï' in f_path.name:
            try:
                logger.warning(f"  - Fantasma detectado: '{f_path.name}'")
                os.remove(f_path)
                logger.info(f"  - Fantasma '{f_path.name}' banido com sucesso.")
                arquivos_removidos += 1
            except Exception as e:
                logger.error(f"Falha ao tentar banir o fantasma '{f_path.name}'. Erro: {e}")

    if arquivos_removidos > 0:
        logger.info(f"Ritual de limpeza concluído. {arquivos_removidos} fantasmas foram banidos.")
    else:
        logger.info("Nenhum artefato fantasma encontrado. O diretório está limpo.")


def exportar_dados(df_final: pd.DataFrame, config: ConfigParser):
    """
    Exporta o DataFrame final para arquivos CSV e depois limpa quaisquer artefatos indesejados.
    """
    logger.info("Iniciando exportação dos dados processados...")
    
    output_dir = Path(config.get('PATHS', 'output_dir'))
    prefix = config.get('SETTINGS', 'output_file_prefix')
    date_str = datetime.now().strftime(config.get('SETTINGS', 'output_date_format'))

    output_dir.mkdir(parents=True, exist_ok=True)

    if 'PRODUTO' not in df_final.columns or df_final['PRODUTO'].isnull().all():
        logger.error("Coluna 'PRODUTO' não encontrada ou vazia no DataFrame final. Não é possível particionar.")
        fallback_path = output_dir / f"{prefix}GERAL_{date_str}.csv"
        df_final.to_csv(fallback_path, sep=';', index=False, encoding='utf-8-sig')
        logger.warning(f"Arquivo de fallback salvo em: {fallback_path}")
        return

    # Executa a exportação normalmente.
    # O problema do BOM deve ser resolvido na origem (no processing_pipeline),
    # mas esta limpeza é a nossa rede de segurança.
    for produto, data in df_final.groupby('PRODUTO'):
        # Limpa o nome do produto para criar um nome de arquivo seguro
        produto_seguro = "".join(c for c in str(produto) if c.isalnum() or c in (' ', '_')).rstrip()
        file_name = f"{prefix}{produto_seguro}_{date_str}.csv"
        output_path = output_dir / file_name
        
        logger.info(f"Exportando {len(data)} linhas para o produto '{produto}' em '{output_path}'...")
        data.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')

    logger.info("Exportação de dados concluída com sucesso.")
    
    # Chama a limpeza ao final de tudo.
    _limpar_artefatos_fantasmas(output_dir)

