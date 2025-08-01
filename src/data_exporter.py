import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)

def exportar_dados(df_final: pd.DataFrame, config: ConfigParser):
    """
    Particiona o DataFrame final por 'PRODUTO' e salva cada partição
    como um arquivo CSV separado.
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

    total_partitions = df_final['PRODUTO'].nunique()
    logger.info(f"Encontrados {total_partitions} produtos únicos para particionamento.")

    for produto, data in df_final.groupby('PRODUTO'):
        file_name = f"{prefix}{produto}_{date_str}.csv"
        output_path = output_dir / file_name
        
        logger.info(f"Exportando {len(data)} linhas para o produto '{produto}' em '{output_path}'...")
        data.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')

    logger.info("Exportação de dados concluída com sucesso.")
