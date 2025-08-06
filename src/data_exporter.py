import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)

def exportar_dados(df_final: pd.DataFrame, config: ConfigParser):
    logger.info("Iniciando exportação dos dados processados...")
    
    output_dir = Path(config.get('PATHS', 'output_dir'))
    prefix = config.get('SETTINGS', 'output_file_prefix')
    date_str = datetime.now().strftime(config.get('SETTINGS', 'output_date_format'))

    output_dir.mkdir(parents=True, exist_ok=True)

    if 'PRODUTO' not in df_final.columns or df_final['PRODUTO'].isnull().all():
        logger.error("Coluna 'PRODUTO' não encontrada ou vazia no DataFrame final. Não é possível particionar.")
        fallback_path = output_dir / f"{prefix}GERAL_{date_str}.csv"
        # 1
        df_final.to_csv(fallback_path, sep=';', index=False, encoding='utf-8-sig', lineterminator='\r\n')
        logger.warning(f"Arquivo de fallback salvo em: {fallback_path}")
        return

    total_partitions = df_final['PRODUTO'].nunique()
    logger.info(f"Encontrados {total_partitions} produtos únicos para particionamento.")

    for produto, data in df_final.groupby('PRODUTO'):
        produto_seguro = "".join(c for c in str(produto) if c.isalnum() or c in (' ', '_')).rstrip()
        file_name = f"{prefix}{produto_seguro}_{date_str}.csv"
        output_path = output_dir / file_name
        
        logger.info(f"Exportando {len(data)} linhas para o produto '{produto}' em '{output_path}'...")
        # 1
        data.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig', lineterminator='\r\n')

    logger.info("Exportação de dados concluída com sucesso.")


