import pandas as pd
import logging
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

logger = logging.getLogger(__name__)

def exportar_dados(df_final: pd.DataFrame, config: ConfigParser):
    """
    Exporta o DataFrame final para arquivos CSV, particionados por 'PRODUTO'.
    Aplica encoding 'utf-8-sig' para compatibilidade com Excel e trata valores nulos.
    """
    logger.info("Iniciando exportação dos dados processados...")
    
    output_dir = Path(config.get('PATHS', 'output_dir'))
    prefix = config.get('SETTINGS', 'output_file_prefix')
    date_str = datetime.now().strftime(config.get('SETTINGS', 'output_date_format'))

    output_dir.mkdir(parents=True, exist_ok=True)

    if 'PRODUTO' not in df_final.columns or df_final['PRODUTO'].isnull().all():
        logger.error("Coluna 'PRODUTO' não encontrada ou vazia no DataFrame final. Não é possível particionar.")
        fallback_path = output_dir / f"{prefix}GERAL_{date_str}.csv"
        
        # 'encoding="utf-8-sig"' garante que o Excel leia caracteres como 'Ç' e 'Ã' corretamente.
        # 'na_rep=""' substitui todos os valores nulos (NaN, None, etc.) por uma string vazia no CSV final.
        df_final.to_csv(fallback_path, sep=';', index=False, encoding='utf-8-sig', lineterminator='\r\n', na_rep='')
        logger.warning(f"Arquivo de fallback salvo em: {fallback_path}")
        return

    total_partitions = df_final['PRODUTO'].nunique()
    logger.info(f"Encontrados {total_partitions} produtos únicos para particionamento.")

    for produto, data in df_final.groupby('PRODUTO'):
        # Garante que o nome do produto seja seguro para nomes de arquivo
        produto_seguro = "".join(c for c in str(produto) if c.isalnum() or c in (' ', '_')).rstrip()
        file_name = f"{prefix}{produto_seguro}_{date_str}.csv"
        output_path = output_dir / file_name
        
        logger.info(f"Exportando {len(data)} linhas para o produto '{produto}' em '{output_path}'...")
        
        # Aplica a mesma lógica de exportação para cada partição
        data.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig', lineterminator='\r\n', na_rep='')

    logger.info("Exportação de dados concluída com sucesso.")

