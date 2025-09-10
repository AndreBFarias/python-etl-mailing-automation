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


def exportar_dados(df_humano: pd.DataFrame, df_robo: pd.DataFrame, config: ConfigParser):
    """
    Exporta os DataFrames de humano e robô para arquivos CSV e depois limpa artefatos.
    """
    logger.info("Iniciando exportação dos dados processados...")
    
    output_dir = Path(config.get('PATHS', 'output_dir'))
    date_str = datetime.now().strftime(config.get('SETTINGS', 'output_date_format'))

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- BLOCO ANTIGO MODIFICADO PARA EXPORTAÇÃO DO MAILING HUMANO ---
    # A assinatura da função foi alterada. O nome do parâmetro df_final foi alterado para df_humano
    # para refletir a nova lógica.
    
    logger.info("--- Exportando Mailing Humano ---")
    if df_humano.empty:
        logger.warning("DataFrame do mailing humano está vazio. Nenhum arquivo será gerado.")
    else:
        prefix_humano = config.get('SETTINGS', 'output_file_prefix')
        if 'PRODUTO' not in df_humano.columns or df_humano['PRODUTO'].isnull().all():
            logger.error("Coluna 'PRODUTO' não encontrada ou vazia no DataFrame humano. Não é possível particionar.")
            fallback_path = output_dir / f"{prefix_humano}GERAL_{date_str}.csv"
            df_humano.to_csv(fallback_path, sep=';', index=False, encoding='utf-8-sig')
            logger.warning(f"Arquivo de fallback (humano) salvo em: {fallback_path}")
        else:
            for produto, data in df_humano.groupby('PRODUTO'):
                produto_seguro = "".join(c for c in str(produto) if c.isalnum() or c in (' ', '_')).rstrip()
                file_name = f"{prefix_humano}{produto_seguro}_{date_str}.csv"
                output_path = output_dir / file_name
                
                logger.info(f"Exportando {len(data)} linhas (humano) para o produto '{produto}' em '{output_path}'...")
                data.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
    
    logger.info("Exportação do mailing humano concluída.")

    # --- NOVO BLOCO PARA EXPORTAÇÃO DO MAILING ROBÔ ---
    logger.info("\n--- Exportando Mailing Robô ---")
    if df_robo.empty:
        logger.warning("DataFrame do mailing robô está vazio. Nenhum arquivo será gerado.")
    else:
        prefix_robo = config.get('ROBO', 'output_file_prefix', fallback='Telecobranca_TOI_Robo_')
        file_name_robo = f"{prefix_robo}{date_str}.csv"
        output_path_robo = output_dir / file_name_robo
        
        logger.info(f"Exportando {len(df_robo)} linhas (robô) para o arquivo consolidado '{output_path_robo}'...")
        # Lógica de pivotagem será implementada em uma etapa futura. Por enquanto, exporta o consolidado.
        df_robo.to_csv(output_path_robo, sep=';', index=False, encoding='utf-8-sig')
        logger.info("Exportação do mailing robô concluída.")
    # --- FIM DO NOVO BLOCO ---

    logger.info("\nExportação de todos os dados concluída com sucesso.")
    
    # Chama a limpeza ao final de tudo.
    _limpar_artefatos_fantasmas(output_dir)
