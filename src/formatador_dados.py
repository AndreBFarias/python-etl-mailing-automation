# -*- coding: utf-8 -*-

import pandas as pd
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)

# HOMOLOGAÇÃO: A lista de colunas foi movida para o início para melhor organização.
COLUNAS_ALVO = ['liquido', 'total_toi', 'valor', 'valorDivida']

# def _formatar_valor_para_duas_casas(valor_str: str) -> str:
#     # HOMOLOGAÇÃO: Lógica antiga que falhava em arredondar corretamente e era
#     # muito agressiva na limpeza, causando erros de formatação.
#     """
#     Tenta converter uma string para float, formata para exatamente 2 casas decimais
#     e retorna como string com vírgula. Lida com lixo na string.
#     """
#     if not isinstance(valor_str, str) or valor_str.strip() == '':
#         return valor_str
#     try:
#         # Limpa tudo que não for dígito, vírgula ou sinal de menos
#         cleaned_str = re.sub(r'[^\d,-]', '', valor_str.strip())
#         # Converte para o padrão float (com ponto)
#         valor_float = float(cleaned_str.replace(',', '.', 1))
#         # Formata para 2 casas decimais e retorna ao padrão brasileiro (com vírgula)
#         return f'{valor_float:.2f}'.replace('.', ',')
#     except (ValueError, TypeError):
#         # Se a conversão falhar, retorna o valor original
#         return valor_str

def _formatar_valor_para_duas_casas(valor_str: str) -> str:
    """
    NOVA LÓGICA: Converte uma string (que pode ter '.' ou ',' como separador) para um float,
    arredonda para 2 casas decimais, e formata de volta para uma string no padrão brasileiro (com ',').
    """
    if not isinstance(valor_str, str) or valor_str.strip() == '':
        return valor_str
    try:
        # Padroniza a string para o formato numérico do Python (ponto como decimal)
        # Remove pontos de milhar e substitui a vírgula decimal.
        cleaned_str = valor_str.strip().replace('.', '').replace(',', '.', 1)
        
        valor_float = float(cleaned_str)
        
        # Arredonda para 2 casas decimais e formata a string de saída.
        return f'{valor_float:.2f}'.replace('.', ',')
    except (ValueError, TypeError):
        return valor_str


def formatar_csvs_para_padrao_br(diretorio_alvo: Path):
    """
    Varre um diretório alvo, lê cada arquivo CSV e aplica a formatação monetária
    padrão brasileiro (2 casas decimais) nas colunas financeiras dos mailings humanos.
    """
    logger.info(f"--- INICIANDO ROTINA DE FORMATAÇÃO (Padrão BR) EM '{diretorio_alvo}' ---")
    
    if not diretorio_alvo.is_dir():
        logger.warning(f"Diretório '{diretorio_alvo}' não encontrado. Etapa de formatação pulada.")
        return

    csv_files = list(diretorio_alvo.glob('*.csv'))
    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em '{diretorio_alvo}' para formatação. Etapa pulada.")
        return

    arquivos_processados = 0
    for file_path in csv_files:
        try:
            # CORREÇÃO: Garante que os arquivos do robô não passem por esta formatação de arredondamento.
            if "Robo" in file_path.name:
                logger.info(f"Pulando formatação de duas casas para o arquivo de robô: '{file_path.name}'")
                continue

            logger.info(f"Formatando o arquivo humano: '{file_path.name}'")
            df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8')

            for coluna in COLUNAS_ALVO:
                if coluna in df.columns:
                    # Aplica a nova função de formatação robusta.
                    df[coluna] = df[coluna].apply(_formatar_valor_para_duas_casas)
                    logger.debug(f"  - Coluna '{coluna}' formatada para duas casas decimais.")
            
            # Salva com 'utf-8-sig' para garantir compatibilidade com Excel.
            df.to_csv(file_path, sep=';', index=False, encoding='utf-8-sig')
            arquivos_processados += 1

        except Exception as e:
            logger.error(f"Falha ao formatar o arquivo '{file_path.name}'. Erro: {e}", exc_info=True)
    
    logger.info(f"--- ROTINA DE FORMATAÇÃO CONCLUÍDA: {arquivos_processados} arquivos humanos processados. ---")
