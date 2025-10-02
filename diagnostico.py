# -*- coding: utf-8 -*-
# 1
import pandas as pd
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
import logging

# Configuração básica de logging para o terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURAÇÕES ---
# 2
INPUT_DIR_CONFIG_KEY = 'input_dir'
PATHS_SECTION = 'PATHS'
COLUNAS_INVESTIGADAS = ['bloq', 'just']
NOME_ARQUIVO_SAIDA = "RELATORIO_DIAGNOSTICO.md"

def carregar_config() -> ConfigParser:
    """Carrega o arquivo de configuração principal."""
    try:
        config = ConfigParser()
        config.read('config.ini', encoding='utf-8')
        return config
    except Exception as e:
        logging.error(f"Não foi possível ler o config.ini: {e}")
        return None

def analisar_arquivo_excel(file_path: Path, colunas_alvo: list) -> dict:
    """
    Lê um arquivo Excel, extrai o schema e os valores únicos das colunas alvo.
    """
    resultado = {
        "arquivo": file_path.name,
        "colunas_totais": [],
        "analise_colunas": {}
    }
    try:
        logging.info(f"Analisando o arquivo: {file_path.name}")
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Normaliza nomes das colunas
        df.columns = [str(col).strip().lower() for col in df.columns]
        resultado["colunas_totais"] = sorted(df.columns)

        for coluna in colunas_alvo:
            if coluna in df.columns:
                valores_unicos = df[coluna].dropna().unique().tolist()
                resultado["analise_colunas"][coluna] = valores_unicos
            else:
                resultado["analise_colunas"][coluna] = "Coluna não encontrada"

    except Exception as e:
        logging.error(f"Falha ao processar o arquivo {file_path.name}: {e}")
        resultado["erro"] = str(e)
        
    return resultado

def gerar_relatorio(resultados_analise: list):
    # 3
    """Gera um arquivo markdown com os resultados da análise."""
    now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    with open(NOME_ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
        f.write(f"# Relatório de Diagnóstico de Dados\n")
        f.write(f"Gerado em: {now}\n\n")
        f.write("---\n\n")
        
        if not resultados_analise:
            f.write("## Nenhum arquivo Excel encontrado ou processado no diretório de entrada.\n")
            return

        for resultado in resultados_analise:
            f.write(f"## 🔎 Arquivo: `{resultado['arquivo']}`\n\n")

            if resultado.get("erro"):
                f.write(f"**ALERTA:** Ocorreu um erro ao processar este arquivo:\n")
                f.write(f"```\n{resultado['erro']}\n```\n")
                continue

            f.write("### Análise de Colunas-Chave\n\n")
            for coluna, valores in resultado['analise_colunas'].items():
                f.write(f"#### Coluna: `{coluna}`\n")
                if isinstance(valores, list):
                    f.write(f"* **Valores Únicos Encontrados ({len(valores)}):**\n")
                    f.write("    ```\n")
                    for valor in sorted(valores):
                        f.write(f"    - {valor}\n")
                    f.write("    ```\n")
                else:
                    f.write(f"* **Status:** {valores}\n")
                f.write("\n")
            
            f.write("<details>\n")
            f.write("<summary>Clique para ver todas as colunas</summary>\n\n")
            f.write("```\n")
            for col in resultado['colunas_totais']:
                f.write(f"- {col}\n")
            f.write("```\n\n")
            f.write("</details>\n\n")
            f.write("---\n\n")
            
    logging.info(f"Relatório de diagnóstico salvo em '{NOME_ARQUIVO_SAIDA}'")

def main():
    # 4
    """Função principal que orquestra a análise."""
    logging.info("Iniciando script de diagnóstico de dados...")
    config = carregar_config()
    if not config or not config.has_section(PATHS_SECTION) or not config.has_option(PATHS_SECTION, INPUT_DIR_CONFIG_KEY):
        logging.critical(f"A chave '{INPUT_DIR_CONFIG_KEY}' na seção '[{PATHS_SECTION}]' não foi encontrada no config.ini. Abortando.")
        return

    input_dir = Path(config.get(PATHS_SECTION, INPUT_DIR_CONFIG_KEY))
    if not input_dir.is_dir():
        logging.critical(f"O diretório de entrada '{input_dir}' não existe. Abortando.")
        return

    arquivos_excel = list(input_dir.glob("*.xlsx"))
    if not arquivos_excel:
        logging.warning(f"Nenhum arquivo .xlsx encontrado em '{input_dir}'.")
    
    resultados = []
    for arquivo in arquivos_excel:
        resultados.append(analisar_arquivo_excel(arquivo, COLUNAS_INVESTIGADAS))
        
    gerar_relatorio(resultados)
    logging.info("Diagnóstico concluído.")

if __name__ == "__main__":
    main()
