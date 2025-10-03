# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
import logging
import zipfile
import tempfile

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- FUNÇÕES AUXILIARES ---

def _sanitize_encoding(text: str) -> str:
    """Tenta corrigir problemas comuns de encoding (Mojibake)."""
    if not isinstance(text, str):
        return text
    try:
        return text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

def carregar_config() -> ConfigParser:
    """Carrega o arquivo de configuração principal."""
    try:
        config = ConfigParser()
        config.read('config.ini', encoding='utf-8')
        return config
    except Exception as e:
        logging.error(f"Não foi possível ler o config.ini: {e}")
        return None

def encontrar_arquivo_recente(diretorio: Path, padrao: str) -> Path | None:
    """Encontra o arquivo mais recente em um diretório que corresponde a um padrão."""
    try:
        files = list(diretorio.glob(padrao))
        if not files:
            return None
        return max(files, key=lambda f: f.stat().st_mtime)
    except Exception as e:
        logging.error(f"Erro ao procurar por arquivos com o padrão '{padrao}': {e}")
        return None

# --- FUNÇÕES DE ANÁLISE ---

def analisar_status_entrada(config: ConfigParser) -> set:
    logging.info("--- Fase 1: Analisando arquivo de ENTRADA ---")
    input_dir = Path(config.get('PATHS', 'input_dir'))
    mailing_pattern = config.get('FILENAMES', 'mailing_nucleo_pattern')
    coluna_bloqueio = config.get('SOURCE_COLUMNS', 'bloqueio').lower()

    arquivo_mailing = encontrar_arquivo_recente(input_dir, mailing_pattern)
    if not arquivo_mailing:
        logging.warning(f"Nenhum arquivo de mailing encontrado em '{input_dir}' com o padrão '{mailing_pattern}'.")
        return set()

    logging.info(f"Lendo arquivo de entrada: {arquivo_mailing.name}")
    df = pd.read_excel(arquivo_mailing, engine='openpyxl')
    df.columns = [str(col).strip().lower() for col in df.columns]

    if coluna_bloqueio not in df.columns:
        logging.warning(f"A coluna de bloqueio '{coluna_bloqueio}' não foi encontrada no arquivo de entrada.")
        return set()

    status_unicos_raw = df[coluna_bloqueio].dropna().unique()
    status_unicos_sanitizados = {_sanitize_encoding(str(s)) for s in status_unicos_raw}
    logging.info(f"Encontrados {len(status_unicos_sanitizados)} status únicos na entrada.")
    return status_unicos_sanitizados

def analisar_arquivos_saida(config: ConfigParser, status_a_remover: set) -> dict:
    logging.info("--- Fase 2: Analisando arquivos de SAÍDA ---")
    output_dir = Path(config.get('PATHS', 'output_dir'))
    resultados = {}

    archive_pattern = f"{config.get('COMPRESSOR', 'archive_name_prefix', fallback='mailing_')}*.zip"
    latest_zip = encontrar_arquivo_recente(output_dir, archive_pattern)

    if not latest_zip:
        logging.warning(f"Nenhum arquivo .zip de saída encontrado em '{output_dir}'. A análise de saída será pulada.")
        return resultados

    logging.info(f"Analisando conteúdo do arquivo: {latest_zip.name}")

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            with zipfile.ZipFile(latest_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            logging.info(f"Arquivos extraídos para diretório temporário.")
        except zipfile.BadZipFile:
            logging.error(f"O arquivo {latest_zip.name} está corrompido ou não é um ZIP válido.")
            return {"ERRO": f"Arquivo {latest_zip.name} corrompido."}

        pasta_base = Path(temp_dir)
        arquivos_csv = list(pasta_base.rglob("*.csv"))

        if not arquivos_csv:
            logging.warning("Nenhum arquivo CSV encontrado dentro do arquivo ZIP.")
            return resultados

        for file_path in arquivos_csv:
            logging.info(f"  -> Verificando arquivo: {file_path.name}")
            
            # 1
            if file_path.name == 'rejeitados_por_status_de_bloqueio.csv':
                logging.info(f"  -> Ignorando arquivo de relatório de rejeição: {file_path.name}")
                continue
            
            try:
                sep = '|' if 'TOI_AD_FF_ENERGISA' in file_path.name else ';'
                df_saida = pd.read_csv(file_path, sep=sep, dtype=str, encoding='utf-8-sig', on_bad_lines='warn')
                
                status_encontrados = set()
                for col in df_saida.columns:
                    valores_coluna = {_sanitize_encoding(str(v)).lower() for v in df_saida[col].dropna().unique()}
                    encontrados_na_coluna = valores_coluna.intersection(status_a_remover)
                    if encontrados_na_coluna:
                        status_encontrados.update(encontrados_na_coluna)
                
                if status_encontrados:
                    resultados[file_path.name] = sorted(list(status_encontrados))
                else:
                    resultados[file_path.name] = "OK"
            except Exception as e:
                logging.error(f"    Falha ao ler ou processar o arquivo {file_path.name}: {e}")
                resultados[file_path.name] = f"ERRO NA LEITURA: {e}"
    
    return resultados

def gerar_relatorio_auditoria(config: ConfigParser, status_entrada: set, resultados_saida: dict):
    logging.info("--- Fase 3: Gerando Relatório de Auditoria ---")
    status_a_remover_raw = config.get('SCHEMA_MAILING', 'status_de_bloqueio_para_remover', fallback='')
    status_a_remover = {s.strip().lower() for s in status_a_remover_raw.split('\n') if s.strip()}
    status_a_remover_sanitizados = {_sanitize_encoding(s) for s in status_a_remover}

    with open("RELATORIO_AUDITORIA_COMPLETA.md", 'w', encoding='utf-8') as f:
        f.write(f"# Relatório de Auditoria Completa de Status\n")
        f.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
        f.write("---\n\n")

        f.write("## 1. Análise do Arquivo de Entrada\n\n")
        f.write("A tabela abaixo mostra todos os status únicos encontrados no arquivo de mailing mais recente e indica se eles estão marcados para remoção no `config.ini`.\n\n")
        f.write("| Status Encontrado no `MAILING_NUCLEO` | Deveria ser Removido? |\n")
        f.write("| :--- | :---: |\n")
        if not status_entrada:
            f.write("| Nenhum status encontrado | - |\n")
        else:
            for status in sorted(list(status_entrada)):
                marcador = "✅ **Sim**" if status.lower() in status_a_remover_sanitizados else "Não"
                f.write(f"| `{status}` | {marcador} |\n")
        f.write("\n---\n\n")

        f.write("## 2. Análise dos Arquivos de Saída\n\n")
        f.write("Esta seção verifica se algum dos status marcados para remoção foi encontrado em qualquer coluna dos arquivos finais.\n\n")
        if not resultados_saida:
            f.write("**Nenhum arquivo de saída foi analisado.**\n")
        else:
            for arquivo, resultado in sorted(resultados_saida.items()):
                if resultado == "OK":
                    f.write(f"- **`{arquivo}`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.\n")
                else:
                    f.write(f"- **`{arquivo}`:** <span style='color:red;'>ALERTA</span> - Status proibidos encontrados:\n")
                    f.write("  ```\n")
                    for status in resultado:
                        f.write(f"  - {status}\n")
                    f.write("  ```\n")
        f.write("\n---\n\n")

    logging.info("Relatório 'RELATORIO_AUDITORIA_COMPLETA.md' gerado com sucesso.")

def main():
    """Função principal que orquestra todo o processo de auditoria."""
    config = carregar_config()
    if not config:
        return

    status_entrada = analisar_status_entrada(config)
    
    status_a_remover_raw = config.get('SCHEMA_MAILING', 'status_de_bloqueio_para_remover', fallback='')
    status_a_remover = {s.strip().lower() for s in status_a_remover_raw.split('\n') if s.strip()}
    status_a_remover_sanitizados = {_sanitize_encoding(s) for s in status_a_remover}
    
    resultados_saida = analisar_arquivos_saida(config, status_a_remover_sanitizados)
    
    gerar_relatorio_auditoria(config, status_entrada, resultados_saida)
    
    logging.info("Auditoria concluída.")

if __name__ == "__main__":
    main()

