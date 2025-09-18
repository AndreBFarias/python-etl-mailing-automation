#1
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import logging
from configparser import ConfigParser

logger = logging.getLogger(__name__)

#2
def get_current_schema(input_dir: Path) -> dict:
    """Escaneia o diretório de entrada e gera um dicionário com a estrutura atual dos arquivos."""
    schema = {'files': {}}
    if not input_dir.is_dir():
        logger.error(f"Diretório de entrada '{input_dir}' não encontrado.")
        return schema
        
    files = [f for f in input_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
    
    for f in sorted(files, key=lambda p: p.name):
        file_info = {}
        if f.suffix.lower() in ['.xlsx', '.xls']:
            try:
                xls = pd.ExcelFile(f)
                file_info['sheets'] = {name: xls.parse(name, nrows=0).columns.to_list() for name in xls.sheet_names}
            except Exception as e:
                file_info['error'] = f"Não foi possível ler o arquivo Excel: {e}"
        elif f.suffix.lower() == '.csv':
            try:
                file_info['columns'] = pd.read_csv(f, nrows=0).columns.to_list()
            except Exception as e:
                file_info['error'] = f"Não foi possível ler o arquivo CSV: {e}"
        
        schema['files'][f.name] = file_info
    return schema

def save_snapshot(schema: dict, snapshot_path: Path):
    """Salva a estrutura de dados atual como o novo 'estado bom conhecido'."""
    schema['timestamp'] = datetime.now().isoformat()
    try:
        with open(snapshot_path, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=4, ensure_ascii=False)
        logger.info(f"Snapshot da estrutura de dados salvo com sucesso em '{snapshot_path}'.")
    except Exception as e:
        logger.error(f"Falha ao salvar o snapshot em '{snapshot_path}': {e}")

def compare_and_report(snapshot_path: Path, input_dir: Path, report_path: Path):
    """Compara a estrutura atual com o snapshot e gera um laudo detalhado."""
    if not snapshot_path.exists():
        logger.error("Nenhum snapshot de estrutura de dados encontrado. Não é possível gerar laudo.")
        return

    with open(snapshot_path, 'r', encoding='utf-8') as f:
        snapshot = json.load(f)
    
    current_schema = get_current_schema(input_dir)
    
    report_lines = [
        "==================================================",
        "  LAUDO DE ALTERAÇÕES NA ESTRUTURA DE DADOS",
        f"  Data da Análise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Estrutura Esperada (Snapshot de {snapshot.get('timestamp', 'data desconhecida')}):",
        "==================================================", ""
    ]
    
    # Lógica de comparação (mantida como no original)
    # ... (a lógica interna de comparação de arquivos, abas e colunas permanece a mesma) ...

    report_content = "\n".join(report_lines)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)

#3
def generate_schema_snapshot(config: ConfigParser, force_laudo: bool = False):
    """
    Função orquestradora que cria um snapshot inicial ou compara com um existente.
    """
    #3.1
    input_dir = Path(config.get('PATHS', 'input_dir', fallback='./data_input'))
    snapshot_path = Path(config.get('PATHS', 'state_dir', fallback='.')).joinpath('schema_snapshot.json')
    report_path = Path('./LAUDO_DE_ALTERACOES.txt')

    #3.2
    if force_laudo:
        logger.info("Forçando a geração do laudo de alterações.")
        compare_and_report(snapshot_path, input_dir, report_path)
        return

    if not snapshot_path.exists():
        logger.info("Nenhum snapshot encontrado. Criando o primeiro a partir da estrutura atual...")
        current_schema = get_current_schema(input_dir)
        save_snapshot(current_schema, snapshot_path)
    else:
        logger.info("Snapshot encontrado. Executando comparação para detectar mudanças...")
        compare_and_report(snapshot_path, input_dir, report_path)
