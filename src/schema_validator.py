#1
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

SNAPSHOT_PATH = Path('./schema_snapshot.json')
INPUT_DIR = Path('./data_input')
REPORT_PATH = Path('./LAUDO_DE_ALTERACOES.txt')

def get_current_schema() -> dict:
    """Escaneia o diretório de entrada e gera um dicionário com a estrutura atual dos arquivos."""
    schema = {'files': {}}
    files = [f for f in INPUT_DIR.iterdir() if f.is_file() and not f.name.startswith('.')]
    
    for f in sorted(files, key=lambda p: p.name):
        file_info = {}
        if f.suffix.lower() in ['.xlsx', '.xls']:
            try:
                xls = pd.ExcelFile(f)
                file_info['sheets'] = {}
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name, nrows=0)
                    file_info['sheets'][sheet_name] = df.columns.to_list()
            except Exception:
                file_info['error'] = "Não foi possível ler o arquivo Excel."
        elif f.suffix.lower() == '.csv':
            try:
                df = pd.read_csv(f, nrows=0)
                file_info['columns'] = df.columns.to_list()
            except Exception:
                file_info['error'] = "Não foi possível ler o arquivo CSV."
        
        schema['files'][f.name] = file_info
    return schema

def save_snapshot(schema: dict):
    """Salva a estrutura de dados atual como o novo 'estado bom conhecido'."""
    schema['timestamp'] = datetime.now().isoformat()
    with open(SNAPSHOT_PATH, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=4, ensure_ascii=False)
    print("INFO: Snapshot da estrutura de dados salvo com sucesso.")

def compare_and_report():
    """Compara a estrutura atual com o snapshot e gera um laudo detalhado."""
    if not SNAPSHOT_PATH.exists():
        return "ERRO CRÍTICO: Nenhum snapshot de estrutura de dados encontrado. Não é possível gerar laudo."

    with open(SNAPSHOT_PATH, 'r', encoding='utf-8') as f:
        snapshot = json.load(f)
    
    current_schema = get_current_schema()
    
    report_lines = [
        "==================================================",
        "  LAUDO DE ALTERAÇÕES NÃO COMUNICADAS NA ESTRUTURA DE DADOS",
        f"  Data da Análise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Estrutura Esperada (Snapshot de {snapshot.get('timestamp', 'data desconhecida')}):",
        "==================================================",
        ""
    ]
    
    snapshot_files = set(snapshot['files'].keys())
    current_files = set(current_schema['files'].keys())

    # Checa arquivos
    added_files = current_files - snapshot_files
    removed_files = snapshot_files - current_files
    if added_files: report_lines.append(f"[ALERTA] Arquivos Adicionados: {', '.join(added_files)}")
    if removed_files: report_lines.append(f"[ALERTA] Arquivos Removidos: {', '.join(removed_files)}")
    
    # Checa estrutura dos arquivos em comum
    for filename in sorted(snapshot_files.intersection(current_files)):
        report_lines.append(f"\n--- Análise do Arquivo: {filename} ---")
        snap_info = snapshot['files'][filename]
        curr_info = current_schema['files'][filename]

        # Para Excels
        if 'sheets' in snap_info:
            snap_sheets = set(snap_info['sheets'].keys())
            curr_sheets = set(curr_info.get('sheets', {}).keys())
            if snap_sheets != curr_sheets:
                report_lines.append(f"  [!] Nomes de Abas (Sheets) Divergentes:")
                report_lines.append(f"      - Esperado: {sorted(list(snap_sheets))}")
                report_lines.append(f"      - Encontrado: {sorted(list(curr_sheets))}")
            
            for sheet_name in snap_sheets.intersection(curr_sheets):
                snap_cols = snap_info['sheets'][sheet_name]
                curr_cols = curr_info['sheets'][sheet_name]
                if snap_cols != curr_cols:
                    report_lines.append(f"  [!] Estrutura da Aba '{sheet_name}' Alterada:")
                    if set(snap_cols) != set(curr_cols):
                        report_lines.append(f"      - Colunas Adicionadas: {sorted(list(set(curr_cols) - set(snap_cols)))}")
                        report_lines.append(f"      - Colunas Removidas: {sorted(list(set(snap_cols) - set(curr_cols)))}")
                    if snap_cols != curr_cols:
                         report_lines.append(f"      - ORDEM DAS COLUNAS MUDOU.")
        
        # Para CSVs
        elif 'columns' in snap_info:
            snap_cols = snap_info['columns']
            curr_cols = curr_info.get('columns', [])
            if snap_cols != curr_cols:
                report_lines.append(f"  [!] Estrutura do CSV Alterada:")
                if set(snap_cols) != set(curr_cols):
                    report_lines.append(f"      - Colunas Adicionadas: {sorted(list(set(curr_cols) - set(snap_cols)))}")
                    report_lines.append(f"      - Colunas Removidas: {sorted(list(set(snap_cols) - set(curr_cols)))}")
                if snap_cols != curr_cols:
                    report_lines.append(f"      - ORDEM DAS COLUNAS MUDOU.")

    report_content = "\n".join(report_lines)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(report_content)
        
    return f"Laudo de alterações gerado em '{REPORT_PATH}'. A automação foi interrompida devido a mudanças não comunicadas na estrutura dos dados de entrada."

if __name__ == '__main__':
    # Bloco para criar o primeiro snapshot manualmente ou para testar a comparação
    if not SNAPSHOT_PATH.exists():
        print("Nenhum snapshot encontrado. Criando o primeiro a partir da estrutura atual...")
        current_schema = get_current_schema()
        save_snapshot(current_schema)
    else:
        print("Snapshot já existe. Executando comparação e gerando laudo...")
        resultado = compare_and_report()
        print(resultado)
