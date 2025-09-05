# 1. Importações
import json
from pathlib import Path
import logging

# Pega o logger já configurado (assumindo que setup_logger foi chamado antes)
logger = logging.getLogger(__name__)

# 2. Função de Leitura de Estado
def read_state(state_path: str) -> dict:
    """
    Lê o arquivo de estado JSON. Se o arquivo não existir ou estiver corrompido,
    retorna um dicionário vazio.

    Args:
        state_path (str): O caminho para o arquivo state.json.

    Returns:
        dict: O estado atual como um dicionário.
    """
    state_file = Path(state_path)
    if not state_file.is_file():
        logger.info(f"Arquivo de estado não encontrado em '{state_path}'. Iniciando um novo estado.")
        return {}
    
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
            logger.info(f"Estado anterior carregado com sucesso de '{state_path}'.")
            return state
    except json.JSONDecodeError:
        logger.warning(f"Arquivo de estado em '{state_path}' está corrompido ou mal formatado. Reiniciando o estado.")
        return {}
    except Exception as e:
        logger.error(f"Erro inesperado ao ler o arquivo de estado '{state_path}': {e}")
        return {}

# 3. Função de Escrita de Estado
def write_state(state_path: str, state_data: dict):
    """
    Escreve o dicionário de estado atual para o arquivo JSON.

    Args:
        state_path (str): O caminho para o arquivo state.json.
        state_data (dict): O dicionário de estado a ser salvo.
    """
    state_file = Path(state_path)
    try:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=4, ensure_ascii=False)
        logger.debug(f"Estado salvo com sucesso em '{state_path}'.")
    except Exception as e:
        logger.error(f"Não foi possível salvar o estado em '{state_path}': {e}")

# 4. Bloco de Teste
if __name__ == '__main__':
    # Para testar, precisamos de um logger. Vamos configurar um básico aqui.
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("Executando teste do módulo de gerenciamento de estado...")
    
    test_file_path = './state_teste.json'
    
    # Teste 1: Ler estado inexistente
    print("\n--- Teste 1: Lendo um estado que não existe ---")
    initial_state = read_state(test_file_path)
    print(f"Estado inicial lido: {initial_state}")
    assert initial_state == {}

    # Teste 2: Escrever e ler um estado
    print("\n--- Teste 2: Escrevendo e lendo um estado ---")
    current_state = {
        "initialization_complete": True,
        "last_run": "2025-07-26T10:00:00"
    }
    write_state(test_file_path, current_state)
    print(f"Estado escrito: {current_state}")
    
    read_back_state = read_state(test_file_path)
    print(f"Estado lido de volta: {read_back_state}")
    assert read_back_state == current_state

    # Teste 3: Atualizar o estado
    print("\n--- Teste 3: Atualizando o estado ---")
    current_state["input_files_validated"] = True
    current_state["last_run"] = "2025-07-26T10:05:00"
    write_state(test_file_path, current_state)
    print(f"Estado atualizado escrito: {current_state}")

    read_back_state_updated = read_state(test_file_path)
    print(f"Estado atualizado lido de volta: {read_back_state_updated}")
    assert read_back_state_updated == current_state

    # Limpeza
    Path(test_file_path).unlink()
    print(f"\nArquivo de teste '{test_file_path}' removido.")
    print("Teste do módulo de estado concluído com sucesso.")
