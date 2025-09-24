#1
import json
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

#2
class StateManager:
    """
    Gerencia o estado da automação, lendo e escrevendo em um arquivo JSON.
    """
    #3
    def __init__(self, state_path: str):
        """
        Inicializa o gerenciador de estado.

        Args:
            state_path (str): O caminho para o arquivo state.json.
        """
        #3.1
        self.state_file = Path(state_path)
        #3.2
        self.state = self._load_state()

    #4
    def _load_state(self) -> dict:
        """
        Lê o arquivo de estado JSON. Retorna um dicionário vazio se não existir.
        """
        #4.1
        if not self.state_file.is_file():
            logger.info(f"Arquivo de estado não encontrado em '{self.state_file}'. Iniciando um novo estado.")
            return {}
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
                logger.info(f"Estado anterior carregado com sucesso de '{self.state_file}'.")
                return state
        #4.2
        except json.JSONDecodeError:
            logger.warning(f"Arquivo de estado em '{self.state_file}' corrompido. Reiniciando o estado.")
            return {}
        except Exception as e:
            logger.error(f"Erro inesperado ao ler o arquivo de estado '{self.state_file}': {e}")
            return {}

    #5
    def _save_state(self):
        """
        Escreve o dicionário de estado atual para o arquivo JSON.
        """
        #5.1
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
            logger.debug(f"Estado salvo com sucesso em '{self.state_file}'.")
        except Exception as e:
            logger.error(f"Não foi possível salvar o estado em '{self.state_file}': {e}")

    #6
    def get_state(self) -> dict:
        """Retorna uma cópia do estado atual."""
        #6.1
        return self.state.copy()

    def save_success(self):
        """Atualiza o estado para sucesso e salva."""
        #6.2
        self.state = {
            'last_successful_run': datetime.now().isoformat(),
            'status': 'COMPLETED'
        }
        self._save_state()

    def save_failure(self, error_message: str):
        """Atualiza o estado para falha, registra o erro e salva."""
        #6.3
        self.state = {
            'last_failed_run': datetime.now().isoformat(),
            'status': 'FAILED',
            'error_message': error_message
        }
        self._save_state()
