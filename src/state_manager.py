# -*- coding: utf-8 -*-
import json
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class StateManager:
    """
    Gerencia o estado da automação, lendo e escrevendo em um arquivo JSON.
    """
    def __init__(self, state_path: str):
        self.state_file = Path(state_path)
        self.state = self._load_state()

    def _load_state(self) -> dict:
        if not self.state_file.is_file():
            logger.info(f"Arquivo de estado não encontrado em '{self.state_file}'. Iniciando um novo estado.")
            return {}
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
                logger.info(f"Estado anterior carregado com sucesso de '{self.state_file}'.")
                return state
        except json.JSONDecodeError:
            logger.warning(f"Arquivo de estado em '{self.state_file}' corrompido. Reiniciando o estado.")
            return {}
        except Exception as e:
            logger.error(f"Erro inesperado ao ler o arquivo de estado '{self.state_file}': {e}")
            return {}

    def _save_state(self):
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
            logger.debug(f"Estado salvo com sucesso em '{self.state_file}'.")
        except Exception as e:
            logger.error(f"Não foi possível salvar o estado em '{self.state_file}': {e}")

    # 1. AJUSTE: Modificado para salvar métricas.
    def save_success(self, metrics: dict):
        """Atualiza o estado para sucesso, salva as métricas e o estado."""
        self.state = {
            'last_successful_run': datetime.now().isoformat(),
            'status': 'COMPLETED',
            'last_metrics': metrics
        }
        self._save_state()

    def save_failure(self, error_message: str):
        """Atualiza o estado para falha, registra o erro e salva."""
        self.state = {
            'last_failed_run': datetime.now().isoformat(),
            'status': 'FAILED',
            'error_message': error_message
        }
        self._save_state()
        
    # 2. AJUSTE: Nova função para recuperar as métricas.
    def get_last_metrics(self) -> dict:
        """Retorna as métricas da última execução bem-sucedida."""
        return self.state.get('last_metrics', {})
