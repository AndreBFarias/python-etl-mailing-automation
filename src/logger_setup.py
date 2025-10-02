# -*- coding: utf-8 -*-
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 1. AJUSTE: Classe ExecutionReporter adicionada para gerar o relatório final.
class ExecutionReporter:
    """Coleta informações durante a execução para gerar um relatório final."""
    def __init__(self):
        self.steps = []
        self.attention_points = []

    def add_step(self, name, initial_count, final_count, message):
        removed = initial_count - final_count
        self.steps.append({
            "name": name,
            "initial": initial_count,
            "removed": removed,
            "final": final_count,
            "message": message
        })

    def add_attention_point(self, source, message):
        self.attention_points.append(f"- {source.upper()}: {message}")

    def generate_final_report(self, current_metrics, last_metrics):
        """Gera e loga a tabela de resumo e os pontos de atenção."""
        report = ["\n\n", "_"*80, "\n", "RELATÓRIO DE EXECUÇÃO DA AUTOMAÇÃO"]
        
        if self.attention_points:
            report.append("\n" + "="*25 + " PONTOS DE ATENÇÃO " + "="*25)
            report.extend(self.attention_points)

        report.append("\n" + "="*25 + " TABELA DE RESULTADOS " + "="*25)
        header = f"| {'ETAPA DE PROCESSAMENTO':<40} | {'REMOVIDOS':>12} | {'RESTANTES':>12} |"
        report.append(header)
        report.append(f"| {'-'*40} | {'-'*12} | {'-'*12} |")
        
        initial_step = next((s for s in self.steps if s["name"] == "Carregamento de Dados"), None)
        if initial_step:
            report.append(f"| {'Registros Iniciais':<40} | {'-':>12} | {initial_step['initial']:>12,} |")

        for step in self.steps[1:]: # Pula o carregamento inicial
            report.append(f"| {step['name']:<40} | {step['removed']:>12,} | {step['final']:>12,} |")
        
        report.append("\n" + "="*25 + " ANÁLISE DE OUTLIERS " + "="*25)
        if not last_metrics:
            report.append("- Esta é a primeira execução com métricas, não há dados para comparação.")
        else:
            try:
                # Compara a volumetria final de humanos
                last_human = last_metrics.get('human', 0)
                current_human = current_metrics.get('human', 0)
                if last_human > 0:
                    diff_percent = ((current_human - last_human) / last_human) * 100
                    if diff_percent >= 0:
                        report.append(f"- Arquivos HUMANOS: Gerado {diff_percent:.2f}% a mais de registros que na última execução ({current_human:,} vs {last_human:,}).")
                    else:
                        report.append(f"- Arquivos HUMANOS: Gerado {abs(diff_percent):.2f}% a menos de registros que na última execução ({current_human:,} vs {last_human:,}).")
                else:
                    report.append("- Arquivos HUMANOS: Não há dados da última execução para comparar.")

                # Compara a volumetria final de robôs
                last_robot = last_metrics.get('robot', 0)
                current_robot = current_metrics.get('robot', 0)
                if last_robot > 0:
                    diff_percent = ((current_robot - last_robot) / last_robot) * 100
                    if diff_percent >= 0:
                        report.append(f"- Arquivos ROBÔ: Gerado {diff_percent:.2f}% a mais de registros que na última execução ({current_robot:,} vs {last_robot:,}).")
                    else:
                        report.append(f"- Arquivos ROBÔ: Gerado {abs(diff_percent):.2f}% a menos de registros que na última execução ({current_robot:,} vs {last_robot:,}).")
                else:
                    report.append("- Arquivos ROBÔ: Não há dados da última execução para comparar.")

            except Exception as e:
                self.add_attention_point("Análise de Outliers", f"Falha ao comparar métricas: {e}")

        for line in report:
            logging.info(line)

def setup_logger(log_dir: str, log_level: str = 'INFO') -> str:
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    )
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    if logger.hasHandlers():
        logger.handlers.clear()

    # 2. AJUSTE: Retorna o caminho do arquivo de log da execução.
    log_file_path = os.path.join(log_dir, f"automacao_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(log_format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("Logger configurado com sucesso.")
    return log_file_path
