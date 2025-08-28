from pathlib import Path
import logging
import os

# Importa a função que queremos testar. Como ela é "privada" (começa com _),
# o Python nos permite importá-la, mas avisa que não é uma prática comum.
# Para um teste, é perfeitamente aceitável.
from src.data_exporter import _limpar_artefatos_fantasmas
from src.logger_setup import setup_logger

def divider(title: str):
    """Função para imprimir um separador bonito."""
    print("\n" + "="*20 + f" {title.upper()} " + "="*20 + "\n")

def main():
    """
    Orquestra o teste da função de limpeza do data_exporter.
    """
    # Configura um logger simples para este teste
    setup_logger('./logs', 'INFO')
    
    output_dir = Path("./data_output")

    if not output_dir.exists():
        print(f"Diretório '{output_dir}' não encontrado. Crie-o e coloque os arquivos de teste dentro.")
        return
    
    # 1. Lista os arquivos antes do teste
    divider("Situação ANTES do Exorcismo")
    print(f"Analisando arquivos em '{output_dir}':")
    files_before = list(output_dir.glob('*.csv'))
    if not files_before:
        print("Nenhum arquivo .csv encontrado para testar.")
        return
        
    for file in files_before:
        marker = "[FANTASMA]" if 'ï' in file.name else "[LEGÍTIMO]"
        print(f"  {marker.ljust(12)} - {file.name}")
    
    # 2. Executa a função de limpeza
    divider("Invocando o Exorcista")
    _limpar_artefatos_fantasmas(output_dir)
    
    # 3. Verifica o resultado
    divider("Situação DEPOIS do Exorcismo")
    print("Arquivos restantes na pasta:")
    files_after = list(output_dir.glob('*.csv'))
    if files_after:
        for file in files_after:
            print(f"  - {file.name}")
    else:
        print("  - Nenhum arquivo .csv restante.")
        
    # 4. Veredito final
    fantasmas_restantes = [f for f in files_after if 'ï' in f.name]
    if not fantasmas_restantes:
        print("\n[VEREDITO] SUCESSO! Nenhum fantasma sobreviveu. Os inocentes foram poupados.")
    else:
        print(f"\n[VEREDITO] FALHA! {len(fantasmas_restantes)} fantasmas ainda assombram o diretório.")

if __name__ == "__main__":
    # Salve este script como 'teste_exorcismo.py' na pasta raiz e execute.
    main()

