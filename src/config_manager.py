# 1. Importações
import configparser
from pathlib import Path

# 2. Função de Carregamento
def load_config(config_path: str = 'config.ini') -> configparser.ConfigParser:
    """
    Lê o arquivo de configuração .ini e retorna um objeto ConfigParser.

    Args:
        config_path (str): O caminho para o arquivo config.ini.

    Returns:
        configparser.ConfigParser: O objeto de configuração parseado.
    
    Raises:
        FileNotFoundError: Se o arquivo de configuração não for encontrado.
    """
    config_file = Path(config_path)
    if not config_file.is_file():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    return config

# 3. Função de Validação
def validate_config(config: configparser.ConfigParser):
    """
    Valida se as seções e chaves necessárias existem no objeto de configuração.

    Args:
        config (configparser.ConfigParser): O objeto de configuração a ser validado.

    Raises:
        ValueError: Se uma seção ou chave obrigatória estiver faltando.
    """


    required_sections = {
        'PATHS': ['input_dir', 'output_dir', 'log_dir', 'state_file'],
        'FILENAMES': ['mailing_nucleo_pattern', 'pagamentos_pattern', 'enriquecimento_file', 'regras_negociacao_file', 'regras_disposicao_file'],
        'SETTINGS': ['log_level', 'output_file_prefix', 'output_date_format']
    }

    for section, keys in required_sections.items():
        if section not in config:
            raise ValueError(f"Seção obrigatória '{section}' não encontrada no config.ini")
        for key in keys:
            if key not in config[section]:
                raise ValueError(f"Chave obrigatória '{key}' não encontrada na seção '[{section}]' do config.ini")
    
    # Validação adicional pode ser adicionada aqui (ex: verificar se os paths existem)
    
    return True

# 4. Bloco de Teste
if __name__ == '__main__':
    print("Executando teste do módulo de configuração...")
    try:
        # Assume que config.ini está no diretório pai se executado de dentro de src/
        # Para um teste robusto, o ideal é ter um config.ini de teste.
        # Por simplicidade, vamos apontar para o arquivo raiz do projeto.
        project_root = Path(__file__).parent.parent
        config_file_path = project_root / 'config.ini'

        print(f"Tentando carregar config de: {config_file_path}")
        
        config = load_config(config_file_path)
        print("Arquivo de configuração carregado com sucesso.")
        
        validate_config(config)
        print("Validação do arquivo de configuração passou.")
        
        # Exemplo de como acessar um valor
        log_dir = config.get('PATHS', 'log_dir')
        print(f"Valor de exemplo lido [PATHS] -> log_dir: {log_dir}")

    except (FileNotFoundError, ValueError) as e:
        print(f"\n--- ERRO NO TESTE ---")
        print(e)
        print("---------------------")
