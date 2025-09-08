<div align="center">

[![Licença](https://img.shields.io/badge/licença-GPL%20v3-blue.svg)](LICENSE.txt)
[![Python](https://img.shields.io/badge/python-3.8+-green.svg)](https://www.python.org/)
[![Estrelas](https://img.shields.io/github/stars/AndreBFarias/Automacao-Mailing.svg?style=social)](https://github.com/AndreBFarias/python-etl-mailing-automation/stargazers)
[![Contribuições](https://img.shields.io/badge/contribuições-bem--vindas-brightgreen.svg)](https://github.com/AndreBFarias/python-etl-mailing-automation/issues)

<div style="text-align: center;">
  <h1 style="font-size: 2em;">Pipeline de Automação de Mailing</h1>
  <img src="logo.png" width="200" alt="Ícone do Pipeline">
</div>
</div>

Um pipeline de ETL robusto, modular e resiliente, escrito em Python, que transforma o processo de criação de mailing em uma coreografia de dados. O sistema foi blindado com mecanismos de autodiagnóstico e flexibilidade para resistir a inconsistências nos dados de entrada, garantindo a integridade do processo.

---

## Estrutura e Funcionalidades

O projeto é uma orquestra de dados, dividida em módulos que trabalham em harmonia:

-   **Configuração Externa**: Um arquivo `config.ini` centraliza os parâmetros, permitindo que a operação seja ajustada sem tocar no código.
-   **Motor de Ingestão Flexível**: O `data_loader.py` é o coração que pulsa, capaz de carregar diversos formatos de arquivos e inteligente o suficiente para encontrar os arquivos de regras (`.xlsx`, `.csv`) mesmo que seus nomes sejam alterados (ex: "Tabulacoes.xlsx" vs "tabulacoes_para_retirar.xlsx").
-   **Core de Processamento Multicamadas**: O `processing_pipeline.py` implementa a lógica de negócio com **quatro camadas de higienização**:
    1.  Remoção por Chave Externa (CPF vs. IdCliente).
    2.  Remoção por Status da Tabulação (Ex: "CLIENTE FALECIDO").
    3.  Remoção de Duplicatas por CPF.
    4.  Remoção por Status do Mailing (Coluna `bloq`).
-   **Módulo de Exportação e Organização**: O `data_exporter.py` exporta os arquivos `.csv` particionados por produto.
-   **Módulo de Compressão e Arquivamento**: Ao final de uma execução bem-sucedida, o `compressor.py` entra em ação, organizando todos os arquivos de saída do dia em uma pasta datada, copiando o log da execução e compactando tudo em um arquivo `.zip` para distribuição e arquivamento.
-   **Validador de Schema e Autópsia Automática**: O `schema_validator.py` é o guardião da estabilidade.
    -   **Em sucesso:** Ele cria um "snapshot" da estrutura de dados bem-sucedida (`schema_snapshot.json`).
    -   **Em falha:** Ele é acionado automaticamente, compara a estrutura atual com o último snapshot e gera um laudo técnico (`LAUDO_DE_ALTERACOES.txt`), detalhando todas as mudanças não comunicadas (colunas, abas, ordem, etc.), expondo a causa raiz do erro.

## Pré-requisitos

-   Python 3.8 ou superior.

## Instalação e Uso

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/AndreBFarias/python-etl-mailing-automation.git](https://github.com/AndreBFarias/python-etl-mailing-automation.git)
    cd python-etl-mailing-automation
    ```
2.  **Crie um ambiente virtual e instale as dependências:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
3.  **Configuração e Execução:**
    -   Ajuste o arquivo `config.ini` com os caminhos e parâmetros desejados.
    -   Coloque os arquivos de entrada na pasta `./data_input`.
    -   Execute o script principal: `python main.py`

### Licença GPL v3

> Livre para modificar e usar da forma que preferir, desde que tudo permaneça livre.
