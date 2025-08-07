<div align="center">

[![Licença](https://img.shields.io/badge/licença-GPL%20v3-blue.svg)](LICENSE.txt)
[![Python](https://img.shields.io/badge/python-3.8+-green.svg)](https://www.python.org/)
[![Estrelas](https://img.shields.io/github/stars/AndreBFarias/Automacao-Mailing.svg?style=social)](https://github.com/AndreBFarias/python-etl-mailing-automation/stargazers)
[![Contribuições](https://img.shields.io/badge/contribuições-bem--vindas-brightgreen.svg)](https://github.com/AndreBFarias/python-etl-mailing-automation/issues)

<div style="text-align: center;">
  <h1 style="font-size: 2em;">Pipeline de Automação de Mailing</h1>
  <img src="logo.png" width="200" alt="Ícone do Pipeline" text-align = "center">
</div>
</div>
Um pipeline de ETL robusto e modular, escrito em Python, que transforma o processo de criação de mailing em uma coreografia de dados. Possui funcionalidades de deduplicação e enriquecimento, garantindo que sua base seja tão pura quanto o primeiro gole de café da manhã. Para otimizar o processamento de grandes volumes, o pipeline agora utiliza a biblioteca `Modin` para paralelização, aproveitando o máximo do hardware disponível.

---

## Estrutura e Funcionalidades

O projeto é uma orquestra de dados, dividida em módulos que trabalham em harmonia:

- **Configuração Externa**: Um arquivo `config.ini` centraliza os parâmetros, permitindo que a operação seja ajustada sem tocar no código, como um maestro que rege sem alterar a partitura.
- **Motor de Ingestão**: O `data_loader.py` é o coração que pulsa, capaz de carregar diversos formatos de arquivos (.xlsx, .csv, .txt), com uma resiliência que o torna imune a erros.
- **Core de Processamento**: O `processing_pipeline.py` implementa a lógica do negócio, com a inteligência de deduplicação e enriquecimento de dados. A performance para grandes datasets foi aprimorada com a inclusão de paralelismo através da biblioteca `Modin`.
- **Módulo de Exportação**: O `data_exporter.py` é o finalizador, que exporta os arquivos `.csv` no layout exato e particionados por produto, prontos para a importação.
- **Resiliência e Auditoria**: O fluxo de trabalho é registrado em detalhes para auditoria e, em caso de falha, um `state.json` permite que a execução seja retomada, pois um bom espetáculo nunca deve ser interrompido.

## Pré-requisitos

- Python 3.8 ou superior.

## Instalação e Uso

### Versão para Execução Local (Modular)

Para esta versão, que é ideal para ambientes controlados e testes locais:

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/AndreBFarias/Energisa-Automacao-Mailing.git](https://github.com/AndreBFarias/Energisa-Automacao-Mailing.git)
    cd Energisa-Automacao-Mailing

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
    - Ajuste o arquivo `config.ini` com os caminhos e parâmetros desejados.
    - Coloque os arquivos de entrada na pasta `./data_input`.
    - Execute o script principal: `python main.py`

### Versão Notebook

Para esta versão, que é ideal para execução em nuvem, especialmente com grandes volumes de dados, uma versão unificada em Notebook (`Mailing_Automação.ipynb`) foi criada.

1.  **Acesse o Colab:** Abra o Google Colab e crie um novo notebook, ou carregue o arquivo `Mailing_Automação.ipynb` do seu repositório.
2.  **Organize os Arquivos:** Crie a pasta `mailing-energisa` no seu Google Drive. Dentro dela, crie as subpastas `data_input`, `data_output` e `logs`, conforme a configuração no script. Coloque todos os arquivos de entrada (`.xlsx`, `.csv`, `.txt`) na pasta `data_input` do Drive.
2.  **Organize os Arquivos:** Crie a pasta `mailing` no seu Google Drive. Dentro dela, crie as subpastas `data_input`, `data_output` e `logs`, conforme a configuração no script. Coloque todos os arquivos de entrada (`.xlsx`, `.csv`, `.txt`) na pasta `data_input` do Drive.
3.  **Execute o Notebook:** O script do Notebook foi projetado para ser executado célula por célula, seguindo a ordem. A primeira célula irá instalar as dependências e a segunda irá montar seu Google Drive, permitindo que o script acesse os arquivos diretamente.
4.  **Atenção à Performance:** A versão do Notebook utiliza `Modin` para paralelismo, garantindo uma performance otimizada para o processamento de grandes arquivos.

### Dependências

As ferramentas que fazem este espetáculo acontecer são:

- `pandas` para manipular os dados com graça e precisão.
- `modin` para paralelizar as operações do pandas e acelerar o processamento de grandes datasets.
- `openpyxl` para ler as nuances dos arquivos `.xlsx`.
- `configparser` para gerir as configurações de forma externa.

### Licença GPL v3

> Livre para modificar e usar da forma que preferir desde que tudo permaneça livre.

