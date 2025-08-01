# python-etl-mailing-automation

<div align="center">

[![Licença](https://img.shields.io/badge/licença-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8+-green.svg)](https://www.python.org/)
[![Estrelas](https://img.shields.io/github/stars/AndreBFarias/Energisa-Automacao-Mailing.svg?style=social)](https://github.com/AndreBFarias/Energisa-Automacao-Mailing/stargazers)
[![Contribuições](https://img.shields.io/badge/contribuições-bem--vindas-brightgreen.svg)](https://github.com/AndreBFarias/Energisa-Automacao-Mailing/issues)

<div style="text-align: center;">
  <h1 style="font-size: 2em;">Pipeline de Automação de Mailing</h1>
  <img src="logo.png" width="200" alt="Ícone do Pipeline" text-align = "center">
</div>
</div>
Um pipeline de ETL robusto e modular, escrito em Python, que transforma o processo de criação de mailing em uma coreografia de dados. Possui funcionalidades de deduplicação e enriquecimento, garantindo que sua base seja tão pura quanto o primeiro gole de café da manhã.

---

## Estrutura e Funcionalidades

O projeto é uma orquestra de dados, dividida em módulos que trabalham em harmonia:

- **Configuração Externa**: Um arquivo `config.ini` centraliza os parâmetros, permitindo que a operação seja ajustada sem tocar no código, como um maestro que rege sem alterar a partitura.
- **Motor de Ingestão**: O `data_loader.py` é o coração que pulsa, capaz de carregar diversos formatos de arquivos (.xlsx, .csv, .txt), com uma resiliência que o torna imune a erros.
- **Core de Processamento**: O `processing_pipeline.py` implementa a lógica do negócio, com a inteligência de deduplicação e enriquecimento de dados.
- **Módulo de Exportação**: O `data_exporter.py` é o finalizador, que exporta os arquivos `.csv` no layout exato e particionados por produto, prontos para a importação.
- **Resiliência e Auditoria**: O fluxo de trabalho é registrado em detalhes para auditoria e, em caso de falha, um `state.json` permite que a execução seja retomada, pois um bom espetáculo nunca deve ser interrompido.

## Pré-requisitos

- Python 3.8 ou superior.

## Instalação

```bash
# Clone o repositório:
git clone [https://github.com/AndreBFarias/Energisa-Automacao-Mailing.git](https://github.com/AndreBFarias/Energisa-Automacao-Mailing.git)
cd Energisa-Automacao-Mailing
# Crie um ambiente virtual e instale as dependências:
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

##### Uso
- Ajuste o arquivo config.ini com os caminhos e parâmetros desejados.
- Execute o script principal
>python main.py


Dependências
As ferramentas que fazem este espetáculo acontecer são:

- pandas para manipular os dados com graça e precisão.

- openpyxl para ler as nuances dos arquivos .xlsx.

- configparser para gerir as configurações de forma externa.

### Licença GPL 
> Livre para modificar e ser entregue aos desejos desde que tudo permaneça livre.


