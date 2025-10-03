# Relatório de Auditoria Completa de Status
Gerado em: 03/10/2025 14:45:19

---

## 1. Análise do Arquivo de Entrada

A tabela abaixo mostra todos os status únicos encontrados no arquivo de mailing mais recente e indica se eles estão marcados para remoção no `config.ini`.

| Status Encontrado no `MAILING_NUCLEO` | Deveria ser Removido? |
| :--- | :---: |
| `AFERIÃ‡ÃƒO MEDIDOR` | Não |
| `ARRECADAÃ‡ÃƒO` | Não |
| `AÃ‡ÃƒO DE COBRANÃ‡A JUDICIAL` | Não |
| `AÃ‡ÃƒO DE COBRANÃ‡A JUDICIAL (AUTOR)` | ✅ **Sim** |
| `AÃ‡ÃƒO JUD. DE COBRANÃ‡A INVIÃVEL` | ✅ **Sim** |
| `AÃ‡ÃƒO JUDICIAL EM AVALIAÃ‡ÃƒO` | ✅ **Sim** |
| `BAIXA_PAGTO_PROTESTO` | Não |
| `BLOQUEIO POR LIGAÃ‡ÃƒO CLANDESTINA` | ✅ **Sim** |
| `BLOQUEIO SOMENTE CORTE` | ✅ **Sim** |
| `BLOQUEIO_ANEEL` | Não |
| `BLOQUEIO_OUVIDORIA` | Não |
| `BLOQUEIO_RECLAMAÃ‡ÃƒO` | Não |
| `CLIENTES HOMECARE` | Não |
| `DECISÃƒO DA EMPRESA` | ✅ **Sim** |
| `DECISÃƒO DA EMPRESA COMPLETO` | Não |
| `DECISÃƒO EMPRESA` | Não |
| `FAT ENV P/PROTESTO` | ✅ **Sim** |
| `FATJUIZ INIBIÃ‡AO A CORTE E NEGAT - PERM. COBR` | Não |
| `FATJUIZ INIBIÃ‡AO A CORTE- PERM. NEG/COBR` | Não |
| `FATURA PROTESTADA` | Não |
| `INIBI NEG. E COB. TERC. - PERMITE CORTE` | Não |
| `LIMINAR IMPEDITIVA DE CORTE` | ✅ **Sim** |
| `MARCACAO CIGANOS PROVISÃ“RIA` | Não |
| `N` | Não |
| `NEGOCIAÃ‡ÃƒO COM CLIENTE` | ✅ **Sim** |
| `OUVIDORIA` | Não |
| `PROCESSOS JUDICIAIS EM ANDAMENTO (REU)` | ✅ **Sim** |
| `PROCON` | Não |
| `PROTESTADA` | Não |
| `PROTESTO` | Não |
| `PROTESTO E FATURA` | Não |
| `PROTESTO ELETRONICO` | Não |
| `SENTENÃ‡A JUDICIAL IMPEDITIVA DE CORTE` | Não |
| `SENTENÃ‡A JUDICIAL IMPEDITIVA DE CORTE/NEG.` | Não |
| `SOMENTE NEGATIVAÃ‡ÃƒO` | ✅ **Sim** |

---

## 2. Análise dos Arquivos de Saída

Esta seção verifica se algum dos status marcados para remoção foi encontrado em qualquer coluna dos arquivos finais.

- **`TOI_AD_FF_ENERGISA_08HRS_143437_03102025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`TOI_AD_FF_ENERGISA_09HRS_143437_03102025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`TOI_AD_FF_ENERGISA_10HRS_143437_03102025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_EAC_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_EMR_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_EMS_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_EMT_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_EPB_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_ERO_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_ESE_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_ESS_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`Telecobranca_TOI_mailing_ETO_03_10_2025.csv`:** <span style='color:green;'>OK</span> - Nenhum status proibido encontrado.
- **`rejeitados_por_status_de_bloqueio.csv`:** <span style='color:red;'>ALERTA</span> - Status proibidos encontrados:
  ```
  - aã‡ãƒo de cobranã‡a judicial (autor)
  - aã‡ãƒo jud. de cobranã‡a inviãvel
  - aã‡ãƒo judicial em avaliaã‡ãƒo
  - bloqueio por ligaã‡ãƒo clandestina
  - bloqueio somente corte
  - decisãƒo da empresa
  - fat env p/protesto
  - liminar impeditiva de corte
  - negociaã‡ãƒo com cliente
  - processos judiciais em andamento (reu)
  - somente negativaã‡ãƒo
  ```

---

