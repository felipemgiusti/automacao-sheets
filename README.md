# Automação de Indicadores Operacionais com Python, Metabase e Google Sheets

## Sobre o projeto

Este projeto automatiza a atualização de indicadores operacionais em uma planilha do Google Sheets a partir de consultas realizadas no Metabase.

A rotina foi desenvolvida em Python e executada automaticamente pelo GitHub Actions em horários programados, eliminando atualizações manuais e garantindo maior agilidade na disponibilização de dados para acompanhamento operacional.

O foco principal do projeto é alimentar abas estratégicas de acompanhamento, como:

- Comitê de Crise
- Plano Operacional
- Reunião de Produção
- Blockers Mec Jurubatuba

## Objetivo

Automatizar a coleta, tratamento e envio de métricas operacionais para uma planilha de acompanhamento, reduzindo esforço manual e aumentando a confiabilidade do processo.

## Como funciona

O script realiza as seguintes etapas:

1. Carrega variáveis de ambiente e credenciais
2. Autentica no Google Sheets via service account
3. Consulta dados no Metabase por meio de requisições HTTP
4. Converte os resultados em DataFrames com Pandas
5. Aplica regras de negócio e filtros por data
6. Atualiza células específicas em abas do Google Sheets
7. Executa automaticamente via GitHub Actions em horário programado

## Principais funcionalidades

- Integração com API do Metabase
- Atualização automatizada de planilhas no Google Sheets
- Uso de credenciais seguras com GitHub Secrets
- Tratamento de datas para regras específicas de operação
- Atualização de múltiplos indicadores e múltiplas abas
- Execução agendada com GitHub Actions

## Tecnologias utilizadas

- Python
- Pandas
- Requests
- Gspread
- OAuth2Client
- Python Dotenv
- GitHub Actions
- Google Sheets API
- Metabase API

## Estrutura do projeto

```bash
.
├── .github/
│   └── workflows/
│       └── atualizar.yml
├── atualiza_planilha.py
├── requirements.txt
└── .gitignore
