# 🚖 NYC Taxi Data Pipeline: Engenharia Analítica com Polars e Airflow

![Python](https://img.shields.io/badge/Python-3.12+-blue?logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-Blazing%20Fast-blue?logo=polars&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED?logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon%20Tech-336791?logo=postgresql&logoColor=white)

## 📌 Visão Geral

Este projeto é uma pipeline de dados ponta a ponta (End-to-End) desenhada para extrair, processar, validar e agregar dados reais das corridas de táxi de Nova Iorque (TLC Trip Record Data). O objetivo de negócio é responder a uma pergunta analítica fundamental: **Quais são as zonas de embarque mais lucrativas e movimentadas da cidade?**

A arquitetura foi construída focando em **alta performance e eficiência de memória**, substituindo o Pandas tradicional pelo Polars (Lazy API) e garantindo a qualidade dos dados através de contratos rigorosos.

## 🏗️ Arquitetura (Padrão Medalhão)

A pipeline orquestrada via **Apache Airflow** segue o fluxo:

1. **Camada Bronze (Extract):** Ingestão automatizada dos ficheiros `.parquet` diretamente da API governamental de NY.
2. **Camada Silver (Transform & Quality):** - Limpeza de anomalias espaciais (GPS fora de NY) e temporais.
   - **Data Contracts:** Validação estrita de _schemas_ e regras de negócio utilizando **Pandera** (ex: distâncias e valores de corrida positivos, limites geográficos).
3. **Camada Gold (Analytics):** Agregação matemática (`group_by` e `agg`) para gerar a tabela final de faturamento por zona.
4. **Load (Cloud DB):** Injeção de dados em alta velocidade num banco PostgreSQL Serverless (**Neon Tech**) utilizando o motor **ADBC** (Arrow Database Connectivity).

## 🛠️ Stack Tecnológica e Decisões de Engenharia

- **Polars:** Escolhido pela sua execução multithreaded em Rust e capacidade de processar gigabytes de dados utilizando a _Lazy API_, evitando estouro de memória RAM (Out-of-Memory).
- **Pandera:** Implementação de um "Escudo de Qualidade" para barrar dados corrompidos antes que poluam o Data Lake.
- **Apache Airflow (Docker):** Orquestração isolada em containers, garantindo reprodutibilidade e permitindo execuções de _Backfill_ (Catchup) para processamento de dados históricos.
- **PostgreSQL (Neon Tech):** Banco de dados em nuvem para servir a tabela final a potenciais ferramentas de BI.

## 🚀 Como Executar o Projeto Localmente

### 1. Pré-requisitos

- [Docker](https://www.docker.com/) e Docker Compose instalados.
- Git.

### 2. Clonar o Repositório

```bash
git clone https://github.com/KelvinSousaDev/NY-TAXI-PIPELINE
cd NY-TAXI-PIPELINE
```

### 3. Configurar Variáveis de Ambiente

Crie um ficheiro .env na raiz do projeto contendo as credenciais do banco de dados e as permissões do Airflow:

```bash
DATABASE_URL=postgresql://usuario:senha@host.neon.tech/nome_do_banco
AIRFLOW_UID=50000
```

### 4. Iniciar a Infraestrutura

Acione os containers do Airflow em background:

```bash
docker compose up -d
```

### 5. Executar a Pipeline

- Aceda à interface do Airflow no navegador: http://localhost:8080 (Credenciais padrão: airflow / airflow).

- Ative a DAG nyc_taxi_pipeline_gold_v2.

- A pipeline iniciará automaticamente o processamento histórico (Catchup) ou pode ser acionada manualmente via botão "Trigger DAG".

> Desenvolvido por Kelvin Sousa - Focado na construção de arquiteturas de dados escaláveis e resilientes.

[![LinkedIn](https://img.shields.io/badge/linkedin-%230077B5.svg?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/okelvinsousa)
