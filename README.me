# 🇭🇺 Fluminense Football Data Pipeline - Arquitetura Medalhão (Airflow 3 & Supabase)

Este repositório contém um projeto de Engenharia de Dados completo e profissional, estruturado sob o conceito de **Arquitetura Medalhão (Medallion Architecture)**. O objetivo principal do pipeline é extrair, processar e analisar dados históricos e estatísticos da equipe do **Fluminense Football Club** utilizando a **Sportmonks API v3**, processando os dados com **Pandas** e persistindo-os na nuvem no **Supabase** (PostgreSQL).

Toda a orquestração é realizada pelo **Apache Airflow 3** rodando em **Docker**, utilizando a funcionalidade moderna de agendamento reativo por meio de **Assets** e o executor de alta eficiência **LocalExecutor**.

---

## 🏗️ Arquitetura do Pipeline de Dados

O pipeline é composto por 3 DAGs totalmente desacopladas que se comunicam e se ativam de forma encadeada por meio da atualização de **Assets** físicos (simulando um Data Lake local) e lógicas relacionais no banco de dados na nuvem:

```mermaid
graph TD
    subgraph 1. Camada Bronze (Ingestão)
        A[Sportmonks API v3] -->|HTTP GET / Token| B[SportmonksHook]
        B -->|Salva JSON Bruto| C[data/bronze/*.json]
        B -->|Insere Payloads JSONB| D[(Supabase: bronze_squad & bronze_fixtures)]
    end

    D -->|Gatilho BRONZE_ASSET| E[2. Camada Silver (Transformação)]

    subgraph 2. Camada Silver (Tratamento)
        E -->|Consome dados Bronze| F[Pandas Data Parser]
        F -->|Salva CSV Relacional| G[data/silver/*.csv]
        F -->|UPSERT Relacional/Tipado| H[(Supabase: silver_squad & silver_fixtures)]
    end

    H -->|Gatilho SILVER_ASSET| I[3. Camada Gold (Analytics)]

    subgraph 3. Camada Gold (Indicadores)]
        I -->|Consome dados Silver| J[Pandas Aggregator]
        J -->|Salva CSV Analítico| K[data/gold/*.csv]
        J -->|UPSERT Visões Agregadas| L[(Supabase: gold_squad_summary & gold_fixtures_summary)]
    end
```

---

## 🛠️ Tecnologias Utilizadas

* **Orquestrador**: Apache Airflow 3.2.2 (em Docker)
* **Banco de Dados (Nuvem)**: Supabase (Postgres)
* **Data Engine (ETL/ELT)**: Python 3.13 & Pandas
* **API de Ingestão**: Sportmonks Football API v3
* **Infraestrutura**: Docker & Docker Compose (LocalExecutor para baixo footprint de memória)

---

## 📂 Estrutura do Repositório

```text
├── .env                          # Variáveis de ambiente e chaves da API (Ignorado no Git)
├── .gitignore                    # Regras de exclusão de arquivos e credenciais para o Git
├── docker-compose.yaml           # Configuração de containers do Apache Airflow 3
├── README.md                     # Documentação do projeto
├── dags/
│   ├── sql/
│   │   └── supabase_schema.sql   # Script DDL para criar as tabelas no Supabase
│   ├── sportmonks_bronze_dag.py  # DAG Bronze: Extrai dados e grava brutos
│   ├── sportmonks_silver_dag.py  # DAG Silver: Limpa, formata e normaliza em tabelas relacionais
│   └── sportmonks_gold_dag.py    # DAG Gold: Consolida métricas analíticas finais
├── plugins/
│   ├── __init__.py
│   └── hooks/
│       ├── __init__.py
│       ├── sportmonks_hook.py    # Hook resiliente de conexão com a API Sportmonks (com Mock Fallback)
│       └── supabase_hook.py      # Hook REST para integração rápida de escrita/consulta no Supabase
└── data/                         # Data Lake Local (Backups Bronze/Silver/Gold - Ignorado no Git)
```

---

## ⚙️ Funcionalidades Sênior de Destaque

1. **Agendamento Reativo com Assets (Airflow 3)**:
   - Em vez do encadeamento tradicional de DAGs via gatilhos manuais, as DAGs comunicam-se via **`from airflow.sdk import Asset`**. 
   - A finalização com sucesso da Bronze gera uma atualização física do arquivo local, o que dispara reativamente a Silver. A finalização da Silver atualiza o asset equivalente, disparando imediatamente a Gold.
2. **LocalExecutor de Baixo Consumo**:
   - O projeto está otimizado para rodar em computadores locais usando `LocalExecutor`. 
   - Isso elimina os containers redundantes do Redis e do Celery Worker, **reduzindo em quase 2 GB** o uso de memória RAM local em relação ao setup de desenvolvimento padrão do Airflow.
3. **Mecanismo de Mock Resiliente (Resilient Hook Fallback)**:
   - A API da Sportmonks restringe consultas sobre o Campeonato Brasileiro de Série A no plano de testes gratuito. 
   - O nosso `SportmonksHook` implementa um mecanismo inteligente: ele faz a chamada real para a API. Caso receba um erro de assinatura (403/401) ou resposta vazia, ele registra um aviso no log e aciona um **fallback contendo dados simulados do Fluminense de alta fidelidade** (elenco real com Ganso, Thiago Silva, Cano e Arias, e partidas do Brasileirão), permitindo a execução e testes de ponta a ponta do projeto.
4. **UPSERTs Relacionais (Supabase REST API)**:
   - O `SupabaseHook` realiza cargas de dados de alta performance usando a API REST nativa do Supabase (PostgREST), executando operações de `UPSERT` seguras baseadas nas chaves primárias dos registros, evitando registros duplicados.

---

## 🚀 Instruções de Instalação e Execução Local

### Passo 1: Pré-requisitos
* Possuir o **Docker** e o **Docker Compose** instalados na sua máquina.
* Possuir uma conta no **Supabase** (gratuita).
* Possuir uma conta e token na **Sportmonks** (gratuita).

### Passo 2: Configurar o Supabase (Banco de Dados)
1. Acesse o painel do seu **Supabase** e crie um novo projeto.
2. No menu lateral, acesse o **SQL Editor** e clique em **New Query**.
3. Copie todo o conteúdo do arquivo [supabase_schema.sql](dags/sql/supabase_schema.sql) deste repositório, cole no editor do Supabase e clique em **Run**.
4. Isso criará instantaneamente todas as tabelas das camadas Bronze, Silver e Gold e os índices necessários.

### Passo 3: Configurar Variáveis de Ambiente
Na raiz do seu projeto local, crie um arquivo chamado **`.env`** (ou ajuste o existente) e configure as suas credenciais fornecidas:
```properties
# Dependências adicionais para o container Airflow
_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow requests

# Chaves e Endereços (Configure com os seus dados reais)
AIRFLOW_VAR_SPORTMONKS_API_TOKEN=SUA_CHAVE_SPORTMONKS_API
AIRFLOW_VAR_SUPABASE_URL=https://SEU_SUBDOMINIO.supabase.co
AIRFLOW_VAR_SUPABASE_KEY=SUA_CHAVE_SERVICE_ROLE_OU_ANON_DO_SUPABASE
AIRFLOW_VAR_TEAM_SEARCH_NAME=Fluminense
```
> [!WARNING]
> Use a chave `service_role` ou chave administrativa do Supabase na variável `AIRFLOW_VAR_SUPABASE_KEY` para que o pipeline tenha permissão de gravação e atualização das tabelas na nuvem.

### Passo 4: Subir o Apache Airflow 3
Abra o seu terminal na pasta do projeto e inicie os containers:
```bash
docker compose up -d postgres airflow-scheduler airflow-apiserver airflow-dag-processor airflow-triggerer
```
*Note que com esta configuração de LocalExecutor, apenas as instâncias essenciais serão ligadas, mantendo o ambiente leve.*

### Passo 5: Ativar e Rodar o Pipeline
1. Acesse a interface Web do Airflow em seu navegador em: `http://localhost:8080` (Usuário: `airflow` / Senha: `airflow`).
2. Você verá as 3 DAGs listadas na interface (`sportmonks_bronze_dag`, `sportmonks_silver_dag`, `sportmonks_gold_dag`).
3. Mude a chave das 3 DAGs de **Paused** para **Active**.
4. Dispare manualmente a primeira execução clicando no botão de play (**Trigger DAG**) na **`sportmonks_bronze_dag`**.
5. Acompanhe a execução reativa em cascata: a Bronze processará e ativará automaticamente a Silver, que processará e ativará de imediato a Gold.

---

## 📈 Resultados e Monitoramento de Dados

Após a execução com sucesso, você poderá auditar os dados em dois locais:
1. **Localmente (Data Lake Local)**:
   - `data/bronze/squad.json` e `fixtures.json`: payloads puros retornados pelo Hook.
   - `data/silver/squad.csv` e `fixtures.csv`: tabelas estruturadas e limpas via Pandas.
   - `data/gold/fixtures_summary.csv` e `squad_summary.csv`: arquivos consolidados de indicadores analíticos.
2. **Na Nuvem (Supabase)**:
   - Acompanhe no visualizador de tabelas do painel Supabase os dados relacionais de elenco e estatísticas de partidas totalmente persistidos e atualizados.
