import os
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Importa os Hooks customizados do diretório de plugins
from hooks.sportmonks_hook import SportmonksHook
from hooks.supabase_hook import SupabaseHook

# Define o Asset que servirá de gatilho para a próxima camada (Silver)
BRONZE_ASSET = Asset("file:///opt/airflow/data/bronze_complete")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="sportmonks_bronze_dag",
    default_args=default_args,
    description="Camada Bronze: Ingestão de dados brutos (JSON) da API Sportmonks para o Supabase",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 5, 25),
    catchup=False,
    tags=["medallion", "bronze", "fluminense"],
)
def sportmonks_bronze_pipeline():

    @task(task_id="fetch_team_id")
    def fetch_team_id() -> int:
        """
        Busca o ID da equipe no Sportmonks dinamicamente baseado no nome configurado.
        """
        team_name = Variable.get("TEAM_SEARCH_NAME", default_var="Fluminense")
        sport_hook = SportmonksHook()
        team_id = sport_hook.search_team_id(team_name)
        return team_id

    @task(task_id="ingest_squad")
    def ingest_squad(team_id: int):
        """
        Coleta o elenco (squad) da API, salva localmente e insere o JSONB no Supabase.
        """
        sport_hook = SportmonksHook()
        supabase_hook = SupabaseHook()
        
        # 1. Coleta dados da API
        squad_data = sport_hook.get_squad(team_id)
        
        # 2. Salva arquivo físico local para backup/data lake local
        local_dir = "/opt/airflow/data/bronze"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "squad.json")
        
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(squad_data, f, ensure_ascii=False, indent=4)
        
        # 3. Insere registro bruto (JSONB) no Supabase
        db_payload = {
            "data": squad_data
        }
        supabase_hook.insert_records("bronze_squad", [db_payload])
        return local_path

    @task(task_id="ingest_fixtures")
    def ingest_fixtures(team_id: int):
        """
        Coleta as partidas (fixtures) do ano vigente, salva localmente e insere no Supabase.
        """
        sport_hook = SportmonksHook()
        supabase_hook = SupabaseHook()
        
        # Define o ano corrente para buscar partidas
        current_year = datetime.now().year
        start_date = f"{current_year}-01-01"
        end_date = f"{current_year}-12-31"
        
        # 1. Coleta dados da API
        fixtures_data = sport_hook.get_fixtures(team_id, start_date, end_date)
        
        # 2. Salva arquivo físico local
        local_dir = "/opt/airflow/data/bronze"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "fixtures.json")
        
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(fixtures_data, f, ensure_ascii=False, indent=4)
        
        # 3. Insere registro bruto (JSONB) no Supabase
        db_payload = {
            "data": fixtures_data
        }
        supabase_hook.insert_records("bronze_fixtures", [db_payload])
        return local_path

    @task(task_id="complete_bronze", outlets=[BRONZE_ASSET])
    def complete_bronze():
        """
        Tarefa de fechamento que notifica a alteração do Dataset Bronze,
        disparando automaticamente a camada Silver.
        """
        print("Bronze Ingestion completa com sucesso. Dataset atualizado para gatilho Silver.")

    # Fluxo de Dependências da DAG
    t_id = fetch_team_id()
    t_squad = ingest_squad(t_id)
    t_fixtures = ingest_fixtures(t_id)
    
    # Ambos os fluxos paralelos devem terminar para declarar a Bronze completa
    [t_squad, t_fixtures] >> complete_bronze()

# Instancia o pipeline
sportmonks_bronze_pipeline()
