import os
import json
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.exceptions import AirflowException

# Importa o Hook do Supabase do diretório de plugins
from hooks.supabase_hook import SupabaseHook

# Define os Assets de entrada (Bronze) e saída (Silver)
BRONZE_ASSET = Asset("file:///opt/airflow/data/bronze_complete")
SILVER_ASSET = Asset("file:///opt/airflow/data/silver_complete")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="sportmonks_silver_dag",
    default_args=default_args,
    description="Camada Silver: Limpeza, normalização e carga relacional (UPSERT) no Supabase",
    schedule=[BRONZE_ASSET], # Dispara de forma reativa pelo Asset do Bronze
    start_date=datetime(2026, 5, 25),
    catchup=False,
    tags=["medallion", "silver", "fluminense"],
)
def sportmonks_silver_pipeline():

    @task(task_id="process_squad")
    def process_squad():
        """
        Consome os dados brutos da tabela bronze_squad, limpa e normaliza os perfis
        dos jogadores, e realiza carga incremental na silver_squad no Supabase.
        """
        supabase_hook = SupabaseHook()
        
        # 1. Busca o registro Bronze mais recente do Supabase
        # Ordenamos por ID desc para obter a ingestão de dados mais recente
        bronze_records = supabase_hook.select_records(
            table_name="bronze_squad",
            select_query="data",
            filters={"order": "id.desc", "limit": "1"}
        )
        
        if not bronze_records:
            # Fallback para o arquivo local se o banco estiver vazio
            local_path = "/opt/airflow/data/bronze/squad.json"
            if os.path.exists(local_path):
                self.log.info("Carregando elenco a partir do backup JSON local...")
                with open(local_path, "r", encoding="utf-8") as f:
                    squad_payload = json.load(f)
            else:
                raise AirflowException("Nenhum dado do elenco encontrado na camada Bronze (Supabase ou Local).")
        else:
            squad_payload = bronze_records[0].get("data", {})

        squad_items = squad_payload.get("data", [])
        if not squad_items:
            raise AirflowException("Nenhum jogador encontrado na lista de dados do elenco.")

        # 2. Faz o parsing e flattening dos dados com Pandas
        cleaned_players = []
        for item in squad_items:
            player = item.get("player", {})
            position = item.get("position", {})
            
            if not player:
                continue

            cleaned_player = {
                "player_id": int(player.get("id")),
                "name": player.get("display_name") or player.get("name", "Desconhecido"),
                "position": position.get("name") if position else "Não Definido",
                "nationality": player.get("nationality", {}).get("name") or "Brasileiro",
                "jersey_number": int(item.get("number")) if item.get("number") is not None else None,
                "birth_date": player.get("date_of_birth"),
                "height": player.get("height"),
                "weight": player.get("weight"),
                "updated_at": datetime.utcnow().isoformat()
            }
            cleaned_players.append(cleaned_player)

        # 3. Salva a versão estruturada em Silver local para backup
        silver_dir = "/opt/airflow/data/silver"
        os.makedirs(silver_dir, exist_ok=True)
        pd.DataFrame(cleaned_players).to_csv(os.path.join(silver_dir, "squad.csv"), index=False, encoding="utf-8")

        # 4. Upsert (carga incremental robusta) no Supabase
        supabase_hook.upsert_records("silver_squad", cleaned_players)
        return len(cleaned_players)

    @task(task_id="process_fixtures")
    def process_fixtures():
        """
        Consome os dados brutos da tabela bronze_fixtures, formata partidas e placares
        e insere na tabela silver_fixtures de forma relacional.
        """
        supabase_hook = SupabaseHook()
        
        # 1. Busca o registro Bronze mais recente do Supabase
        bronze_records = supabase_hook.select_records(
            table_name="bronze_fixtures",
            select_query="data",
            filters={"order": "id.desc", "limit": "1"}
        )
        
        if not bronze_records:
            local_path = "/opt/airflow/data/bronze/fixtures.json"
            if os.path.exists(local_path):
                self.log.info("Carregando partidas a partir do backup JSON local...")
                with open(local_path, "r", encoding="utf-8") as f:
                    fixtures_payload = json.load(f)
            else:
                raise AirflowException("Nenhum dado de partidas encontrado na camada Bronze (Supabase ou Local).")
        else:
            fixtures_payload = bronze_records[0].get("data", {})

        fixtures_items = fixtures_payload.get("data", [])
        if not fixtures_items:
            raise AirflowException("Nenhuma partida encontrada na lista de dados de fixtures.")

        # 2. Faz o parsing e flattening das partidas
        cleaned_fixtures = []
        for fixture in fixtures_items:
            participants = fixture.get("participants", [])
            scores = fixture.get("scores", [])
            venue = fixture.get("venue", {})

            # Resolve Home e Away teams dos participantes
            home_team = "Não Definido"
            away_team = "Não Definido"
            home_id = None
            away_id = None

            for p in participants:
                location = p.get("meta", {}).get("location")
                if location == "home":
                    home_team = p.get("name", "Mandante")
                    home_id = p.get("id")
                elif location == "away":
                    away_team = p.get("name", "Visitante")
                    away_id = p.get("id")

            # Resolve Placar das equipes
            home_score = None
            away_score = None
            for s in scores:
                p_id = s.get("participant_id")
                goals = s.get("score", {}).get("goals")
                
                # Coleta o gol mais atual do placar (geralmente CURRENT ou FT/Fim do tempo regulamentar)
                if p_id == home_id:
                    home_score = int(goals) if goals is not None else None
                elif p_id == away_id:
                    away_score = int(goals) if goals is not None else None

            cleaned_fixture = {
                "fixture_id": int(fixture.get("id")),
                "date": fixture.get("starting_at"),
                "status": fixture.get("result_info") or "Agendado",
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "venue": venue.get("name") if venue else "Estádio Indefinido",
                "winner_id": int(fixture.get("winner_id")) if fixture.get("winner_id") is not None else None,
                "updated_at": datetime.utcnow().isoformat()
            }
            cleaned_fixtures.append(cleaned_fixture)

        # 3. Salva a versão estruturada localmente
        silver_dir = "/opt/airflow/data/silver"
        os.makedirs(silver_dir, exist_ok=True)
        pd.DataFrame(cleaned_fixtures).to_csv(os.path.join(silver_dir, "fixtures.csv"), index=False, encoding="utf-8")

        # 4. Upsert no Supabase
        supabase_hook.upsert_records("silver_fixtures", cleaned_fixtures)
        return len(cleaned_fixtures)

    @task(task_id="complete_silver", outlets=[SILVER_ASSET])
    def complete_silver():
        """
        Tarefa de encerramento que dispara a alteração do Dataset Silver,
        ativando em cascata a execução da Gold.
        """
        print("Silver Processing completa com sucesso. Dataset atualizado para gatilho Gold.")

    # Fluxos de execução paralelos
    t_squad = process_squad()
    t_fixtures = process_fixtures()

    [t_squad, t_fixtures] >> complete_silver()

sportmonks_silver_pipeline()
