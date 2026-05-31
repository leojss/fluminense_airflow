import os
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Importa o Hook do Supabase do diretório de plugins
from hooks.supabase_hook import SupabaseHook

# Define o Asset de entrada (Silver)
SILVER_ASSET = Asset("file:///opt/airflow/data/silver_complete")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="sportmonks_gold_dag",
    default_args=default_args,
    description="Camada Gold: Geração de indicadores de performance analíticos no Supabase",
    schedule=[SILVER_ASSET], # Dispara automaticamente ao atualizar a camada Silver
    start_date=datetime(2026, 5, 25),
    catchup=False,
    tags=["medallion", "gold", "fluminense"],
)
def sportmonks_gold_pipeline():

    @task(task_id="generate_squad_metrics")
    def generate_squad_metrics():
        """
        Lê a tabela silver_squad, calcula o número total de jogadores e a idade média
        por posição de jogo, e salva na tabela gold_squad_summary.
        """
        supabase_hook = SupabaseHook()
        
        # 1. Consulta todos os jogadores estruturados na Silver do Supabase
        silver_players = supabase_hook.select_records("silver_squad")
        
        if not silver_players:
            # Fallback para o arquivo local
            local_path = "/opt/airflow/data/silver/squad.csv"
            if os.path.exists(local_path):
                self.log.info("Carregando elenco da Silver a partir do backup CSV local...")
                df = pd.read_csv(local_path)
            else:
                raise AirflowException("Nenhum registro encontrado na camada Silver (Supabase ou Local).")
        else:
            df = pd.DataFrame(silver_players)

        if df.empty:
            raise AirflowException("A tabela de elenco Silver está vazia. Não é possível gerar métricas.")

        # 2. Calcula a idade de cada jogador
        df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")
        current_time = pd.Timestamp.utcnow().tz_localize(None)
        df["age"] = (current_time - df["birth_date"]).dt.days / 365.25

        # 3. Agrupa por posição para obter contagem e idade média
        summary_df = df.groupby("position").agg(
            player_count=("player_id", "count"),
            average_age=("age", "mean")
        ).reset_index()

        # Limpa valores nulos na média de idade
        summary_df["average_age"] = summary_df["average_age"].fillna(0).round(2)
        summary_df["updated_at"] = datetime.utcnow().isoformat()

        # Converte para dicionário para inserção no banco
        gold_squad_records = summary_df.to_dict(orient="records")

        # 4. Grava os dados na Gold local para backup
        gold_dir = "/opt/airflow/data/gold"
        os.makedirs(gold_dir, exist_ok=True)
        summary_df.to_csv(os.path.join(gold_dir, "squad_summary.csv"), index=False, encoding="utf-8")

        # 5. UPSERT no Supabase
        supabase_hook.upsert_records("gold_squad_summary", gold_squad_records)
        return len(gold_squad_records)

    @task(task_id="generate_fixtures_metrics")
    def generate_fixtures_metrics():
        """
        Lê a tabela silver_fixtures, calcula o desempenho (jogos, vitórias, empates, gols)
        da equipe orquestrada (Fluminense) e salva na tabela gold_fixtures_summary.
        """
        supabase_hook = SupabaseHook()
        team_name = Variable.get("TEAM_SEARCH_NAME", default_var="Fluminense")
        
        # 1. Consulta todas as partidas estruturadas na Silver do Supabase
        silver_fixtures = supabase_hook.select_records("silver_fixtures")
        
        if not silver_fixtures:
            local_path = "/opt/airflow/data/silver/fixtures.csv"
            if os.path.exists(local_path):
                self.log.info("Carregando partidas da Silver a partir do backup CSV local...")
                df = pd.read_csv(local_path)
            else:
                raise AirflowException("Nenhum registro de partida encontrado na camada Silver (Supabase ou Local).")
        else:
            df = pd.DataFrame(silver_fixtures)

        if df.empty:
            raise AirflowException("A tabela de partidas Silver está vazia. Não é possível gerar métricas.")

        # 2. Filtra partidas que já ocorreram (possuem placar registrado)
        # Se os placares estão nulos, a partida ainda não ocorreu (agendada)
        played_games = df[df["home_score"].notna() & df["away_score"].notna()].copy()
        
        if played_games.empty:
            # Caso não tenha jogos ocorridos, cria um registro zerado
            self.log.warning("Nenhum jogo concluído encontrado na Silver. Gerando registro com métricas zeradas.")
            gold_record = {
                "team_name": team_name,
                "total_games": 0,
                "wins": 0,
                "draws": 0,
                "losses": 0,
                "goals_scored": 0,
                "goals_conceded": 0,
                "updated_at": datetime.utcnow().isoformat()
            }
        else:
            # 3. Identifica se o Fluminense jogou como Mandante ou Visitante
            played_games["is_home"] = played_games["home_team"].str.contains(team_name, case=False, na=False)
            played_games["is_away"] = played_games["away_team"].str.contains(team_name, case=False, na=False)

            # Filtra apenas jogos que envolvem a equipe buscada
            our_games = played_games[played_games["is_home"] | played_games["is_away"]].copy()

            if our_games.empty:
                raise AirflowException(f"Nenhum jogo concluído envolvendo o time '{team_name}' foi encontrado.")

            # 4. Calcula Gols Marcados e Sofridos
            our_games["goals_scored"] = our_games.apply(
                lambda row: row["home_score"] if row["is_home"] else row["away_score"], axis=1
            )
            our_games["goals_conceded"] = our_games.apply(
                lambda row: row["away_score"] if row["is_home"] else row["home_score"], axis=1
            )

            # 5. Calcula Resultados (Vitórias, Empates e Derrotas)
            our_games["win"] = our_games.apply(
                lambda row: (row["is_home"] and row["home_score"] > row["away_score"]) or 
                            (row["is_away"] and row["away_score"] > row["home_score"]), axis=1
            )
            our_games["loss"] = our_games.apply(
                lambda row: (row["is_home"] and row["home_score"] < row["away_score"]) or 
                            (row["is_away"] and row["away_score"] < row["home_score"]), axis=1
            )
            our_games["draw"] = our_games["home_score"] == our_games["away_score"]

            # 6. Sumariza as métricas da equipe
            gold_record = {
                "team_name": team_name,
                "total_games": int(our_games.shape[0]),
                "wins": int(our_games["win"].sum()),
                "draws": int(our_games["draw"].sum()),
                "losses": int(our_games["loss"].sum()),
                "goals_scored": int(our_games["goals_scored"].sum()),
                "goals_conceded": int(our_games["goals_conceded"].sum()),
                "updated_at": datetime.utcnow().isoformat()
            }

        # 7. Grava dados na Gold local para backup
        gold_dir = "/opt/airflow/data/gold"
        os.makedirs(gold_dir, exist_ok=True)
        pd.DataFrame([gold_record]).to_csv(os.path.join(gold_dir, "fixtures_summary.csv"), index=False, encoding="utf-8")

        # 8. UPSERT no Supabase
        supabase_hook.upsert_records("gold_fixtures_summary", [gold_record])
        return 1

    @task(task_id="complete_gold")
    def complete_gold():
        """
        Finaliza a orquestração do pipeline analítico Medallion.
        """
        print("Gold Processing concluída com sucesso! Indicadores atualizados no Supabase.")

    # Fluxos de execução paralelos
    t_squad = generate_squad_metrics()
    t_fixtures = generate_fixtures_metrics()

    [t_squad, t_fixtures] >> complete_gold()

sportmonks_gold_pipeline()
