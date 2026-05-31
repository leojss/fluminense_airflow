import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

class SupabaseHook(BaseHook):
    """
    Hook customizado para interagir com o Supabase via API REST (PostgREST).
    """
    def __init__(self):
        super().__init__()
        # Recupera as credenciais diretamente das Variáveis do Airflow (injetadas via .env)
        try:
            self.supabase_url = Variable.get("SUPABASE_URL").rstrip('/')
            self.supabase_key = Variable.get("SUPABASE_KEY")
        except KeyError as e:
            raise AirflowException(f"Erro ao carregar credenciais do Supabase das Variáveis do Airflow: {e}")
        
        self.base_url = f"{self.supabase_url}/rest/v1"

    def _get_headers(self, prefer_header: str = None) -> dict:
        """
        Retorna os cabeçalhos de autenticação exigidos pelo Supabase.
        """
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json"
        }
        if prefer_header:
            headers["Prefer"] = prefer_header
        return headers

    def insert_records(self, table_name: str, records: list) -> list:
        """
        Insere uma lista de registros (dicionários) em uma tabela do Supabase.
        """
        if not records:
            self.log.info("Nenhum registro fornecido para inserção.")
            return []

        url = f"{self.base_url}/{table_name}"
        headers = self._get_headers(prefer_header="return=representation")

        self.log.info(f"Inserindo {len(records)} registros na tabela '{table_name}' do Supabase...")
        try:
            response = requests.post(url, json=records, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Erro ao inserir registros no Supabase: {e}")
            if response is not None:
                self.log.error(f"Detalhes do erro do Supabase: {response.text}")
            raise AirflowException(f"Falha de gravação no Supabase: {e}")

    def upsert_records(self, table_name: str, records: list) -> list:
        """
        Realiza UPSERT (insere ou atualiza em caso de conflito de chave primária) de registros no Supabase.
        """
        if not records:
            self.log.info("Nenhum registro fornecido para upsert.")
            return []

        url = f"{self.base_url}/{table_name}"
        # No PostgREST (Supabase), o upsert é configurado via Prefer header
        headers = self._get_headers(prefer_header="resolution=merge-duplicates, return=representation")

        self.log.info(f"Executando UPSERT de {len(records)} registros na tabela '{table_name}' do Supabase...")
        try:
            response = requests.post(url, json=records, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Erro ao executar upsert no Supabase: {e}")
            if response is not None:
                self.log.error(f"Detalhes do erro do Supabase: {response.text}")
            raise AirflowException(f"Falha de upsert no Supabase: {e}")

    def select_records(self, table_name: str, select_query: str = "*", filters: dict = None) -> list:
        """
        Consulta registros de uma tabela do Supabase.
        Exemplo de filtros: {"coluna": "eq.valor"} (formato PostgREST)
        """
        url = f"{self.base_url}/{table_name}"
        params = {"select": select_query}
        if filters:
            params.update(filters)

        headers = self._get_headers()

        self.log.info(f"Consultando registros na tabela '{table_name}' do Supabase...")
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Erro ao consultar registros no Supabase: {e}")
            if response is not None:
                self.log.error(f"Detalhes do erro: {response.text}")
            raise AirflowException(f"Falha de leitura no Supabase: {e}")
