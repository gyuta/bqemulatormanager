from typing import List

import pandas as pd
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from bqemulatormanager.emulator import Emulator
from bqemulatormanager.schema import SchemaManager


class Manager:

    def __init__(self, project: str = 'test', port: int = 9050, schema_path: str = 'master_schema.yaml'):
        self.emulator = Emulator(project, port)
        self.client = self._make_client(project, port)

        prod_client = bigquery.Client(project)

        self.schema_manager = SchemaManager(client=prod_client, master_path=schema_path)
        self.structure = {}
        self.project_name = project

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        del self.emulator
        del self.schema_manager

    @staticmethod
    def _make_client(project_name: str, port: int) -> bigquery.Client:
        client_options = ClientOptions(api_endpoint=f"http://0.0.0.0:{port}")
        client = bigquery.Client(
            project_name,
            client_options=client_options,
            credentials=AnonymousCredentials(),
        )
        return client

    def load(self, data, path):
        dataset, table = path.split('.')
        if dataset not in self.structure:
            self.create_dataset(dataset)

        if table not in self.structure[dataset]:
            self.create_table(dataset, table, [])

        table = self.client.get_table(f'{self.project_name}.{path}')
        self.client.insert_rows_from_dataframe(table, data)

    def create_dataset(self, dataset_name: str, exists_ok=True):
        dataset = bigquery.Dataset(f'{self.project_name}.{dataset_name}')
        self.client.create_dataset(dataset)
        self.structure[dataset_name] = {}

    def create_table(self, dataset_name: str, table_name: str, schema: List[bigquery.SchemaField]):
        if schema == []:
            schema = self.schema_manager.get_schema(f'{self.project_name}.{dataset_name}.{table_name}')

        table = bigquery.Table(f'{self.project_name}.{dataset_name}.{table_name}', schema=schema)
        self.client.create_table(table)
        self.structure[dataset_name][table_name] = {}

    def query(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).to_dataframe(create_bqstorage_client=False)
