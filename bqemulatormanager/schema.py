import os
from typing import List

import yaml
from google.cloud import bigquery


class SchemaManager:

    def __init__(self, master_path: str = 'master_schema.yaml', client=None):
        self.client = client
        self.master_path = master_path
        self.change_flg = False
        if os.path.isfile(master_path):
            with open(master_path) as f:
                master_schema = yaml.safe_load(f)
        else:
            master_schema = {}
        self.master_schema = master_schema

    def get_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        project, dataset, table = table_id.split('.')

        schema = self.master_schema.get(project, {}).get(dataset, {}).get(table, {})

        if schema == {}:
            schema = self._get_schema_from_production(table_id)
            deepupdate(self.master_schema, {project: {dataset: {table: [s._properties for s in schema]}}})
            self.change_flg = True
            return schema
        else:
            return [bigquery.SchemaField.from_api_repr(s) for s in schema]

    def _get_schema_from_production(self, table_id: str) -> List[bigquery.SchemaField]:
        if not self.client:
            raise Exception('set client')
        table = self.client.get_table(table_id)
        return table.schema

    def save(self):
        if self.change_flg:
            with open(self.master_path, 'w') as f:
                yaml.dump(self.master_schema, f, encoding='utf8', allow_unicode=True)

    def __del__(self):
        self.save()


def deepupdate(dict_base, other):
    for k, v in other.items():
        if isinstance(v, dict) and k in dict_base:
            deepupdate(dict_base[k], v)
        else:
            dict_base[k] = v
