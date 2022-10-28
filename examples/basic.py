import bqemulatormanager as bqem
import pandas as pd

manager = bqem.Manager(project='test', schema_path='resources/schema_basic.yaml')

with manager:
    data = pd.DataFrame([
        {'id': 1, 'name': 'sato'},
        {'id': 2, 'name': 'yamada'}
    ])

    manager.load(data, 'dataset1.table_a')

    sql = 'SELECT id, name FROM `dataset1.table_a`'

    df = manager.query(sql)
print(df)
