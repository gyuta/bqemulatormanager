# BiqQueryEmulator Manager


this package is wrapper of [bigquery-emulator](https://github.com/goccy/bigquery-emulator) which provides us BigQuery mock working in local machine.

using this package, you can

- do unit test of your sql
- download the schema of big query, and use it to make test data

## usage
1. following [instruction](https://github.com/goccy/bigquery-emulator#install),  download `bigquery-emulator` command.

2. install this package. 
```
pip install bqemulatormanager
```

3. test your sql.
```python
import bqemulatormanager as bqem
import pandas as pd

manager = bqem.Manager(project='test', schema_path='resources/schema_example.yaml')

with manager:
    data = pd.DataFrame([
        {'id': 1, 'name': 'sato'},
        {'id': 2, 'name': 'yamada'}
    ])

    manager.load(data, 'dataset1.table_a')

    sql = 'SELECT id, name FROM `dataset1.table_a`'

    df = manager.query(sql)
print(df)
```

unit test example is [here](https://github.com/gyuta/bqemulatormanager/blob/main/examples/testing.py).

### automatically detect schema
When called `Manager.load`, `SchemaManager` search correspond table schema from `schema_path` (default is `master_schema.yaml`).

If schema definition canot be found, `SchemaManager` request it from BigQuery in production environmant and update master schema file.