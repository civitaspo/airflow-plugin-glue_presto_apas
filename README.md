airflow-plugin-glue_presto_apas
==========================

[![PyPi](https://img.shields.io/pypi/v/airflow-plugin-glue-presto-apas.svg)](https://pypi.org/project/airflow-plugin-glue-presto-apas/)

An Airflow Plugin to **Add a Partition As Select(APAS)** on Presto that uses Glue Data Catalog as a Hive metastore.

# Usage

```
from datetime import timedelta

import airflow
from airflow.models import DAG

from airflow.operators.glue_add_partition import GlueAddPartitionOperator
from airflow.operators.glue_presto_apas import GluePrestoApasOperator

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='example-dag',
    schedule_interval='0 0 * * *',
    default_args=args,
)

GluePrestoApasOperator(task_id='example-task-1',
                       db='example_db',
                       table='example_table',
                       sql='example.sql',
                       partition_kv={
                           'table_schema': 'example_db',
                           'table_name': 'example_table'
                       },
                       catalog_region_name='ap-northeast-1',
                       dag=dag,
                       )

GlueAddPartitionOperator(task_id='example-task-2',
                         db='example_db',
                         table='example_table',
                         partition_kv={
                             'table_schema': 'example_db',
                             'table_name': 'example_table'
                         },
                         catalog_region_name='ap-northeast-1',
                         dag=dag,
                         )

if __name__ == "__main__":
    dag.cli()
```

# Configuration

## glue_presto_apas.GluePrestoApasOperator

- **db**: database name for parititioning (string, required)
- **table**: table name for parititioning (string, required)
- **sql**: sql file name for selecting data (string, required)
- **fmt**: data format when storing data (string, default = `parquet`)
- **additional_properties**: additional properties for creating table. (dict[string, string], optional)
- **location**: location for the data (string, default = auto generated by hive repairable way)
- **partition_kv**: key values for partitioning (dict[string, string], required)
- **save_mode**: mode when storing data (string, default = `overwrite`, available values are `skip_if_exists`, `error_if_exists`, `ignore`, `overwrite`)
- **catalog_id**: glue data catalog id if you use a catalog different from account/region default catalog. (string, optional)
- **catalog_region_name**: glue data catalog region if you use a catalog different from account/region default catalog. (string, us-east-1 )
- **presto_conn_id**: connection id for presto (string, default = 'presto_default')
- **aws_conn_id**: connection id for aws (string, default = 'aws_default')

Templates can be used in the options[**db**, **table**, **sql**, **location**, **partition_kv**].

## glue_add_partition.GlueAddPartitionOperator

- **db**: database name for parititioning (string, required)
- **table**: table name for parititioning (string, required)
- **location**: location for the data (string, default = auto generated by hive repairable way)
- **partition_kv**: key values for partitioning (dict[string, string], required)
- **mode**: mode when storing data (string, default = `overwrite`, available values are `skip_if_exists`, `error_if_exists`, `overwrite`)
- **follow_location**: Skip to add a partition and drop the partition if the location does not exist. (boolean, default = `True`)
- **catalog_id**: glue data catalog id if you use a catalog different from account/region default catalog. (string, optional)
- **catalog_region_name**: glue data catalog region if you use a catalog different from account/region default catalog. (string, us-east-1 )
- **aws_conn_id**: connection id for aws (string, default = 'aws_default')

Templates can be used in the options[**db**, **table**, **location**, **partition_kv**].

# Development

## Run Example

```
PRESTO_HOST=${YOUR PRESTO HOST} PRESTO_PORT=${YOUR PRESTO PORT} ./run-example.sh
```

## Release

```
poetry publish --build
```
