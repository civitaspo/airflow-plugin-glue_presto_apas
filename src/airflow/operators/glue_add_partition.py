import logging
from typing import Dict, List

from airflow.models import BaseOperator

from airflow.hooks.glue_presto_apas import GlueDataCatalogHook

OverwriteMode = "overwrite"
ErrorIfExistsMode = "error_if_exists"
SkipIfExistsMode = 'skip_if_exists'

AvailableModes = [
    OverwriteMode,
    ErrorIfExistsMode,
    SkipIfExistsMode,
]


class GlueAddPartitionOperator(BaseOperator):
    def __init__(
            self,
            db: str,
            table: str,
            partition_kv: Dict[str, str],
            location: str = None,
            mode: str = 'overwrite',
            catalog_id: str = None,
            catalog_region_name: str = None,
            presto_conn_id: str = 'presto_default',
            aws_conn_id: str = 'aws_default',
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.table = table
        self.location = location
        self.partition_kv = partition_kv
        self.mode = mode
        self.catalog_id = catalog_id
        self.catalog_region_name = catalog_region_name
        self.presto_conn_id = presto_conn_id
        self.aws_conn_id = aws_conn_id

        if mode not in AvailableModes:
            raise ConfigError(f"Save mode[{mode}] is unsupported."
                              f" Supported save modes are {AvailableModes}.")

    def _glue_data_catalog_hook(self) -> GlueDataCatalogHook:
        return GlueDataCatalogHook(aws_conn_id=self.aws_conn_id,
                                   catalog_id=self.catalog_id,
                                   region_name=self.catalog_region_name)

    def _gen_partition_location(self):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()
        partition_elems: List[str] = []
        for pk in glue.get_partition_keys(db=self.db, name=self.table):
            if pk not in self.partition_kv:
                raise ConfigError(f"Partition key[{pk}] is not found in 'partition_kv'")
            partition_elems.append(f"{pk}={self.partition_kv[pk]}")
        table_location = glue.get_table_location(db=self.db, name=self.table)
        if not table_location.endswith('/'):
            table_location = table_location + '/'
        return table_location + '/'.join(partition_elems)

    def pre_execute(self, context):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        if not glue.does_database_exists(name=self.db):
            raise ConfigError(f"DB[{self.db}] does not exist.")
        if not glue.does_table_exists(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] does not exist.")
        if not glue.get_partition_keys(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] does not have partition keys.")
        if not self.location:
            self.location = self._gen_partition_location()
        if not self.location.endswith('/'):
            self.location = self.location + '/'

    def execute(self, context):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        if not glue.does_partition_exists(db=self.db,
                                          table_name=self.table,
                                          partition_values=list(self.partition_kv.values())):
            glue.create_partition(db=self.db,
                                  table_name=self.table,
                                  partition_values=list(self.partition_kv.values()),
                                  location=self.location)
            logging.info(f"Partition[{self.partition_kv}] is created.")
            return

        if self.mode == ErrorIfExistsMode:
            raise ConfigError(f"Partition[{self.partition_kv}] already exists.")
        elif self.mode == SkipIfExistsMode:
            logging.info(f"Partition[{self.partition_kv}] already exists. Skip to add a partition.")
            return
        elif self.mode == OverwriteMode:
            glue.update_partition(db=self.db,
                                  table_name=self.table,
                                  partition_values=list(self.partition_kv.values()),
                                  location=self.location)
        else:
            raise UnknownError()
        logging.info(f"Partition[{self.partition_kv}] is updated.")


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class UnknownError(Error):
    pass
