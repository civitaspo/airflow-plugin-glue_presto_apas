import logging
import re
from typing import Dict, List

from airflow.hooks.S3_hook import S3Hook
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
    template_fields = [
        'db',
        'table',
        'partition_keys',
        'partition_values',
        'location',
    ]

    def __init__(
            self,
            db: str,
            table: str,
            partition_kv: Dict[str, str],
            location: str = None,
            mode: str = 'overwrite',
            follow_location: bool = True,
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
        self.partition_keys = list(partition_kv.keys())
        self.partition_values = list(partition_kv.values())
        self.mode = mode
        self.follow_location = follow_location
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

    def _s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def _is_sufficient_partition_kv(self) -> bool:
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()
        glue_pks = glue.get_partition_keys(db=self.db, name=self.table)
        if len(glue_pks) != len(self.partition_keys):
            logging.error(f"partition_kv must includes keys[{glue_pks}]")
            return False
        for pk in glue_pks:
            if pk not in self.partition_keys:
                logging.error(f"Partition keys in Glue{glue_pks}")
                return False
        return True

    def _get_ordered_partition_kv(self):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()
        ordered_partition_values = []
        for pk in glue.get_partition_keys(db=self.db, name=self.table):
            ordered_partition_values.append({
                'key': pk,
                'value': self.partition_values[self.partition_keys.index(pk)],
            })
        return ordered_partition_values

    def _gen_partition_location(self):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()
        table_location = glue.get_table_location(db=self.db, name=self.table)
        if not table_location.endswith('/'):
            table_location = table_location + '/'

        partition_elems: List[str] = []
        for h in self._get_ordered_partition_kv():
            partition_elems.append(f"{h['key']}={h['value']}")

        return table_location + '/'.join(partition_elems)

    def _does_location_exists(self):
        s3: S3Hook = self._s3_hook()
        if not self.location:
            raise ConfigError(f"'location' is not set")
        bucket, prefix = self._extract_s3_uri(self.location)
        return s3.check_for_prefix(bucket_name=bucket, prefix=prefix, delimiter='/')

    @staticmethod
    def _extract_s3_uri(uri) -> (str, str):
        m = re.search('^s3://([^/]+)/(.+)', uri)
        if not m:
            raise Error(f"URI[{uri}] is invalid for S3.")
        bucket = m.group(1)
        prefix = m.group(2)
        return bucket, prefix

    def pre_execute(self, context):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        if not glue.does_database_exists(name=self.db):
            raise ConfigError(f"DB[{self.db}] does not exist.")
        if not glue.does_table_exists(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] does not exist.")
        if not glue.get_partition_keys(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] does not have partition keys.")
        if not self._is_sufficient_partition_kv():
            raise ConfigError(f"partition keys{self.partition_keys} and partition values{self.partition_values} are insufficient.")
        if not self.location:
            self.location = self._gen_partition_location()
        if not self.location.endswith('/'):
            self.location = self.location + '/'

    def execute(self, context):
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()
        ordered_partition_kv = self._get_ordered_partition_kv()
        ordered_partition_values = []
        for h in ordered_partition_kv:
            ordered_partition_values.append(h["value"])

        if self.follow_location:
            if not self._does_location_exists():
                if not glue.does_partition_exists(db=self.db,
                                                  table_name=self.table,
                                                  partition_values=ordered_partition_values):
                    logging.info(f"Skip partitioning because Location[{self.location}] does not exist.")
                    return
                logging.info(f"Delete Partition{ordered_partition_kv}"
                             f" because Location[{self.location}] does not exist.")
                glue.delete_partition(db=self.db,
                                      table_name=self.table,
                                      partition_values=ordered_partition_values)
                return

        if not glue.does_partition_exists(db=self.db,
                                          table_name=self.table,
                                          partition_values=ordered_partition_values):
            glue.create_partition(db=self.db,
                                  table_name=self.table,
                                  partition_values=ordered_partition_values,
                                  location=self.location)
            logging.info(f"Partition{ordered_partition_kv} is created.")
            return

        if self.mode == ErrorIfExistsMode:
            raise ConfigError(f"Partition{ordered_partition_kv} already exists.")
        elif self.mode == SkipIfExistsMode:
            logging.info(f"Partition{ordered_partition_kv} already exists. Skip to add a partition.")
            return
        elif self.mode == OverwriteMode:
            glue.update_partition(db=self.db,
                                  table_name=self.table,
                                  partition_values=ordered_partition_values,
                                  location=self.location)
        else:
            raise UnknownError()
        logging.info(f"Partition{ordered_partition_kv}, Location[{self.location}] is updated.")


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class UnknownError(Error):
    pass
