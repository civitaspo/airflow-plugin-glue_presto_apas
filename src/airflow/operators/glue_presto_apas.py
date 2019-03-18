import logging
import random
import re
import string
from typing import Dict, List
from datetime import datetime, timezone

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.glue_presto_apas import GlueDataCatalogHook

SaveModeDoNothingIfExists = 'do_nothing_if_exists'
SaveModeErrorIfExists = 'error_if_exists'
SaveModeIgnore = 'ignore'
SaveModeOverwrite = 'overwrite'

SupportedSaveModes = [
    SaveModeDoNothingIfExists,
    SaveModeErrorIfExists,
    SaveModeIgnore,
    SaveModeOverwrite,
]


class GluePrestoApasOperator(BaseOperator):
    template_fields = ['sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(
            self,
            db: str,
            table: str,
            sql: str,
            partition_kv: Dict[str, str],
            fmt: str = 'parquet',
            additional_properties: Dict[str, str] = {},
            location: str = None,
            save_mode: str = 'overwrite',
            catalog_id: str = None,
            catalog_region_name: str = None,
            presto_conn_id: str = 'presto_default',
            aws_conn_id: str = 'aws_default',
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.table = table
        self.sql = sql
        self.fmt = fmt
        self.additional_properties = additional_properties
        self.location = location
        self.partition_kv = partition_kv
        self.save_mode = save_mode
        self.catalog_id = catalog_id
        self.catalog_region_name = catalog_region_name
        self.presto_conn_id = presto_conn_id
        self.aws_conn_id = aws_conn_id

        if save_mode not in SupportedSaveModes:
            raise ConfigError(f"Save mode[{save_mode}] is unsupported."
                              f" Supported save modes are {SupportedSaveModes}.")
        for p in ['format', 'external_location']:
            if p in additional_properties:
                raise ConfigError(f"Additional properties must not includes '{p}'"
                                  f" because this plugin uses.")

    def _presto_hook(self) -> PrestoHook:
        return PrestoHook(presto_conn_id=self.presto_conn_id)

    def _glue_data_catalog_hook(self) -> GlueDataCatalogHook:
        return GlueDataCatalogHook(aws_conn_id=self.aws_conn_id,
                                   region_name=self.catalog_region_name,
                                   catalog_id=self.catalog_id)

    def _s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    @staticmethod
    def _extract_s3_uri(uri) -> (str, str):
        m = re.search('^s3://([^/]+)/(.+)', uri)
        if not m:
            raise Error(f"URI[{uri}] is invalid for S3.")
        bucket = m.group(1)
        prefix = m.group(2)
        return bucket, prefix

    @staticmethod
    def _random_str(size: int = 10, chars: str = string.ascii_uppercase + string.digits) -> str:
        return ''.join(random.choice(chars) for _ in range(size))

    def pre_execute(self, context) -> None:
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        if not glue.does_database_exists(name=self.db):
            raise ConfigError(f"DB[{self.db}] is not found.")
        if not glue.does_table_exists(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] is not found.")

        partition_keys: List[str] = glue.get_partition_keys(db=self.db, name=self.table)
        if not partition_keys:
            raise ConfigError(f"Table[{self.db}.{self.table}] does not have partition keys.")
        logging.info(f"Table[{self.db}.{self.table}] has partitions{partition_keys}")

        if not self.location:
            partition_elems: List[str] = []
            for pk in partition_keys:
                if pk not in self.partition_kv:
                    raise ConfigError(f"Partition key[{pk}] is not found in 'partition_kv'")
                partition_elems.append(f"{pk}={self.partition_kv[pk]}")
            table_location = glue.get_table_location(db=self.db, name=self.table)
            if not table_location.endswith('/'):
                table_location = table_location + '/'
            self.location = table_location + '/'.join(partition_elems)

    def execute(self, context) -> None:
        s3: S3Hook = self._s3_hook()
        presto: PrestoHook = self._presto_hook()
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        bucket, prefix = self._extract_s3_uri(self.location)
        if not prefix.endswith('/'):
            prefix = prefix + '/'
        if s3.check_for_prefix(bucket_name=bucket, prefix=prefix, delimiter='/'):
            if self.save_mode == SaveModeDoNothingIfExists:
                logging.info(f"Skip this execution because location[{self.location}] exists"
                             f" and save_mode[{self.save_mode}] is defined.")
                return
            elif self.save_mode == SaveModeErrorIfExists:
                raise Error(f"Raise a exception because location[{self.location}] exists"
                            f" and save_mode[{self.save_mode}] is defined.")
            elif self.save_mode == SaveModeIgnore:
                logging.info(f"Continue the execution regardless that location[{self.location}] exists"
                             f" because save_mode[{self.save_mode}] is defined.")
            elif self.save_mode == SaveModeOverwrite:
                logging.info(f"Delete all objects in location[{self.location}]"
                             f" because save_mode[{self.save_mode}] is defined.")
                keys = s3.list_keys(bucket_name=bucket, prefix=prefix, delimiter='/')
                s3.delete_objects(bucket=bucket, keys=keys)
            else:
                raise UnknownError()

        tmp_table = f"__work_airflow_glue_presto_apas" \
            f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}" \
            f"_{self._random_str()}"

        # columns detection
        col_stmts = []
        try:
            presto.get_first(f"CREATE VIEW {self.db}.{tmp_table} AS {self.sql}")
            for c in presto.get_records(f"DESCRIBE {self.db}.{tmp_table}"):
                col_name = c[0]
                col_type = c[1]
                col_stmts.append(f"{col_name} {col_type}")
        finally:
            presto.get_first(f"DROP VIEW {self.db}.{tmp_table}")
        logging.info(f"Detect columns{col_stmts}")

        dummy_fname = '_DUMMY'
        try:
            # NOTE: Avoid `failed: External location must be a directory`.
            logging.info(f"Upload '{dummy_fname}' -> s3://{bucket}/{prefix + dummy_fname}")
            s3.load_string(string_data="", key=prefix + dummy_fname, bucket_name=bucket)

            props = self.additional_properties.copy()
            props['external_location'] = f"'{self.location}'"
            props['format'] = f"'{self.fmt}'"
            props_stmts = []
            for k, v in props.items():
                props_stmts.append(f"{k} = {v}")
            try:
                presto.get_first(f"CREATE TABLE {self.db}.{tmp_table} ( {','.join(col_stmts)} )"
                                 f"WITH ( {','.join(props_stmts)} )")
                presto.get_first(f"INSERT INTO {self.db}.{tmp_table} {self.sql}")
                if glue.does_partition_exists(db=self.db,
                                              table_name=self.table,
                                              partition_values=list(self.partition_kv.values())):
                    logging.info(f"Delete a partition[{self.partition_kv.items()}]")
                    glue.delete_partition(db=self.db,
                                          table_name=self.table,
                                          partition_values=list(self.partition_kv.values()))
                logging.info(f"Convert table[{self.db}.{tmp_table}]"
                             f" to partition[{self.partition_kv.items()}]")
                glue.convert_table_to_partition(src_db=self.db,
                                                src_table=tmp_table,
                                                dst_db=self.db,
                                                dst_table=self.table,
                                                partition_values=list(self.partition_kv.values()))
            finally:
                if glue.does_table_exists(db=self.db, name=tmp_table):
                    glue.delete_table(db=self.db, name=tmp_table)
        finally:
            s3.delete_objects(bucket, prefix + dummy_fname)


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class UnknownError(Error):
    pass
