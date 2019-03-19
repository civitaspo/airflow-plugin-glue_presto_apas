import logging
import random
import re
import string
from datetime import datetime, timezone
from typing import Dict, List

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.glue_presto_apas import GlueDataCatalogHook

SkipIfExistsSaveMode = 'skip_if_exists'
ErrorIfExistsSaveMode = 'error_if_exists'
IgnoreSaveMode = 'ignore'
OverwriteSaveMode = 'overwrite'

AvailableSaveModes = [
    SkipIfExistsSaveMode,
    ErrorIfExistsSaveMode,
    IgnoreSaveMode,
    OverwriteSaveMode,
]


class GluePrestoApasOperator(BaseOperator):
    template_fields = [
        'db',
        'table',
        'sql',
        'partition_keys',
        'partition_values',
        'location',
    ]
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
        self.partition_keys: List[str] = list(partition_kv.keys())
        self.partition_values: List[str] = list(partition_kv.values())
        self.save_mode = save_mode
        self.catalog_id = catalog_id
        self.catalog_region_name = catalog_region_name
        self.presto_conn_id = presto_conn_id
        self.aws_conn_id = aws_conn_id

        if save_mode not in AvailableSaveModes:
            raise ConfigError(f"Save mode[{save_mode}] is unsupported."
                              f" Supported save modes are {AvailableSaveModes}.")
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
        if not glue.get_partition_keys(db=self.db, name=self.table):
            raise ConfigError(f"Table[{self.db}.{self.table}] does not have partition keys.")
        if not self._is_sufficient_partition_kv():
            raise ConfigError(f"partition keys{self.partition_keys} and partition values{self.partition_values} are insufficient.")
        if not self.location:
            self.location = self._gen_partition_location()
        if not self.location.endswith('/'):
            self.location = self.location + '/'

    def _processable_check_n_prepare_location(self) -> bool:
        s3: S3Hook = self._s3_hook()

        bucket, prefix = self._extract_s3_uri(self.location)
        if not prefix.endswith('/'):
            prefix = prefix + '/'
        if s3.check_for_prefix(bucket_name=bucket, prefix=prefix, delimiter='/'):
            if self.save_mode == SkipIfExistsSaveMode:
                logging.info(f"Skip this execution because location[{self.location}] exists"
                             f" and save_mode[{self.save_mode}] is defined.")
                return False
            elif self.save_mode == ErrorIfExistsSaveMode:
                raise ConfigError(f"Raise a exception because location[{self.location}] exists"
                                  f" and save_mode[{self.save_mode}] is defined.")
            elif self.save_mode == IgnoreSaveMode:
                logging.info(f"Continue the execution regardless that location[{self.location}] exists"
                             f" because save_mode[{self.save_mode}] is defined.")
            elif self.save_mode == OverwriteSaveMode:
                logging.info(f"Delete all objects in location[{self.location}]"
                             f" because save_mode[{self.save_mode}] is defined.")
                keys = s3.list_keys(bucket_name=bucket, prefix=prefix, delimiter='/')
                s3.delete_objects(bucket=bucket, keys=keys)
            else:
                raise UnknownError()
        return True

    def _desc_columns_by_query(self, sql: str):
        presto: PrestoHook = self._presto_hook()
        tmp_table = f"__work_airflow_glue_presto_apas" \
            f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}" \
            f"_{self._random_str()}"
        columns: List[Dict[str, str]] = []
        try:
            presto.get_first(f"CREATE VIEW {self.db}.{tmp_table} AS {self.sql}")
            for c in presto.get_records(f"DESCRIBE {self.db}.{tmp_table}"):
                col_name = c[0]
                col_type = c[1]
                columns.append({
                    'name': col_name,
                    'type': col_type,
                })
        finally:
            presto.get_first(f"DROP VIEW {self.db}.{tmp_table}")
        return columns

    def _prepare_create_table_properties_stmt(self):
        props = self.additional_properties.copy()
        props['external_location'] = f"'{self.location}'"
        props['format'] = f"'{self.fmt}'"
        props_stmts = []
        for k, v in props.items():
            props_stmts.append(f"{k} = {v}")
        return ','.join(props_stmts)

    def execute(self, context) -> None:
        s3: S3Hook = self._s3_hook()
        presto: PrestoHook = self._presto_hook()
        glue: GlueDataCatalogHook = self._glue_data_catalog_hook()

        if not self._processable_check_n_prepare_location():
            return

        # columns detection
        col_stmts: List[str] = []
        for c in self._desc_columns_by_query(self.sql):
            col_stmts.append(f"{c['name']} {c['type']}")
        logging.info(f"Detect columns{col_stmts}")

        tmp_table = f"__work_airflow_glue_presto_apas" \
            f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}" \
            f"_{self._random_str()}"

        bucket, prefix = self._extract_s3_uri(self.location)
        ordered_partition_kv = self._get_ordered_partition_kv()
        ordered_partition_values = []
        for h in ordered_partition_kv:
            ordered_partition_values.append(h["value"])
        dummy_fname = '_DUMMY'
        try:
            # NOTE: Avoid `failed: External location must be a directory`.
            logging.info(f"Upload '{dummy_fname}' -> s3://{bucket}/{prefix + dummy_fname}")
            s3.load_string(string_data="", key=prefix + dummy_fname, bucket_name=bucket)

            try:
                prop_stmt = self._prepare_create_table_properties_stmt()
                presto.get_first(f"CREATE TABLE {self.db}.{tmp_table} ( {','.join(col_stmts)} )"
                                 f"WITH ( {prop_stmt} )")
                presto.get_first(f"INSERT INTO {self.db}.{tmp_table} {self.sql}")
                if glue.does_partition_exists(db=self.db,
                                              table_name=self.table,
                                              partition_values=ordered_partition_values):
                    logging.info(f"Delete a partition{ordered_partition_kv}")
                    glue.delete_partition(db=self.db,
                                          table_name=self.table,
                                          partition_values=ordered_partition_values)
                logging.info(f"Convert table[{self.db}.{tmp_table}]"
                             f" to partition{ordered_partition_kv}")
                glue.convert_table_to_partition(src_db=self.db,
                                                src_table=tmp_table,
                                                dst_db=self.db,
                                                dst_table=self.table,
                                                partition_values=ordered_partition_values)
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
