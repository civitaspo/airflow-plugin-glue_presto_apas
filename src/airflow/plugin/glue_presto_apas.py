from typing import List, Dict
from enum import Flag, auto
import logging

from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook
from airflow.hooks.presto_hook import PrestoHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class GluePrestoApasOperator(BaseOperator):
    template_fields = ['sql']
    template_ext = ['sql']

    class SaveMode(Flag):
        DoNothingIfExists = auto()
        ErrorIfExists = auto()
        Ignore = auto()
        Overwrite = auto()

    class GlueApasHook(AwsGlueCatalogHook):
        def get_records(self, sql):
            raise NotImplementedError(f"{__class__}#get_records is not implemented")

        def get_pandas_df(self, sql):
            raise NotImplementedError(f"{__class__}#get_pandas_df is not implemented")

        def run(self, sql):
            raise NotImplementedError(f"{__class__}#run is not implemented")

        def __init__(
                self,
                aws_conn_id: str = 'aws_default',
                region_name: str = None,
                catalog_id: str = None,
                *args,
                **kwargs):
            self.catalog_id = '' if not catalog_id else catalog_id
            super().__init__(aws_conn_id=aws_conn_id, region_name=region_name, *args, **kwargs)

        def does_database_exists(self, name: str) -> bool:
            res = self.get_conn().get_database(
                # CatalogId=self.catalog_id,
                Name=name,
            )
            if res['Database']:
                return True
            else:
                return False

    @apply_defaults
    def __init__(
            self,
            schema: str,
            table: str,
            sql: str,
            location: str = None,
            save_mode: SaveMode = SaveMode.Overwrite,
            catalog_id: str = None,
            catalog_region_name: str = None,
            presto_conn_id: str = 'presto_default',
            aws_conn_id: str = 'aws_default',
            table_properties: Dict[str, str] = None,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        if table_properties is None:
            table_properties = {}
        self.schema = schema
        self.table = table
        self.sql = sql
        self.location = location
        self.save_mode = save_mode
        self.catalog_id = catalog_id
        self.catalog_region_name = catalog_region_name
        self.presto_conn_id = presto_conn_id
        self.aws_conn_id = aws_conn_id
        self.table_properties = table_properties

    def _presto_hook(self) -> PrestoHook:
        return PrestoHook(presto_conn_id=self.presto_conn_id)

    def _glue_apas_hook(self) -> GlueApasHook:
        return __class__.GlueApasHook(aws_conn_id=self.aws_conn_id,
                                      region_name=self.catalog_region_name,
                                      catalog_id=self.catalog_id)

    def _s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def pre_execute(self, context) -> None:
        presto: PrestoHook = self._presto_hook()
        glue: __class__.GlueApasHook = self._glue_apas_hook()
        s3: S3Hook = self._s3_hook()

        if not glue.does_database_exists(name=self.schema):
            raise SchemaNotFoundError(f"Schema[{self.schema}] is not found.")

        if not presto.get_first(f"SELECT 1 "
                                f"FROM information_schema.schemata "
                                f"WHERE schema_name = '{self.schema}'"):
            raise SchemaNotFoundError(f"Schema[{self.schema}] is not found.")

        if not presto.get_first(f"SELECT 1 "
                                f"FROM information_schema.tables "
                                f"WHERE table_schema = '{self.schema}' "
                                f"AND table_name = '{self.table}'"):
            raise TableNotFoundError(f"Table[{self.schema}.{self.table}] is not found.")

        partition_keys: List[str] = []
        for col in presto.get_records(f"SELECT column_name "
                                      f"FROM information_schema.columns "
                                      f"WHERE table_schema = '{self.schema}' "
                                      f"AND table_name = '{self.table}' "
                                      f"AND extra_info = 'partition key' "
                                      f"ORDER BY ordinal_position ASC"):
            partition_keys.append(col[0])
        if not partition_keys:
            raise PartitionKeyNotFoundError(f"Table[{self.schema}.{self.table}] does not have partition keys.")
        logging.info(f"Table[{self.schema}.{self.table}] has partitions{partition_keys}")

        pass

    def execute(self, context) -> None:
        pass

    def post_execute(self, context, result=None) -> None:
        pass

    def on_kill(self) -> None:
        pass


class GluePrestoApasPlugin(AirflowPlugin):
    name = 'glue_presto_apas'
    operators = [GluePrestoApasOperator]


# ===== private =====

class Error(Exception):
    pass


class SchemaNotFoundError(Error):
    pass


class TableNotFoundError(Error):
    pass


class PartitionKeyNotFoundError(Error):
    pass


class NotImplementedError(Error):
    pass
