from enum import Flag, auto
import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PrestoApasOperator(BaseOperator):
    template_fields = ['sql']
    template_ext = ['sql']

    class SaveMode(Flag):
        Overwrite = auto()
        Ignore = auto()
        ErrorIfExists = auto()

    @apply_defaults
    def __init__(
            self,
            schema: str,
            table: str,
            sql: str,
            location: str = None,
            save_mode: SaveMode = SaveMode.Overwrite,
            presto_conn_id: str = 'presto_default',
            aws_conn_id: str = 'aws_default',
            table_properties: dict = None,
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
        self.presto_conn_id = presto_conn_id
        self.aws_conn_id = aws_conn_id
        self.table_properties = table_properties

    def _presto_hook(self) -> PrestoHook:
        return PrestoHook(presto_conn_id=self.presto_conn_id)

    def _s3_hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def pre_execute(self, context) -> None:
        pass

    def execute(self, context) -> None:
        pass

    def post_execute(self, context, result=None) -> None:
        pass

    def on_kill(self) -> None:
        pass


class PrestoApasPlugin(AirflowPlugin):
    name = 'presto_apas'
    operators = [PrestoApasOperator]
