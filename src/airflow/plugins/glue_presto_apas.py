from airflow.plugins_manager import AirflowPlugin

from airflow.hooks.glue_presto_apas import GlueDataCatalogHook
from airflow.operators.glue_presto_apas import GluePrestoApasOperator


class GluePrestoApasPlugin(AirflowPlugin):
    name = 'glue_presto_apas'
    operators = [GluePrestoApasOperator]
    hooks = [GlueDataCatalogHook]
