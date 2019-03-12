from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PrestoApasOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context) -> None:
        pass


class PrestoApasPlugin(AirflowPlugin):
    name = 'presto_apas'
    operators = [PrestoApasOperator]
