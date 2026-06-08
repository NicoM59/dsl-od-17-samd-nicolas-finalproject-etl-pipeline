import os
import sys
from unittest.mock import MagicMock

# Mock all Airflow modules so etl.py can be imported without Airflow installed.
# AirflowSkipException must be a real Exception subclass so `raise` works.
class AirflowSkipException(Exception):
    pass

_exceptions_mock = MagicMock()
_exceptions_mock.AirflowSkipException = AirflowSkipException

sys.modules.update({
    "airflow":                                        MagicMock(),
    "airflow.exceptions":                             _exceptions_mock,
    "airflow.operators":                              MagicMock(),
    "airflow.operators.python":                       MagicMock(),
    "airflow.providers":                              MagicMock(),
    "airflow.providers.amazon":                       MagicMock(),
    "airflow.providers.amazon.aws":                   MagicMock(),
    "airflow.providers.amazon.aws.hooks":             MagicMock(),
    "airflow.providers.amazon.aws.hooks.s3":          MagicMock(),
    "airflow.providers.postgres":                     MagicMock(),
    "airflow.providers.postgres.hooks":               MagicMock(),
    "airflow.providers.postgres.hooks.postgres":      MagicMock(),
    "airflow.providers.postgres.operators":           MagicMock(),
    "airflow.providers.postgres.operators.postgres":  MagicMock(),
})

# Make dags/ importable so test files can do `from etl import ...`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
