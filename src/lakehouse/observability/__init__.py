"""Pipeline observability (ticket #23): run ledger, structured logs, freshness/volume alerts.

Spark-free on purpose (pyarrow + PyYAML only) so it runs in the plain ``pytest`` job and inside
Airflow workers. Public surface: :func:`track_run`, :class:`Ledger`, :func:`evaluate`,
:class:`Alert`. Stable output schema: ``docs/observability.md``.
"""

from .alerts import Alert, dispatch
from .checks import evaluate
from .config import load_config
from .ledger import Ledger
from .schema import SCHEMA_VERSION
from .tracker import track_run

__all__ = [
    "SCHEMA_VERSION",
    "Alert",
    "Ledger",
    "dispatch",
    "evaluate",
    "load_config",
    "track_run",
]
