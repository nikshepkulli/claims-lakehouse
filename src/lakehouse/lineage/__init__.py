"""OpenLineage events for the lakehouse (ticket #24). Spark-free: stdlib + PyYAML + pyarrow.

The layer jobs are not modified. Events come from a CLI wrapper (``python -m lakehouse.lineage
wrap ... -- <cmd>``), a replay of the run ledger (#23) and the data-quality result JSON (#22).
Naming convention, event example and column-level map: ``docs/lineage.md``.
"""

from .column_lineage import ColumnLineage, load
from .events import RunSpec, Stats, build_event, run_uuid
from .naming import DatasetRef, dataset_ref, table_ref
from .posthoc import ledger_events, quality_events
from .topology import Roots
from .transport import ConsoleTransport, EmitStats, Emitter, FileTransport, HttpTransport

__all__ = [
    "ColumnLineage",
    "ConsoleTransport",
    "DatasetRef",
    "EmitStats",
    "Emitter",
    "FileTransport",
    "HttpTransport",
    "Roots",
    "RunSpec",
    "Stats",
    "build_event",
    "dataset_ref",
    "ledger_events",
    "load",
    "quality_events",
    "run_uuid",
    "table_ref",
]
