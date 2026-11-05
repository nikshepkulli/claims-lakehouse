"""Silver table locations. Same plain-URI convention as bronze (``lakehouse.bronze.paths``)."""

from __future__ import annotations

from lakehouse.bronze.paths import table_uri as _table_uri

QUARANTINE = "silver_quarantine"
RECONCILIATION = "silver_reconciliation"


def table_uri(silver_root: str, name: str) -> str:
    """URI of a silver table: a dataset name, ``silver_quarantine`` or ``silver_reconciliation``."""
    return _table_uri(silver_root, name)
