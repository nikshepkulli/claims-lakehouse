"""Gold table locations. Same plain-URI convention as bronze and silver.

``table_uri`` delegates to the one function bronze uses, so a storage-resolver change (ticket
#20, ``lakehouse_common.storage``) is a one-place swap for all three layers.
"""

from __future__ import annotations

from lakehouse.bronze.paths import table_uri as _table_uri

RECONCILIATION = "gold_reconciliation"
EXCLUSIONS = "gold_exclusions"
EXPORT_DIR = "export"


def table_uri(gold_root: str, name: str) -> str:
    """URI of a gold Delta table: a star-schema table or one of the operational tables."""
    return _table_uri(gold_root, name)


def export_uri(gold_root: str, name: str) -> str:
    """URI of the Parquet export of a star-schema table: ``<gold>/export/<name>/``."""
    return _table_uri(gold_root, f"{EXPORT_DIR}/{name}")
