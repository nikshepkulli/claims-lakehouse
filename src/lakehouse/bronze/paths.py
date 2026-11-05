"""Single place where bronze table locations are built.

Ticket #20 (storage URI abstraction) will replace this with a config-driven resolver. Until
then the bronze root is a plain URI string (``/tmp/bronze``, ``file:///...``, ``s3a://bronze``)
and every table path is derived here, so swapping the implementation is a one-function change.
"""

from __future__ import annotations

META_PREFIX = "_meta"
SCHEMA_LOG = f"{META_PREFIX}/schema_log"
QUARANTINE = f"{META_PREFIX}/quarantine"


def table_uri(bronze_root: str, name: str) -> str:
    """URI of a bronze table. ``name`` is a dataset (``claims``) or a meta path (``_meta/...``)."""
    return f"{bronze_root.rstrip('/')}/{name}"
