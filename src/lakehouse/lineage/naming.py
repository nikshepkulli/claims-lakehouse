"""Job and dataset naming (docs/lineage.md, "Naming convention").

Datasets follow the OpenLineage naming spec (https://openlineage.io/docs/spec/naming): the
*namespace* identifies the storage system, the *name* the object inside it. Both are derived
from the storage URI alone, so the same table gets the same identity whichever job reads or
writes it, and whichever cloud it lives on.

=========  =====================================================  ==========================
scheme     namespace                                              name
=========  =====================================================  ==========================
path/file  ``file://<host>`` (``localhost``; env override)        absolute path
s3a/s3     ``s3://<bucket>``                                      object key, no leading /
abfss      ``abfss://<container>@<account>.dfs.core.windows.net`` ``/<path>``
gs         ``gs://<bucket>``                                      object key
=========  =====================================================  ==========================

Jobs live in one namespace (``claims-lakehouse``, env ``LAKEHOUSE_LINEAGE_NAMESPACE``) and are
named by the job as the ledger / wrapper knows it (``bronze``, ``silver_batch``, ...).
"""

from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

PRODUCER = "https://github.com/nikshepkulli/claims-lakehouse/tree/main/src/lakehouse/lineage"
DEFAULT_JOB_NAMESPACE = "claims-lakehouse"
NAMESPACE_ENV = "LAKEHOUSE_LINEAGE_NAMESPACE"
FILE_HOST_ENV = "LAKEHOUSE_LINEAGE_FILE_HOST"


class NamingError(ValueError):
    """The URI cannot be turned into a dataset identity."""


@dataclass(frozen=True)
class DatasetRef:
    namespace: str
    name: str

    def child(self, table: str) -> DatasetRef:
        """The dataset ``table`` under this one (a layer root -> a table)."""
        sep = "" if self.name.endswith("/") or not self.name else "/"
        return DatasetRef(self.namespace, f"{self.name}{sep}{table.strip('/')}")


def job_namespace() -> str:
    return os.environ.get(NAMESPACE_ENV) or DEFAULT_JOB_NAMESPACE


def dataset_ref(uri: str) -> DatasetRef:
    """Identity of the dataset stored at ``uri`` (plain path, ``file://``, ``s3a://``, ...)."""
    if not uri or not uri.strip():
        raise NamingError("empty dataset URI")
    u = urlparse(uri.strip())
    scheme = u.scheme.lower()
    if scheme in ("", "file"):
        if scheme == "file" and u.netloc not in ("", "localhost"):
            raise NamingError(f"file:// URI with a host is not supported: {uri!r}")
        path = unquote(u.path) if scheme == "file" else uri.strip()
        host = os.environ.get(FILE_HOST_ENV) or "localhost"
        return DatasetRef(f"file://{host}", posixpath.normpath(os.path.abspath(path)))
    if scheme in ("s3", "s3a", "s3n", "gs"):
        if not u.netloc:
            raise NamingError(f"{scheme}:// URI without a bucket: {uri!r}")
        bucket_scheme = "gs" if scheme == "gs" else "s3"
        key = u.path.strip("/")
        return DatasetRef(f"{bucket_scheme}://{u.netloc}", posixpath.normpath(key) if key else "")
    if scheme == "abfss":
        if "@" not in u.netloc or not u.netloc.endswith(".dfs.core.windows.net"):
            raise NamingError(
                f"abfss URI must be abfss://<container>@<account>.dfs.core.windows.net/..: {uri!r}"
            )
        return DatasetRef(f"abfss://{u.netloc}", "/" + u.path.strip("/"))
    raise NamingError(f"unsupported storage scheme {scheme!r} in {uri!r}")


def table_ref(root: str, table: str) -> DatasetRef:
    """Identity of ``table`` under a layer root URI (bronze/silver/gold/landing)."""
    return dataset_ref(root).child(table)
