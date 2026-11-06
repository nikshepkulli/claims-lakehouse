"""Notebook-free entrypoint for the Databricks jobs (``spark_python_task``).

Runs one layer of the existing CLIs (``lakehouse.bronze|silver|gold``) on Databricks::

    entrypoint.py --layer silver --bronze <root> --silver <root> --batch-date all

Everything after ``--layer`` is forwarded unchanged to that layer's CLI, so the CLI stays the
single definition of arguments and local runs and Databricks runs share one code path.

Two Databricks differences are handled here, without touching ``src/``:

* The CLIs build their own session through ``lakehouse.bronze.spark.build_spark`` (pip Delta
  jars, local master). On Databricks the session already exists and has Delta, so that module
  is replaced by a shim returning the runtime's session.
* Serverless and shared clusters use Spark Connect, which has no ``sparkContext``; the CLIs call
  ``spark.sparkContext.setLogLevel``. The shim's session proxy makes that a no-op, and makes
  ``stop()`` a no-op so the job does not tear down the runtime's session.

Root arguments are validated up front: ``/Volumes/...`` paths are accepted as is, any other URI
must pass ``lakehouse_common.storage.parse_uri`` (``s3a://`` or ``abfss://``; ``s3://``,
``wasbs://`` and friends are rejected with the resolver's hint).

Not verified on a live workspace (see docs/databricks.md).
"""

from __future__ import annotations

import argparse
import importlib
import sys
import types
from typing import Any

LAYERS = ("bronze", "silver", "gold")
ROOT_FLAGS = ("--bronze", "--silver", "--gold")
VOLUME_PREFIX = "/Volumes/"


class _ContextStub:
    """Stand-in for ``SparkContext`` under Spark Connect."""

    def setLogLevel(self, _level: str) -> None:  # noqa: N802 - mirrors the PySpark name
        return None


class SessionProxy:
    """Delegates to the runtime session; neutralises ``sparkContext`` and ``stop``."""

    def __init__(self, session: Any) -> None:
        self._session = session
        self.sparkContext = _ContextStub()

    def stop(self) -> None:
        return None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


def make_build_spark(get_session: Any = None) -> Any:
    """Return a ``build_spark`` replacement. ``get_session`` is injectable for tests."""

    def build_spark(*_args: Any, **_kwargs: Any) -> SessionProxy:
        if get_session is not None:
            return SessionProxy(get_session())
        from pyspark.sql import SparkSession

        return SessionProxy(SparkSession.builder.getOrCreate())

    return build_spark


def install_session_shim(get_session: Any = None) -> None:
    """Make ``from lakehouse.bronze.spark import build_spark`` resolve to the shim."""
    shim = types.ModuleType("lakehouse.bronze.spark")
    shim.build_spark = make_build_spark(get_session)  # type: ignore[attr-defined]
    sys.modules["lakehouse.bronze.spark"] = shim


def validate_roots(argv: list[str]) -> None:
    """Fail fast on a root that is neither a /Volumes path nor a resolver-valid URI."""
    from lakehouse_common.storage import StorageConfigError, parse_uri

    for i, token in enumerate(argv):
        if token not in ROOT_FLAGS:
            continue
        if i + 1 >= len(argv):
            raise SystemExit(f"{token} needs a value")
        value = argv[i + 1]
        if value.startswith(VOLUME_PREFIX):
            continue
        if "://" not in value:
            raise SystemExit(f"{token} {value!r}: use a /Volumes/... path, s3a:// or abfss:// URI")
        try:
            parse_uri(value)
        except StorageConfigError as exc:
            raise SystemExit(f"{token}: {exc}") from exc


def parse(argv: list[str] | None = None) -> tuple[str, list[str]]:
    ap = argparse.ArgumentParser(description=__doc__, prog="entrypoint.py")
    ap.add_argument("--layer", required=True, choices=LAYERS)
    args, rest = ap.parse_known_args(argv)
    return args.layer, rest


def main(argv: list[str] | None = None, get_session: Any = None) -> int:
    layer, rest = parse(sys.argv[1:] if argv is None else argv)
    validate_roots(rest)
    install_session_shim(get_session)
    cli = importlib.import_module(f"lakehouse.{layer}.cli")
    return int(cli.main(rest))


if __name__ == "__main__":
    sys.exit(main())
