"""Delta table plumbing for the gold builders: typed creation, aligned writes, replaceWhere."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.bronze.ingest import _write_replace as write_replace  # the one replaceWhere helper

from .schema import PARTITION_BY, TABLES, Col

__all__ = ["write_replace", "struct_of", "align", "ensure_table", "read", "exists"]


def struct_of(cols: list[Col]) -> T.StructType:
    return T.StructType(
        [T.StructField(c.name, T._parse_datatype_string(c.type), c.nullable) for c in cols]
    )


def align(df: DataFrame, cols: list[Col]) -> DataFrame:
    """Select ``cols`` in order, casting each to its declared type (so the write cannot drift)."""
    return df.select(*[F.col(c.name).cast(c.type).alias(c.name) for c in cols])


def exists(spark: SparkSession, uri: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, uri)


def read(spark: SparkSession, uri: str) -> DataFrame:
    return spark.read.format("delta").load(uri)


def _sql_path(uri: str) -> str:
    """Spark SQL resolves a bare relative path as ``schema.table``; make local paths absolute."""
    return uri if "://" in uri else os.path.abspath(uri)


def ensure_table(spark: SparkSession, uri: str, name: str, cols: list[Col] | None = None) -> None:
    """Create the Delta table with its declared schema (incl. NOT NULL) if it is missing.

    Creating it with DDL, instead of letting the first write infer a schema, is what makes
    NOT NULL a real Delta constraint (``DataFrameWriter`` makes every column nullable): a null in
    a required column fails the write, and the table schema equals the Snowflake DDL.
    """
    if exists(spark, uri):
        return
    ddl = ", ".join(
        f"`{c.name}` {c.type}{'' if c.nullable else ' NOT NULL'}" for c in (cols or TABLES[name])
    )
    part = PARTITION_BY.get(name)
    by = f" PARTITIONED BY (`{part}`)" if part else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{_sql_path(uri)}` ({ddl}) USING DELTA{by}")
