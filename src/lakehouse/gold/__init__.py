"""Gold layer: Kimball star schema (facts + SCD2 dimensions) built from silver Delta (ticket #8).

Importing this package must stay cheap and Spark-free; PySpark is imported lazily by
``lakehouse.gold.job`` / ``lakehouse.gold.cli`` only. ``lakehouse.gold.schema`` is pure Python
and is the in-code copy of the Snowflake DDL contract (``warehouse/snowflake/01_gold_ddl.sql``).
"""
