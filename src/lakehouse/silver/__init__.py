"""Silver layer: typed, deduplicated, validated Delta tables with a quarantine path (ticket #7).

Importing this package must stay cheap and Spark-free; PySpark is imported lazily by
``lakehouse.silver.job`` / ``lakehouse.silver.cli`` only.
"""
