"""Bronze layer: raw, append-only Delta tables landed from claimsgen files (ticket #6).

Importing this package must stay cheap and Spark-free; PySpark is imported lazily by
``lakehouse.bronze.ingest`` / ``lakehouse.bronze.spark`` only.
"""
