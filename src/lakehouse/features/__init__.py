"""Point-in-time-correct member / provider / claim features for the denial-risk model (#26).

Reads the gold Parquet export (``<gold>/export``, see docs/gold.md), writes a partitioned
Parquet feature store plus a separate label table. DuckDB + pyarrow only: no Spark, no Java.
Importing the package is cheap; DuckDB is imported lazily by the modules that need it.
See docs/features.md.
"""
