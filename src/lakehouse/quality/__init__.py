"""Data quality framework (ticket #22): Great Expectations suites per medallion layer.

Importing this package is cheap; Great Expectations, pandas and deltalake are imported lazily by
``lakehouse.quality.runner`` / ``loader``. Suites live in ``quality/expectations/<layer>.yaml``.
See docs/data-quality.md.
"""
