import os
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest  # noqa: E402

# The gold suites run thousands of small Spark jobs in one JVM; the 1 GB default driver heap
# ends in OutOfMemory/GC thrash near the end of the run. Must be set before the JVM starts.
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 3g pyspark-shell")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """One Delta-enabled local SparkSession per test module (same pattern as tests/silver).

    Imported lazily so the plain `pytest -m "not spark"` run never needs PySpark.
    """
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.bronze.spark import build_spark

    wh = tmp_path_factory.mktemp("warehouse")
    s = build_spark(
        "gold-tests",
        master="local[2]",
        conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.sql.adaptive.enabled": "false",  # tiny tables: AQE planning is pure overhead
            "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": str(wh),
            "spark.databricks.delta.snapshotPartitions": "2",
        },
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
