import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """One Delta-enabled local SparkSession per test module (same pattern as tests/bronze).

    Imported lazily so the plain `pytest -m "not spark"` run never needs PySpark.
    """
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.bronze.spark import build_spark

    wh = tmp_path_factory.mktemp("warehouse")
    s = build_spark(
        "silver-tests",
        master="local[2]",
        conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": str(wh),
            "spark.databricks.delta.snapshotPartitions": "2",
        },
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
