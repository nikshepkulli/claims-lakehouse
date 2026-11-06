"""Daily claims pipeline DAG. Logic lives in ``claims_dags`` (importable without Airflow)."""

import sys
from pathlib import Path

# Airflow puts dags/ on sys.path; the package sits one level up.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from claims_dags.factory import build_dag  # noqa: E402

dag = build_dag()
