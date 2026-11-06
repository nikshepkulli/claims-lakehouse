import sys
from pathlib import Path

AIRFLOW_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "airflow"
sys.path.insert(0, str(AIRFLOW_DIR))
