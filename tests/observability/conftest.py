import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest  # noqa: E402

NOW = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)


def make_row(table="silver", hours_ago=1.0, rows_out=1000, status="success", i=0, **kw):
    ended = NOW - timedelta(hours=hours_ago)
    row = {
        "run_id": f"r{i}-{table}",
        "layer": "silver",
        "job": "j",
        "table_name": table,
        "batch_id": None,
        "started_at": ended - timedelta(minutes=5),
        "ended_at": ended,
        "duration_s": 300.0,
        "status": status,
        "rows_in": rows_out,
        "rows_out": rows_out,
        "rows_quarantined": 0,
        "bytes_out": None,
        "error": "boom" if status == "failed" else None,
        "schema_version": 1,
    }
    row.update(kw)
    return row


def history(table="silver", values=(1000, 1010, 990, 1005, 995, 1000), step_h=24, last_ago=1.0):
    """Oldest-first successful runs, one per ``step_h`` hours, the newest ``last_ago`` h old."""
    n = len(values)
    return [make_row(table, last_ago + (n - 1 - k) * step_h, v, i=k) for k, v in enumerate(values)]


@pytest.fixture
def now():
    return NOW
