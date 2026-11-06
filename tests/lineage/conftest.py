import json
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest  # noqa: E402

from lakehouse.observability.ledger import Ledger, RunRecord  # noqa: E402

T0 = datetime(2024, 1, 5, 6, 0, tzinfo=UTC)


@pytest.fixture
def roots(tmp_path):
    from lakehouse.lineage import Roots

    return Roots(
        landing=str(tmp_path / "raw"),
        bronze=str(tmp_path / "bronze"),
        silver=str(tmp_path / "silver"),
        gold=str(tmp_path / "gold"),
    )


def make_record(
    layer="silver", job="silver_batch", table=None, status="success", run_id=None, **kw
):
    start = kw.pop("started_at", T0)
    end = kw.pop("ended_at", start + timedelta(minutes=3))
    return RunRecord(
        run_id=run_id or "a" * 32,
        layer=layer,
        job=job,
        started_at=start,
        ended_at=end,
        status=status,
        table_name=table or layer,
        **kw,
    )


@pytest.fixture
def ledger(tmp_path):
    led = Ledger(tmp_path / "ops")
    led.append(
        [
            make_record("bronze", "bronze_batch", run_id="1" * 32, rows_in=100, rows_out=100),
            make_record(
                "silver",
                "silver_batch",
                run_id="2" * 32,
                started_at=T0 + timedelta(minutes=10),
                rows_in=100,
                rows_out=97,
                rows_quarantined=3,
                bytes_out=2048,
            ),
        ]
    )
    led.append(
        [
            make_record(
                "gold",
                "gold_reconciliation",
                table="fact_claim",
                run_id="3" * 32,
                started_at=T0 + timedelta(minutes=20),
                rows_in=97,
                rows_out=95,
                rows_quarantined=2,
                batch_id="b20240105",
            ),
            make_record(
                "gold",
                "gold_reconciliation",
                table="fact_claim_line",
                run_id="3" * 32,
                started_at=T0 + timedelta(minutes=20),
                rows_in=230,
                rows_out=228,
                rows_quarantined=2,
                batch_id="b20240105",
            ),
        ]
    )
    led.append(
        [
            make_record(
                "gold",
                "gold_batch",
                run_id="4" * 32,
                started_at=T0 + timedelta(minutes=30),
                status="failed",
                error="ReconciliationError: silver does not reconcile",
            )
        ]
    )
    return led


class FakeMarquez:
    """Records POSTs; ``script`` is a list of status codes consumed one per request."""

    def __init__(self, script=None):
        self.requests, self.script = [], list(script or [])
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                n = int(self.headers.get("Content-Length", 0))
                outer.requests.append(
                    (self.path, dict(self.headers), json.loads(self.rfile.read(n) or b"{}"))
                )
                code = outer.script.pop(0) if outer.script else 201
                self.send_response(code)
                self.end_headers()
                self.wfile.write(b'{"error":"nope"}' if code >= 400 else b"")

            def log_message(self, *a):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def make_marquez():
    """Factory for a local stand-in for Marquez' POST /api/v1/lineage (NOT the real Marquez)."""
    made = []

    def _make(script=None):
        m = FakeMarquez(script)
        made.append(m)
        return m

    yield _make
    for m in made:
        m.close()
