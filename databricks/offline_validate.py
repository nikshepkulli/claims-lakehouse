"""Run ``databricks bundle validate`` with no Databricks account.

``bundle validate`` calls the workspace only to learn who the current user is (SCIM ``/Me``) and
host metadata. This script serves canned answers for those from 127.0.0.1 and points the CLI at
it with a dummy token, so schema and variable errors in ``databricks.yml`` still surface. It
validates the bundle's *shape*; it cannot prove a deploy or a run would succeed.

Usage: python databricks/offline_validate.py [-t dev|prod ...]   (default: both targets)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BUNDLE_DIR = Path(__file__).resolve().parent
TARGETS = ("dev", "prod")


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/api/2.0/preview/scim/v2/Me"):
            body = {"id": "1", "userName": "ci-validate@example.com", "active": True}
            code = 200
        elif self.path.startswith("/api/2.0/workspace/get-status"):
            body = {"object_type": "DIRECTORY", "object_id": 1, "path": "/stub"}
            code = 200
        else:
            body, code = {"error_code": "NOT_FOUND", "message": self.path}, 404
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:  # e.g. workspace/mkdirs: acknowledge, create nothing
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        raw = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args: object) -> None:  # keep CI logs quiet
        pass


def validate(targets: tuple[str, ...] = TARGETS, cli: str = "databricks") -> int:
    if shutil.which(cli) is None:
        print(f"{cli!r} not found on PATH", file=sys.stderr)
        return 127
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {
        "HOME": os.environ.get("HOME", "/tmp"),
        "PATH": os.environ["PATH"],
        "DATABRICKS_HOST": f"http://127.0.0.1:{server.server_port}",
        "DATABRICKS_TOKEN": "offline-validate-not-a-credential",
    }
    rc = 0
    try:
        for target in targets:
            print(f"== databricks bundle validate -t {target}", flush=True)
            proc = subprocess.run(
                [cli, "bundle", "validate", "-t", target],
                cwd=BUNDLE_DIR,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            print(proc.stdout + proc.stderr, end="")
            # The CLI exits 0 on schema warnings (unknown field, wrong type); treat them as errors.
            # The host-metadata warning is expected: the stub is not a real workspace.
            warnings = [
                ln for ln in (proc.stdout + proc.stderr).splitlines() if ln.startswith("Warning:")
            ]
            if warnings:
                print(f"{len(warnings)} bundle warning(s) treated as errors", file=sys.stderr)
            rc = rc or proc.returncode or (1 if warnings else 0)
    finally:
        server.shutdown()
    return rc


if __name__ == "__main__":
    args = sys.argv[1:]
    chosen = tuple(a for a in args if a in TARGETS) or TARGETS
    sys.exit(validate(chosen))
