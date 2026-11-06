"""Append-only JSONL audit log: one line per question, whatever the outcome.

Result cells are NOT logged (only counts), and the question / SQL are scrubbed of
identifier-shaped strings first. The full prompt is not stored, only its SHA-256 and length.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path


class AuditLog:
    def __init__(self, path: Path | str | None):
        self.path = Path(path) if path else None
        self._lock = threading.Lock()

    def write(self, record: dict) -> None:
        if self.path is None:
            return
        line = json.dumps(record, sort_keys=True, default=str)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    def read(self) -> list[dict]:
        if self.path is None or not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x.strip()]
