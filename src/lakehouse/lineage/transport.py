"""Transports and the failure-tolerant emitter.

Lineage is observability, not part of the data path: :meth:`Emitter.emit` never raises. A failed
send is logged (``lineage_emit_failed``), counted in :class:`EmitStats`, and the pipeline goes on.
The CLI prints the counters at the end and, only with ``--fail-on-transport-error``, turns them
into a non-zero exit for the standalone ``emit`` commands (never for ``wrap``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

LOGGER_NAME = "lakehouse.observability"  # same JSON-lines logger as the run ledger tooling
API_PATH = "/api/v1/lineage"
MARQUEZ_URL_ENV = "MARQUEZ_URL"
MARQUEZ_KEY_ENV = "MARQUEZ_API_KEY"
DEFAULT_MARQUEZ_URL = "http://localhost:5002"


def canonical(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Transport(Protocol):
    name: str

    def send(self, event: dict[str, Any]) -> bool:
        """Deliver one event. True = delivered, False = skipped as a duplicate. Raise on failure."""


class ConsoleTransport:
    name = "console"

    def __init__(self, stream: Any = None):
        self.stream = stream or sys.stderr

    def send(self, event: dict[str, Any]) -> bool:
        print(canonical(event), file=self.stream, flush=True)
        return True


class FileTransport:
    """Append-only JSON lines. An event identical to one already in the file is skipped, so
    re-emitting the same run (same ids, same times) does not duplicate lines."""

    name = "file"

    def __init__(self, path: str | Path, dedupe: bool = True):
        self.path = Path(path)
        self.dedupe = dedupe
        self._seen: set[str] | None = None

    def _load(self) -> set[str]:
        if self._seen is None:
            self._seen = set()
            if self.dedupe and self.path.exists():
                for line in self.path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        self._seen.add(hashlib.sha256(line.encode()).hexdigest())
        return self._seen

    def send(self, event: dict[str, Any]) -> bool:
        line = canonical(event)
        h = hashlib.sha256(line.encode()).hexdigest()
        seen = self._load()
        if self.dedupe and h in seen:
            return False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        seen.add(h)
        return True


class HttpTransport:
    """POST to Marquez (or any OpenLineage backend) ``/api/v1/lineage``.

    Retries connection errors and 5xx with exponential backoff; 4xx fails immediately (the event
    is wrong, resending will not help). The optional bearer token is read from the env var named by
    ``api_key_env`` and never logged. Marquez upserts by run id + event time, so a re-sent event
    updates rather than duplicates (documented behaviour; not verified against a live Marquez here).
    """

    name = "http"

    def __init__(
        self,
        url: str | None = None,
        *,
        timeout_s: float = 5.0,
        retries: int = 2,
        backoff_s: float = 0.25,
        api_key_env: str = MARQUEZ_KEY_ENV,
    ):
        base = (url or os.environ.get(MARQUEZ_URL_ENV) or DEFAULT_MARQUEZ_URL).rstrip("/")
        if urlparse(base).scheme not in ("http", "https"):
            raise ValueError(f"Marquez URL must be http(s), got {base!r}")
        self.url = base if base.endswith(API_PATH) else base + API_PATH
        self.timeout_s, self.retries, self.backoff_s = timeout_s, retries, backoff_s
        self.api_key_env = api_key_env

    def send(self, event: dict[str, Any]) -> bool:
        headers = {"Content-Type": "application/json"}
        key = os.environ.get(self.api_key_env)
        if key:
            headers["Authorization"] = f"Bearer {key}"
        body = canonical(event).encode()
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            req = urllib.request.Request(self.url, data=body, headers=headers, method="POST")  # noqa: S310
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:  # noqa: S310
                    if 200 <= resp.status < 300:
                        return True
                    last = RuntimeError(f"HTTP {resp.status}")
            except urllib.error.HTTPError as e:
                detail = e.read(300).decode("utf-8", "replace")
                last = RuntimeError(f"HTTP {e.code}: {detail}")
                if e.code < 500:
                    break  # client error: retrying cannot help
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = e
            if attempt < self.retries:
                time.sleep(self.backoff_s * (2**attempt))
        raise RuntimeError(f"POST {self.url} failed: {last}")


@dataclass
class EmitStats:
    attempted: int = 0  # events handed to emit()
    delivered: int = 0  # (event, transport) deliveries that succeeded
    skipped_duplicates: int = 0
    failed: int = 0  # (event, transport) deliveries that failed, plus invalid events
    invalid: int = 0
    by_transport: dict[str, dict[str, int]] = field(default_factory=dict)
    last_error: str | None = None

    def bump(self, transport: str, key: str) -> None:
        self.by_transport.setdefault(transport, {"delivered": 0, "skipped": 0, "failed": 0})[
            key
        ] += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempted": self.attempted,
            "delivered": self.delivered,
            "skipped_duplicates": self.skipped_duplicates,
            "failed": self.failed,
            "invalid": self.invalid,
            "by_transport": self.by_transport,
            "last_error": self.last_error,
        }


class Emitter:
    def __init__(
        self,
        transports: list[Transport],
        *,
        validate: bool = False,
        logger: logging.Logger | None = None,
    ):
        self.transports = transports
        self.validate = validate
        self.log = logger or logging.getLogger(LOGGER_NAME)
        self.stats = EmitStats()

    def _fail(self, transport: str, event: dict[str, Any], err: Exception) -> None:
        self.stats.failed += 1
        self.stats.last_error = f"{type(err).__name__}: {err}"[:300]
        self.stats.bump(transport, "failed")
        self.log.error(
            "lineage_emit_failed",
            extra={
                "fields": {
                    "transport": transport,
                    "event_type": event.get("eventType"),
                    "run_id": (event.get("run") or {}).get("runId"),
                    "job": (event.get("job") or {}).get("name"),
                    "error": self.stats.last_error,
                }
            },
        )

    def emit(self, event: dict[str, Any]) -> bool:
        """Send to every transport. Never raises; True when every transport accepted the event."""
        self.stats.attempted += 1
        if self.validate:
            try:
                from .validate import check

                check(event)
            except ImportError as e:  # jsonschema missing: say so once, keep going unvalidated
                self.validate = False
                self.log.warning(
                    "lineage_validation_unavailable", extra={"fields": {"error": str(e)}}
                )
            except Exception as e:
                self.stats.invalid += 1
                self._fail("validation", event, e)
                return False
        ok = True
        for t in self.transports:
            try:
                if t.send(event):
                    self.stats.delivered += 1
                    self.stats.bump(t.name, "delivered")
                else:
                    self.stats.skipped_duplicates += 1
                    self.stats.bump(t.name, "skipped")
            except Exception as e:
                ok = False
                self._fail(t.name, event, e)
        return ok
