"""Minimal Schema Registry HTTP client (stdlib only; Confluent-compatible API)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryError(RuntimeError):
    def __init__(self, status: int, body: str):
        super().__init__(f"registry HTTP {status}: {body}")
        self.status = status


class RegistryClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, body: dict | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            raise RegistryError(exc.code, exc.read().decode(errors="replace")) from exc

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> None:
        self._call("PUT", f"/config/{subject}", {"compatibility": level})

    def get_compatibility(self, subject: str) -> str:
        return self._call("GET", f"/config/{subject}?defaultToGlobal=true")["compatibilityLevel"]

    def is_compatible(self, subject: str, schema: dict) -> bool:
        """Registry-side check against the latest version. A new subject is compatible."""
        try:
            res = self._call(
                "POST",
                f"/compatibility/subjects/{subject}/versions/latest",
                {"schemaType": "JSON", "schema": json.dumps(schema)},
            )
        except RegistryError as exc:
            if exc.status == 404:
                return True
            raise
        return bool(res["is_compatible"])

    def register(self, subject: str, schema: dict) -> int:
        """Register (idempotent: identical schema returns its existing id)."""
        res = self._call(
            "POST",
            f"/subjects/{subject}/versions",
            {"schemaType": "JSON", "schema": json.dumps(schema)},
        )
        return int(res["id"])

    def latest(self, subject: str) -> dict:
        return self._call("GET", f"/subjects/{subject}/versions/latest")

    def latest_schema_id(self, subject: str) -> int:
        return int(self.latest(subject)["id"])
