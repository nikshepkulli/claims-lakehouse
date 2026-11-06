"""Fingerprint of the feature table a model was trained on / scored from."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def read_store_manifest(store: str | Path) -> dict:
    p = Path(store) / "_manifest.json"
    if not p.is_file():
        raise FileNotFoundError(
            f"{p}: not a feature store (no _manifest.json); run lakehouse.features"
        )
    return json.loads(p.read_text())


def feature_manifest_sha256(store: str | Path) -> str:
    """sha256 over the identity of the claim feature table: store schema version, registry
    hash, the claim_features content hash and the feature config. Changes when any feature
    value, the catalogue or the build config changes; no wall-clock input."""
    m = read_store_manifest(store)
    ident = {
        "schema_version": m["schema_version"],
        "registry_sha256": m["registry_sha256"],
        "claim_features_sha256": m["tables"]["claim_features"]["sha256"],
        "config": m["config"],
    }
    return hashlib.sha256(json.dumps(ident, sort_keys=True).encode()).hexdigest()
