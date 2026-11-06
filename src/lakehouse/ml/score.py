"""Batch scorer (``python -m lakehouse.ml score``): feature store + model dir -> scores Parquet.

Output (``<out>``)::

    part-00000.parquet   claim_id, as_of, denial_risk_score, model_name, model_version,
                         feature_manifest_sha256
    _manifest.json       model version, feature-manifest hash, input key, row count, scores hash

Idempotent: the output has no wall-clock, rows are ordered by ``claim_id`` and the manifest holds
an input key (model version + feature manifest + row filter). Re-running with the same inputs
is a no-op ("skipped"); ``force`` rewrites byte-identical scores. Only ``claim_features`` is read;
the label table is never opened. Scores are written to a temp dir and swapped in, so a crash
never leaves a half-written table behind.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import artifact, data
from .manifest import feature_manifest_sha256

SCORE_COLUMN = "denial_risk_score"


def _scores_hash(claim_ids, scores: np.ndarray) -> str:
    h = hashlib.sha256()
    h.update("\n".join(claim_ids).encode())
    h.update(np.asarray(scores, dtype="<f8").tobytes())
    return h.hexdigest()


def input_key(model_version: str, manifest_sha: str, as_of_from: str | None, as_of_to: str | None):
    ident = {"m": model_version, "f": manifest_sha, "from": as_of_from, "to": as_of_to}
    return hashlib.sha256(json.dumps(ident, sort_keys=True).encode()).hexdigest()


def score(
    store_dir: str | Path,
    model_dir: str | Path,
    out: str | Path,
    *,
    force: bool = False,
    as_of_from: str | None = None,
    as_of_to: str | None = None,
) -> dict:
    out = Path(out)
    art = artifact.load(model_dir)
    manifest_sha = feature_manifest_sha256(store_dir)
    if (
        art.meta["registry_sha256"]
        != json.loads((Path(store_dir) / "_manifest.json").read_text())["registry_sha256"]
    ):
        raise ValueError("model was trained against a different feature registry than this store")
    key = input_key(art.version, manifest_sha, as_of_from, as_of_to)
    mpath = out / "_manifest.json"
    if mpath.is_file() and not force:
        prev = json.loads(mpath.read_text())
        if prev.get("input_key") == key and (out / "part-00000.parquet").is_file():
            return {**prev, "status": "skipped"}

    frame = data.load_features(store_dir, art.specs)
    keep = np.ones(len(frame), dtype=bool)
    days = frame.as_of.astype("datetime64[D]")
    if as_of_from:
        keep &= days >= np.datetime64(as_of_from)
    if as_of_to:
        keep &= days <= np.datetime64(as_of_to)
    frame = frame.take(np.flatnonzero(keep))
    scores = art.predict(frame.cols)
    if not np.isfinite(scores).all() or scores.min() < 0 or scores.max() > 1:
        raise ValueError("scorer produced non-finite or out-of-range probabilities")

    n = len(frame)
    table = pa.table(
        {
            "claim_id": pa.array(frame.claim_id.tolist(), pa.string()),
            "as_of": pa.array(frame.as_of, pa.timestamp("us")),
            SCORE_COLUMN: pa.array(scores, pa.float64()),
            "model_name": pa.array([art.meta["model_name"]] * n, pa.string()),
            "model_version": pa.array([art.version] * n, pa.string()),
            "feature_manifest_sha256": pa.array([manifest_sha] * n, pa.string()),
        }
    )
    manifest = {
        "model_name": art.meta["model_name"],
        "model_version": art.version,
        "feature_manifest_sha256": manifest_sha,
        "input_key": key,
        "rows": n,
        "scores_sha256": _scores_hash(frame.claim_id.tolist(), scores),
        "as_of_from": as_of_from,
        "as_of_to": as_of_to,
        "data_is_synthetic": True,
    }
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    pq.write_table(table, tmp / "part-00000.parquet", compression="zstd")
    (tmp / "_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if out.exists():
        shutil.rmtree(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp.rename(out)
    return {**manifest, "status": "written"}
