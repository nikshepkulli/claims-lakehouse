"""SageMaker inference handler contract: ``model_fn / input_fn / predict_fn / output_fn``.

These four functions are the interface the SageMaker inference toolkit (and the scikit-learn
container's script mode) calls. They are exercised locally by tests/ml; **no endpoint or batch
transform job has been created** (see docs/ml.md). Supported request formats:

* ``application/json``: ``{"instances": [{feature: value, ...}, ...]}`` or a bare list of objects
* ``text/csv``: headerless rows in the model's feature order (what batch transform sends)

Requests may carry only the model's registered features: anything else (``member_id``, ``claim_id``,
free-form columns) is rejected, so an upstream bug cannot push identifiers into the model.
"""

from __future__ import annotations

import csv
import io
import json

from . import artifact


def model_fn(model_dir: str):
    return artifact.load(model_dir)


def input_fn(request_body, request_content_type: str = "application/json") -> dict:
    """Parse a request into columnar form ``{feature: [values]}`` (missing -> None)."""
    if isinstance(request_body, bytes | bytearray):
        request_body = request_body.decode("utf-8")
    ctype = (request_content_type or "application/json").split(";")[0].strip().lower()
    if ctype == "application/json":
        payload = json.loads(request_body)
        rows = payload["instances"] if isinstance(payload, dict) else payload
        if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
            raise ValueError('JSON body must be {"instances": [{...}]} or a list of objects')
        names = sorted({k for r in rows for k in r})
        return {"__rows__": len(rows), **{n: [r.get(n) for r in rows] for n in names}}
    if ctype == "text/csv":
        return {"__csv__": [r for r in csv.reader(io.StringIO(request_body)) if r]}
    raise ValueError(f"unsupported content type {request_content_type!r}")


def predict_fn(input_data: dict, model) -> dict:
    cols = dict(input_data)
    if "__csv__" in cols:  # headerless CSV in the model's feature order
        names = [s.name for s in model.specs]
        rows = cols.pop("__csv__")
        for i, r in enumerate(rows):
            if len(r) != len(names):
                raise ValueError(f"CSV row {i}: {len(r)} fields, expected {len(names)}")
        cols = {n: [r[j].strip() or None for r in rows] for j, n in enumerate(names)}
        for s in model.specs:
            if s.kind == "numeric":
                cols[s.name] = [None if v is None else _num(v, s.name) for v in cols[s.name]]
    cols.pop("__rows__", None)
    n = len(next(iter(cols.values()))) if cols else 0
    scores = model.predict(cols) if n else []
    return {
        "model_version": model.version,
        "feature_manifest_sha256": model.meta["feature_manifest_sha256"],
        "scores": [float(s) for s in scores],
    }


def _num(v: str, name: str) -> float:
    try:
        return float(v)
    except ValueError as e:
        raise ValueError(f"feature {name}: {v!r} is not numeric") from e


def output_fn(prediction: dict, accept: str = "application/json") -> str:
    accept = (accept or "application/json").split(";")[0].strip().lower()
    if accept == "application/json":
        return json.dumps(prediction)
    if accept == "text/csv":
        return "\n".join(f"{s:.8f}" for s in prediction["scores"])
    raise ValueError(f"unsupported accept type {accept!r}")
