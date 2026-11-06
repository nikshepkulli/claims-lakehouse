"""Post-hoc emitters: rebuild OpenLineage events for runs that already finished.

* :func:`ledger_events` replays the run ledger (#23): per ``run_id`` one START (at the earliest
  ``started_at``) and one COMPLETE or FAIL (at the latest ``ended_at``). Everything is derived
  from ledger rows, so emitting the same ledger twice produces identical events (idempotent;
  the file transport skips them, Marquez upserts by run id and event time).
* :func:`quality_events` turns a #22 result JSON into a ``quality.<layer>`` run whose input
  datasets carry the data-quality facets.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from . import column_lineage as CL
from . import dq as DQ
from .events import RunSpec, Stats, build_event
from .naming import table_ref
from .topology import IO, Roots, layer_io


def _sum(rows: list[dict[str, Any]], key: str) -> int | None:
    vals = [r[key] for r in rows if r.get(key) is not None]
    return sum(vals) if vals else None


def _run_spec(
    rows: list[dict[str, Any]], roots: Roots, mapping: CL.ColumnLineage | None
) -> RunSpec:
    first = rows[0]
    layer = first["layer"]
    inputs, outputs = layer_io(layer, roots)
    out_tables = {o.table for o in outputs}
    stats: dict[str, Stats] = {}
    for table in out_tables:
        mine = [r for r in rows if r["table_name"] == table]
        if mine:
            stats[table] = Stats(_sum(mine, "rows_out"), _sum(mine, "bytes_out"))
    info: dict[str, Any] = {
        "layer": layer,
        "batch_ids": sorted({r["batch_id"] for r in rows if r.get("batch_id")}),
        "tables": sorted({r["table_name"] for r in rows}),
        "ledger_rows": len(rows),
    }
    if len(rows) == 1:  # unambiguous run-level counts
        info.update(
            {k: first.get(k) for k in ("rows_in", "rows_out", "rows_quarantined", "bytes_out")}
        )
    return RunSpec(
        run_id=first["run_id"],
        job=first["job"],
        inputs=inputs,
        outputs=outputs,
        layer_roots=roots.layer_roots(),
        stats=stats,
        run_info=info,
        layer=layer,
        mapping=mapping,
    )


def ledger_events(
    rows: list[dict[str, Any]], roots: Roots, mapping: CL.ColumnLineage | None = None
) -> list[dict[str, Any]]:
    """START + COMPLETE/FAIL for every run in ``rows`` (ledger rows), ordered by event time."""
    by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_run[r["run_id"]].append(r)
    mapping = mapping or CL.load()
    events: list[tuple[datetime, int, str, dict[str, Any]]] = []
    for run_id, rs in by_run.items():
        spec = _run_spec(rs, roots, mapping)
        start = min(r["started_at"] for r in rs)
        end = max(r["ended_at"] for r in rs)
        failed = [r for r in rs if r["status"] == "failed"]
        events.append((start, 0, run_id, build_event(spec, "START", start)))
        if failed:
            msg = next((r["error"] for r in failed if r.get("error")), "run failed")
            events.append((end, 1, run_id, build_event(spec, "FAIL", end, error=msg)))
        else:
            events.append((end, 1, run_id, build_event(spec, "COMPLETE", end)))
    events.sort(key=lambda t: (t[0], t[1], t[2]))
    return [e for *_, e in events]


def quality_events(result: dict[str, Any], roots: Roots | None = None) -> list[dict[str, Any]]:
    """START + COMPLETE/FAIL of a ``quality.<layer>`` run from a #22 result JSON.

    The run id is a UUIDv5 of (layer, generated_at): re-emitting the same result file is
    idempotent. The layer root comes from ``roots`` when given, else from the result's ``root``.
    """
    layer = result["layer"]
    root = {
        "bronze": getattr(roots, "bronze", None),
        "silver": getattr(roots, "silver", None),
        "gold": getattr(roots, "gold", None),
    }.get(layer) or result["root"]
    generated = datetime.fromisoformat(result["generated_at"])
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=UTC)
    inputs: list[IO] = []
    facets: dict[str, dict[str, Any]] = {}
    for name, t in result["tables"].items():
        f = DQ.table_facets(t)
        if not f:
            continue
        rel = t.get("path") or name
        inputs.append(IO(table_ref(root, rel), layer, name, f"{root.rstrip('/')}/{rel}"))
        facets[name] = f
    run_id = uuid.uuid5(
        uuid.NAMESPACE_URL, f"claims-lakehouse:dq:{layer}:{result['generated_at']}"
    ).hex
    summary = DQ.summary(result)
    spec = RunSpec(
        run_id=run_id,
        job=f"quality.{layer}",
        inputs=inputs,
        input_facets=facets,
        run_info={"layer": layer, "success": bool(result.get("success")), **summary},
        layer=layer,
    )
    start = build_event(spec, "START", generated)
    if result.get("success"):
        return [start, build_event(spec, "COMPLETE", generated)]
    n = summary.get("failed_blocking", "some")
    return [
        start,
        build_event(spec, "FAIL", generated, error=f"{n} blocking expectation(s) failed"),
    ]
